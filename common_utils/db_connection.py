"""
MySQL Database Connection Manager

This module provides MySQL database connection management with:
- Connection pooling for efficient resource usage
- Retry logic for handling shared database scenarios
- Health check capabilities
- Basic query execution with automatic retries
"""

import sys
import time
from typing import Dict, Any, Optional, List, Tuple
from contextlib import contextmanager
from functools import wraps

try:
    from sqlalchemy import create_engine, Engine, text
    from sqlalchemy.exc import OperationalError, DisconnectionError, DatabaseError
    from sqlalchemy.pool import QueuePool
    import pymysql
    from pymysql.err import OperationalError as PyMySQLOperationalError
except ImportError as e:
    print(f"⚠️  Required database libraries not installed: {e}", file=sys.stderr)
    print("   Install with: pip install sqlalchemy pymysql", file=sys.stderr)
    raise


class DatabaseConnection:
    """
    Manages MySQL database connections with pooling and retry logic.
    
    Designed to handle shared database scenarios where the MySQL server
    is used by multiple programs simultaneously.
    """
    
    # MySQL error codes that are retryable
    RETRYABLE_ERROR_CODES = [
        2006,  # MySQL server has gone away
        2013,  # Lost connection to MySQL server during query
        1205,  # Lock wait timeout exceeded
        1213,  # Deadlock found when trying to get lock
        1040,  # Too many connections
        1317,  # Query execution was interrupted
    ]
    
    def __init__(
        self,
        config: Dict[str, Any],
        retry_config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize database connection manager.
        
        Args:
            config: Database configuration dictionary with keys:
                - host: MySQL server host
                - port: MySQL server port (default: 3306)
                - user: Database username
                - password: Database password
                - database: Database name
                - charset: Character set (default: utf8mb4)
                - pool_size: Connection pool size (default: 5)
                - max_overflow: Max overflow connections (default: 10)
            retry_config: Retry configuration dictionary with keys:
                - max_retries: Maximum retry attempts (default: 3)
                - backoff_factor: Backoff multiplier (default: 1.0)
                - retry_on_timeout: Whether to retry on timeout (default: True)
        """
        self.config = config
        self.retry_config = retry_config or {
            'max_retries': 3,
            'backoff_factor': 1.0,
            'retry_on_timeout': True
        }
        
        # Extract connection parameters
        self.host = config.get('host', 'localhost')
        self.port = config.get('port', 3306)
        self.user = config.get('user')
        self.password = config.get('password')
        self.database = config.get('database')
        self.charset = config.get('charset', 'utf8mb4')
        
        # Pool configuration
        self.pool_size = config.get('pool_size', 5)
        self.max_overflow = config.get('max_overflow', 10)
        
        # Validate required parameters
        if not all([self.user, self.password, self.database]):
            raise ValueError("Missing required database configuration: user, password, or database")
        
        # SQLAlchemy engine (will be created on first use)
        self._engine: Optional[Engine] = None
        self._is_connected = False
    
    def _build_connection_string(self) -> str:
        """
        Build SQLAlchemy connection string.
        
        Note: We use SQLAlchemy instead of pymysql.connect() directly because:
        1. Connection pooling: Essential for FastAPI web service handling concurrent requests
        2. Connection lifecycle: Automatic connection recycling and health checks
        3. Resource management: Prevents connection exhaustion with multiple concurrent API calls
        4. Built-in retry support: Better handling of connection failures
        
        Using pymysql.connect() directly would require:
        - Manual connection pool implementation
        - Manual connection lifecycle management
        - More complex retry logic
        - Risk of connection exhaustion under load
        """
        return (
            f"mysql+pymysql://{self.user}:{self.password}@"
            f"{self.host}:{self.port}/{self.database}?"
            f"charset={self.charset}"
        )
    
    def connect(self) -> None:
        """
        Establish database connection pool.
        
        Raises:
            OperationalError: If connection fails
        """
        if self._is_connected and self._engine is not None:
            return
        
        try:
            connection_string = self._build_connection_string()
            
            self._engine = create_engine(
                connection_string,
                poolclass=QueuePool,
                pool_size=self.pool_size,
                max_overflow=self.max_overflow,
                pool_pre_ping=True,  # Verify connections before using
                pool_recycle=3600,    # Recycle connections after 1 hour
                echo=False            # Set to True for SQL logging
            )
            
            # Test connection
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            self._is_connected = True
            print(f"✓ Connected to MySQL database: {self.database}@{self.host}", file=sys.stderr)
            
        except Exception as e:
            self._is_connected = False
            error_msg = f"Failed to connect to MySQL database: {e}"
            print(f"✗ {error_msg}", file=sys.stderr)
            raise OperationalError(error_msg, None, e) from e
    
    def disconnect(self) -> None:
        """Close all database connections and dispose of the connection pool."""
        if self._engine is not None:
            try:
                self._engine.dispose()
                print(f"✓ Disconnected from MySQL database: {self.database}@{self.host}", file=sys.stderr)
            except Exception as e:
                print(f"⚠️  Error during disconnect: {e}", file=sys.stderr)
            finally:
                self._engine = None
                self._is_connected = False
    
    def is_connected(self) -> bool:
        """Check if database connection is active."""
        if not self._is_connected or self._engine is None:
            return False
        
        try:
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception:
            self._is_connected = False
            return False
    
    def health_check(self) -> Dict[str, Any]:
        """
        Perform health check on database connection.
        
        Returns:
            Dictionary with health status:
                - healthy: bool
                - latency_ms: float (connection test latency)
                - error: str (if unhealthy)
        """
        if not self._is_connected:
            return {
                'healthy': False,
                'latency_ms': None,
                'error': 'Not connected to database'
            }
        
        try:
            start_time = time.time()
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            latency_ms = (time.time() - start_time) * 1000
            
            return {
                'healthy': True,
                'latency_ms': round(latency_ms, 2),
                'error': None
            }
        except Exception as e:
            self._is_connected = False
            return {
                'healthy': False,
                'latency_ms': None,
                'error': str(e)
            }
    
    def _is_retryable_error(self, error: Exception) -> bool:
        """
        Check if an error is retryable.
        
        Args:
            error: Exception to check
            
        Returns:
            True if error is retryable, False otherwise
        """
        # Check for SQLAlchemy operational errors
        if isinstance(error, (OperationalError, DisconnectionError)):
            # Try to extract MySQL error code
            error_code = None
            if hasattr(error, 'orig'):
                orig_error = error.orig
                if hasattr(orig_error, 'args') and len(orig_error.args) > 0:
                    error_code = orig_error.args[0]
                elif hasattr(orig_error, 'errno'):
                    error_code = orig_error.errno
            
            if error_code in self.RETRYABLE_ERROR_CODES:
                return True
        
        # Check for PyMySQL operational errors
        if isinstance(error, PyMySQLOperationalError):
            if hasattr(error, 'args') and len(error.args) > 0:
                error_code = error.args[0]
                if error_code in self.RETRYABLE_ERROR_CODES:
                    return True
        
        # Check for generic database errors that might be transient
        if isinstance(error, DatabaseError):
            error_str = str(error).lower()
            retryable_keywords = [
                'lost connection',
                'server has gone away',
                'deadlock',
                'lock wait timeout',
                'too many connections',
                'connection reset'
            ]
            if any(keyword in error_str for keyword in retryable_keywords):
                return True
        
        return False
    
    def _create_retry_wrapper(self, func):
        """
        Create a retry wrapper for database operations.
        
        Args:
            func: Function to wrap with retry logic
            
        Returns:
            Wrapped function with retry logic
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            max_retries = self.retry_config['max_retries']
            backoff_factor = self.retry_config['backoff_factor']
            retry_on_timeout = self.retry_config['retry_on_timeout']
            
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    if attempt > 0:
                        wait_time = backoff_factor * (2 ** (attempt - 1))
                        print(
                            f"   Retrying database operation (attempt {attempt + 1}/{max_retries + 1}) "
                            f"after {wait_time:.1f}s...",
                            file=sys.stderr
                        )
                        time.sleep(wait_time)
                    
                    return func(*args, **kwargs)
                    
                except (OperationalError, DisconnectionError, DatabaseError) as e:
                    last_exception = e
                    
                    if self._is_retryable_error(e) and attempt < max_retries:
                        print(f"   Database error (retryable): {e}", file=sys.stderr)
                        # Try to reconnect if connection was lost
                        if isinstance(e, (OperationalError, DisconnectionError)):
                            try:
                                self._is_connected = False
                                self.connect()
                            except Exception:
                                pass  # Will retry on next attempt
                        continue
                    else:
                        # Not retryable or exhausted retries
                        break
                        
                except TimeoutError as e:
                    last_exception = e
                    if retry_on_timeout and attempt < max_retries:
                        print(f"   Database operation timeout, will retry...", file=sys.stderr)
                        continue
                    else:
                        break
                        
                except Exception as e:
                    # Non-retryable errors, raise immediately
                    raise
            
            # All retries exhausted
            if last_exception:
                raise last_exception
            else:
                raise Exception(f"Database operation failed after {max_retries + 1} attempts")
        
        return wrapper
    
    @contextmanager
    def get_connection(self):
        """
        Get a database connection from the pool (context manager).
        
        Usage:
            with db.get_connection() as conn:
                result = conn.execute(text("SELECT * FROM table"))
        
        Yields:
            SQLAlchemy Connection object
        """
        if not self._is_connected:
            self.connect()
        
        if self._engine is None:
            raise RuntimeError("Database engine not initialized. Call connect() first.")
        
        conn = None
        try:
            conn = self._engine.connect()
            yield conn
        finally:
            if conn:
                conn.close()
    
    def execute_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Execute a SELECT query and return results.
        
        Args:
            query: SQL SELECT query string
            params: Optional dictionary of parameters for parameterized query
            
        Returns:
            List of dictionaries, each representing a row
            
        Raises:
            OperationalError: If query execution fails after retries
        """
        # Wrap the actual execution with retry logic
        def _execute():
            if not self._is_connected:
                self.connect()
            
            with self.get_connection() as conn:
                result = conn.execute(text(query), params or {})
                rows = result.fetchall()
                
                # Convert rows to list of dictionaries
                if rows:
                    columns = result.keys()
                    return [dict(zip(columns, row)) for row in rows]
                return []
        
        return self._create_retry_wrapper(_execute)()
    
    def execute_update(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None
    ) -> int:
        """
        Execute an INSERT, UPDATE, or DELETE query.
        
        Args:
            query: SQL INSERT/UPDATE/DELETE query string
            params: Optional dictionary of parameters for parameterized query
            
        Returns:
            Number of affected rows
            
        Raises:
            OperationalError: If query execution fails after retries
        """
        # Wrap the actual execution with retry logic
        def _execute():
            if not self._is_connected:
                self.connect()
            
            with self.get_connection() as conn:
                result = conn.execute(text(query), params or {})
                conn.commit()
                return result.rowcount
        
        return self._create_retry_wrapper(_execute)()
    
    def get_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get table column information.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of dictionaries with column information:
                - column_name: str
                - data_type: str
                - is_nullable: str ('YES' or 'NO')
                - column_key: str (e.g., 'PRI' for primary key)
                - column_default: Any
                - extra: str (e.g., 'auto_increment')
        """
        query = """
            SELECT 
                COLUMN_NAME as column_name,
                DATA_TYPE as data_type,
                IS_NULLABLE as is_nullable,
                COLUMN_KEY as column_key,
                COLUMN_DEFAULT as column_default,
                EXTRA as extra
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = :database
            AND TABLE_NAME = :table_name
            ORDER BY ORDINAL_POSITION
        """
        
        return self.execute_query(
            query,
            params={'database': self.database, 'table_name': table_name}
        )
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
    
    def __del__(self):
        """Cleanup on deletion."""
        self.disconnect()

