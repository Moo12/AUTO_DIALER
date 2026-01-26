"""
Item Manager Module

Manages adding items to database tables with validation.
Validates that all mandatory fields are provided before insertion.
"""

import sys
import re
from typing import Dict, Any, Optional, List, Tuple
from common_utils.db_connection import DatabaseConnection
from common_utils.config_manager import ConfigManager


class ItemManager:
    """
    Manages adding items to database tables with field validation.
    """
    
    def __init__(self, item_type: str, db_connection: DatabaseConnection, config_manager: ConfigManager):
        """
        Initialize Item Manager.
        
        Args:
            item_type: Type key that matches data_base_tables configuration
            db_connection: DatabaseConnection instance for MySQL operations
            config_manager: ConfigManager instance
        """
        self.db = db_connection
        self.item_type = item_type
        self.config_manager = config_manager
        self.config = config_manager.get_config()
        
        # Load table configurations from data_base_tables
        self.data_base_tables = self.config.get('data_base_tables', {}).get(item_type, {})
        
        if not self.data_base_tables:
            raise ValueError(
                f"Configuration not found for item_type '{item_type}'. "
                f"Please ensure it exists in data_base_tables section of config.yaml"
            )

    def _get_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """Get and return table schema (cached per call-site by reusing the returned list)."""
        return self.db.get_table_schema(table_name)

    def _get_schema_field_names(self, schema: List[Dict[str, Any]]) -> set:
        return {col.get('column_name') for col in schema if col.get('column_name')}

    def _get_schema_meta_map(self, schema: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """Map column_name -> schema dict."""
        meta: Dict[str, Dict[str, Any]] = {}
        for col in schema:
            name = col.get('column_name')
            if name:
                meta[name] = col
        return meta
    
    def _get_foreign_keys(self, table_name: str) -> List[Dict[str, str]]:
        """
        Fetch foreign key metadata for a table.

        Returns list of dicts with:
            {
                'column_name': ...,
                'referenced_table': ...,
                'referenced_column': ...
            }
        """
        query = """
            SELECT
                COLUMN_NAME as column_name,
                REFERENCED_TABLE_NAME as referenced_table,
                REFERENCED_COLUMN_NAME as referenced_column
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = :database
              AND TABLE_NAME = :table_name
              AND REFERENCED_TABLE_NAME IS NOT NULL
        """
        rows = self.db.execute_query(query, {
            'database': self.db.database,
            'table_name': table_name
        })
        return [
            {
                'column_name': r['column_name'],
                'referenced_table': r['referenced_table'],
                'referenced_column': r['referenced_column']
            }
            for r in rows
            if r.get('column_name') and r.get('referenced_table') and r.get('referenced_column')
        ]
    
    def _get_table_name(self) -> str:
        """
        Get table name from configuration.
        
        Returns:
            Table name string
            
        Raises:
            ValueError: If table_name not found in configuration
        """
        # Try to get table_name directly (for simple configs like network_companies)
        table_name = self.data_base_tables.get('table_name')
        
        # If not found, try to get from users table (for complex configs like special_users)
        if not table_name:
            users_config = self.data_base_tables.get('users', {})
            table_name = users_config.get('table_name')
        
        if not table_name:
            raise ValueError(
                f"table_name not found in configuration for item_type '{self.item_type}'. "
                f"Please ensure 'table_name' or 'users.table_name' exists in data_base_tables section"
            )
        
        # Validate table name contains only safe characters
        if not re.match(r'^[a-zA-Z0-9_]+$', table_name):
            raise ValueError(
                f"Invalid table_name '{table_name}': must contain only alphanumeric characters and underscores"
            )
        
        return table_name
    
    def _get_mandatory_fields(self, table_name: str) -> List[str]:
        """
        Get list of mandatory (required) fields from table schema.
        
        A field is mandatory if:
        - IS_NULLABLE = 'NO' (NOT NULL constraint)
        - No default value (COLUMN_DEFAULT is None)
        - Not auto_increment (EXTRA does not contain 'auto_increment')
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of mandatory field names
        """
        schema = self._get_table_schema(table_name)
        mandatory_fields = []
        
        for column in schema:
            column_name = column.get('column_name')
            is_nullable = column.get('is_nullable', 'YES')
            column_default = column.get('column_default')
            extra = column.get('extra', '')
            
            # Field is mandatory if:
            # 1. NOT NULL
            # 2. No default value
            # 3. Not auto_increment
            if (is_nullable == 'NO' and 
                column_default is None and 
                'auto_increment' not in extra.lower()):
                mandatory_fields.append(column_name)
        
        return mandatory_fields
    
    def _get_primary_key(self, table_name: str) -> Optional[str]:
        """
        Get primary key column name from table schema.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Primary key column name or None
        """
        schema = self._get_table_schema(table_name)
        
        for column in schema:
            column_key = column.get('column_key', '')
            if column_key == 'PRI':
                return column.get('column_name')
        
        return None
    
    def _is_auto_increment(self, table_name: str, column_name: str) -> bool:
        """
        Check if a column is auto_increment.
        
        Args:
            table_name: Name of the table
            column_name: Name of the column
            
        Returns:
            True if column is auto_increment, False otherwise
        """
        schema = self._get_table_schema(table_name)
        
        for column in schema:
            if column.get('column_name') == column_name:
                extra = column.get('extra', '')
                return 'auto_increment' in extra.lower()
        
        return False
    
    def _validate_field_names(self, table_name: str, field_values: Dict[str, Any]) -> List[str]:
        """
        Validate that all field names exist in the table.
        
        Args:
            table_name: Name of the table
            field_values: Dictionary of field-value pairs
            
        Returns:
            List of invalid field names (empty if all valid)
        """
        schema = self._get_table_schema(table_name)
        valid_fields = self._get_schema_field_names(schema)
        
        invalid_fields = []
        for field_name in field_values.keys():
            if field_name not in valid_fields:
                invalid_fields.append(field_name)
        
        return invalid_fields

    def _build_where_clause(
        self,
        table_name: str,
        where: Dict[str, Any]
    ) -> Tuple[str, Dict[str, Any], Optional[str]]:
        """
        Build a safe WHERE clause from exact-match field filters.

        Args:
            table_name: Name of the table
            where: Dict of column -> value used for equality matching (ANDed)

        Returns:
            (where_sql, where_params, error_msg)
        """
        if not isinstance(where, dict) or not where:
            return "", {}, "where must be a non-empty dictionary"

        # Validate field names exist in table
        invalid_fields = self._validate_field_names(table_name, where)
        if invalid_fields:
            return "", {}, (
                f"Invalid WHERE field names: {', '.join(invalid_fields)}. "
                f"These fields do not exist in table '{table_name}'"
            )

        # WHERE values cannot be None (ambiguous: '=' NULL is always false)
        none_fields = [k for k, v in where.items() if v is None]
        if none_fields:
            return "", {}, f"WHERE fields cannot be null: {', '.join(none_fields)}"

        clauses: List[str] = []
        params: Dict[str, Any] = {}
        for key, value in where.items():
            # prefix params to avoid collision with set params
            param_name = f"w_{key}"
            clauses.append(f"`{key}` = :{param_name}")
            params[param_name] = value

        return " AND ".join(clauses), params, None

    def _validate_update_values(
        self,
        table_name: str,
        field_values: Dict[str, Any]
    ) -> Optional[str]:
        """
        Validate update payload against schema.
        - Ensures all field names exist
        - Prevents setting NOT NULL fields to None
        """
        if not isinstance(field_values, dict) or not field_values:
            return "field_values must be a non-empty dictionary"

        invalid_fields = self._validate_field_names(table_name, field_values)
        if invalid_fields:
            return (
                f"Invalid field names: {', '.join(invalid_fields)}. "
                f"These fields do not exist in table '{table_name}'"
            )

        schema = self._get_table_schema(table_name)
        meta_map = self._get_schema_meta_map(schema)

        for field_name, value in field_values.items():
            if value is not None:
                continue
            meta = meta_map.get(field_name, {})
            is_nullable = meta.get('is_nullable', 'YES')
            column_default = meta.get('column_default')
            extra = (meta.get('extra') or '')
            if is_nullable == 'NO' and column_default is None and 'auto_increment' not in str(extra).lower():
                return f"Field '{field_name}' is NOT NULL and cannot be set to null"

        return None
    
    def add_item(self, field_values: Dict[str, Any]) -> Dict[str, Any]:
        """
        Add an item to the configured table.
        
        Validates that all mandatory fields are provided and inserts the item.
        
        Args:
            field_values: Dictionary mapping field names to values
            
        Returns:
            Dictionary with operation results:
                {
                    'success': bool,
                    'item_id': Optional[Any],  # Primary key value of inserted item
                    'error': Optional[str],
                    'error_type': Optional[str]
                }
        """
        try:
            # Get table name from config
            table_name = self._get_table_name()
            
            # Validate field names exist in table
            invalid_fields = self._validate_field_names(table_name, field_values)
            if invalid_fields:
                return {
                    'success': False,
                    'item_id': None,
                    'error': f"Invalid field names: {', '.join(invalid_fields)}. These fields do not exist in table '{table_name}'",
                    'error_type': 'ValidationError'
                }
            
            # Get mandatory fields
            mandatory_fields = self._get_mandatory_fields(table_name)
            
            # Check that all mandatory fields are provided
            missing_fields = [field for field in mandatory_fields if field not in field_values]
            if missing_fields:
                return {
                    'success': False,
                    'item_id': None,
                    'error': f"Missing mandatory fields: {', '.join(missing_fields)}",
                    'error_type': 'ValidationError'
                }
            
            # Get primary key info
            primary_key = self._get_primary_key(table_name)
            is_auto_increment = False
            if primary_key:
                is_auto_increment = self._is_auto_increment(table_name, primary_key)
            
            # Build INSERT query
            # Only include fields that are provided and exist in the table
            schema = self._get_table_schema(table_name)
            valid_field_names = self._get_schema_field_names(schema)
            
            insert_fields = [field for field in field_values.keys() if field in valid_field_names]
            
            if not insert_fields:
                return {
                    'success': False,
                    'item_id': None,
                    'error': "No valid fields provided for insertion",
                    'error_type': 'ValidationError'
                }
            
            # Build parameterized INSERT query
            columns = ', '.join([f"`{field}`" for field in insert_fields])
            placeholders = ', '.join([f":{field}" for field in insert_fields])
            insert_query = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"
            
            # Prepare parameters
            insert_params = {field: field_values[field] for field in insert_fields}
            
            # Execute insert
            if is_auto_increment:
                # For auto_increment, we need to get the last insert ID
                if not self.db._is_connected:
                    self.db.connect()
                
                with self.db.get_connection() as conn:
                    from sqlalchemy import text
                    result = conn.execute(text(insert_query), insert_params)
                    conn.commit()
                    
                    # Get the last insert ID
                    item_id = result.lastrowid
                    if item_id is None or item_id == 0:
                        # Fallback: query for the inserted row if lastrowid is not available
                        # This is a best-effort approach - may not work for all cases
                        return {
                            'success': True,
                            'item_id': None,
                            'error': None,
                            'error_type': None
                        }
                    
                    return {
                        'success': True,
                        'item_id': item_id,
                        'error': None,
                        'error_type': None
                    }
            else:
                # For non-auto_increment, just execute the insert
                self.db.execute_update(insert_query, insert_params)
                
                # Try to get the primary key value if provided
                item_id = field_values.get(primary_key) if primary_key else None
                
                return {
                    'success': True,
                    'item_id': item_id,
                    'error': None,
                    'error_type': None
                }
            
        except Exception as e:
            error_type = type(e).__name__
            error_msg = str(e)
            print(f"✗ Error adding item: {error_msg}", file=sys.stderr)
            return {
                'success': False,
                'item_id': None,
                'error': error_msg,
                'error_type': error_type
            }

    def edit_item(
        self,
        where: Dict[str, Any],
        field_values: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Edit (update) one or more items in the configured table.

        This is a general UPDATE endpoint helper:
        - Uses exact match equality filters from `where` (ANDed)
        - Updates columns from `field_values`
        - Validates column names exist
        - Prevents setting NOT NULL columns to null
        
        Default behavior: If `where` is empty and primary key exists in `field_values`,
        automatically uses primary key for WHERE clause. This makes the common case
        of updating by ID more convenient.

        Args:
            where: Dictionary mapping identifying fields to values (e.g., {"id": 123}).
                   If empty and primary key exists in field_values, will use primary key automatically.
            field_values: Dictionary mapping fields to new values

        Returns:
            {
                'success': bool,
                'rows_affected': int,
                'error': Optional[str],
                'error_type': Optional[str]
            }
        """
        try:
            table_name = self._get_table_name()

            # Default behavior: If where is empty, try to use primary key from field_values
            effective_where = where.copy() if where else {}
            effective_field_values = field_values.copy()
            
            if not effective_where:
                primary_key = self._get_primary_key(table_name)
                if primary_key and primary_key in effective_field_values:
                    # Use primary key from field_values as WHERE clause
                    effective_where[primary_key] = effective_field_values[primary_key]
                    # Remove primary key from field_values to avoid updating it
                    effective_field_values = {k: v for k, v in effective_field_values.items() if k != primary_key}

            # Validate update values (after potentially removing PK)
            update_err = self._validate_update_values(table_name, effective_field_values)
            if update_err:
                print(f"✗ Error validating update values: {update_err}", file=sys.stderr)
                return {
                    'success': False,
                    'rows_affected': 0,
                    'error': update_err,
                    'error_type': 'ValidationError'
                }

            where_sql, where_params, where_err = self._build_where_clause(table_name, effective_where)
            if where_err:
                print(f"✗ Error building where clause: {where_err}", file=sys.stderr)
                return {
                    'success': False,
                    'rows_affected': 0,
                    'error': where_err,
                    'error_type': 'ValidationError'
                }

            set_clauses: List[str] = []
            set_params: Dict[str, Any] = {}
            for key, value in effective_field_values.items():
                param_name = f"s_{key}"
                set_clauses.append(f"`{key}` = :{param_name}")
                set_params[param_name] = value

            set_sql = ", ".join(set_clauses)
            query = f"UPDATE `{table_name}` SET {set_sql} WHERE {where_sql}"
            params = {**set_params, **where_params}

            rows_affected = self.db.execute_update(query, params)

            return {
                'success': True,
                'rows_affected': rows_affected,
                'error': None,
                'error_type': None
            }
        except Exception as e:
            error_type = type(e).__name__
            error_msg = str(e)
            print(f"✗ Error editing item: {error_msg}", file=sys.stderr)
            return {
                'success': False,
                'rows_affected': 0,
                'error': error_msg,
                'error_type': error_type
            }

    def update_item(
        self,
        where: Dict[str, Any],
        field_values: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Alias for edit_item() for API naming consistency.

        Args:
            where: Dictionary mapping identifying fields to values (e.g., {"id": 123})
            field_values: Dictionary mapping fields to new values

        Returns:
            Same structure as edit_item(): {success, rows_affected, error, error_type}
        """
        return self.edit_item(where=where, field_values=field_values)

    def remove_item(self, where: Dict[str, Any], item_id: Optional[Any] = None) -> Dict[str, Any]:
        """
        Remove (delete) one or more items from the configured table.

        Uses exact match equality filters from `where` (ANDed).
        
        Default behavior: If `where` is empty and `item_id` is provided,
        automatically uses primary key with `item_id` value for WHERE clause.

        Args:
            where: Dictionary mapping identifying fields to values (e.g., {"id": 123}).
                   If empty and item_id is provided, will use primary key automatically.
            item_id: Optional primary key value to use when where is empty

        Returns:
            {
                'success': bool,
                'rows_affected': int,
                'error': Optional[str],
                'error_type': Optional[str]
            }
        """
        try:
            table_name = self._get_table_name()

            # Default behavior: If where is empty, try to use primary key from item_id
            effective_where = where.copy() if where else {}
            if not effective_where:
                if item_id is not None:
                    primary_key = self._get_primary_key(table_name)
                    if primary_key:
                        # Use primary key with item_id value for WHERE clause
                        effective_where[primary_key] = item_id
                    else:
                        return {
                            'success': False,
                            'rows_affected': 0,
                            'error': "Cannot use item_id: table has no primary key",
                            'error_type': 'ValidationError'
                        }
                else:
                    return {
                        'success': False,
                        'rows_affected': 0,
                        'error': "Either 'where' must be provided or 'item_id' must be provided",
                        'error_type': 'ValidationError'
                    }

            where_sql, where_params, where_err = self._build_where_clause(table_name, effective_where)
            if where_err:
                return {
                    'success': False,
                    'rows_affected': 0,
                    'error': where_err,
                    'error_type': 'ValidationError'
                }

            query = f"DELETE FROM `{table_name}` WHERE {where_sql}"
            rows_affected = self.db.execute_update(query, where_params)

            return {
                'success': True,
                'rows_affected': rows_affected,
                'error': None,
                'error_type': None
            }
        except Exception as e:
            error_type = type(e).__name__
            error_msg = str(e)
            print(f"✗ Error removing item: {error_msg}", file=sys.stderr)
            return {
                'success': False,
                'rows_affected': 0,
                'error': error_msg,
                'error_type': error_type
            }

    def get_items(self, include_foreign: bool = False) -> Dict[str, Any]:
        """
        Get all items from the configured table.

        Optionally enriches foreign key columns with the referenced rows.

        Args:
            include_foreign: If True, include referenced rows as `<column>_obj`

        Returns:
            {
                'success': bool,
                'items': List[Dict[str, Any]],
                'error': Optional[str],
                'error_type': Optional[str]
            }
        """
        try:
            table_name = self._get_table_name()

            # Fetch rows
            rows = self.db.execute_query(f"SELECT * FROM `{table_name}`")

            if not include_foreign or not rows:
                return {
                    'success': True,
                    'items': rows,
                    'error': None,
                    'error_type': None
                }

            # Fetch FK metadata
            fks = self._get_foreign_keys(table_name)
            if not fks:
                return {
                    'success': True,
                    'items': rows,
                    'error': None,
                    'error_type': None
                }

            # Build lookup for each FK to minimize queries
            fk_values_by_table: Dict[Tuple[str, str], set] = {}
            for fk in fks:
                col = fk['column_name']
                ref_table = fk['referenced_table']
                ref_col = fk['referenced_column']
                key = (ref_table, ref_col)
                values = fk_values_by_table.setdefault(key, set())
                for row in rows:
                    val = row.get(col)
                    if val is not None:
                        values.add(val)

            fk_results: Dict[Tuple[str, str], Dict[Any, Dict[str, Any]]] = {}
            for (ref_table, ref_col), values in fk_values_by_table.items():
                if not values:
                    fk_results[(ref_table, ref_col)] = {}
                    continue
                placeholders = ", ".join([f":v{i}" for i in range(len(values))])
                params = {f"v{i}": v for i, v in enumerate(values)}
                query = f"SELECT * FROM `{ref_table}` WHERE `{ref_col}` IN ({placeholders})"
                fetched = self.db.execute_query(query, params)
                fk_results[(ref_table, ref_col)] = {row[ref_col]: row for row in fetched}

            # Enrich rows
            for fk in fks:
                col = fk['column_name']
                ref_table = fk['referenced_table']
                ref_col = fk['referenced_column']
                lookup = fk_results.get((ref_table, ref_col), {})
                for row in rows:
                    val = row.get(col)
                    if val is None:
                        continue
                    row[f"{col}_obj"] = lookup.get(val)

            return {
                'success': True,
                'items': rows,
                'error': None,
                'error_type': None
            }

        except Exception as e:
            error_type = type(e).__name__
            error_msg = str(e)
            print(f"✗ Error fetching items: {error_msg}", file=sys.stderr)
            return {
                'success': False,
                'items': [],
                'error': error_msg,
                'error_type': error_type
            }

