"""
Shared generic item and list endpoints for FastAPI routers.

This module provides reusable endpoint functions and models for CRUD operations
on database tables using ItemManager and ListManager.
"""

import os
import sys
from pathlib import Path
from typing import Optional, Dict, Any, List, Callable, Union
from fastapi import HTTPException, Query
from pydantic import BaseModel

from common_utils.db_connection import DatabaseConnection
from common_utils.config_manager import ConfigManager
from common_utils.item_manager import ItemManager
from common_utils.list_manager import ListManager


# ============================================================================
# Request/Response Models
# ============================================================================

class AddItemRequest(BaseModel):
    """Request model for adding an item."""
    item_type: str  # Type key that matches data_base_tables configuration
    field_values: Dict[str, Any]  # Dictionary mapping field names to values


class AddItemResponse(BaseModel):
    """Response model for add item endpoint."""
    success: bool
    item_id: Optional[Any] = None  # Primary key value of inserted item
    error: Optional[str] = None
    error_type: Optional[str] = None


class UpdateItemRequest(BaseModel):
    """Request model for updating an item (generic update)."""
    item_type: str  # Type key that matches data_base_tables configuration
    where: Dict[str, Any] = {}  # Exact-match filters (ANDed) to locate row(s). If empty, uses primary key from field_values
    field_values: Dict[str, Any]  # Fields to update


class UpdateItemResponse(BaseModel):
    """Response model for update item endpoint."""
    success: bool
    rows_affected: int = 0
    error: Optional[str] = None
    error_type: Optional[str] = None


class RemoveItemRequest(BaseModel):
    """Request model for removing an item."""
    item_type: str  # Type key that matches data_base_tables configuration
    where: Dict[str, Any] = {}  # Exact-match filters (ANDed) to locate row(s). If empty, uses item_id
    item_id: Optional[Any] = None  # Primary key value to use when where is empty


class RemoveItemResponse(BaseModel):
    """Response model for remove item endpoint."""
    success: bool
    rows_affected: int = 0
    error: Optional[str] = None
    error_type: Optional[str] = None


class GetItemsResponse(BaseModel):
    """Response model for get items endpoint."""
    success: bool
    items: List[Dict[str, Any]] = []
    error: Optional[str] = None
    error_type: Optional[str] = None


class ListInfo(BaseModel):
    """Model for list information."""
    id: int
    list_name: str
    is_active: Optional[int] = None
    created_at: Optional[str] = None
    time_activate_modify: Optional[str] = None
    users: List[Dict[str, Any]] = []  # Array of user dictionaries


class GetListsResponse(BaseModel):
    """Response model for get all lists endpoint."""
    success: bool
    lists: List[ListInfo] = []
    error: Optional[str] = None
    error_type: Optional[str] = None


class EditListRequest(BaseModel):
    """Request model for editing a list."""
    list_id: int
    list_name: Optional[str] = None  # New name for the list
    is_active: Optional[int] = None  # 1 to activate, 0 to deactivate
    users: Optional[List[Dict[str, Any]]] = None  # Users to add/update. Behavior depends on add_users_only flag.
    add_users_only: bool = False  # If True, only add/update users without removing existing ones. If False, users represents desired final state (full sync).


class EditListResponse(BaseModel):
    """Response model for edit list endpoint."""
    success: bool
    list_id: int
    list_name: Optional[str] = None
    is_active: Optional[int] = None
    users_added: int = 0
    users_updated: int = 0
    users_removed: int = 0
    error: Optional[str] = None
    error_type: Optional[str] = None


class RemoveListRequest(BaseModel):
    """Request model for removing a list."""
    list_id: int  # ID of the list to remove


class RemoveListResponse(BaseModel):
    """Response model for remove list endpoint."""
    success: bool
    list_id: int
    rows_affected: int = 0
    error: Optional[str] = None
    error_type: Optional[str] = None


# ============================================================================
# Database Connection and Config Management
# ============================================================================

# Note: These functions do NOT cache instances globally.
# Each module (settings_backend, auto_caller_logic) should manage its own
# config manager and database connection instances to avoid conflicts.


def get_db_connection(
    config_path: Optional[str] = None,
    env_config_var: Optional[str] = None,
    fallback_paths: Optional[List[str]] = None
) -> DatabaseConnection:
    """
    Create a new database connection instance.
    
    Note: This function does NOT cache the connection. Each module should
    manage its own connection instance to avoid conflicts.
    
    Args:
        config_path: Optional explicit path to config file
        env_config_var: Optional environment variable name to check for config path
        fallback_paths: Optional list of fallback config file paths to try
    
    Returns:
        DatabaseConnection instance
        
    Raises:
        HTTPException: If database connection fails
    """
    try:
        config_manager = get_config(
            config_path=config_path,
            env_config_var=env_config_var,
            fallback_paths=fallback_paths
        )
        
        config = config_manager.get_config()
        
        # Get database config
        db_config = config.get('database', {})
        if not db_config:
            raise ValueError(
                "Database configuration not found in config file. "
                "Please add 'database' section to config file with: "
                "host, port, user, password, database, charset, pool_size, max_overflow"
            )
        
        # Validate required database config fields
        required_fields = ['host', 'user', 'password', 'database']
        missing_fields = [field for field in required_fields if not db_config.get(field)]
        if missing_fields:
            raise ValueError(
                f"Missing required database configuration fields: {', '.join(missing_fields)}"
            )
        
        # Get retry config
        retry_config = config.get('database', {}).get('retry', {
            'max_retries': 3,
            'backoff_factor': 1.0,
            'retry_on_timeout': True
        })

        print(f"db_config: {db_config}", file=sys.stderr)
        print(f"retry_config: {retry_config}", file=sys.stderr)
        
        db_connection = DatabaseConnection(db_config, retry_config)
        db_connection.connect()
        
        return db_connection
        
    except Exception as e:
        error_msg = f"Failed to initialize database connection: {str(e)}"
        print(f"✗ {error_msg}", file=sys.stderr)
        raise HTTPException(status_code=500, detail=error_msg)


def get_config(
    config_path: Optional[str] = None,
    env_config_var: Optional[str] = None,
    fallback_paths: Optional[List[str]] = None
) -> ConfigManager:
    """
    Create a new ConfigManager instance.
    
    Note: This function does NOT cache the config manager. Each module should
    manage its own config manager instance to avoid conflicts.
    
    Args:
        config_path: Optional explicit path to config file
        env_config_var: Optional environment variable name to check for config path
        fallback_paths: Optional list of fallback config file paths to try
    
    Returns:
        ConfigManager instance
    """
    print(f"config_path: {config_path}", file=sys.stderr)
    # Try explicit path first
    if config_path:
        print(f"config_path: {config_path}", file=sys.stderr)
        if Path(config_path).exists():
            return ConfigManager(config_path)
    
    # Try environment variable
    if env_config_var:
        env_path = os.getenv(env_config_var)
        if env_path and Path(env_path).exists():
            return ConfigManager(env_path)
    
    # Try fallback paths
    if fallback_paths:
        for fallback_path in fallback_paths:
            if Path(fallback_path).exists():
                print(f"fallback_path: {fallback_path}", file=sys.stderr)
                return ConfigManager(fallback_path)
    
    # Default: try common locations
    default_paths = [
        "config_server.yaml",
        "settings_backend/config.yaml",
        "auto_caller_logic/config.yaml"
    ]
    
    for default_path in default_paths:
        if Path(default_path).exists():
            return ConfigManager(default_path)
    
    # If nothing found, raise error
    raise FileNotFoundError(
        f"Config file not found. Tried: {config_path or 'N/A'}, "
        f"env var {env_config_var or 'N/A'}, fallbacks: {fallback_paths or 'N/A'}, "
        f"defaults: {default_paths}"
    )


# ============================================================================
# Endpoint Functions
# ============================================================================

async def add_item_endpoint(
    request: AddItemRequest,
    get_db_func: Optional[Callable[[], DatabaseConnection]] = None,
    get_config_func: Optional[Callable[[], ConfigManager]] = None,
    converter_func: Optional[Callable[[Dict[str, Any], str, ConfigManager], Dict[str, Any]]] = None
) -> AddItemResponse:
    """
    Add an item to a database table.
    
    Args:
        request: AddItemRequest with item_type and field_values
        get_db_func: Optional function to get database connection (if None, uses default)
        get_config_func: Optional function to get config manager (if None, uses default)
        converter_func: Optional function to convert request data before insertion.
                       Signature: (request_data: Dict[str, Any], item_type: str, config_manager: ConfigManager) -> Dict[str, Any]
                       If None, uses field_values as-is.
    
    Returns:
        AddItemResponse with operation result
    """
    try:
        if get_db_func:
            db_connection = get_db_func()
        else:
            db_connection = get_db_connection()
        
        if get_config_func:
            config_manager = get_config_func()
        else:
            config_manager = get_config()

        print(f"request.item_type: {request.item_type}", file=sys.stderr)
        print(f"request.field_values: {request.field_values}", file=sys.stderr)
        
        # Convert data if converter function is provided
        field_values = request.field_values
        if converter_func:
            field_values = converter_func(request.field_values, request.item_type, config_manager)
            print(f"converted field_values: {field_values}", file=sys.stderr)
        
        item_manager = ItemManager(request.item_type, db_connection, config_manager)
        result = item_manager.add_item(field_values)
        
        return AddItemResponse(
            success=result.get('success', False),
            item_id=result.get('item_id'),
            error=result.get('error'),
            error_type=result.get('error_type')
        )
        
    except Exception as e:
        error_type = type(e).__name__
        error_msg = str(e)
        print(f"✗ Error adding item: {error_msg}", file=sys.stderr)
        return AddItemResponse(
            success=False,
            item_id=None,
            error=error_msg,
            error_type=error_type
        )


async def update_item_endpoint(
    request: UpdateItemRequest,
    get_db_func: Optional[Callable[[], DatabaseConnection]] = None,
    get_config_func: Optional[Callable[[], ConfigManager]] = None,
    converter_func: Optional[Callable[[Dict[str, Any], str, ConfigManager], Dict[str, Any]]] = None
) -> UpdateItemResponse:
    """
    Update one or more rows in a database table.
    
    Args:
        request: UpdateItemRequest with item_type, where, and field_values
        get_db_func: Optional function to get database connection (if None, uses default)
        get_config_func: Optional function to get config manager (if None, uses default)
        converter_func: Optional function to convert request data before update.
                       Signature: (request_data: Dict[str, Any], item_type: str, config_manager: ConfigManager) -> Dict[str, Any]
                       If None, uses field_values as-is.
    
    Returns:
        UpdateItemResponse with operation result
    """
    try:
        if get_db_func:
            db_connection = get_db_func()
        else:
            db_connection = get_db_connection()
        
        if get_config_func:
            config_manager = get_config_func()
        else:
            config_manager = get_config()
        
        # Convert data if converter function is provided
        field_values = request.field_values
        if converter_func:
            field_values = converter_func(request.field_values, request.item_type, config_manager)
            print(f"converted field_values: {field_values}", file=sys.stderr)
        
        item_manager = ItemManager(request.item_type, db_connection, config_manager)
        result = item_manager.update_item(where=request.where, field_values=field_values)

        return UpdateItemResponse(
            success=result.get('success', False),
            rows_affected=result.get('rows_affected', 0),
            error=result.get('error'),
            error_type=result.get('error_type')
        )
    except Exception as e:
        error_type = type(e).__name__
        error_msg = str(e)
        print(f"✗ Error updating item: {error_msg}", file=sys.stderr)
        return UpdateItemResponse(
            success=False,
            rows_affected=0,
            error=error_msg,
            error_type=error_type
        )


async def remove_item_endpoint(
    request: RemoveItemRequest,
    get_db_func: Optional[Callable[[], DatabaseConnection]] = None,
    get_config_func: Optional[Callable[[], ConfigManager]] = None
) -> RemoveItemResponse:
    """
    Remove (delete) one or more items from a database table.
    
    Args:
        request: RemoveItemRequest with item_type, where, and optional item_id
        get_db_func: Optional function to get database connection (if None, uses default)
        get_config_func: Optional function to get config manager (if None, uses default)
    
    Returns:
        RemoveItemResponse with operation result
    """
    try:
        if get_db_func:
            db_connection = get_db_func()
        else:
            db_connection = get_db_connection()
        
        if get_config_func:
            config_manager = get_config_func()
        else:
            config_manager = get_config()
        
        item_manager = ItemManager(request.item_type, db_connection, config_manager)
        result = item_manager.remove_item(where=request.where, item_id=request.item_id)
        
        return RemoveItemResponse(
            success=result.get('success', False),
            rows_affected=result.get('rows_affected', 0),
            error=result.get('error'),
            error_type=result.get('error_type')
        )
        
    except Exception as e:
        error_type = type(e).__name__
        error_msg = str(e)
        print(f"✗ Error removing item: {error_msg}", file=sys.stderr)
        return RemoveItemResponse(
            success=False,
            rows_affected=0,
            error=error_msg,
            error_type=error_type
        )


async def get_items_endpoint(
    item_type: str,
    include_foreign: bool = False,
    get_db_func: Optional[Callable[[], DatabaseConnection]] = None,
    get_config_func: Optional[Callable[[], ConfigManager]] = None
) -> GetItemsResponse:
    """
    Get all items from a configured table.
    
    Args:
        item_type: Type key that matches data_base_tables configuration
        include_foreign: Whether to hydrate foreign key fields with referenced rows
        get_db_func: Optional function to get database connection (if None, uses default)
        get_config_func: Optional function to get config manager (if None, uses default)
    
    Returns:
        GetItemsResponse with items array
    """
    try:
        if get_db_func:
            db_connection = get_db_func()
        else:
            db_connection = get_db_connection()
        
        if get_config_func:
            config_manager = get_config_func()
        else:
            config_manager = get_config()

        item_manager = ItemManager(item_type, db_connection, config_manager)

        result = item_manager.get_items(include_foreign=include_foreign)

        return GetItemsResponse(
            success=result.get('success', False),
            items=result.get('items', []),
            error=result.get('error'),
            error_type=result.get('error_type')
        )

    except Exception as e:
        error_type = type(e).__name__
        error_msg = str(e)
        print(f"✗ Error fetching items: {error_msg}", file=sys.stderr)
        return GetItemsResponse(
            success=False,
            items=[],
            error=error_msg,
            error_type=error_type
        )


async def get_lists_endpoint(
    list_type: str,
    get_db_func: Optional[Callable[[], DatabaseConnection]] = None,
    get_config_func: Optional[Callable[[], ConfigManager]] = None
) -> GetListsResponse:
    """
    Get all lists with their associated users for a given list type.
    
    Args:
        list_type: Type key that matches data_base_tables configuration
        get_db_func: Optional function to get database connection (if None, uses default)
        get_config_func: Optional function to get config manager (if None, uses default)
    
    Returns:
        GetListsResponse with lists array
    """
    try:
        if get_db_func:
            db_connection = get_db_func()
        else:
            db_connection = get_db_connection()
        
        if get_config_func:
            config_manager = get_config_func()
        else:
            config_manager = get_config()
        
        list_manager = ListManager(list_type, db_connection, config_manager)
        lists = list_manager.get_all_lists_with_users()

        print(f"lists: {lists}", file=sys.stderr)
        
        return GetListsResponse(
            success=True,
            lists=lists,
            error=None,
            error_type=None
        )
        
    except Exception as e:
        error_type = type(e).__name__
        error_msg = str(e)
        print(f"✗ Error fetching lists: {error_msg}", file=sys.stderr)
        return GetListsResponse(
            success=False,
            lists=[],
            error=error_msg,
            error_type=error_type
        )


async def edit_list_endpoint(
    request: EditListRequest,
    list_type: str,
    get_db_func: Optional[Callable[[], DatabaseConnection]] = None,
    get_config_func: Optional[Callable[[], ConfigManager]] = None
) -> EditListResponse:
    """
    Edit a list: update name, activate/deactivate, add/remove users.
    
    Args:
        request: EditListRequest with list_id and optional operations
        list_type: Type key that matches data_base_tables configuration
        get_db_func: Optional function to get database connection (if None, uses default)
        get_config_func: Optional function to get config manager (if None, uses default)
    
    Returns:
        EditListResponse with operation result
    """
    try:
        # Validate that at least one operation is requested
        has_operation = (
            request.list_name is not None or
            request.is_active is not None or
            request.users is not None
        )
        
        if not has_operation:
            return EditListResponse(
                success=False,
                list_id=request.list_id,
                error="At least one operation must be specified (list_name, is_active, or users)",
                error_type="ValueError"
            )
        
        if get_db_func:
            db_connection = get_db_func()
        else:
            db_connection = get_db_connection()
        
        if get_config_func:
            config_manager = get_config_func()
        else:
            config_manager = get_config()
        
        list_manager = ListManager(list_type, db_connection, config_manager)
        result = list_manager.edit_list(
            list_id=request.list_id,
            list_name=request.list_name,
            is_active=request.is_active,
            users=request.users,
            add_users_only=request.add_users_only
        )
        
        if result.get('success', False):
            return EditListResponse(
                success=True,
                list_id=result['list_id'],
                list_name=result.get('list_name'),
                is_active=result.get('is_active'),
                users_added=result.get('users_added', 0),
                users_updated=result.get('users_updated', 0),
                users_removed=result.get('users_removed', 0),
                error=None,
                error_type=None
            )
        else:
            errors = result.get('errors', [])
            error_msg = '; '.join(errors) if errors else "Unknown error"
            return EditListResponse(
                success=False,
                list_id=request.list_id,
                error=error_msg,
                error_type="OperationError"
            )
        
    except Exception as e:
        error_type = type(e).__name__
        error_msg = str(e)
        print(f"✗ Error editing list: {error_msg}", file=sys.stderr)
        return EditListResponse(
            success=False,
            list_id=request.list_id if hasattr(request, 'list_id') else 0,
            error=error_msg,
            error_type=error_type
        )


async def remove_list_endpoint(
    request: RemoveListRequest,
    list_type: str,
    get_db_func: Optional[Callable[[], DatabaseConnection]] = None,
    get_config_func: Optional[Callable[[], ConfigManager]] = None
) -> RemoveListResponse:
    """
    Remove (delete) a list and all its user associations.
    
    Args:
        request: RemoveListRequest with list_id
        list_type: Type key that matches data_base_tables configuration
        get_db_func: Optional function to get database connection (if None, uses default)
        get_config_func: Optional function to get config manager (if None, uses default)
    
    Returns:
        RemoveListResponse with operation result
    """
    try:
        if get_db_func:
            db_connection = get_db_func()
        else:
            db_connection = get_db_connection()
        
        if get_config_func:
            config_manager = get_config_func()
        else:
            config_manager = get_config()
        
        list_manager = ListManager(list_type, db_connection, config_manager)
        result = list_manager.remove_list(list_id=request.list_id)
        
        if result.get('success', False):
            return RemoveListResponse(
                success=True,
                list_id=result['list_id'],
                rows_affected=result.get('rows_affected', 0),
                error=None,
                error_type=None
            )
        else:
            error_msg = result.get('error', 'Unknown error')
            return RemoveListResponse(
                success=False,
                list_id=request.list_id,
                rows_affected=0,
                error=error_msg,
                error_type=result.get('error_type', 'OperationError')
            )
        
    except Exception as e:
        error_type = type(e).__name__
        error_msg = str(e)
        print(f"✗ Error removing list: {error_msg}", file=sys.stderr)
        return RemoveListResponse(
            success=False,
            list_id=request.list_id if hasattr(request, 'list_id') else 0,
            rows_affected=0,
            error=error_msg,
            error_type=error_type
        )

