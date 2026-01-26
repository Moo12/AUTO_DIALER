"""
Settings Backend API Routes

FastAPI router for settings backend operations.
"""

import sys
import base64
import tempfile
import os
from pathlib import Path
from typing import Optional, Dict, Any, List
from fastapi import APIRouter, HTTPException, Query, Depends
from pydantic import BaseModel

from common_utils.db_connection import DatabaseConnection
from common_utils.excel_handler import ExcelHandler
from common_utils.config_manager import ConfigManager
from common_utils.item_endpoints import (
    AddItemRequest, AddItemResponse,
    UpdateItemRequest, UpdateItemResponse,
    RemoveItemRequest, RemoveItemResponse,
    GetItemsResponse,
    GetListsResponse, EditListRequest, EditListResponse,
    get_db_connection, get_config,
    add_item_endpoint, update_item_endpoint, remove_item_endpoint,
    get_items_endpoint, get_lists_endpoint, edit_list_endpoint
)

router = APIRouter(prefix="/api/settings", tags=["settings-backend"])

# Request/Response Models
class ConvertTableToExcelRequest(BaseModel):
    table_name: str
    filters: Optional[Dict[str, Any]] = None
    columns: Optional[List[str]] = None
    sheet_name: Optional[str] = "Sheet1"
    output_path: Optional[str] = None
    column_mapping: Optional[Dict[str, str]] = None  # Maps DB column names to display names
    column_converters: Optional[Dict[str, str]] = None  # Maps column names to converter names
    
    @classmethod
    def from_query_params(
        cls,
        table_name: str,
        filters: Optional[str] = None,
        columns: Optional[str] = None,
        sheet_name: Optional[str] = "Sheet1",
        output_path: Optional[str] = None,
        column_mapping: Optional[str] = None,
        column_converters: Optional[str] = None
    ) -> "ConvertTableToExcelRequest":
        """Create request from GET query parameters."""
        import json
        
        # Parse filters if provided
        filters_dict = None
        if filters:
            try:
                filters_dict = json.loads(filters)
            except json.JSONDecodeError:
                raise ValueError("Invalid JSON format for filters parameter")
        
        # Parse columns if provided
        columns_list = None
        if columns:
            columns_list = [col.strip() for col in columns.split(',')]
        
        # Parse column_mapping if provided
        column_mapping_dict = None
        if column_mapping:
            try:
                column_mapping_dict = json.loads(column_mapping)
            except json.JSONDecodeError:
                raise ValueError("Invalid JSON format for column_mapping parameter")
        
        # Parse column_converters if provided
        column_converters_dict = None
        if column_converters:
            try:
                column_converters_dict = json.loads(column_converters)
            except json.JSONDecodeError:
                raise ValueError("Invalid JSON format for column_converters parameter")
        
        return cls(
            table_name=table_name,
            filters=filters_dict,
            columns=columns_list,
            sheet_name=sheet_name,
            output_path=output_path,
            column_mapping=column_mapping_dict,
            column_converters=column_converters_dict
        )


class ConvertTableToExcelResponse(BaseModel):
    success: bool
    file_content_base64: Optional[str] = None
    error: Optional[str] = None
    error_type: Optional[str] = None


class ConvertExcelToTableRequest(BaseModel):
    """Request model for Excel to MySQL conversion."""
    table_name: Optional[str] = None
    file_content_base64: str  # Base64-encoded Excel file content
    sheet_name: Optional[str] = None
    header_row: int = 0
    start_row: int = 1
    column_mapping: Optional[Dict[str, str]] = None
    column_converters: Optional[Dict[str, str]] = None  # Maps MySQL column names to converter names
    update_on_duplicate: bool = True
    batch_size: int = 100
    list_config: Optional[Dict[str, Any]] = None  # Dictionary with 'type' (matches data_base_tables key) and 'list_name'
    
    class Config:
        json_schema_extra = {
            "example": {
                "table_name": "phone_numbers",
                "file_content_base64": "UEsDBBQAAAAIA...",
                "sheet_name": "Sheet1",
                "header_row": 0,
                "start_row": 1,
                "column_mapping": {"Excel Name": "name", "Excel Phone": "phone_number"},
                "column_converters": {"color_rgb": "color_to_rgb"},
                "update_on_duplicate": True,
                "batch_size": 100,
                "list_config": None
            }
        }


class ConvertExcelToTableResponse(BaseModel):
    success: bool
    rows_inserted: List[Any] = []  # List of primary keys for inserted rows
    rows_updated: List[Any] = []   # List of primary keys for updated rows
    rows_skipped: int = 0
    total_rows: int = 0
    errors: List[Dict[str, Any]] = []
    error: Optional[str] = None
    error_type: Optional[str] = None
    # Additional fields for list creation
    list_id: Optional[int] = None  # ID of the created list (if list_config provided)
    rows_linked: int = 0  # Number of users linked to the list (if list_config provided)


# List models imported from common_utils.item_endpoints

# Item and List models imported from common_utils.item_endpoints


# Module-specific instances (cached per module)
_db_connection: Optional[DatabaseConnection] = None
_config_manager: Optional[ConfigManager] = None
_excel_handler: Optional[ExcelHandler] = None

def _get_db_connection() -> DatabaseConnection:
    """Get or create database connection instance for this module."""
    global _db_connection
    
    if _db_connection is not None and _db_connection.is_connected():
        return _db_connection
    
    _db_connection = get_db_connection(
        env_config_var='MAIN_CONFIG_PATH',
        fallback_paths=[
            str(Path(__file__).parent.parent / "config.yaml"),
            str(Path(__file__).parent / "config.yaml")
        ]
    )
    return _db_connection
        
def _get_config() -> ConfigManager:
    """Get or create config manager instance for this module."""
    global _config_manager
    
    if _config_manager is not None:
        return _config_manager
    
    _config_manager = get_config(
        env_config_var='SETTINGS_BACKEND_CONFIG_PATH',
        fallback_paths=[str(Path(__file__).parent / "config.yaml")]
    )
    return _config_manager

def _get_excel_handler() -> ExcelHandler:
    """Get or create Excel handler instance."""
    global _excel_handler
    
    if _excel_handler is None:
        db_conn = _get_db_connection()
        _excel_handler = ExcelHandler(db_conn)
    
    return _excel_handler

@router.post("/convert-table-to-excel", response_model=ConvertTableToExcelResponse)
async def convert_table_to_excel(request: ConvertTableToExcelRequest):
    """
    Convert MySQL table to Excel file.
    
    Exports data from a MySQL table to an Excel file with optional filtering,
    column selection, and column name mapping.
    
    Args:
        request: ConvertTableToExcelRequest with:
            - table_name: Name of MySQL table to export
            - filters: Optional dictionary of filters {column: value}
            - columns: Optional list of column names to export
            - sheet_name: Name of Excel sheet (default: "Sheet1")
            - output_path: Optional output file path (if None, returns file content in response only)
            - column_mapping: Optional dictionary mapping DB column names to display names
                            e.g., {'user_id': 'User ID', 'created_at': 'Created Date'}
            - column_converters: Optional dictionary mapping column names to converter names
                              e.g., {'color_rgb': 'rgb_to_color', 'is_active': 'bool_to_yesno'}
                              Available: rgb_to_color, bool_to_yesno, bool_to_hebrew, date_format, null_to_empty
    
    Returns:
        ConvertTableToExcelResponse with:
            - success: bool
            - file_content_base64: Base64 encoded file content (for download)
            - error: Error message if failed
    """
    try:
        excel_handler = _get_excel_handler()
        
        # Convert table to Excel (returns bytes if no output_path, or file path if output_path provided)
        result = excel_handler.mysql_to_excel(
            table_name=request.table_name,
            output_path=request.output_path,
            filters=request.filters,
            columns=request.columns,
            sheet_name=request.sheet_name or "Sheet1",
            column_mapping=request.column_mapping,
            column_converters=request.column_converters
        )
        
        # Handle return value: bytes or file path
        file_content_base64 = None
        
        if isinstance(result, bytes):
            # Result is bytes content (no file was created)
            file_content_base64 = base64.b64encode(result).decode('utf-8')
        elif isinstance(result, str):
            # Result is file path (file was saved) - read and encode
            try:
                with open(result, 'rb') as f:
                    file_content = f.read()
                    file_content_base64 = base64.b64encode(file_content).decode('utf-8')
            except Exception as e:
                print(f"⚠️  Warning: Could not read file for base64 encoding: {e}", file=sys.stderr)
        
        return ConvertTableToExcelResponse(
            success=True,
            file_content_base64=file_content_base64,
            error=None,
            error_type=None
        )
        
    except ValueError as e:
        return ConvertTableToExcelResponse(
            success=False,
            file_content_base64=None,
            error=str(e),
            error_type="ValueError"
        )
    except Exception as e:
        error_type = type(e).__name__
        error_msg = str(e)
        print(f"✗ Error converting table to Excel: {error_msg}", file=sys.stderr)
        return ConvertTableToExcelResponse(
            success=False,
            file_content_base64=None,
            error=error_msg,
            error_type=error_type
        )


def get_convert_table_request(
    table_name: str = Query(..., description="Name of MySQL table to export"),
    filters: Optional[str] = Query(None, description="JSON string of filters {column: value}"),
    columns: Optional[str] = Query(None, description="Comma-separated list of column names"),
    sheet_name: Optional[str] = Query("Sheet1", description="Name of Excel sheet"),
    output_path: Optional[str] = Query(None, description="Optional output file path"),
    column_mapping: Optional[str] = Query(None, description="JSON string mapping DB column names to display names"),
    column_converters: Optional[str] = Query(None, description="JSON string mapping column names to converter names")
) -> ConvertTableToExcelRequest:
    """Dependency function to parse query parameters into request model."""
    try:
        return ConvertTableToExcelRequest.from_query_params(
            table_name=table_name,
            filters=filters,
            columns=columns,
            sheet_name=sheet_name,
            output_path=output_path,
            column_mapping=column_mapping,
            column_converters=column_converters
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/convert-table-to-excel", response_model=ConvertTableToExcelResponse)
async def convert_table_to_excel_get(
    request: ConvertTableToExcelRequest = Depends(get_convert_table_request)
):
    """
    Convert MySQL table to Excel file (GET version).
    
    Same functionality as POST version but using query parameters.
    Useful for simple exports without complex filters.
    """
    try:
        # Call POST handler
        return await convert_table_to_excel(request)
        
    except Exception as e:
        error_type = type(e).__name__
        error_msg = str(e)
        return ConvertTableToExcelResponse(
            success=False,
            file_content_base64=None,
            error=error_msg,
            error_type=error_type
        )


@router.post("/convert-excel-to-table", response_model=ConvertExcelToTableResponse)
async def convert_excel_to_table(
    request: ConvertExcelToTableRequest
):
    """
    Convert Excel file to MySQL table rows.
    
    Uploads an Excel file (as base64-encoded content) and inserts/updates rows in a MySQL table.
    Supports column mapping and automatic INSERT/UPDATE based on primary key.
    
    Args:
        request: ConvertExcelToTableRequest with:
            - table_name: Name of MySQL table to import data into
            - file_content_base64: Base64-encoded Excel file content
            - sheet_name: Optional name of sheet to read (if None, uses first sheet)
            - header_row: Row number containing headers (0-indexed, default: 0)
            - start_row: Row number where data starts (0-indexed, default: 1)
            - column_mapping: Optional dictionary mapping Excel column names to MySQL column names
                           e.g., {"Excel Name": "mysql_name", "Excel Email": "email"}
            - update_on_duplicate: If True, update existing rows based on primary key (default: True)
            - batch_size: Number of rows to process in each batch (default: 100)
    
    Returns:
        ConvertExcelToTableResponse with:
            - success: bool
            - rows_inserted: List of primary keys for inserted rows
            - rows_updated: List of primary keys for updated rows
            - rows_skipped: Number of rows skipped due to errors
            - total_rows: Total number of rows processed
            - errors: List of errors with row numbers
            - error: Error message if operation failed
            - list_id: ID of the created list (if list_config provided)
            - rows_linked: Number of users linked to the list (if list_config provided)
    """
    temp_file_path = None

    print(f"convert_excel_to_table request")
    try:
        # Decode base64 file content
        try:
            file_content = base64.b64decode(request.file_content_base64)
        except Exception as e:
            print(f"✗ Error converting Excel to table: {str(e)}", file=sys.stderr)
            return ConvertExcelToTableResponse(
                success=False,
                error=f"Invalid base64 file content: {str(e)}",
                error_type="ValueError"
            )
        
        # Save decoded file temporarily
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx')
        temp_file.write(file_content)
        temp_file.close()
        temp_file_path = temp_file.name

        print(f"temp_file_path: {temp_file_path}")
        
        # Check if we need to create a list (3-step process)
        if request.list_config:
            if not isinstance(request.list_config, dict):
                return ConvertExcelToTableResponse(
                    success=False,
                    error="list_config must be a dictionary with 'type' and 'list_name' keys",
                    error_type="ValueError"
                )
            
            list_type = request.list_config.get('type')
            list_name = request.list_config.get('list_name')
            
            if not list_type:
                return ConvertExcelToTableResponse(
                    success=False,
                    error="list_config must contain 'type' key (matches data_base_tables key)",
                    error_type="ValueError"
                )
            
            if not list_name:
                return ConvertExcelToTableResponse(
                    success=False,
                    error="list_config must contain 'list_name' key",
                    error_type="ValueError"
                )
            
            # Get database connection and list manager
            from common_utils.list_manager import ListManager
            db_connection = _get_db_connection()
            config_manager = _get_config()
            list_manager = ListManager(list_type, db_connection, config_manager)
            
            # Execute 3-step process
            result = list_manager.import_excel_and_create_list(
                file_path=temp_file_path,
                list_name=list_name,
                sheet_name=request.sheet_name,
                mapping=request.column_mapping,
                header_row=request.header_row,
                start_row=request.start_row,
                update_on_duplicate=request.update_on_duplicate,
                batch_size=request.batch_size,
                column_converters=request.column_converters
            )
            
            print(f"result: {result}")
            
            # Return result with list information
            return ConvertExcelToTableResponse(
                success=result.get('success', False),
                rows_inserted=result.get('rows_inserted', []),
                rows_updated=result.get('rows_updated', []),
                rows_skipped=result.get('step1_result', {}).get('rows_skipped', 0),
                total_rows=result.get('total_rows', 0),
                errors=result.get('errors', []),
                error=None,
                error_type=None,
                list_id=result.get('list_id'),
                rows_linked=result.get('rows_linked', 0)
            )
        else:
            # Standard 1-step process (just import Excel)
            excel_handler = _get_excel_handler()
            
            # Convert Excel to MySQL using request model
            result = excel_handler.excel_to_mysql(
                file_path=temp_file_path,
                table_name=request.table_name,
                sheet_name=request.sheet_name,
                mapping=request.column_mapping,
                header_row=request.header_row,
                start_row=request.start_row,
                update_on_duplicate=request.update_on_duplicate,
                batch_size=request.batch_size,
                column_converters=request.column_converters
            )
            
            print(f"result: {result}")
            
            # Return result
            return ConvertExcelToTableResponse(
                success=result.get('success', False),
                rows_inserted=result.get('rows_inserted', []),  # List of primary keys
                rows_updated=result.get('rows_updated', []),     # List of primary keys
                rows_skipped=result.get('rows_skipped', 0),
                total_rows=result.get('total_rows', 0),
                errors=result.get('errors', []),
                error=None,
                error_type=None
            )
        
    except ValueError as e:
        print(f"✗ Error converting Excel to table: {str(e)}", file=sys.stderr)
        return ConvertExcelToTableResponse(
            success=False,
            error=str(e),
            error_type="ValueError"
        )
    except Exception as e:
        print(f"✗ Error converting Excel to table: {str(e)}", file=sys.stderr)
        error_type = type(e).__name__
        error_msg = str(e)
        print(f"✗ Error converting Excel to table: {error_msg}", file=sys.stderr)
        return ConvertExcelToTableResponse(
            success=False,
            error=error_msg,
            error_type=error_type
        )
    finally:
        # Clean up temporary file
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                os.unlink(temp_file_path)
            except Exception as e:
                print(f"⚠️  Warning: Could not delete temp file {temp_file_path}: {e}", file=sys.stderr)


@router.get("/lists", response_model=GetListsResponse)
async def get_all_lists(
    list_type: str = Query(..., description="Type key that matches data_base_tables configuration (e.g., 'special_users')")
):
    """
    Get all lists with their associated users for a given list type.
    
    Args:
        list_type: Type key that matches data_base_tables configuration
        
    Returns:
        GetListsResponse with:
            - success: bool
            - lists: Array of list objects, each containing:
                - id: int
                - list_name: str
                - is_active: int
                - created_at: str (timestamp)
                - time_activate_modify: str (timestamp)
                - users: Array of user dictionaries with all user fields
            - error: Error message if operation failed
    """
    return await get_lists_endpoint(list_type, _get_db_connection, _get_config)


@router.post("/lists/edit", response_model=EditListResponse)
async def edit_list(
    request: EditListRequest,
    list_type: str = Query(..., description="Type key that matches data_base_tables configuration (e.g., 'special_users')")
):
    """
    Edit a list: update name, activate/deactivate, add/remove users.
    
    Args:
        request: EditListRequest with:
            - list_id: ID of the list to edit (required)
            - list_name: Optional new name for the list
            - is_active: Optional 1 to activate, 0 to deactivate
            - add_users: Optional array of user IDs to add to the list
            - remove_users: Optional array of user IDs to remove from the list
        list_type: Type key that matches data_base_tables configuration
        
    Returns:
        EditListResponse with:
            - success: bool
            - list_id: int
            - list_name: Updated list name (if changed)
            - is_active: Updated is_active value (if changed)
            - users_added: Number of users added
            - users_removed: Number of users removed
            - error: Error message if operation failed
    """
    return await edit_list_endpoint(request, list_type, _get_db_connection, _get_config)


@router.get("/items", response_model=GetItemsResponse)
async def get_items(
    item_type: str = Query(..., description="Type key that matches data_base_tables configuration"),
    include_foreign: bool = Query(False, description="Include referenced rows for foreign keys (as <column>_obj)")
):
    """
    Get all items from a configured table.

    Args:
        item_type: Type key that matches data_base_tables configuration
        include_foreign: Whether to hydrate foreign key fields with referenced rows

    Returns:
        GetItemsResponse with:
            - success: bool
            - items: Array of rows (optionally enriched with foreign rows)
            - error: Error message if operation failed
            - error_type: Type of error if operation failed
    """
    return await get_items_endpoint(item_type, include_foreign, _get_db_connection, _get_config)


@router.post("/add-item", response_model=AddItemResponse)
async def add_item(request: AddItemRequest):
    """
    Add an item to a database table.
    
    Validates that all mandatory fields are provided and inserts the item.
    The item_type maps to a table_name via the data_base_tables configuration.
    
    Args:
        request: AddItemRequest with:
            - item_type: Type key that matches data_base_tables configuration
            - field_values: Dictionary mapping field names to values
                          e.g., {"name": "John", "email": "john@example.com"}
        
    Returns:
        AddItemResponse with:
            - success: bool
            - item_id: Primary key value of inserted item (if available)
            - error: Error message if operation failed
            - error_type: Type of error if operation failed
    """
    return await add_item_endpoint(request, _get_db_connection, _get_config)


@router.post("/update-item", response_model=UpdateItemResponse)
async def update_item(request: UpdateItemRequest):
    """
    Update one or more rows in a database table (generic update).

    - Table is derived from request.item_type via data_base_tables config.
    - Rows are selected by exact-match filters in `where` (ANDed).
    - Columns updated are taken from `field_values`.
    """
    return await update_item_endpoint(request, _get_db_connection, _get_config)


@router.post("/remove-item", response_model=RemoveItemResponse)
async def remove_item(request: RemoveItemRequest):
    """
    Remove (delete) one or more items from a database table.
    
    Validates that either `where` is provided or `item_id` is provided.
    The item_type maps to a table_name via the data_base_tables configuration.
    
    Default behavior: If `where` is empty and `item_id` is provided, automatically
    uses the primary key with `item_id` value for the WHERE clause.
    
    Args:
        request: RemoveItemRequest with:
            - item_type: Type key that matches data_base_tables configuration
            - where: Dictionary mapping identifying fields to values (e.g., {"id": 123}).
                     If empty and item_id is provided, will use primary key automatically.
            - item_id: Optional primary key value to use when where is empty
        
    Returns:
        RemoveItemResponse with:
            - success: bool
            - rows_affected: Number of rows deleted
            - error: Error message if operation failed
            - error_type: Type of error if operation failed
    """
    return await remove_item_endpoint(request, _get_db_connection, _get_config)











