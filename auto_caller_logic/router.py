import os
import sys
import json
import base64
import tempfile
import sqlite3
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List
from urllib.parse import unquote

from fastapi import APIRouter, Query
from pydantic import BaseModel
from typing import Optional

from .config import _get_default_config
from .filter_file import create_filter_google_manager
from .customers_file import create_customers_google_manager
from .gaps_actions_file import create_gaps_actions_google_manager
from .paycall_utils import get_paycall_data
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
from common_utils.db_connection import DatabaseConnection
from common_utils.config_manager import ConfigManager

router = APIRouter(prefix="/api/auto_caller")

# Request/Response Models
class CreateFilterRequest(BaseModel):
    caller_id: str
    start_date: str  # Format: "dd-mm-YYYY HH:MM:SS"
    end_date: str    # Format: "dd-mm-YYYY HH:MM:SS"
    customers_input_file: Optional[str] = None  # Base64 encoded file content (optional)
    customers_input_file_name: Optional[str] = None


class CreateFilterResponse(BaseModel):
    success: bool
    data: Optional[List[str]] = None
    google_spreadsheet_links: Optional[List[Dict[str, Any]]] = None
    error: Optional[str] = None
    error_type: Optional[str] = None
    excel_buffer: Optional[str] = None
    file_name: Optional[str] = None
    summarize_data: Optional[Dict[str, Any]] = None


class ImportCustomersResponse(BaseModel):
    success: bool
    excel_buffer: Optional[str] = None
    file_name: Optional[str] = None
    error: Optional[str] = None
    error_type: Optional[str] = None


class SheetConfig(BaseModel):
    wb_id: str
    sheet_name: Optional[str] = None
    asterix_column_letter: str
    filter_column_letter: Optional[str] = None  # Only for sheet_2


class GetSettingsResponse(BaseModel):
    success: bool
    config_settings: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    error_type: Optional[str] = None

class ModifySettingsRequest(BaseModel):
    customers_sheets_config: Optional[Dict[str, Dict[str, Any]]] = None  # Dictionary where keys are sheet names ("sheet_1", "sheet_2") and values are sheet configs (can include "url" key)
    filter_sheets_config: Optional[Dict[str, Dict[str, Any]]] = None  # Dictionary where keys are sheet names ("allowed_gaps_sheet", "gaps_sheet") and values are sheet configs
    main_google_folder_id: Optional[str] = None  # Main Google folder ID to update
    gaps_sheet_config: Optional[Dict[str, Any]] = None  # Gaps sheet configuration to update

class ModifySettingsResponse(BaseModel):
    success: bool
    updated_items: Optional[List[str]] = None  # List of updated item names (e.g., ["customers_sheets_config", "main_google_folder_id"])
    config_settings: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    error_type: Optional[str] = None


class EnterGapsRequest(BaseModel):
    """Request model for entering callers into gaps sheets."""
    callers_gap: List[str]  # List of caller gap values to insert
    customers_file_name: str  # Customers input file name
    nick_name: str = None # Nick name
    caller_id: str  # Caller ID
    date_str: str  # Format: "dd-mm-YYYY HH:MM:SS"
    time_str: str    # Format: "dd-mm-YYYY HH:MM:SS"


class EnterGapsResponse(BaseModel):
    """Response model for entering callers into gaps sheets."""
    success: bool
    global_gap_sheet_config: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    error_type: Optional[str] = None


# Item and List models imported from common_utils.item_endpoints


# Module-specific instances (cached per module)
_db_connection: Optional[DatabaseConnection] = None
_config_manager: Optional[ConfigManager] = None

def _get_db_connection():
    """Get or create database connection instance for this module."""
    global _db_connection
    
    if _db_connection is not None and _db_connection.is_connected():
        return _db_connection
    
    project_root = Path(__file__).parent.parent
    print(f"project root: {project_root}", file=sys.stderr)
    _db_connection = get_db_connection(
        env_config_var='MAIN_CONFIG_PATH',
        fallback_paths=[
            str(project_root / "config.yaml"),
        ]
    )
    return _db_connection

def _get_config():
    """Get or create config manager instance for this module."""
    global _config_manager
    
    project_root = Path(__file__).parent.parent
    _config_manager = get_config(
        env_config_var='AUTO_CALLER_CONFIG_PATH',
        config_path=str(Path(__file__).parent / "config.yaml"),
        fallback_paths=[
            str(project_root / "config_server.yaml"),
            str(project_root / "settings_backend" / "config.yaml"),
        ]
    )
    return _config_manager


def _get_caller_nick_name(phone_number: str) -> Optional[str]:
    """
    Get nick_name from auto_calls_callers table by phone_number.
    
    Args:
        phone_number: Phone number (caller_id) to look up
        
    Returns:
        nick_name if found, None otherwise
    """
    try:
        db_connection = _get_db_connection()
        
        # Query the database
        query = "SELECT nick_name FROM auto_calls_callers WHERE phone_number = :phone_number LIMIT 1"
        results = db_connection.execute_query(query, {'phone_number': phone_number})
        
        if results and len(results) > 0:
            nick_name = results[0].get('nick_name')
            if nick_name:
                print(f"📞 Found nick_name '{nick_name}' for phone_number '{phone_number}'", file=sys.stderr)
                return nick_name
            else:
                print(f"📞 No nick_name found for phone_number '{phone_number}' (nick_name is NULL)", file=sys.stderr)
                return None
        else:
            print(f"📞 No record found for phone_number '{phone_number}'", file=sys.stderr)
            return None
            
    except Exception as e:
        print(f"⚠️  Error fetching nick_name from database: {e}", file=sys.stderr)
        # Don't raise - return None to fall back to caller_id
        return None


# Simple SQLite-backed counter store
COUNTER_DB_PATH = Path(__file__).parent / "counters.db"
COUNTER_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS counters (
    name TEXT PRIMARY KEY,
    value INTEGER NOT NULL
)
"""


def _init_counter_db():
    conn = sqlite3.connect(COUNTER_DB_PATH)
    try:
        with conn:
            conn.execute(COUNTER_TABLE_SQL)
    finally:
        conn.close()


def _increment_counter(name: str) -> int:
    _init_counter_db()
    conn = sqlite3.connect(COUNTER_DB_PATH)
    try:
        with conn:
            cursor = conn.execute(
                "INSERT INTO counters(name, value) VALUES(?, 1) "
                "ON CONFLICT(name) DO UPDATE SET value = value + 1 RETURNING value",
                (name,)
            )
            row = cursor.fetchone()
            return int(row[0]) if row else 1
    finally:
        conn.close()


@router.get("/")
async def root():
    """Health check endpoint."""
    return {"status": "ok", "service": "Auto Dialer Web Service"}


@router.post("/create-filter", response_model=CreateFilterResponse)
async def create_filter(request: CreateFilterRequest):
    """
    Create filter file from imported customers and call data.
    """
    temp_file_path = None
    try:
        # Parse dates
        start_date = datetime.strptime(request.start_date, "%d-%m-%Y %H:%M:%S")
        end_date = datetime.strptime(request.end_date, "%d-%m-%Y %H:%M:%S")

        # Handle file content if provided
        customers_input_file = None
        if request.customers_input_file:
            file_content = base64.b64decode(request.customers_input_file)
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx')
            temp_file.write(file_content)
            temp_file.close()
            temp_file_path = temp_file.name
            customers_input_file_path = temp_file_path
            customers_input_file = { "file_name": request.customers_input_file_name, "file_path": customers_input_file_path }

        config_manager = _get_config()

        # Get call data
        calls = get_paycall_data(
            config_manager=config_manager,
            caller_id=request.caller_id,
            start_date=start_date,
            end_date=end_date
        )

        nick_name = _get_caller_nick_name(request.caller_id)

        # Create filter
        config_manager = _get_config()
        filter_google_manager = create_filter_google_manager(config_manager)
        process_result = filter_google_manager.run(
            calls=calls,
            customers_input_file= customers_input_file,
            caller_id=request.caller_id,
            nick_name=nick_name
        )

        filter_excel_info = process_result['filter']

        if filter_excel_info is None or filter_excel_info['file_id'] is None or len(filter_excel_info['file_id']) == 0:
            raise ValueError("Google sheet ID is not found")

        google_sheet_id = filter_excel_info['file_id']


        excel_buffer = process_result['callers_gap']['post_excel_buffer']
        file_name = process_result['callers_gap']['file_name']
        
        # Convert BytesIO to base64 string for JSON serialization
        excel_buffer.seek(0)
        excel_base64 = base64.b64encode(excel_buffer.getvalue()).decode('utf-8')

        # Get missing customers
        post_data = filter_google_manager.get_post_data()

        if post_data is None or "callers_gap" not in post_data:
            raise ValueError("Missing customers is not found")

        missing_customers = post_data['callers_gap']

        globals_links = filter_google_manager.get_global_gap_sheet_config()

        print(f"Globals links: {globals_links}", file=sys.stderr)

        summarize_data = filter_google_manager.get_generated_data()

        print(f"Missing customers: {missing_customers}", file=sys.stderr)

        print(f"Summarize data: {summarize_data}", file=sys.stderr)

        return CreateFilterResponse(
            success=True,
            data=missing_customers,
            google_spreadsheet_links=globals_links,
            excel_buffer=excel_base64,
            file_name=file_name,
            summarize_data=summarize_data
        )

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        print(f"Error type: {type(e).__name__}", file=sys.stderr)
        return CreateFilterResponse(
            success=False,
            error=str(e),
            error_type=type(e).__name__
        )
    finally:
        # Clean up temporary file if created
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                os.unlink(temp_file_path)
            except Exception:
                pass


@router.post("/enter-gaps", response_model=EnterGapsResponse)
async def enter_gaps(request: EnterGapsRequest):
    """
    Enter callers into gaps sheets from POST request.
    """
    try:
        config_manager = _get_config()
        gaps_actions_manager = create_gaps_actions_google_manager(config_manager)
        
        # Run the process with callers_gap and metadata
        process_result = gaps_actions_manager.run(
            callers_gap=request.callers_gap,
            caller_id=request.caller_id,
            time_str=request.time_str,
            date_str=request.date_str,
            nick_name=request.nick_name,
            customers_input_file_name=request.customers_file_name
        )
        
        # Get post-process data
        post_data = gaps_actions_manager.get_post_data()
        
        if post_data is None:
            raise ValueError("Post-process data is not available")
        
        global_gap_sheet_config = post_data.get('global_gap_sheet_config')
        
        print(f"Entered gaps: {len(request.callers_gap)} callers", file=sys.stderr)
        
        return EnterGapsResponse(
            success=True,
            global_gap_sheet_config=global_gap_sheet_config
        )
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        print(f"Error type: {type(e).__name__}", file=sys.stderr)
        return EnterGapsResponse(
            success=False,
            error=str(e),
            error_type=type(e).__name__
        )


@router.get("/import-customers", response_model=ImportCustomersResponse)
async def import_customers():
    """
    Import customers from Google Sheets.
    """
    print(f"Importing customers", file=sys.stderr)
    try:
        config_manager = _get_config()
        customers_file = create_customers_google_manager(config_manager)
        process_result = customers_file.run()
        if process_result is None or process_result['auto_dialer'] is None:
            raise ValueError("auto dialer workbook config is not found")

        print(f"Process result: {process_result}", file=sys.stderr)

        customers = customers_file.get_generated_data().get('customers')

        number_of_customers = 0

        if customers is not None:
            number_of_customers = len(customers)

        file_name = process_result['auto_dialer']['file_name']

        print(f"File name: {file_name}", file=sys.stderr)

        excel_buffer = process_result['auto_dialer']['excel_buffer']
        
        # Convert BytesIO to base64 string for JSON serialization
        excel_buffer.seek(0)
        excel_base64 = base64.b64encode(excel_buffer.getvalue()).decode('utf-8')

        counter = _increment_counter("import_customers")

        file_name = f"{counter:02d} NUMBER {number_of_customers} {file_name}"
        print(f"Counter: {counter}", file=sys.stderr)

        return ImportCustomersResponse(
            success=True,
            excel_buffer=excel_base64,
            file_name=file_name,
        )

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return ImportCustomersResponse(
            success=False,
            error=str(e),
            error_type=type(e).__name__
        )


@router.get("/get-settings", response_model=GetSettingsResponse)
async def get_settings(settings_config: str = Query(..., description="JSON dictionary with 'sheets_config' and/or 'main_google_folder_id' keys")):
    """
    Get current settings configuration.
    
    Args:
        settings_config: JSON string dictionary. Example: '{"customers_sheets_config": "all", "main_google_folder_id": true}'
    """
    try:
        # URL decode the settings_config string first (in case it's URL-encoded)
        decoded_config = unquote(settings_config)
        
        # Parse JSON string to dictionary
        try:
            settings_config_dict = json.loads(decoded_config)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON format for settings_config: {str(e)}. Received: {settings_config[:100]}")
        
        if not settings_config_dict:
            raise ValueError("Config is not found")

        config_response = {}

        print(f"Settings config dict: {settings_config_dict}", file=sys.stderr)

        config_manager = _get_config()
        config = _get_default_config(config_manager)
        if 'customers_sheets_config' in settings_config_dict:
            customers_sheets_config_str = settings_config_dict['customers_sheets_config']

            if customers_sheets_config_str == 'all':
                customers_sheet_names = ['sheet_1', 'sheet_2']
            else:
                customers_sheet_names = [s.strip() for s in customers_sheets_config_str.split(',')]

            customers_sheets_config_res = config.get_input_user_display("customers", customers_sheet_names)
            config_response["customers_sheets_config"] = customers_sheets_config_res

        if 'filter_sheets_config' in settings_config_dict:
            filter_sheets_config_str = settings_config_dict['filter_sheets_config']

            if filter_sheets_config_str == 'all':
                filter_sheet_names = ['allowed_gaps_sheet']
            else:
                filter_sheet_names = [s.strip() for s in filter_sheets_config_str.split(',')]

            filter_sheets_config_res = config.get_input_user_display("filter", filter_sheet_names)
            config_response["filter_sheets_config"] = filter_sheets_config_res

        if 'gaps_sheet_config' in settings_config_dict:
            gaps_sheet_config_str = settings_config_dict['gaps_sheet_config']
            if gaps_sheet_config_str == 'all':
                gaps_sheet_config_names = ['gaps_sheet_archive', 'gaps_sheet_runs']
            else:
                gaps_sheet_config_names = [s.strip() for s in gaps_sheet_config_str.split(',')]

            gaps_sheet_config_filter_names = config.get_output_files_config("filter")
            
            gaps_sheet_config_gaps_actions_names = config.get_output_files_config("gaps_actions")
            config_response["gaps_sheet_config"] = gaps_sheet_config_filter_names | gaps_sheet_config_gaps_actions_names

        if "main_google_folder_id" in settings_config_dict:
            main_google_folder_id = config.get_main_google_folder_id()
            config_response["main_google_folder_id"] = main_google_folder_id
                
        return GetSettingsResponse(
            success=True,
            config_settings=config_response,
            error=None,
            error_type=None
        )
    except Exception as e:
        return GetSettingsResponse(
            success=False,
            error=str(e),
            error_type=type(e).__name__
        )


@router.post("/modify-settings", response_model=ModifySettingsResponse)
async def modify_settings(request: ModifySettingsRequest):
    """
    Modify settings configuration.
    Handles the same keys as get-settings:
    - customers_sheets_config: Update customer input sheets (sheet_1, sheet_2)
    - filter_sheets_config: Update filter input sheets (allowed_gaps_sheet, gaps_sheet)
    - main_google_folder_id: Update main Google folder ID
    - gaps_sheet_config: Update gaps sheet configuration
    """
    try:
        config_manager = _get_config()
        config = _get_default_config(config_manager)
        updated_items = []
        config_response = {}
        
        request_dict = request.dict(exclude_none=True)
        
        # Handle customers_sheets_config
        if 'customers_sheets_config' in request_dict:
            customers_sheets_config = request_dict['customers_sheets_config']
            if customers_sheets_config:
                config.update_customers_input_sheet(customers_sheets_config)
                updated_items.append('customers_sheets_config')
                config_response['customers_sheets_config'] = customers_sheets_config
        
        # Handle filter_sheets_config
        if 'filter_sheets_config' in request_dict:
            filter_sheets_config = request_dict['filter_sheets_config']
            if filter_sheets_config:
                config.update_filter_input_sheet(filter_sheets_config)
                updated_items.append('filter_sheets_config')
                config_response['filter_sheets_config'] = filter_sheets_config
        
        # Handle main_google_folder_id
        if 'main_google_folder_id' in request_dict:
            main_google_folder_id = request_dict['main_google_folder_id']
            if main_google_folder_id:
                config.update_main_google_folder_id(main_google_folder_id)
                updated_items.append('main_google_folder_id')
                config_response['main_google_folder_id'] = main_google_folder_id
        
        # Handle gaps_sheet_config
        if 'gaps_sheet_config' in request_dict:
            gaps_sheet_config = request_dict['gaps_sheet_config']
            if gaps_sheet_config:
                if "gaps_sheet_archive" in gaps_sheet_config:
                    config.update_output_files("filter", { "gaps_sheet_archive": gaps_sheet_config["gaps_sheet_archive"] })
                if "gaps_sheet_runs" in gaps_sheet_config:
                    config.update_output_files("gaps_actions", { "gaps_sheet_runs": gaps_sheet_config["gaps_sheet_runs"] })
                
                updated_items.append('gaps_sheet_config')
                config_response['gaps_sheet_config'] = gaps_sheet_config
        
        if not updated_items:
            return ModifySettingsResponse(
                success=False,
                error="No configuration items provided to update",
                error_type="ValueError"
            )

        return ModifySettingsResponse(
            success=True,
            updated_items=updated_items,
            config_settings=config_response
        )
    except Exception as e:
        return ModifySettingsResponse(
            success=False,
            error=str(e),
            error_type=type(e).__name__
        )


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
    
    Args:
        request: UpdateItemRequest with:
            - item_type: Type key that matches data_base_tables configuration
            - where: Dictionary mapping identifying fields to values (e.g., {"id": 123}).
                     If empty, uses primary key from field_values automatically.
            - field_values: Dictionary mapping fields to new values
    
    Returns:
        UpdateItemResponse with:
            - success: bool
            - rows_affected: Number of rows updated
            - error: Error message if operation failed
            - error_type: Type of error if operation failed
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





