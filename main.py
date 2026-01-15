"""
FastAPI Web Service for Auto Dialer

This service exposes 3 APIs:
1. POST /api/create-filter - Create filter file
2. POST /api/import-customers - Import customers
3. GET/POST /api/modify-settings - Modify settings
"""

import sys
import os
import base64
import tempfile
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List
from fastapi import FastAPI
from pydantic import BaseModel

# Import local modules
from config_manager import ConfigManager, update_customers_sheet_config
from filter_file import create_filter_google_manager
from customers_file import create_customers_google_manager
from paycall_utils import get_paycall_data

app = FastAPI(title="Auto Dialer Web Service", version="1.0.0")


# Request/Response Models
class CreateFilterRequest(BaseModel):
    caller_id: str
    start_date: str  # Format: "dd-mm-YYYY HH:MM:SS"
    end_date: str    # Format: "dd-mm-YYYY HH:MM:SS"
    customers_input_file: Optional[str] = None  # Base64 encoded file content (optional)


class CreateFilterResponse(BaseModel):
    success: bool
    data: Optional[List[str]] = None
    google_sheet_id: Optional[str] = None
    error: Optional[str] = None
    error_type: Optional[str] = None


class ImportCustomersResponse(BaseModel):
    success: bool
    output_path: Optional[str] = None
    error: Optional[str] = None
    error_type: Optional[str] = None


class SheetConfig(BaseModel):
    wb_id: str
    sheet_name: Optional[str] = None
    asterix_column_letter: str
    filter_column_letter: Optional[str] = None  # Only for sheet_2


class ModifySettingsRequest(BaseModel):
    sheets_config: Dict[str, SheetConfig]  # Dictionary where keys are sheet names ("sheet_1", "sheet_2") and values are SheetConfig


class ModifySettingsResponse(BaseModel):
    success: bool
    updated_sheets: Optional[List[str]] = None
    sheets_config: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    error_type: Optional[str] = None


@app.get("/")
async def root():
    """Health check endpoint."""
    return {"status": "ok", "service": "Auto Dialer Web Service"}


@app.post("/api/create-filter", response_model=CreateFilterResponse)
async def create_filter(request: CreateFilterRequest):
    """
    Create filter file from imported customers and call data.
    
    This endpoint:
    1. Fetches call data from PayCall API
    2. Creates a filtered Excel workbook
    3. Uploads it to Google Drive
    4. Returns the list of missing customers
    """
    temp_file_path = None
    try:
        # Parse dates
        start_date = datetime.strptime(request.start_date, "%d-%m-%Y %H:%M:%S")
        end_date = datetime.strptime(request.end_date, "%d-%m-%Y %H:%M:%S")
        
        # Handle file content if provided
        customers_input_file_path = None
        if request.customers_input_file:
            # Decode base64 content
            file_content = base64.b64decode(request.customers_input_file)
            
            # Create temporary file
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx')
            temp_file.write(file_content)
            temp_file.close()
            
            temp_file_path = temp_file.name
            customers_input_file_path = temp_file_path
        
        # Get call data
        calls = get_paycall_data(
            caller_id=request.caller_id,
            start_date=start_date,
            end_date=end_date
        )
        
        # Create filter
        filter_google_manager = create_filter_google_manager()
        google_sheet_ids = filter_google_manager.run(
            calls=calls,
            customers_input_file=customers_input_file_path,
            caller_id=request.caller_id
        )
        
        if google_sheet_ids is None or len(google_sheet_ids) == 0:
            raise ValueError("Google sheet ID is not found")
        
        google_sheet_id = google_sheet_ids[0]
        
        # Get missing customers
        missing_customers = filter_google_manager.get_list_of_missing_customers(google_sheet_id)
        
        return CreateFilterResponse(
            success=True,
            data=missing_customers,
            google_sheet_id=google_sheet_id
        )
        
    except Exception as e:
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
                pass  # Ignore errors during cleanup


@app.get("/api/import-customers", response_model=ImportCustomersResponse)
async def import_customers():
    """
    Import customers from Google Sheets.
    
    This endpoint:
    1. Fetches customers from configured Google Sheets
    2. Filters and merges the data
    3. Generates Excel files
    4. Uploads to Google Drive
    5. Returns the output file path
    """
    try:
        customers_file = create_customers_google_manager()
        customers_file.run()
        
        output_path = customers_file.get_excel_output_file_path('auto_dialer')
        
        if not output_path:
            raise ValueError("Output path for auto dialer not found")
        
        return ImportCustomersResponse(
            success=True,
            output_path=str(Path(output_path).resolve())
        )
        
    except Exception as e:
        return ImportCustomersResponse(
            success=False,
            error=str(e),
            error_type=type(e).__name__
        )


@app.get("/api/modify-settings", response_model=ModifySettingsResponse)
async def get_settings(sheets: Optional[str] = None):
    """
    Get current settings configuration.
    
    Args:
        sheets: Comma-separated list of sheet names (e.g., "sheet_1" or "sheet_1,sheet_2" or "all")
    """
    try:
        manager = ConfigManager()
        
        if sheets is None or sheets.lower() == 'all':
            sheet_names = ['sheet_1', 'sheet_2']
        else:
            sheet_names = [s.strip() for s in sheets.split(',')]
        
        sheets_config = manager.get_customers_input_sheets(sheet_names)
        
        return ModifySettingsResponse(
            success=True,
            sheets_config=sheets_config
        )
        
    except Exception as e:
        return ModifySettingsResponse(
            success=False,
            error=str(e),
            error_type=type(e).__name__
        )


@app.post("/api/modify-settings", response_model=ModifySettingsResponse)
async def modify_settings(request: ModifySettingsRequest):
    """
    Modify settings configuration.
    
    Updates the configuration for one or more sheets (sheet_1 and/or sheet_2).
    """
    try:
        # Extract sheets_config from request and convert nested Pydantic models to dict
        sheets_config_dict = request.dict(exclude_none=True)["sheets_config"]

        print(sheets_config_dict)
        
        # Update configuration
        update_customers_sheet_config(sheets_config_dict)

        return ModifySettingsResponse(
            success=True,
            updated_sheets=list(sheets_config_dict.keys())
        )

    except Exception as e:
        print(e)
        return ModifySettingsResponse(
            success=False,
            error=str(e),
            error_type=type(e).__name__
        )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

