"""
Base class for updating existing Google Sheets.

This module provides the abstract base class for updating spreadsheets that already exist,
as opposed to creating new Excel files and uploading them.
"""

import sys
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Set

NAME_TO_DISPLAY_NAME_DICT = {
    'gaps_sheet_archive': 'ארכיון פערים',
    'gaps_sheet_runs': 'פערים'
}

class BaseSpreadsheetUpdater(ABC):
    """
    Abstract base class for updating existing Google Sheets.
    
    This class provides common functionality for finding empty rows, executing
    batch updates, and managing spreadsheet operations. Subclasses implement
    specific data preparation logic.
    """
    
    def __init__(self, drive_service, spreadsheet_config: Dict[str, Any], name: str):
        """
        Initialize the spreadsheet updater.
        
        Args:
            drive_service: GDriveService instance for API calls
            spreadsheet_config: Dictionary containing a single spreadsheet configuration.
                               Format: {wb_id: str, sheet_id: int, ...}
        
        Raises:
            ValueError: If spreadsheet_config is invalid or missing required fields
        """

        
        self.drive_service = drive_service
        self.name = name
        self.display_name = NAME_TO_DISPLAY_NAME_DICT.get(name, name)
        
        # Validate spreadsheet_config structure
        if not spreadsheet_config:
            raise ValueError("spreadsheet_config cannot be empty")
        
        if not isinstance(spreadsheet_config, dict):
            raise ValueError(f"spreadsheet_config must be a dictionary, got {type(spreadsheet_config)}")
        
        # Validate required fields
        wb_id = spreadsheet_config.get('wb_id')
        sheet_id = spreadsheet_config.get('sheet_id')
        
        if not wb_id or sheet_id is None:
            raise ValueError("spreadsheet_config missing required fields 'wb_id' or 'sheet_id'")
        
        self.spreadsheet_config = spreadsheet_config
    
    @abstractmethod
    def update_spreadsheets(self, **kwargs) -> Dict[str, Any]:
        """
        Update the spreadsheet configured in spreadsheet_config.
        
        This method should be implemented by subclasses to define how
        the spreadsheet is updated based on the specific use case.
        
        Args:
            **kwargs: Arguments specific to the implementation
        
        Returns:
            Dictionary with summary information (format depends on implementation)
        """
        pass

    def get_display_name_to_link_dict(self) -> Dict[str, str]:
        wb_id = self.spreadsheet_config.get('wb_id')
        sheet_id = self.spreadsheet_config.get('sheet_id')
        if not wb_id or sheet_id is None:
            raise ValueError("Missing wb_id or sheet_id in spreadsheet_config")
        
        sheet_link = 'https://docs.google.com/spreadsheets/d/' + wb_id + '/edit#gid=' + str(sheet_id)    
        return {self.display_name: sheet_link}

    def get_spreadsheet_config(self) -> Dict[str, Any]:
        return self.spreadsheet_config

    def _find_first_empty_row(self, sheet_name: str, column: str = 'A') -> int:
        """
        Find the first empty row in the specified column.
        
        Args:
            sheet_name: Sheet name (string)
            column: Column letter to check (default: 'A')
        
        Returns:
            Row number (1-based) of the first empty row
        """
        # Get wb_id and sheet_id from self.spreadsheet_config
        wb_id = self.spreadsheet_config.get('wb_id')
        sheet_id = self.spreadsheet_config.get('sheet_id')
        
        if not wb_id or sheet_id is None:
            raise ValueError("Missing wb_id or sheet_id in spreadsheet_config")
        
        try:
            # Read column to find the first empty row
            # Start from row 1 and read in chunks
            chunk_size = 1000
            current_row = 1
            
            while current_row < 10000:  # Safety limit
                range_name = f"'{sheet_name}'!{column}{current_row}:{column}{current_row + chunk_size - 1}"
                
                result = self.drive_service.sheets_service.spreadsheets().values().get(
                    spreadsheetId=wb_id,
                    range=range_name
                ).execute()
                
                values = result.get('values', [])
                
                # If we got fewer values than requested, we've reached the end
                if len(values) < chunk_size:
                    # Find first empty in this chunk
                    for i, row in enumerate(values):
                        if not row or (len(row) == 0) or (len(row) > 0 and not str(row[0]).strip()):
                            return current_row + i
                    # All rows in chunk are filled, return next row
                    return current_row + len(values)
                
                # Check if any row in this chunk is empty
                for i, row in enumerate(values):
                    if not row or (len(row) == 0) or (len(row) > 0 and not str(row[0]).strip()):
                        return current_row + i
                
                # All rows in chunk are filled, move to next chunk
                current_row += chunk_size
            
            # If we've reached here, return a safe default
            print(f"⚠️  Reached safety limit while finding empty row, returning {current_row}", file=sys.stderr)
            return current_row
            
        except Exception as e:
            print(f"⚠️  Error finding first empty row: {e}", file=sys.stderr)
            raise
    
    def _get_sheet_name_from_id(self) -> str:
        """
        Get the sheet name from self.spreadsheet_config.
        
        Returns:
            Sheet name (string)
        
        Raises:
            ValueError: If sheet not found
        """
        wb_id = self.spreadsheet_config.get('wb_id')
        sheet_id = self.spreadsheet_config.get('sheet_id')
        
        if not wb_id or sheet_id is None:
            raise ValueError("Missing wb_id or sheet_id in spreadsheet_config")
        
        try:
            spreadsheet = self.drive_service.sheets_service.spreadsheets().get(
                spreadsheetId=wb_id
            ).execute()
            
            for sheet in spreadsheet.get('sheets', []):
                if sheet['properties']['sheetId'] == sheet_id:
                    return sheet['properties']['title']
            
            raise ValueError(f"Sheet with ID {sheet_id} not found in spreadsheet {wb_id}")
        except Exception as e:
            print(f"⚠️  Error getting sheet name from ID: {e}", file=sys.stderr)
            raise
    
    def _get_column_letter_offset(self, start_column: str, offset: int) -> str:
        """
        Get column letter offset from start column.
        
        Args:
            start_column: Starting column letter (e.g., 'A', 'C', 'Z')
            offset: Number of columns to offset (e.g., 3 means start + 3 columns)
        
        Returns:
            Column letter after offset (e.g., 'C' + 3 = 'F')
        """
        def column_to_number(col: str) -> int:
            result = 0
            for char in col.upper():
                result = result * 26 + (ord(char) - ord('A') + 1)
            return result
        
        def number_to_column(num: int) -> str:
            result = ""
            while num > 0:
                num -= 1
                result = chr(ord('A') + (num % 26)) + result
                num //= 26
            return result
        
        start_num = column_to_number(start_column)
        end_num = start_num + offset
        return number_to_column(end_num)
    
    def _execute_batch_update(self, updates: List[Dict[str, Any]]) -> None:
        """
        Execute batch update to Google Sheets.
        
        Args:
            updates: List of update dictionaries with 'range' and 'values' keys
        
        Raises:
            RuntimeError: If batch update fails
        """
        if not updates:
            return
        
        # Get wb_id from self.spreadsheet_config
        wb_id = self.spreadsheet_config.get('wb_id')
        if not wb_id:
            raise ValueError("Missing wb_id in spreadsheet_config")
        
        body = {
            'valueInputOption': 'USER_ENTERED',
            'data': updates
        }
        
        try:
            self.drive_service.sheets_service.spreadsheets().values().batchUpdate(
                spreadsheetId=wb_id,
                body=body
            ).execute()
        except Exception as e:
            print(f"⚠️  Error executing batch update: {e}", file=sys.stderr)
            raise RuntimeError(f"Failed to execute batch update: {e}") from e
    

