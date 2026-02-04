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
        
        If space_row is True:
            - Returns the first empty row if there is one empty line after the last row
            - Unless the first empty line is 2, then returns 2
        Else:
            - Returns the first empty line
        
        Args:
            sheet_name: Sheet name (string)
            column: Column letter to check (default: 'A')
        
        Returns:
            Row number (1-based) of the first empty row
        """
        # Get wb_id and sheet_id from self.spreadsheet_config
        wb_id = self.spreadsheet_config.get('wb_id')
        sheet_id = self.spreadsheet_config.get('sheet_id')
        space_row = self.spreadsheet_config.get('space_row', False)
        
        if not wb_id or sheet_id is None:
            raise ValueError("Missing wb_id or sheet_id in spreadsheet_config")
        
        try:
            # Read column to find empty rows
            # Start from row 1 and read in chunks
            chunk_size = 1000
            current_row = 1
            first_empty_row = None
            last_non_empty_row = 0
            
            while current_row < 10000:  # Safety limit
                range_name = f"'{sheet_name}'!{column}{current_row}:{column}{current_row + chunk_size - 1}"
                
                result = self.drive_service.sheets_service.spreadsheets().values().get(
                    spreadsheetId=wb_id,
                    range=range_name
                ).execute()
                
                values = result.get('values', [])
                
                # Process rows in this chunk
                for i, row in enumerate(values):
                    row_num = current_row + i
                    is_empty = not row or (len(row) == 0) or (len(row) > 0 and not str(row[0]).strip())
                    
                    if is_empty:
                        # Found first empty row
                        if first_empty_row is None:
                            first_empty_row = row_num
                    else:
                        # This row has data, update last non-empty row
                        last_non_empty_row = row_num
                
                # If we got fewer values than requested, we've reached the end
                if len(values) < chunk_size:
                    # If we haven't found first empty row yet, it's after the last row
                    if first_empty_row is None:
                        first_empty_row = current_row + len(values)
                    break
                
                # If we found first empty row and space_row is False, we can return early
                if not space_row and first_empty_row is not None:
                    return first_empty_row
                
                # Move to next chunk
                current_row += chunk_size
            
            # If we haven't found any empty row, return after the last row
            if first_empty_row is None:
                first_empty_row = current_row
            
            # Apply space_row logic
            if space_row:
                # Check if first empty row is 2
                if first_empty_row == 2:
                    return 2
                
                # Check if there is one empty line after the last row
                # The row after last_non_empty_row should be empty
                if first_empty_row == last_non_empty_row + 1:
                    return first_empty_row
                else:
                    # No empty line after last row, return first empty row anyway
                    return first_empty_row
            else:
                # space_row is False, return first empty row
                return first_empty_row
            
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

    def _insert_rows(self, start_row: int, num_rows: int) -> None:
        """
        Insert empty rows into the sheet (shifts existing rows down).

        Args:
            start_row: 1-based row number where new rows should be inserted
            num_rows: Number of rows to insert
        """
        if num_rows <= 0:
            return

        wb_id = self.spreadsheet_config.get('wb_id')
        sheet_id = self.spreadsheet_config.get('sheet_id')
        if not wb_id or sheet_id is None:
            raise ValueError("Missing wb_id or sheet_id in spreadsheet_config")

        # Optional: copy formulas from a template row into newly inserted rows.
        # This is useful when row 2 contains formulas that should apply to all data rows.
        copy_formulas_on_insert = bool(self.spreadsheet_config.get('copy_formulas_on_insert', False))
        template_row_1based = int(self.spreadsheet_config.get('formula_template_row', 2))
        copy_start_col = int(self.spreadsheet_config.get('formula_copy_start_col', 0))
        copy_end_col = self.spreadsheet_config.get('formula_copy_end_col', None)  # exclusive, 0-based

        # Google Sheets API uses 0-based indices; endIndex is exclusive
        start_index = max(0, start_row - 1)
        end_index = start_index + num_rows

        requests: List[Dict[str, Any]] = [
            {
                "insertDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": start_index,
                        "endIndex": end_index,
                    },
                    # Inherit formatting from the row AFTER the insertion point
                    # (e.g., inserting at row 2 should inherit from the existing data row 2,
                    # not from the header row 1).
                    "inheritFromBefore": False,
                }
            }
        ]

        if copy_formulas_on_insert:
            # Resolve end column: if not provided, use the sheet grid column count.
            end_col_exclusive: Optional[int]
            if copy_end_col is None:
                try:
                    spreadsheet = self.drive_service.sheets_service.spreadsheets().get(
                        spreadsheetId=wb_id,
                        fields="sheets(properties(sheetId,gridProperties(columnCount)))"
                    ).execute()
                    end_col_exclusive = None
                    for sheet in spreadsheet.get("sheets", []):
                        props = sheet.get("properties", {})
                        if props.get("sheetId") == sheet_id:
                            end_col_exclusive = props.get("gridProperties", {}).get("columnCount")
                            break
                    if end_col_exclusive is None:
                        # Fallback: copy a reasonable range
                        end_col_exclusive = 26
                except Exception as e:
                    print(f"⚠️  Could not read sheet columnCount for formula copy: {e}", file=sys.stderr)
                    end_col_exclusive = 26
            else:
                end_col_exclusive = int(copy_end_col)

            # If we insert rows before (or at) the template row, the template row shifts down.
            template_row_index = max(0, template_row_1based - 1)
            source_row_index = template_row_index + (num_rows if start_row <= template_row_1based else 0)

            # Copy formulas from the template row into the newly inserted rows
            requests.append(
                {
                    "copyPaste": {
                        "source": {
                            "sheetId": sheet_id,
                            "startRowIndex": source_row_index,
                            "endRowIndex": source_row_index + 1,
                            "startColumnIndex": max(0, copy_start_col),
                            "endColumnIndex": max(0, end_col_exclusive),
                        },
                        "destination": {
                            "sheetId": sheet_id,
                            "startRowIndex": start_index,
                            "endRowIndex": end_index,
                            "startColumnIndex": max(0, copy_start_col),
                            "endColumnIndex": max(0, end_col_exclusive),
                        },
                        "pasteType": "PASTE_FORMULA",
                        "pasteOrientation": "NORMAL",
                    }
                }
            )

        body = {"requests": requests}

        try:
            self.drive_service.sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=wb_id,
                body=body
            ).execute()
        except Exception as e:
            print(f"⚠️  Error inserting rows: {e}", file=sys.stderr)
            raise
    
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
    

