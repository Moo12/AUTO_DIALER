"""
Gaps actions file processor for entering callers into gaps sheets.

This module contains the GapsActionsFile class that processes callers_gap
from POST requests and inserts them into configured gaps sheets.
"""

import sys
from typing import Dict, Any, Optional, List
from .google_drive_utils import BaseProcess
from .config import _get_default_config
from common_utils.config_manager import ConfigManager


class GapsActionsFile(BaseProcess):
    """Process for entering callers into gaps sheets from POST request."""

    def __init__(
        self,
        drive_service,
        config_manager: ConfigManager,
        gaps_sheet_config: Dict[str, Any]
    ):
        super().__init__(drive_service, config_manager, "gaps_actions")
        
        self.gaps_sheet_config = gaps_sheet_config
        # Store metadata for gaps sheet insertion
        self._metadata = None

        print(f"Gaps sheet config: {self.gaps_sheet_config}", file=sys.stderr)

    def generate_data(self, **kwargs):
        """
        Generate data - returns empty dict as all logic is in post_process_implementation.
        
        Args:
            **kwargs: May contain metadata (caller_id, date, time, customers_input_file, nick_name, start_date, end_date)
        """
        # Store metadata for use in post_process_implementation
        self._metadata = {
            'caller_id': kwargs.get('caller_id'),
            'date_str': kwargs.get('date_str'),
            'time_str': kwargs.get('time_str'),
            'customers_input_file_name': kwargs.get('customers_input_file_name'),
            'nick_name': kwargs.get('nick_name'),
        }
        # Store callers_gap for use in post_process
        self._callers_gap = kwargs.get('callers_gap', [])
        return {}

    def post_process_implementation(self, excel_info: Dict[str, Any]):
        """
        The specific logic to post process - insert callers_gap into gaps sheets.
        
        Args:
            excel_info: Dictionary containing information about created Excel files (unused here)
        
        Returns:
            Dictionary with callers_gap and global_gap_sheet_config
        """
        callers_gap = self._callers_gap
        print(f"Processing callers gap from POST request. len of callers gap: {len(callers_gap)}", file=sys.stderr)

        # Insert data into gaps sheet
        if self.gaps_sheet_config and callers_gap:
            self._insert_data_to_gaps_sheet(callers_gap)
        else:
            print(f"No callers gap to insert", file=sys.stderr)

        return {'callers_gap': callers_gap, 'global_gap_sheet_config': self.gaps_sheet_config}
    
    def _get_sheet_name_from_id(self, spreadsheet_id: str, sheet_id: int) -> str:
        """
        Get the sheet name for a given sheet ID.
        
        Args:
            spreadsheet_id: Google Sheets spreadsheet ID
            sheet_id: Sheet ID (integer)
            
        Returns:
            Sheet name (string)
            
        Raises:
            ValueError: If sheet not found
        """
        try:
            spreadsheet = self.drive_service.sheets_service.spreadsheets().get(
                spreadsheetId=spreadsheet_id
            ).execute()
            
            for sheet in spreadsheet.get('sheets', []):
                if sheet['properties']['sheetId'] == sheet_id:
                    return sheet['properties']['title']
            
            raise ValueError(f"Sheet with ID {sheet_id} not found in spreadsheet {spreadsheet_id}")
        except Exception as e:
            print(f"⚠️  Error getting sheet name from ID: {e}", file=sys.stderr)
            raise
    
    def _insert_data_to_gaps_sheet(self, callers_gap: list):
        """
        Insert data into all gaps Google Sheets configured in gaps_sheet_config.
        
        Args:
            callers_gap: List of caller gap values to insert
        """
        if not self.gaps_sheet_config:
            print("⚠️  Gaps sheet config is not set, skipping insertion", file=sys.stderr)
            return
        
        if not callers_gap:
            print(f"⚠️  No gaps to insert", file=sys.stderr)
            return
        
        # Normalize callers_gap values
        normalized_gaps = [str(item).strip() for item in callers_gap]
        
        print(f"📝 Inserting {len(normalized_gaps)} gaps into sheets", file=sys.stderr)

        # Iterate over each sub-item in gaps_sheet_config (e.g., 'gaps_sheet_archive', 'gaps_runs')
        for sheet_name_key, sheet_config in self.gaps_sheet_config.items():
            if not isinstance(sheet_config, dict):
                print(f"⚠️  Skipping {sheet_name_key}: config is not a dictionary", file=sys.stderr)
                continue
            
            wb_id = sheet_config.get('wb_id')
            sheet_id = sheet_config.get('sheet_id')
            
            if not wb_id or sheet_id is None:
                print(f"⚠️  Skipping {sheet_name_key}: missing wb_id or sheet_id: {sheet_config}", file=sys.stderr)
                continue
            
            print(f"📝 Processing gaps sheet: {sheet_name_key} (wb_id: {wb_id}, sheet_id: {sheet_id})", file=sys.stderr)
            try:
                self._insert_data_to_single_gaps_sheet(sheet_config, normalized_gaps, sheet_name_key)
            except Exception as e:
                print(f"⚠️  Error inserting data to {sheet_name_key}: {e}", file=sys.stderr)
                # Continue with other sheets even if one fails
                continue
    
    def _insert_data_to_single_gaps_sheet(self, sheet_config: Dict[str, Any], callers_gap: list, sheet_name_key: str = None):
        """
        Insert data into a single gaps Google Sheet.
        Inserts each gap as a row starting from the next empty line. Each row contains:
        - Column A: caller gap
        - Column B: empty (untouched)
        - Column C (or start_column_gap_info): caller_display (nick_name or caller_id)
        - Column D (or start_column_gap_info + 1): caller_id
        - Column E (or start_column_gap_info + 2): date
        - Column F (or start_column_gap_info + 3): time
        - Column G (or start_column_gap_info + 4): customers_input_file
        
        Args:
            sheet_config: Dictionary containing sheet configuration with keys 'wb_id' and 'sheet_id'
            callers_gap: List of caller gap values to insert
            sheet_name_key: Optional name/key of the sheet config (for logging)
        """
        try:
            # Extract wb_id and sheet_id from config
            wb_id = sheet_config.get('wb_id')
            sheet_id = sheet_config.get('sheet_id')
            
            if not wb_id or sheet_id is None:
                raise ValueError(f"Sheet config missing wb_id or sheet_id: {sheet_config}")
            
            # Get sheet name from sheet_id for range construction
            sheet_name = self._get_sheet_name_from_id(wb_id, sheet_id)
            
            log_prefix = f"[{sheet_name_key}] " if sheet_name_key else ""
            
            if not callers_gap:
                print(f"{log_prefix}⚠️  No gaps to insert", file=sys.stderr)
                return
            
            # Step 1: Find the first empty row in column A
            first_empty_row = self._find_first_empty_row(wb_id, sheet_id, sheet_name)
            print(f"{log_prefix}📝 First empty row found at row {first_empty_row}", file=sys.stderr)
            
            # Step 1.5: Check if space_row is enabled - if so, skip the first empty row
            if sheet_config.get('space_row') is True and first_empty_row > 2:
                start_row = first_empty_row + 1
                print(f"{log_prefix}📝 space_row is enabled, data will start from row {start_row} (skipping row {first_empty_row})", file=sys.stderr)
            else:
                start_row = first_empty_row
            
            # Step 2: Prepare data from metadata
            if not self._metadata:
                raise ValueError("metadata is not available. Make sure generate_data was called first with metadata.")
            
            metadata = self._metadata
            date = metadata.get('date_str', '')
            time = metadata.get('time_str', '')
            customers_input_file = metadata.get('customers_input_file_name', '')
            caller_id = metadata.get('caller_id', '')
            nick_name = metadata.get('nick_name')
            
            # Use nick_name if available, otherwise fall back to caller_id
            caller_display = nick_name if nick_name else caller_id
            
            # Step 3: Get start column for gap info from config
            start_column_gap_info = sheet_config.get('start_column_gap_info', 'C')  # Default to 'C' if not specified
            
            # Step 4: Prepare values for columns A and gap info columns
            # Column A: caller gap
            column_a_values = [[gap] for gap in callers_gap]
            
            # Gap info columns: caller_display (nick_name or caller_id), caller_id, date, time, customers_input_file
            # We write 5 columns starting from start_column_gap_info
            # Format caller_id as text to preserve leading zeros (e.g., 0522574817)
            caller_id_text = f"'{caller_id}" if caller_id else caller_id
            
            gap_info_values = []
            for gap in callers_gap:
                gap_info_values.append([
                    caller_display,         # First column: nick_name or caller_id
                    caller_id_text,        # Second column: caller_id (formatted as text to preserve leading zeros)
                    date,                   # Third column: date
                    time,                   # Fourth column: time
                    customers_input_file    # Fifth column: customers_input_file
                ])
            
            # Calculate end column for gap info (5 columns total)
            start_col_letter = start_column_gap_info.upper()
            end_col_letter = self._get_column_letter_offset(start_col_letter, 4)  # 4 columns after start (start + 4 = 5 columns total)
            
            # Step 5: Write all rows starting from start_row (which may be adjusted if space_row was inserted)
            # Write column A and gap info columns separately
            end_row = start_row + len(callers_gap) - 1
            
            # Prepare batch update with two ranges: A and gap info columns
            data = [
                {
                    'range': f"'{sheet_name}'!A{start_row}:A{end_row}",
                    'values': column_a_values
                },
                {
                    'range': f"'{sheet_name}'!{start_col_letter}{start_row}:{end_col_letter}{end_row}",
                    'values': gap_info_values
                }
            ]
            
            body = {
                'valueInputOption': 'USER_ENTERED',
                'data': data
            }
            
            print(f"{log_prefix}📝 Writing {len(callers_gap)} rows to gaps sheet starting from row {start_row} (column A and columns {start_col_letter}-{end_col_letter})", file=sys.stderr)
            
            self.drive_service.sheets_service.spreadsheets().values().batchUpdate(
                spreadsheetId=wb_id,
                body=body
            ).execute()
            
            print(f"{log_prefix}✅ Data inserted successfully into gaps sheet", file=sys.stderr)
            
        except Exception as e:
            print(f"⚠️  Error inserting data to gaps sheet (wb_id: {wb_id}, sheet_id: {sheet_id}): {e}", file=sys.stderr)
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
        # Convert column letter to number (A=1, B=2, ..., Z=26, AA=27, etc.)
        def column_to_number(col: str) -> int:
            result = 0
            for char in col.upper():
                result = result * 26 + (ord(char) - ord('A') + 1)
            return result
        
        # Convert number to column letter
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
    
    def _find_first_empty_row(self, wb_id: str, sheet_id: int, sheet_name: str) -> int:
        """
        Find the first empty row in column A of the sheet.
        Checks that 2 consecutive lines are empty before returning the first empty line.
        
        Args:
            wb_id: Google Sheets workbook ID
            sheet_id: Sheet ID (integer)
            sheet_name: Sheet name (string)
            
        Returns:
            Row number (1-based) of the first empty row in column A (where 2 consecutive rows are empty)
        """
        try:
            # Read column A to find the first empty row
            # Start from row 1 and read a reasonable chunk (e.g., first 1000 rows)
            range_name = f"'{sheet_name}'!A1:A1000"
            
            result = self.drive_service.sheets_service.spreadsheets().values().get(
                spreadsheetId=wb_id,
                range=range_name
            ).execute()
            
            values = result.get('values', [])
            
            # Find the first row where both it and the next row are empty (1-based index)
            for i in range(len(values) - 1):
                current_row = values[i] if i < len(values) else []
                next_row = values[i + 1] if i + 1 < len(values) else []
                
                # Check if current row is empty or if column A is empty/None
                current_empty = not current_row or (len(current_row) > 0 and (current_row[0] is None or str(current_row[0]).strip() == ''))
                
                # Check if next row is empty or if column A is empty/None
                next_empty = not next_row or (len(next_row) > 0 and (next_row[0] is None or str(next_row[0]).strip() == ''))
                
                # If both current and next rows are empty, return the current row number (1-based)
                if current_empty and next_empty:
                    return i + 1  # Convert to 1-based index
            
            # If no two consecutive empty rows found in first 1000 rows, return 1001
            return len(values) + 1
            
        except Exception as e:
            print(f"⚠️  Error finding first empty row: {e}, defaulting to row 1", file=sys.stderr)
            # Default to row 1 if there's an error
            return 1


def create_gaps_actions_google_manager(config_manager: ConfigManager):
    """Factory function to create GapsActionsFile instance."""
    from .gaps_actions_file import GapsActionsFile
    from .google_drive_utils import GDriveService
    config = _get_default_config(config_manager)
    print(f"Config created", file=sys.stderr)
    service_config = config.get_service_config()
    drive_service = GDriveService(service_config)

    print(f"Drive service created", file=sys.stderr)

    output_files_config = config.get_output_files_config('gaps_actions')

    print(f"Gaps actions output files config: {output_files_config}", file=sys.stderr)

    return GapsActionsFile(drive_service, config_manager, output_files_config)

