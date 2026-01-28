"""
Filter file generator for Google Drive uploads.

This module contains the FilterFile class that generates Excel workbooks
with Hebrew headers and ARRAYFORMULA formulas.
"""

import io
import sys
import os
from typing import Dict, Any, Optional
from pathlib import Path
from .google_drive_utils import BaseProcess
from .config import _get_default_config
from common_utils.config_manager import ConfigManager
from common_utils.item_endpoints import get_db_connection
class FilterFile(BaseProcess):

    def __init__(self, drive_service, config_manager: ConfigManager, customers_google_folder_id: str, customers_file_name_pattern: str, auto_dialer_file_name_pattern: str, gaps_sheet_config: Dict[str, Any], allowed_gaps_sheet_config: Dict[str, Any]):
        super().__init__(drive_service, config_manager, "filter")
        
        self.customers_google_folder_id = customers_google_folder_id
        self.customers_file_name_pattern = customers_file_name_pattern
        self.auto_dialer_file_name_pattern = auto_dialer_file_name_pattern
        self.gaps_sheet_config = gaps_sheet_config
        self.allowed_gaps_sheet_config = allowed_gaps_sheet_config
        # Store summarize_data for gaps sheet insertion
        self._summarize_data = None

        print(f"Gaps sheet config: {self.gaps_sheet_config}", file=sys.stderr)

    def generate_data(self, **kwargs):
        """
        Generate an Excel workbook with Hebrew headers and ARRAYFORMULA formulas.

        Returns:
            BytesIO buffer containing the Excel file
        """
        try:
            calls = kwargs.get('calls')
            caller_id = kwargs.get('caller_id')
            input_file_path = kwargs.get('customers_input_file')
            nick_name = kwargs.get('nick_name')
            customers, customers_input_file = self._get_customers(input_file_path)

            print(f"Input file path: {customers_input_file} caller id: {caller_id}", file=sys.stderr)
        
            # Create summarize_data with header values (A1-A4): [date, time, customers_input_file, caller_id]
            from datetime import datetime
            current_datetime = datetime.now()
            date_str = current_datetime.strftime("%d.%m.%Y")
            time_str = current_datetime.strftime("%H.%M")
            summarize_data = [date_str, time_str, customers_input_file, caller_id, nick_name]
            
            # Store for later use in gaps sheet insertion
            self._summarize_data = summarize_data
        
            return {
                'calls': calls,
                'customers': customers,
                'summarize_data': summarize_data
            }

        except ImportError:
            print("Error: openpyxl is required. Install it with: pip install openpyxl", file=sys.stderr)
            raise ImportError("openpyxl is required. Install it with: pip install openpyxl")    
        except Exception as e:
            print(f"Error creating Auto Calls Excel workbook: {e}", file=sys.stderr)
            raise RuntimeError(f"Error creating Auto Calls Excel workbook: {e}")

    def post_process_implementation(self, excel_info: Dict[str, Any]):
        """The specific logic to post process the file."""
        # Safely access nested dictionary
        filter_info = excel_info.get('filter')
        if filter_info is None:
            raise ValueError("Filter workbook info is not found in excel_info")
        
        filter_google_sheet_id = filter_info.get('file_id')
        filter_google_sheet_first_sheet_id = filter_info.get('sheet_id')
        print(f"Filter Google sheet ID: {filter_google_sheet_id}", file=sys.stderr)

        if filter_google_sheet_id is None:
            raise ValueError("Filter Google sheet ID is not found")

        callers_gap = self.get_list_of_missing_customers(filter_google_sheet_id, filter_google_sheet_first_sheet_id)

        print (f"finished .... post process implementation. len of callers gap: {len(callers_gap)}", file=sys.stderr)
        

        # Insert data into gaps sheet
        if self.gaps_sheet_config and callers_gap:
            # Get allowed gaps list for coloring
            allowed_gaps = self._get_allowed_gaps_list()

            print(f"Allowed gaps: {[item for item in allowed_gaps]}", file=sys.stderr)
            self._insert_data_to_gaps_sheet(callers_gap, allowed_gaps)
            
            # Filter callers_gap to only include items that are in allowed_gaps
            filtered_callers_gap = [
                item for item in callers_gap 
                if str(item).strip() not in allowed_gaps
            ]
            print(f"🔍 Filtered callers_gap: {len(callers_gap)} -> {len(filtered_callers_gap)} items (only those in allowed_gaps)", file=sys.stderr)
            
            # Use filtered list in return value
            callers_gap = filtered_callers_gap
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
    
    def _read_column_from_google_sheet(
        self, 
        spreadsheet_id: str, 
        range_name: str, 
        sheet_id: int = None,
        skip_header_rows: int = 0,
        extract_column_index: int = None,
        flatten: bool = True
    ) -> list:
        """
        Generic method to read column data from Google Sheets.
        
        Args:
            spreadsheet_id: Google Sheets spreadsheet ID
            range_name: Range to read (e.g., "A:A", "A1:A100", "D2:D", or "'Sheet1'!A:A" if sheet name is included)
            sheet_id: Optional sheet ID (integer). If provided, will convert to sheet name for the range.
                      If None and range_name doesn't include sheet name, uses default sheet
            skip_header_rows: Number of header rows to skip (default 0)
            extract_column_index: If specified, extract only this column index (0-based). 
                                  If None, returns all columns as nested lists
            flatten: If True, flatten the result to a single list. Works with or without extract_column_index.
        
        Returns:
            List of values. If extract_column_index is set, returns a flat list of that column's values.
            If flatten is True and extract_column_index is None, returns a flat list of all values.
            Otherwise, returns a list of rows (each row is a list of cell values).
        """
        try:
            # Check if range_name already includes a sheet name (contains '!')
            if '!' in range_name:
                # Range already includes sheet name, use it as-is
                full_range = range_name
            elif sheet_id is not None:
                # Convert sheet_id to sheet_name and construct range
                sheet_name = self._get_sheet_name_from_id(spreadsheet_id, sheet_id)
                full_range = f"'{sheet_name}'!{range_name}"
            else:
                # Use range as-is (default sheet)
                full_range = range_name
            
            print(f"📖 Reading from Google Sheet {spreadsheet_id}, range: {full_range}", file=sys.stderr)
            
            result = self.drive_service.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=full_range
            ).execute()
            
            values = result.get('values', [])
            
            # Skip header rows if specified
            if skip_header_rows > 0 and len(values) > skip_header_rows:
                values = values[skip_header_rows:]
            
            # Extract specific column if requested
            if extract_column_index is not None:
                extracted = []
                for row in values:
                    if row and len(row) > extract_column_index:
                        extracted.append(row[extract_column_index])
                    else:
                        extracted.append('')  # Empty cell if row is too short
                return extracted
            
            # Flatten if requested
            if flatten:
                flattened = []
                for row in values:
                    if row:
                        flattened.extend(row)
                return flattened
            
            return values
            
        except Exception as e:
            print(f"⚠️  Error reading from Google Sheet: {e}", file=sys.stderr)
            raise
    
    def _get_allowed_gaps_list(self) -> set:
        """
        Read allowed gaps from Google Sheet and filter for 4-digit values.
        
        Returns:
            Set of allowed gap values (4-digit strings)
        """
        if not self.allowed_gaps_sheet_config:
            print("⚠️  Allowed gaps sheet config is not set, returning empty set", file=sys.stderr)
            return set()
        
        wb_id = self.allowed_gaps_sheet_config.get('wb_id')
        sheet_id = self.allowed_gaps_sheet_config.get('sheet_id')
        content_column_letter = self.allowed_gaps_sheet_config.get('content_column_letter', 'A')
        
        if not wb_id or sheet_id is None:
            print(f"⚠️  Allowed gaps sheet config missing wb_id or sheet_id: {self.allowed_gaps_sheet_config}", file=sys.stderr)
            return set()
        
        try:
            # Read column from row 1 onwards (we'll skip row 1 which is the header)
            column_range = f"{content_column_letter}1:{content_column_letter}"
            
            # Read the column, skip first row (header), and flatten
            all_values = self._read_column_from_google_sheet(
                spreadsheet_id=wb_id,
                range_name=column_range,
                sheet_id=sheet_id,
                skip_header_rows=1,  # Skip header row
                extract_column_index=0,  # Extract first (and only) column
            )
            
            # Filter for 4-digit values only
            filtered_values = self._filter_four_digit_cells(all_values)
            
            print(f"✅ Found {len(filtered_values)} allowed gaps (4-digit values)", file=sys.stderr)
            
            return set(filtered_values)
            
        except Exception as e:
            print(f"⚠️  Error reading allowed gaps sheet: {e}", file=sys.stderr)
            return set()
    
    def _filter_four_digit_cells(self, cell_values):
        """
        Filter and return only those cell values that contain exactly 4 digits (ignoring leading/trailing whitespace).
        Handles both string and numeric values from Google Sheets.
        
        Args:
            cell_values: Iterable of cell values (strings, numbers, or None)
            
        Returns:
            List of strings with exactly 4 digits.
        """
        filtered = []
        for v in cell_values:
            if v is None:
                continue
            
            # Convert to string and strip whitespace
            v_str = str(v).strip()
            
            # Check if it's exactly 4 digits (all characters are digits and length is 4)
            if v_str.isdigit() and len(v_str) == 4:
                filtered.append(v_str)
        return filtered
    
    def _insert_data_to_gaps_sheet(self, callers_gap: list, allowed_gaps: set):
        """
        Insert data into all gaps Google Sheets configured in gaps_sheet_config.
        Filters callers_gap to exclude items in allowed_gaps, then iterates over each 
        sub-item in gaps_sheet_config and inserts filtered data into each sheet.
        
        Args:
            callers_gap: List of caller gap values to insert
            allowed_gaps: Set of allowed gap values (4-digit strings) - these will be excluded
        """
        if not self.gaps_sheet_config:
            print("⚠️  Gaps sheet config is not set, skipping insertion", file=sys.stderr)
            return
        
        # Filter callers_gap to exclude items in allowed_gaps
        filtered_gaps = [
            str(item).strip() for item in callers_gap 
            if str(item).strip() not in allowed_gaps
        ]
        
        if not filtered_gaps:
            print(f"⚠️  No gaps to insert (all {len(callers_gap)} were filtered out by allowed_gaps)", file=sys.stderr)
            return
        
        print(f"📝 Filtered {len(callers_gap)} gaps to {len(filtered_gaps)} gaps (excluded {len(callers_gap) - len(filtered_gaps)} in allowed_gaps)", file=sys.stderr)

        
        
        # Iterate over each sub-item in gaps_sheet_config (e.g., 'gaps_sheet', 'gaps_sheet_runs')
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
                self._insert_data_to_single_gaps_sheet(sheet_config, filtered_gaps, sheet_name_key)
            except Exception as e:
                print(f"⚠️  Error inserting data to {sheet_name_key}: {e}", file=sys.stderr)
                # Continue with other sheets even if one fails
                continue
    
    def _insert_data_to_single_gaps_sheet(self, sheet_config: Dict[str, Any], filtered_gaps: list, sheet_name_key: str = None):
        """
        Insert data into a single gaps Google Sheet.
        Inserts each gap as a row starting from the next empty line. Each row contains:
        - Column A: caller gap
        - Column B: empty (untouched)
        - Column C: caller_id
        - Column D: date
        - Column E: time
        - Column F: customers_input_file
        
        Args:
            sheet_config: Dictionary containing sheet configuration with keys 'wb_id' and 'sheet_id'
            filtered_gaps: List of already filtered caller gap values to insert (items in allowed_gaps have been excluded)
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
            
            if not filtered_gaps:
                print(f"{log_prefix}⚠️  No gaps to insert (filtered list is empty)", file=sys.stderr)
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
            
            # Step 2: Prepare data from summarize_data
            if not self._summarize_data:
                raise ValueError("summarize_data is not available. Make sure generate_data was called first.")
            
            summarize_data = self._summarize_data
            # summarize_data format: [date, time, customers_input_file, caller_id, nick_name]
            date = summarize_data[0]
            time = summarize_data[1]
            customers_input_file = summarize_data[2]
            caller_id = summarize_data[3]
            nick_name = summarize_data[4] if len(summarize_data) > 4 else None
            
            # Use nick_name if available, otherwise fall back to caller_id
            caller_display = nick_name if nick_name else caller_id
            
            # Step 3: Get start column for gap info from config
            start_column_gap_info = sheet_config.get('start_column_gap_info', 'C')  # Default to 'C' if not specified
            
            # Step 4: Prepare values for columns A and gap info columns
            # Column A: caller gap
            column_a_values = [[gap] for gap in filtered_gaps]
            
            # Gap info columns: caller_display (nick_name or caller_id), caller_id, date, time, customers_input_file
            # We write 5 columns starting from start_column_gap_info
            # Format caller_id as text to preserve leading zeros (e.g., 0522574817)
            caller_id_text = f"'{caller_id}" if caller_id else caller_id
            
            gap_info_values = []
            for gap in filtered_gaps:
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
            end_row = start_row + len(filtered_gaps) - 1
            
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
            
            print(f"{log_prefix}📝 Writing {len(filtered_gaps)} rows to gaps sheet starting from row {start_row} (column A and columns {start_col_letter}-{end_col_letter})", file=sys.stderr)
            
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
        
        Args:
            wb_id: Google Sheets workbook ID
            sheet_id: Sheet ID (integer)
            sheet_name: Sheet name (string)
            
        Returns:
            Row number (1-based) of the first empty row in column A
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
            
            # Find the first empty row (1-based index)
            for i, row in enumerate(values, start=1):
                # Check if row is empty or if column A is empty/None
                if not row or (len(row) > 0 and (row[0] is None or str(row[0]).strip() == '')):
                    return i
            
            # If no empty row found in first 1000 rows, return 1001
            return len(values) + 1
            
        except Exception as e:
            print(f"⚠️  Error finding first empty row: {e}, defaulting to row 1", file=sys.stderr)
            # Default to row 1 if there's an error
            return 1
    
    def _apply_red_formatting(self, spreadsheet_id: str, sheet_id: int, row_indices: list):
        """
        Apply red background color to specified rows in column A.
        
        Args:
            spreadsheet_id: Google Sheets spreadsheet ID
            sheet_id: Sheet ID (integer)
            row_indices: List of row indices (0-based) to format
        """
        try:
            # Create format requests for each row
            requests = []
            for row_index in row_indices:
                requests.append({
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': row_index,
                            'endRowIndex': row_index + 1,
                            'startColumnIndex': 0,  # Column A
                            'endColumnIndex': 1
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'backgroundColor': {
                                    'red': 1.0,
                                    'green': 0.0,
                                    'blue': 0.0
                                }
                            }
                        },
                        'fields': 'userEnteredFormat.backgroundColor'
                    }
                })
            
            if requests:
                batch_update_body = {
                    'requests': requests
                }
                
                self.drive_service.sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body=batch_update_body
                ).execute()
                
                print(f"✅ Red formatting applied to {len(requests)} cells", file=sys.stderr)
        except Exception as e:
            print(f"⚠️  Error applying red formatting: {e}", file=sys.stderr)
            # Don't raise - formatting is not critical

    def _get_customers(self, customers_input_file: Optional[Dict[str, Any]] = None):
        """
        Read column A from row 2 onwards from input Excel file or Google Sheets and return as a list.
        
        Args:
            customers_input_file: Either:
                - None: use latest customers file from Google Drive
                - Dict with keys:
                    - file_path: local path to an uploaded Excel file
                    - file_name: optional original file name (for display/logging)
            
        Returns:
            List of values from column A starting from row 2
        """
        if customers_input_file is None:
            customers_input_file_id, customers_input_file_name, first_sheet_id = self.get_last_customers_file()
            customers_input_file = customers_input_file_name
            customers = self._get_customers_from_google_drive(customers_input_file_id, first_sheet_id)
            return customers, customers_input_file

        if "file_path" not in customers_input_file or customers_input_file.get('file_path') is None:
            raise ValueError("customers_input_file['file_path'] is required when customers_input_file is provided")

        if "file_name" not in customers_input_file or customers_input_file.get('file_name') is None:
            raise ValueError("customers_input_file['file_name'] is required when customers_input_file is provided")
    

        customers_input_file_path = customers_input_file.get('file_path')
        customers_input_file_name = customers_input_file.get('file_name')

        # sys.path is a list (import paths) — use filesystem check instead
        if not os.path.exists(str(customers_input_file_path)):
            raise ValueError(f"File not found: {customers_input_file_path}")

        # Try reading as Excel file
        customers = self._get_customers_from_excel_file(customers_input_file_path)
        
        return customers, customers_input_file_name

    def _get_customers_from_excel_file(self, customers_input_file):
        """
        Read column A from row 2 onwards from an Excel file path.
        
        Args:
            customers_input_file: Path to Excel file (string)
            
        Returns:
            List of values from column A starting from row 2
            
        Raises:
            FileNotFoundError: If the file doesn't exist
            ValueError: If the file is not a valid Excel file
        """
        from openpyxl import load_workbook
        from openpyxl.utils.exceptions import InvalidFileException
        
        print(f"Reading customers from Excel file: {customers_input_file}", file=sys.stderr)
        
        # Validate file exists
        if not os.path.exists(customers_input_file):
            raise FileNotFoundError(f"Excel file not found: {customers_input_file}")
        
        # Check file extension (allow temp files without extensions from PHP uploads)
        file_ext = os.path.splitext(customers_input_file)[1].lower()
        
        # Validate file extension if present
        if file_ext and file_ext not in ('.xlsx', '.xlsm', '.xltx', '.xltm', '.xls'):
            raise ValueError(f"File is not a supported Excel format: {customers_input_file}. Supported: .xlsx, .xlsm, .xltx, .xltm, .xls")
        elif not file_ext:
            # No extension - likely a temp file from PHP upload, try to read it anyway
            print(f"File has no extension, attempting to read as Excel file", file=sys.stderr)
        
        try:
            # Try to open the file - openpyxl will validate the actual file format
            # This will raise InvalidFileException if it's not a valid Excel file
            input_wb = load_workbook(customers_input_file, read_only=True)
        except InvalidFileException as e:
            raise ValueError(f"Invalid Excel file format. Please ensure the file is a valid Excel file (.xlsx, .xlsm, .xltx, .xltm, .xls). Error: {e}")
        except Exception as e:
            raise ValueError(f"Error reading Excel file: {e}")
        
        input_ws = input_wb.active
        
        # Read column A from row 2 onwards
        customers = []
        row = 2
        while True:
            cell_value = input_ws.cell(row=row, column=1).value
            if cell_value is None:
                break
            customers.append(cell_value)
            row += 1
        
        input_wb.close()
        return customers

    def _get_customers_from_google_sheets_id(self, file_id: str, sheet_id: int):
        """
        Get customers from Google Sheets using file ID.
        
        Args:
            file_id: Google Sheets file ID
            
        Returns:
            List of values from column A starting from row 2
        """
        # Get all data from column A (A:A gets the entire column)
        main_col_range = "A:A"
        return self._read_column_from_google_sheet(
            spreadsheet_id=file_id,
            range_name=main_col_range,
            sheet_id=sheet_id,
            skip_header_rows=1,  # Skip header row
            extract_column_index=0,  # Extract column A (first column)
        )
    
    def _get_customers_from_google_drive(self, customers_input_file_id, sheet_id: int = None):
        """
        Get customers from Google Drive using file ID.
        
        Args:
            customers_input_file_id: Google Sheets file ID

        Returns:
            List of values from column A starting from row 2
        """
        print(f"Customers input file: {customers_input_file_id}", file=sys.stderr)

        if customers_input_file_id is None:
            raise ValueError("No customers file found in Google Drive and no input file provided")

        # Get all data from column A (A:A gets the entire column)
        main_col_range = "A:A"
        return self._read_column_from_google_sheet(
            spreadsheet_id=customers_input_file_id,
            range_name=main_col_range,
            sheet_id=sheet_id,
            skip_header_rows=1,  # Skip header row
            extract_column_index=0,  # Extract column A (first column)
        )
    
    def get_last_customers_file(self):
        """
        Get the latest customers file ID from Google Drive folder that matches the filename pattern.
        
        Returns:
            File ID (str) of the latest matching file, or None if not found
            
        Raises:
            ValueError: If customers_google_folder_id is not set
        """
        if self.customers_google_folder_id is None:
            raise ValueError("Customers Google folder ID is not set")
        
        if self.customers_file_name_pattern is None:
            raise ValueError("Customers file name pattern is not set")

        print(f"Customers Google folder ID: {self.customers_google_folder_id}, Customers file name pattern: {self.customers_file_name_pattern}", file=sys.stderr)
        
        # Get the latest file matching the pattern from Google Drive
        result = self.drive_service.get_latest_file_by_pattern(
            folder_id=self.customers_google_folder_id,
            file_name_pattern=self.customers_file_name_pattern
        )
        
        if result is None:
            return None, None
        
        latest_file_id, latest_file_name, first_sheet_id = result
        return latest_file_id, latest_file_name, first_sheet_id

    def get_list_of_missing_customers(self, spread_sheet_id: str, sheet_id: int):
        """
        Get list of missing customers from the filter workbook.
        
        Args:
            spread_sheet_id: Google Sheets spreadsheet ID
            
        Returns:
            List of missing customer values (flattened)
        """
        if "filter" in self.excel_to_google_workbook:
            filter_workbook = self.excel_to_google_workbook["filter"]
            
            main_col_range = filter_workbook.get_summary_missing_customers_range()
            
            # Read the range and flatten the values
            return self._read_column_from_google_sheet(
                spreadsheet_id=spread_sheet_id,
                range_name=main_col_range,
                sheet_id=sheet_id,
                skip_header_rows=0,
                extract_column_index=None,  # Get all columns
            )

        else:
            print(f"FilterWorkbook not found in excel_to_google_workbook", file=sys.stderr)
            return None


def create_filter_google_manager(config_manager: ConfigManager):
    from .filter_file import FilterFile
    from .google_drive_utils import GDriveService
    config = _get_default_config(config_manager)
    print(f"Config created", file=sys.stderr)
    service_config = config.get_service_config()
    drive_service = GDriveService(service_config)

    print(f"Drive service created", file=sys.stderr)

    output_files_config = config.get_output_files_config('filter')

    allowed_gaps_sheet_config = config.get_input_files_config('filter').get('allowed_gaps_sheet', {})


    customers_google_folder_id = config.get_google_folder_id_by_name('customers', 'intermidiate')

    customers_file_name_pattern = config.get_output_file_pattern_by_name('customers', 'intermidiate')

    auto_dialer_file_name_pattern = config.get_excel_workbooks_config_by_name('customers').get('auto_dialer', {}).get('file_name_pattern', '')

    print(f"Customers Google folder ID: {customers_google_folder_id}, Customers file name pattern: {customers_file_name_pattern} auto dialer file name pattern: {auto_dialer_file_name_pattern}", file=sys.stderr)

    return FilterFile(drive_service, config_manager, customers_google_folder_id, customers_file_name_pattern, auto_dialer_file_name_pattern, output_files_config, allowed_gaps_sheet_config)