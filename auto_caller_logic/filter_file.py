"""
Filter file generator for Google Drive uploads.

This module contains the FilterFile class that generates Excel workbooks
with Hebrew headers and ARRAYFORMULA formulas.
"""

import io
import sys
import os
from .google_drive_utils import BaseProcess, GDriveService
from .config import _get_default_config

class FilterFile(BaseProcess):

    def __init__(self, drive_service, customers_google_folder_id: str, customers_file_name_pattern: str, auto_dialer_file_name_pattern: str):
        super().__init__(drive_service, "filter")
        self.customers_google_folder_id = customers_google_folder_id
        self.customers_file_name_pattern = customers_file_name_pattern
        self.auto_dialer_file_name_pattern = auto_dialer_file_name_pattern

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
            customers, customers_input_file = self._get_customers(input_file_path)

            print(f"Input file path: {customers_input_file} caller id: {caller_id}", file=sys.stderr)
        
            return {
                'calls': calls,
                'customers': customers,
                'customers_input_file': customers_input_file,
                'caller_id': caller_id
            }
            
        except ImportError:
            print("Error: openpyxl is required. Install it with: pip install openpyxl", file=sys.stderr)
            raise ImportError("openpyxl is required. Install it with: pip install openpyxl")    
        except Exception as e:
            print(f"Error creating Auto Calls Excel workbook: {e}", file=sys.stderr)
            raise RuntimeError(f"Error creating Auto Calls Excel workbook: {e}")

    def _get_customers(self, customers_input_file):
        """
        Read column A from row 2 onwards from input Excel file or Google Sheets and return as a list.
        
        Args:
            customers_input_file: Path to input Excel file, file object, or None to get from Google Drive
            
        Returns:
            List of values from column A starting from row 2
        """
        if customers_input_file is None:
            customers_input_file_id, customers_input_file_name = self.get_last_customers_file()
            customers_input_file = customers_input_file_name
            customers = self._get_customers_from_google_drive(customers_input_file_id)
            return customers, customers_input_file
        
        # Check if it's a Google Sheets file ID (typically a long alphanumeric string)
        # Google Drive file IDs are usually 25-44 characters long
        if isinstance(customers_input_file, str) and len(customers_input_file) > 20 and not os.path.exists(customers_input_file):
            # Might be a Google Sheets file ID, try reading from Google Sheets API
            print(f"Detected potential Google Sheets file ID, reading from Google Sheets API...", file=sys.stderr)
            try:
                customers = self._get_customers_from_google_sheets_id(customers_input_file)
                return customers, customers_input_file
            except Exception as e:
                print(f"Failed to read as Google Sheets ID, trying as file path: {e}", file=sys.stderr)
                # Fall through to try as file path
        
        # Try reading as Excel file
        customers = self._get_customers_from_excel_file(customers_input_file)
        # If it's a full path, extract the file name
        if isinstance(customers_input_file, str):
            file_name = os.path.basename(customers_input_file)
            # Extract only the part that matches the auto dialer file name pattern
            import re
            if self.auto_dialer_file_name_pattern:
                # Extract the prefix from the pattern (everything before the first placeholder)
                # For example: "DIALER_{date}_{time}" -> "DIALER_"
                pattern_prefix = self.auto_dialer_file_name_pattern.split('{')[0]
                # Find where the pattern prefix starts in the file name
                prefix_index = file_name.upper().find(pattern_prefix.upper())

                if prefix_index != -1:
                    # Extract everything from the prefix to the end of the filename
                    file_name = file_name[prefix_index:]
            
            customers_input_file = file_name
        return customers, customers_input_file

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

    def _get_customers_from_google_sheets_id(self, file_id: str):
        """
        Get customers from Google Sheets using file ID.
        
        Args:
            file_id: Google Sheets file ID
            
        Returns:
            List of values from column A starting from row 2
        """
        # Get all data from column A (A:A gets the entire column)
        main_col_range = "A:A"
        print(f"Reading from Google Sheets ID: {file_id}, range: {main_col_range}", file=sys.stderr)
        result = self.drive_service.sheets_service.spreadsheets().values().get(
            spreadsheetId=file_id,
            range=main_col_range
        ).execute()

        values = result.get('values', [])
        
        # Get all column A data excluding the first row (header)
        # Skip first row (index 0) and extract first element of each row (column A)
        customers = []
        if len(values) > 1:
            for row in values[1:]:  # Skip header row
                if row and len(row) > 0:
                    customers.append(row[0])  # Get first column (column A)
                else:
                    customers.append('')  # Empty cell
        
        return customers
    
    def _get_customers_from_google_drive(self, customers_input_file_id):
        

        print(f"Customers input file: {customers_input_file_id}", file=sys.stderr)

        if customers_input_file_id is None:
            raise ValueError("No customers file found in Google Drive and no input file provided")

        # Get all data from column A (A:A gets the entire column)
        main_col_range = "A:A"
        print(f"Main col range: {main_col_range}", file=sys.stderr)
        result = self.drive_service.sheets_service.spreadsheets().values().get(
            spreadsheetId=customers_input_file_id,
            range=main_col_range
        ).execute()

        values = result.get('values', [])
        
        # Get all column A data excluding the first row (header)
        # Skip first row (index 0) and extract first element of each row (column A)
        customers = []
        if len(values) > 1:
            for row in values[1:]:  # Skip header row
                if row and len(row) > 0:
                    customers.append(row[0])  # Get first column (column A)
                else:
                    customers.append('')  # Empty cell
        
        return customers
    
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
        latest_file_id, latest_file_name = self.drive_service.get_latest_file_by_pattern(
            folder_id=self.customers_google_folder_id,
            file_name_pattern=self.customers_file_name_pattern
        )
        
        return latest_file_id, latest_file_name

    def get_list_of_missing_customers(self, spread_sheet_id: str):
        
        if "filter" in self.excel_to_google_workbook:
            filter_workbook = self.excel_to_google_workbook["filter"]
            
            main_col_range = filter_workbook.get_summary_missing_customers_range()
            result = self.drive_service.sheets_service.spreadsheets().values().get(
                spreadsheetId=spread_sheet_id,
                range=main_col_range
            ).execute()

            values = result.get('values', [])

            # Flatten the list of lists `values` to a single list of values
            flat_values = []
            for row in values:
                flat_values.extend(row)
            values = flat_values
            return values

        else:
            print(f"FilterWorkbook not found in excel_to_google_workbook", file=sys.stderr)
            return None


def create_filter_google_manager():
    from .filter_file import FilterFile
    from .google_drive_utils import GDriveService
    config = _get_default_config()
    service_config = config.get_service_config()
    drive_service = GDriveService(service_config)

    customers_google_folder_id = config.get_google_folder_id_by_name('customers', 'intermidiate')

    customers_file_name_pattern = config.get_output_file_pattern_by_name('customers', 'intermidiate')

    auto_dialer_file_name_pattern = config.get_excel_workbooks_config_by_name('customers').get('auto_dialer', {}).get('file_name_pattern', '')

    print(f"Customers Google folder ID: {customers_google_folder_id}, Customers file name pattern: {customers_file_name_pattern} auto dialer file name pattern: {auto_dialer_file_name_pattern}", file=sys.stderr)

    return FilterFile(drive_service, customers_google_folder_id, customers_file_name_pattern, auto_dialer_file_name_pattern)