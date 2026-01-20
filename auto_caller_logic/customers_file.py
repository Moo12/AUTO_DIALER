from .google_drive_utils import BaseProcess, GDriveService
from .config import _get_default_config
from pathlib import Path
import os
import sys
import io

class CustomersFile(BaseProcess):
    def __init__(self, drive_service, column_letter_1: str, column_letter_2: str, column_letter_2_filter: str, sheet_1_id: str, sheet_2_id: str, sheet_1_name: str, sheet_2_name: str):
        super().__init__(drive_service, "customers")

        print(f"CustomersFile initialized with column_letter_1: {column_letter_1}, column_letter_2: {column_letter_2}, column_letter_2_filter: {column_letter_2_filter}, sheet_1_id: {sheet_1_id}, sheet_2_id: {sheet_2_id}, sheet_1_name: {sheet_1_name}, sheet_2_name: {sheet_2_name}", file=sys.stderr)
        self.column_letter_1 = column_letter_1
        self.column_letter_2_filter = column_letter_2_filter
        self.column_letter_2 = column_letter_2
        self.sheet_1_id = sheet_1_id
        self.sheet_2_id = sheet_2_id
        self.sheet_1_name = sheet_1_name
        self.sheet_2_name = sheet_2_name

    def generate_data(self, **kwargs):
        data = self.get_data_from_google_sheets()
        
        return {
            'customers': data
        }

    def get_excel_output_file_path(self, workbook_name: str):
        if workbook_name not in self.excel_to_google_workbook:
            raise ValueError(f"Workbook name {workbook_name} not found")
        
        output_file_path = self.excel_to_google_workbook[workbook_name].output_file_path
        return output_file_path

    def get_data_from_google_sheets(self):
        # Get sheet 1 data with optional filter column
        sheet_1_data = self.get_data_from_google_sheet(
            sheet_id=self.sheet_1_id, 
            column_letter=self.column_letter_1,
            column_condition_letter=self.column_letter_1_filter if hasattr(self, 'column_letter_1_filter') else None
        )
        if not sheet_1_data:
            raise ValueError(f"No data found in sheet 1 {self.column_letter_1}")
        sheet_2_data = self.get_data_from_google_sheet(sheet_id=self.sheet_2_id, 
            column_letter=self.column_letter_2,
            column_condition_letter = self.column_letter_2_filter if hasattr(self, 'column_letter_2_filter') else None
        )
        if not sheet_2_data:
            raise ValueError(f"No data found in sheet 2 {self.column_letter_2}")
        
        sheet_1_data_filtered = self._filter_four_digit_cells(sheet_1_data)
        sheet_2_data_filtered = self._filter_four_digit_cells(sheet_2_data)
        
        merged_set = set(sheet_1_data_filtered) | set(sheet_2_data_filtered)
        return sorted(list(merged_set)) 

    def get_data_from_google_sheet(self, sheet_id: str, column_letter: str, column_condition_letter: str = None, sheet_name: str = None):
        """
        Get data from a Google Sheet column, optionally filtered by a condition column.
        
        Args:
            sheet_id: Google Sheet ID
            column_letter: Column letter to retrieve data from
            column_condition_letter: Optional column letter to use as filter condition
            sheet_name: Optional sheet name (if None, uses first sheet)
            
        Returns:
            List of 4-digit values from column_letter, filtered by condition rows that have digits in the condition column
        """
        try:
            column_letter = column_letter.upper()
            data_condition = None
            
            # Step 1: Get data from condition column if provided
            if column_condition_letter is not None:
                column_condition_letter = column_condition_letter.upper()
                condition_col_range = f"{column_condition_letter}:{column_condition_letter}"
                condition_range_str = f"{sheet_name}!{condition_col_range}" if sheet_name else condition_col_range
                
                condition_result = self.drive_service.sheets_service.spreadsheets().values().get(
                    spreadsheetId=sheet_id,
                    range=condition_range_str
                ).execute()
                
                condition_values = condition_result.get('values', [])
                # Get condition column data (excluding header)
                data_condition = [row[0] if row else '' for row in condition_values[1:]] if len(condition_values) > 1 else []

                # Filter condition column with _filter_four_digit_cells
                condition_filtered_set = set(
                    idx for idx, v in enumerate(data_condition) if self._filter_if_any_digits_in_string(str(v))
                )


                print(f"Condition filtered set: {condition_filtered_set}", file=sys.stderr)
            
            # Step 2: Get data from main column
            main_col_range = f"{column_letter}:{column_letter}"
            main_range_str = f"{sheet_name}!{main_col_range}" if sheet_name else main_col_range
            
            result = self.drive_service.sheets_service.spreadsheets().values().get(
                spreadsheetId=sheet_id,
                range=main_range_str
            ).execute()
            
            values = result.get('values', [])
            # Get main column data (excluding header)
            data = [row[0] if row else '' for row in values[1:]] if len(values) > 1 else []
            
            # Step 3: Filter data based on condition
            filtered_data = []
            for index, row_data in enumerate(data):
                # Check if main column data contains at least one digit
                main_has_digits = self._filter_if_any_digits_in_string(str(row_data)) if row_data else False
                
                if not main_has_digits:
                    continue  # Skip rows where main column has no digits
                
                # If condition column is provided, check if condition is met
                if column_condition_letter is not None:
                    # Ensure we don't go out of bounds
                    if index not in condition_filtered_set:
                        continue
                
                # Add row data that passed all filters
                filtered_data.append(row_data)
            
            # Step 4: Apply _filter_four_digit_cells to final result

            print(f"Filtered data: {filtered_data}", file=sys.stderr)
            return filtered_data
            
        except Exception as e:
            print(f"Error fetching column {column_letter} from sheet {sheet_id}: {e}", file=sys.stderr)
            raise e

    def _filter_four_digit_cells(self, cell_values):
        """
        Filter and return only those cell values that contain exactly 4 digits (ignoring leading/trailing whitespace).
        Args:
            cell_values: Iterable of cell values (strings or None)
        Returns:
            List of strings with exactly 4 digits.
        """
        filtered = []
        for v in cell_values:
            if isinstance(v, str):
                v_stripped = v.strip()
                if v_stripped.isdigit() and len(v_stripped) == 4:
                    filtered.append(v_stripped)
        return filtered

    def _filter_if_any_digits_in_string(self, string: str):
        for char in string:
            if char.isdigit():
                return True
        return False

    def get_data_from_json(self):
        pass
def create_customers_google_manager():

    print(f"Creating CustomersFile manager", file=sys.stderr)

    config = _get_default_config()
    service_config = config.get_service_config()
    drive_service = GDriveService(service_config)

    print(f"Drive service created", file=sys.stderr)

    config_customers_input = config.get_customers_input_config()

    print(f"Config customers input: {config_customers_input}", file=sys.stderr)
    sheet_1_id = config_customers_input['sheet_1']['wb_id']
    sheet_1_name = config_customers_input['sheet_1'].get('sheet_name', None)
    sheet_2_id = config_customers_input['sheet_2']['wb_id']
    sheet_2_name = config_customers_input['sheet_2'].get('sheet_name', None)

    column_letter_1 = config_customers_input['sheet_1']['asterix_column_letter']
    column_letter_2 = config_customers_input['sheet_2']['asterix_column_letter']
    column_letter_2_filter = config_customers_input['sheet_2']['filter_column_letter']

    print(f"Sheet 1 ID: {sheet_1_id}, Sheet 1 name: {sheet_1_name}, Sheet 2 ID: {sheet_2_id}, Sheet 2 name: {sheet_2_name}, Column letter 1: {column_letter_1}, Column letter 2: {column_letter_2}, Column letter 2 filter: {column_letter_2_filter}", file=sys.stderr)
    
    if not sheet_1_id or not sheet_2_id:
        raise ValueError("Google Sheet IDs not configured. Please set customer_sheet_1_id and customer_sheet_2_id in config.yaml")

    return CustomersFile(drive_service, column_letter_1=column_letter_1, column_letter_2=column_letter_2, column_letter_2_filter=column_letter_2_filter, sheet_1_id=sheet_1_id, sheet_2_id=sheet_2_id, sheet_1_name=sheet_1_name, sheet_2_name=sheet_2_name)

    
