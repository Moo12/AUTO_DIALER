# Auto Dialer

Python package for generating dial files from Google Sheets and processing call data. Integrates with Google Drive, Google Sheets, and PayCall API to create filtered Excel workbooks.

## Features

1. **Import Customers**: Fetch customers from Google Sheets, filter and merge data, generate Excel files
2. **Create Filter File**: Process customers data, fetch call data from PayCall API, and create filtered Excel workbooks
3. **Google Drive Integration**: Automatically upload generated files to Google Drive folders
4. **Configuration Management**: Update configuration via CLI or programmatically
5. **Multiple Workbook Types**: Support for intermediate, auto dialer, and filter workbooks

## Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Install package in development mode
pip install -e .
```

## Configuration

1. Copy `config.example.yaml` to `config.yaml`
2. Fill in your Google Sheets IDs, Google Drive folder IDs, and credentials
3. Configure PayCall API settings

### Environment Variables

You can override the config file path using the `CONFIG_FILE_PATH` environment variable:

```bash
export CONFIG_FILE_PATH="/path/to/your/config.yaml"
```

## Usage

### CLI Commands

#### Import Customers

```bash
python import_customers.py
```

Outputs JSON to stdout:
```json
{
  "success": true,
  "output_path": "/absolute/path/to/output/file.xlsx"
}
```

#### Create Filter File

```bash
python create_filter_file.py \
  --caller_id "1234567890" \
  --start_date "28-12-2025 00:00:00" \
  --end_date "28-12-2025 23:59:00" \
  --customers_input_file "/path/to/customers.xlsx"
```

If `--customers_input_file` is not provided, the system will automatically fetch the latest customers file from Google Drive.

Outputs JSON to stdout:
```json
{
  "success": true
}
```

#### Update Configuration

```bash
# Update sheet_1 configuration
python config_manager_modifier.py \
  --sheet_1 '{"wb_id": "...", "sheet_name": "...", "asterix_column_letter": "Y"}'

# Update sheet_2 configuration
python config_manager_modifier.py \
  --sheet_2 '{"wb_id": "...", "sheet_name": "...", "filter_column_letter": "AY", "asterix_column_letter": "F"}'

# Get current configuration
python config_manager_modifier.py --get_sheets all
```

### Programmatic Usage

```python
from customers_file import create_customers_google_manager
from filter_file import create_filter_google_manager
from paycall_utils import get_paycall_data
from datetime import datetime

# Import customers
customers_file = create_customers_google_manager()
customers_file.run()
output_path = customers_file.get_excel_output_file_path('auto_dialer')

# Create filter file
start_date = datetime(2025, 12, 28, 0, 0, 0)
end_date = datetime(2025, 12, 28, 23, 59, 0)
calls = get_paycall_data(caller_id="1234567890", start_date=start_date, end_date=end_date)

filter_file = create_filter_google_manager()
filter_file.run(calls=calls, customers_input_file="/path/to/customers.xlsx")
```

## Project Structure

```
auto_dialer/
├── __init__.py                 # Package initialization
├── config.py                   # Configuration management
├── config_manager.py           # Configuration update utilities
├── config_manager_modifier.py  # CLI for updating configuration
├── customers_file.py           # CustomersFile class
├── filter_file.py              # FilterFile class
├── google_drive_utils.py       # Google Drive/Sheets integration
├── import_customers.py         # CLI entry point for importing customers
├── create_filter_file.py       # CLI entry point for creating filter files
├── paycall_utils.py            # PayCall API integration
├── workbooks/                  # Workbook modules
│   ├── __init__.py
│   ├── base_workbook.py        # Base workbook class
│   ├── intermediate_workbook.py
│   ├── auto_dialer_workbook.py
│   └── filter_workbook.py
├── config.yaml                 # Configuration file (create from config.example.yaml)
├── config.example.yaml         # Example configuration
├── requirements.txt            # Python dependencies
├── setup.py                    # Package setup
└── README.md                   # This file
```

## Architecture

### Base Classes

- **`BaseProcess`**: Abstract base class for all file-creating processes
- **`ExcelToGoogleWorkbook`**: Abstract base class for Excel workbook generators
- **`GDriveService`**: Handles Google Drive and Sheets API interactions

### Process Classes

- **`CustomersFile`**: Imports customers from Google Sheets, generates intermediate and auto dialer workbooks
- **`FilterFile`**: Creates filter workbooks from customers data and PayCall call data

### Workbook Classes

Each workbook type has its own module in `workbooks/`:
- **`IntermediateWorkbook`**: Intermediate Excel file structure
- **`AutoDialerWorkbook`**: Auto dialer Excel file structure
- **`FilterWorkbook`**: Filter Excel file structure with formulas

## Configuration Structure

The `config.yaml` file contains:

- **`service`**: Google API credentials (pickle file path, credentials file path)
- **`files`**: File configurations for customers and filter processes
  - **`customers`**: 
    - `excel_workbooks`: Intermediate and auto dialer workbook configs
    - `input`: Google Sheets input configurations (sheet_1, sheet_2)
  - **`filter`**: Filter workbook configuration
- **`paycall`**: PayCall API configuration (account, API URL, retry settings)

## Output Format

All CLI commands output JSON to `stdout` for easy parsing by other processes (e.g., PHP):
- Success: `{"success": true, ...}`
- Error: `{"success": false, "error": "...", "error_type": "..."}`

Status messages and debug information are printed to `stderr`.

## Dependencies

- `pandas`: Data manipulation
- `openpyxl`: Excel file generation
- `gspread`: Google Sheets API
- `google-auth`, `google-api-python-client`: Google API authentication
- `PyYAML`: Configuration file parsing
- `requests`: HTTP requests for PayCall API

## License

[Add your license here]
