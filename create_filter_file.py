"""
Create filter file from imported customers and call data.

This module processes the output from import_customers, fetches call data from a service,
and creates a filtered output in a Google Sheet.
"""

import os
import sys
import argparse
from pathlib import Path
from typing import Optional
from datetime import datetime
import json
from filter_file import create_filter_google_manager


# Handle imports for both script and module usage
try:
    from .paycall_utils import get_paycall_data
except ImportError:
    # If running as a script, use absolute import
    from paycall_utils import get_paycall_data


def main():
    """CLI entry point for create_filter_file."""
    parser = argparse.ArgumentParser(description="Create filter file from imported customers and call data.")
    parser.add_argument("--config_path", help="Path to config file (optional)", default=None)
    parser.add_argument("--caller_id", help="Caller ID (digits)", default=None)
    parser.add_argument("--start_date", help="Start date string", default=None)
    parser.add_argument("--end_date", help="End date string", default=None)
    parser.add_argument("--customers_input_file", help="Customers input file", default=None)
    args = parser.parse_args()

    start_date = datetime.strptime(args.start_date, "%d-%m-%Y %H:%M:%S")
    end_date = datetime.strptime(args.end_date, "%d-%m-%Y %H:%M:%S")

    try:
        calls = get_paycall_data(
            caller_id=args.caller_id,
            start_date=start_date,
            end_date=end_date
        )

        print(f"caller id: {args.caller_id} start date: {start_date} end date: {end_date} customers input file: {args.customers_input_file}", file=sys.stderr)

        filter_google_manager = create_filter_google_manager()
        google_sheet_ids = filter_google_manager.run(calls=calls, customers_input_file=args.customers_input_file, caller_id=args.caller_id)
        if google_sheet_ids is None or len(google_sheet_ids) == 0: 
            raise ValueError("Google sheet ID is not found")
        
        google_sheet_id = google_sheet_ids[0]
        print(f"Google sheet ID: {google_sheet_id}", file=sys.stderr)
        missing_customers = filter_google_manager.get_list_of_missing_customers(google_sheet_id)

        print(f"Missing customers: {missing_customers}", file=sys.stderr)

        output_json = {
            'success': True,
        }
        print(json.dumps(output_json, ensure_ascii=False), file=sys.stdout)
        sys.exit(0)
        exit_code = 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        error_json = {
            'success': False,
            'error': str(e),
            'error_type': type(e).__name__
        }
        print(json.dumps(error_json, ensure_ascii=False), file=sys.stdout)
        sys.exit(1)
if __name__ == "__main__":
    main()

