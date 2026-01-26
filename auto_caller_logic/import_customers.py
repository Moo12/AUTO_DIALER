"""
Import customers from Google Sheets.

This module fetches customers from 2 Google Sheets based on a column letter.
"""
import json
import argparse
import sys
import os
from pathlib import Path
from .customers_file import create_customers_google_manager


def main():
    """CLI entry point for import_customers."""

    
    try:
        customers_file = create_customers_google_manager()

        print(f"Running CustomersFile manager", file=sys.stderr)
        process_result = customers_file.run()

        if process_result is None or process_result['excel_buffer'] is None:
            raise ValueError("Excel buffer is not found")

        excel_bytes = process_result['auto_dialer']['excel_buffer']
        file_name = process_result['auto_dialer']['file_name']

        import base64
        excel_bytes.seek(0)
        excel_bytes_base64 = base64.b64encode(excel_bytes.getvalue()).decode('utf-8')
        
        # Output JSON to stdout for easy parsing by PHP/other processes
        output_json = {
            'success': True,
            'excel_buffer': excel_bytes_base64,
            'file_name': file_name,
        }
        print(json.dumps(output_json, ensure_ascii=False), file=sys.stdout)
        sys.exit(0)
    except Exception as e:
        # Output error as JSON to stdout for consistent parsing
        error_json = {
            'success': False,
            'error': str(e),
            'error_type': type(e).__name__
        }
        print(json.dumps(error_json, ensure_ascii=False), file=sys.stdout)
        # Also print to stderr for logging
        print(f"ERROR: {type(e).__name__}: {str(e)}", file=sys.stderr)
        sys.exit(1)

# Export main as import_customers for package imports
import_customers = main

if __name__ == "__main__":
    main()

