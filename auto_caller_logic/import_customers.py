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
        customers_file.run()

        output_path = customers_file.get_excel_output_file_path('auto_dialer')

        if not output_path:
            raise ValueError(f"Output path for auto dialer not found")
        
        
        # Output JSON to stdout for easy parsing by PHP/other processes
        output_json = {
            'success': True,
            'output_path': str(Path(output_path).resolve()),
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

