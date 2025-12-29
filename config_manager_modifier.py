"""
Example usage of ConfigManager for updating customers input sheet configurations.

This demonstrates how to use the ConfigManager to update sheet_1 and sheet_2 configurations.
"""

from config_manager import ConfigManager, update_customers_sheet_config
import json
import sys
import argparse

def example_update_sheet_1():
    """Example: Update sheet_1 configuration."""
    # Method 1: Using the convenience function
    sheet_1_config = {
        'wb_id': '1wnfKAVJkU5BgKE_E9nDB_V2dfARGasZVtCU0jv9mLVs',
        'sheet_name': 'גיליון1',
        'asterix_column_letter': 'Y'
    }
    update_customers_sheet_config('sheet_1', sheet_1_config)


def example_update_sheet_2():
    """Example: Update sheet_2 configuration."""
    # Method 2: Using the ConfigManager class directly
    manager = ConfigManager()
    
    sheet_2_config = {
        'wb_id': '1VNxGGzR5j1MBNqjhKAIMmdLgqMfTgNTh6kGH4tg1sqw',
        'sheet_name': 'לקוחות',
        'filter_column_letter': 'AY',
        'asterix_column_letter': 'F'
    }
    
    manager.update_and_save_customers_input_sheet('sheet_2', sheet_2_config)


def example_get_sheet_config():
    """Example: Get current sheet configuration."""
    manager = ConfigManager()
    
    # Get sheet_1 config
    sheet_1 = manager.get_customers_input_sheets(['sheet_1'])
    print(f"Sheet 1 config: {sheet_1}")
    
    # Get sheet_2 config
    sheet_2 = manager.get_customers_input_sheets(['sheet_2'])
    print(f"Sheet 2 config: {sheet_2}")
    
    # Get all sheets
    all_sheets = manager.get_customers_input_sheets(['sheet_1', 'sheet_2'])
    print(f"All sheets: {all_sheets}")


def example_frontend_usage():
    """
    Example: How a frontend would use this.
    
    Frontend would typically send JSON data like:
    {
        "sheet_name": "sheet_1",
        "config": {
            "wb_id": "1wnfKAVJkU5BgKE_E9nDB_V2dfARGasZVtCU0jv9mLVs",
            "sheet_name": "גיליון1",
            "asterix_column_letter": "Y"
        }
    }
    """
    # Simulate frontend request data
    frontend_data = {
        'sheet_name': 'sheet_1',
        'config': {
            'wb_id': '1wnfKAVJkU5BgKE_E9nDB_V2dfARGasZVtCU0jv9mLVs',
            'sheet_name': 'גיליון1',
            'asterix_column_letter': 'Y'
        }
    }
    
    # Update configuration
    update_customers_sheet_config(
        sheet_name=frontend_data['sheet_name'],
        sheet_config=frontend_data['config']
    )
    
    print(f"Updated {frontend_data['sheet_name']} configuration")


def main():
    """Main function with argument parsing."""
    parser = argparse.ArgumentParser(
        description='Update or get customers input sheet configurations (sheet_1 or sheet_2)'
    )
    
    parser.add_argument(
        '--sheet_1',
        type=str,
        help='JSON string with sheet_1 configuration. Example: \'{"wb_id": "...", "sheet_name": "...", "asterix_column_letter": "Y"}\''
    )
    
    parser.add_argument(
        '--sheet_2',
        type=str,
        help='JSON string with sheet_2 configuration. Example: \'{"wb_id": "...", "sheet_name": "...", "filter_column_letter": "AY", "asterix_column_letter": "F"}\''
    )
    
    parser.add_argument(
        '--get_sheets',
        type=str,
        help='Get configuration for sheets. Comma-separated list of sheet names (e.g., "sheet_1" or "sheet_1,sheet_2" or "all" for both)'
    )
    
    args = parser.parse_args()
    
    updated_sheets = []
    sheets_config = None
    
    # Get sheets configuration if requested
    if args.get_sheets:
        manager = ConfigManager()
        
        # Handle "all" keyword
        if args.get_sheets.lower() == 'all':
            sheet_names = ['sheet_1', 'sheet_2']
        else:
            # Parse comma-separated list
            sheet_names = [s.strip() for s in args.get_sheets.split(',')]
        
        try:
            sheets_config = manager.get_customers_input_sheets(sheet_names)
            print(f"Retrieved configuration for {', '.join(sheet_names)}", file=sys.stderr)
        except Exception as e:
            raise ValueError(f"Error getting sheets configuration: {e}")
    
    # Update sheet_1 if provided
    if args.sheet_1:
        try:
            sheet_1_config = json.loads(args.sheet_1)
            update_customers_sheet_config('sheet_1', sheet_1_config)
            updated_sheets.append('sheet_1')
            print(f"Updated sheet_1 configuration", file=sys.stderr)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON for --sheet_1: {e}")
    
    # Update sheet_2 if provided
    if args.sheet_2:
        try:
            sheet_2_config = json.loads(args.sheet_2)
            update_customers_sheet_config('sheet_2', sheet_2_config)
            updated_sheets.append('sheet_2')
            print(f"Updated sheet_2 configuration", file=sys.stderr)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON for --sheet_2: {e}")
    
    # If no arguments provided, show help
    if not args.sheet_1 and not args.sheet_2 and not args.get_sheets:
        parser.print_help()
        return None
    
    # Return results
    if args.get_sheets:
        return {'sheets_config': sheets_config}
    else:
        return {'updated_sheets': updated_sheets}


if __name__ == "__main__":
    try:
        result = main()
        
        if result is None:
            # Help was shown, exit normally
            sys.exit(0)
        
        output_json = {
            'success': True,
        }
        
        # Add result data to output
        if 'updated_sheets' in result:
            output_json['updated_sheets'] = result['updated_sheets']
        if 'sheets_config' in result:
            output_json['sheets_config'] = result['sheets_config']
        
        print(json.dumps(output_json, ensure_ascii=False), file=sys.stdout)
        sys.exit(0)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        error_json = {
            'success': False,
            'error': str(e),
            'error_type': type(e).__name__
        }
        print(json.dumps(error_json, ensure_ascii=False), file=sys.stdout)
        sys.exit(1)