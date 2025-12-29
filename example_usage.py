"""
Example usage of dial_file_generator module.

This script demonstrates how to use the two main functions:
1. import_customers - Fetch customers from Google Sheets
2. create_filter_file - Process customers and create filter file
"""

from dial_file_generator import import_customers, create_filter_file
from dial_file_generator.config import Config


def main():
    """Example usage of the dial file generator."""
    
    # Load configuration
    print("Loading configuration...")
    try:
        config = Config()
        config.load()
        print("✅ Configuration loaded")
    except Exception as e:
        print(f"❌ Error loading config: {e}")
        print("Please copy config.example.yaml to config.yaml and fill in your settings.")
        return
    
    # Example 1: Import customers from Google Sheets
    print("\n" + "="*50)
    print("Example 1: Import Customers")
    print("="*50)
    
    try:
        column_letter = 'A'  # Change to desired column
        output_file = import_customers(column_letter=column_letter)
        print(f"✅ Customers imported to: {output_file}")
    except Exception as e:
        print(f"❌ Error importing customers: {e}")
        return
    
    # Example 2: Create filter file
    print("\n" + "="*50)
    print("Example 2: Create Filter File")
    print("="*50)
    
    try:
        sheet_id = create_filter_file(input_file=output_file)
        print(f"✅ Filter file created in Google Sheet: {sheet_id}")
    except Exception as e:
        print(f"❌ Error creating filter file: {e}")


if __name__ == "__main__":
    main()

