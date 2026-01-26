"""
Configuration management for dial file generator.
"""

import os
import yaml
import sys
from pathlib import Path
from typing import Dict, Any, Optional
from common_utils.config_manager import ConfigManager


class Config:
    """
    Configuration manager for dial file generator.
    
    Handles loading and accessing configuration from YAML files.
    """
    
    def __init__(self, config_manager: ConfigManager):
        """
        Initialize Config instance.
        
        Args:
            config_path: Path to config file. If None, checks environment variable
                         CONFIG_FILE_PATH, then defaults to config.yaml in project root.
        """
        self._config_manager = config_manager
    
    def get_config(self) -> Dict[str, Any]:
        """
        Get loaded configuration.
        
        Returns:
            Dictionary containing configuration
            
        Raises:
            RuntimeError: If config hasn't been loaded yet
        """
        if self._config_manager is None:
            raise RuntimeError(
                "Configuration not loaded. Call load() first."
            )
        return self._config_manager.get_config()

    def get_output_files_config(self, name: str) -> Dict[str, Any]:
        """
        Get output files configuration.
        
        Returns:
            Dictionary with output files configuration
        """
        config = self.get_config()
        return config.get('files', {}).get(name, {}).get('output', {})

    def get_input_files_config(self, name: str) -> Dict[str, Any]:
        """
        Get input files configuration.
        
        Returns:
            Dictionary with input files configuration
        """
        config = self.get_config()
        return config.get('files', {}).get(name, {}).get('input', {})
    
    def get_main_google_folder_id(self) -> str:
        """
        Get main Google folder ID.

        Returns:
            String with main Google folder ID
        """
        config = self.get_config()
        return config.get('files', {}).get('main_google_folder_id', '')
    def get_customers_input_config(self) -> Dict[str, str]:
        """
        Get Google Sheet IDs from config.
        
        Returns:
            Dictionary with sheet_1_id, sheet_2_id, and output_sheet_id
        """
        config = self.get_config()
        return config.get('files', {}).get('customers', {}).get('input', {})

    def get_customers_input_sheet_config(self, sheet_name: str) -> Dict[str, str]:
        """
        Get Google Sheet IDs from config.
        
        Returns:
            Dictionary with sheet_1_id, sheet_2_id, and output_sheet_id
        """
        config = self.get_config()
        return config.get('files', {}).get('customers', {}).get('input', {}).get(sheet_name, {})

    def get_excel_workbooks_config_by_name(self, name: str) -> Dict[str, Any]:
        """
        Get excel_workbooks configuration for a given file name.
        
        Args:
            name: File name (e.g., 'customers', 'filter')
            
        Returns:
            Dict[str, Dict[str, Any]]: Dictionary mapping workbook names to their configs
            Example: {'~ intermidiate': {...}, '~ outo_dialer': {...}}
        """
        files_config = self.get_config().get('files', {})
        if name not in files_config:
            raise ValueError(f"Invalid file name: {name}. Available: {list(files_config.keys())}")
        
        excel_workbooks = files_config.get(name, {}).get('excel_workbooks', {})
        if not excel_workbooks:
            raise ValueError(f"No excel_workbooks found for file name: {name}")
        
        return excel_workbooks

    def get_output_excel_file_config_by_name(self, name_method: str, name_excel_workbook: str) -> Dict[str, str]:
        if name_method not in self._config_manager.load().get('files', {}):
            raise ValueError(f"Invalid file name: {name_method}")
        return self.get_excel_workbooks_config_by_name(name_method).get(name_excel_workbook, {}).get('output_folder_path', '')
    
    def get_output_file_pattern_by_name(self, name_method: str, name_excel_workbook: str) -> str:
        return self.get_excel_workbooks_config_by_name(name_method).get(name_excel_workbook, {}).get('file_name_pattern', '')
    
    def get_google_folder_id_by_name(self, name_method: str, name_excel_workbook: str) -> str:
        return self.get_excel_workbooks_config_by_name(name_method).get(name_excel_workbook, {}).get('google_folder_id', '')

    def get_service_config(self) -> Dict[str, str]:
        """
        Get service configuration for API calls.
        
        Returns:
            Dictionary with pickle_file_path and credentials_file_path
        """
        config = self.get_config()
        service_config = config.get('service', {})
        
        return {
            'pickle_file_path': service_config.get('pickle_file_path', ''),
            'credentials_file_path': service_config.get('credentials_file_path', '')
        }
    
    def get_output_config(self) -> Dict[str, str]:
        """
        Get output configuration.
        
        Returns:
            Dictionary with temp_dir and file patterns
        """
        config = self.get_config()
        output_config = config.get('output', {})
        
        return {
            'temp_dir': output_config.get('temp_dir', './temp'),
            'customer_file_pattern': output_config.get('customer_file_pattern', 'customers_{column_letter}_{timestamp}.xlsx'),
            'filter_file_pattern': output_config.get('filter_file_pattern', 'filter_file_{timestamp}.xlsx')
        }

    def get_paycall_account(self) -> Dict[str, str]:
        """
        Get paycall account configuration.
        
        Returns:
            Dictionary with email, password, and paycall_id
        """
        config = self.get_config()
        paycall_account = config.get('paycall', {}).get('account', {})
        return {
            'email': paycall_account.get('email', ''),
            'password': paycall_account.get('password', ''),
            'paycall_id': paycall_account.get('paycall_id', '')
        }

    def get_paycall_api_url(self) -> str:
        """
        Get paycall API URL.
        
        Returns:
            String with paycall API URL
        """
        config = self.get_config()
        return config.get('paycall', {}).get('api_url', '')
    
    def get_paycall_limit(self) -> int:
        """
        Get paycall limit.
        
        Returns:
            Integer with paycall limit
        """
        config = self.get_config()
        return config.get('paycall', {}).get('limit', 500)
    
    def get_paycall_order_by(self) -> str:
        """
        Get paycall order by.
        
        Returns:
            String with paycall order by
        """
        config = self.get_config()
        return config.get('paycall', {}).get('order_by', 'asc')
    
    def get_paycall_retry_config(self) -> Dict[str, Any]:
        """
        Get paycall retry configuration.
        
        Returns:
            Dictionary with max_retries, backoff_factor, and retryable_status_codes
        """
        config = self.get_config()
        retry_config = config.get('paycall', {}).get('retry', {})
        return {
            'max_retries': retry_config.get('max_retries', 3),
            'backoff_factor': retry_config.get('backoff_factor', 1.0),
            'retryable_status_codes': retry_config.get('retryable_status_codes', [500, 502, 503, 504]),
            'retry_on_timeout': retry_config.get('retry_on_timeout', True)
        }
    def get_google_drive_config(self) -> Dict[str, str]:
        """
        Get google drive configuration.
        
        Returns:
            Dictionary with main_archive_folder_id, filter_folder_id, and auto_calls_folder_id
        """
        config = self.get_config()
        google_drive_config = config.get('google_drive', {})
        return {
            'main_archive_folder_id': google_drive_config.get('main_archive_folder_id', ''),
            'filter_folder_id': google_drive_config.get('filter_folder_id', ''),
            'intermediate_folder_id': google_drive_config.get('intermediate_folder_id', ''),
            'auto_calls_folder_id': google_drive_config.get('auto_calls_folder_id', '')
        }

    
    def update_filter_input_sheet(self, sheet_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update configuration for filter input sheets (e.g., allowed_gaps_sheet, gaps_sheet).
        
        Args:
            sheet_config: Dictionary where keys are sheet names (e.g., 'allowed_gaps_sheet', 'gaps_sheet')
                         and values are dictionaries with sheet configuration:
                - wb_id: Google Sheets workbook ID (required)
                - sheet_name: Sheet name within the workbook (required)
                - content_column_letter: Column letter for content (optional, e.g., for allowed_gaps_sheet)
                - Any other sheet-specific configuration fields
        
        Returns:
            Updated configuration dictionary
            
        Raises:
            ValueError: If required fields are missing
        """

        print(f"Updating filter input sheet: {sheet_config}", file=sys.stderr)
        # Validate required fields for each sheet
        required_fields = ['wb_id', 'sheet_name']
        for sheet_name, sheet_config_item in sheet_config.items():
            if not isinstance(sheet_config_item, dict):
                raise ValueError(f"Sheet config for {sheet_name} must be a dictionary")
            
            for field in required_fields:
                if field not in sheet_config_item:
                    raise ValueError(f"Missing required field '{field}' for sheet '{sheet_name}'")

        config = self._config_manager.get_config()
        
        # Ensure the structure exists
        if 'files' not in config:
            config['files'] = {}
        if 'filter' not in config['files']:
            config['files']['filter'] = {}
        if 'input' not in config['files']['filter']:
            config['files']['filter']['input'] = {}
        
        # Update each sheet configuration
        for sheet_name, sheet_config_item in sheet_config.items():
            config['files']['filter']['input'][sheet_name] = sheet_config_item

        self._config_manager.save_config(config)
        
        return config


    def update_customers_input_sheet(self, sheet_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update configuration for customers input sheet (sheet_1 or sheet_2).
        
        Args:
            sheet_name: Either 'sheet_1' or 'sheet_2'
            sheet_config: Dictionary with sheet configuration:
                - wb_id: Google Sheets workbook ID
                - sheet_name: Sheet name within the workbook
                - asterix_column_letter: Column letter for asterix (required for sheet_1)
                - filter_column_letter: Column letter for filter (required for sheet_2)
                - asterix_column_letter: Column letter for asterix (required for sheet_2)
        
        Returns:
            Updated configuration dictionary
            
        Raises:
            ValueError: If sheet_name is invalid or required fields are missing
        """
        for sheet_name in sheet_config.keys():
            if sheet_name not in ['sheet_1', 'sheet_2']:
                raise ValueError(f"Invalid sheet_name: {sheet_name}. Must be 'sheet_1' or 'sheet_2'")
        
        # Validate required fields
        required_fields_base = ['wb_id', 'sheet_name', 'asterix_column_letter']
        for sheet_name, sheet_config_item in sheet_config.items():
            print(f"Sheet name: {sheet_name}, Sheet config item: {sheet_config_item}", file=sys.stderr)
            required_fields = required_fields_base + ['filter_column_letter'] if sheet_name == 'sheet_2' else required_fields_base
            for field in required_fields:
                if field not in sheet_config_item:
                    raise ValueError(f"Missing required field: {field}")

        config = self._config_manager.get_config()
        
        # Ensure the structure exists
        if 'files' not in config:
            config['files'] = {}
        if 'customers' not in config['files']:
            config['files']['customers'] = {}
        if 'input' not in config['files']['customers']:
            config['files']['customers']['input'] = {}
        
        for sheet_name, sheet_config_item in sheet_config.items():
            config['files']['customers']['input'][sheet_name] = sheet_config_item
        

        self._config_manager.save_config(config)

    def get_filter_input_sheets(self, sheets_names):
        """
        Get configuration for filter input sheet (allowed_gaps_sheet or gaps_sheet).
        
        Args:
            sheet_name: Either 'allowed_gaps_sheet' or 'gaps_sheet'
            
        Returns:
            Dictionary with sheet configuration, or None if not found
        """
        if isinstance(sheets_names, str):
            sheets_names = [sheets_names]
        elif not isinstance(sheets_names, list):
            raise ValueError(f"sheets_names must be a list of strings")
        
        if any(sheet_name not in ['allowed_gaps_sheet', 'gaps_sheet'] for sheet_name in sheets_names):
            raise ValueError(f"Invalid sheet_name: {sheets_names}. Must be 'allowed_gaps_sheet' or 'gaps_sheet'")
        
        config = self._config_manager.get_config()

        sheet_configs = {}

        for sheet_name in sheets_names:
            if sheet_name not in config.get('files', {}).get('filter', {}).get('input', {}):
                raise ValueError(f"Sheet {sheet_name} not found in config")
            sheet_config = config.get('files', {}).get('filter', {}).get('input', {}).get(sheet_name)
            if sheet_config is None:
                raise ValueError(f"Sheet {sheet_name} not found in config")
            sheet_configs[sheet_name] = sheet_config
        
        return sheet_configs

    def get_gaps_sheet_config(self) -> Dict[str, Any]:
        """
        Get configuration for gaps sheet.
        
        Returns:
            Dictionary with gaps sheet configuration
        """
        config = self.get_config()
        return config.get('files', {}).get('filter', {}).get('output', {}).get('gaps_sheet', {})
    
    def update_main_google_folder_id(self, main_google_folder_id: str) -> Dict[str, Any]:
        """
        Update main Google folder ID configuration.
        
        Args:
            main_google_folder_id: Main Google folder ID to set
            
        Returns:
            Updated configuration dictionary
        """
        if not main_google_folder_id:
            raise ValueError("main_google_folder_id cannot be empty")
        
        config = self._config_manager.get_config()
        
        # Ensure the structure exists
        if 'files' not in config:
            config['files'] = {}
        
        config['files']['main_google_folder_id'] = main_google_folder_id
        
        self._config_manager.save_config(config)
        
        return config
    
    def update_gaps_sheet_config(self, gaps_sheet_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update configuration for gaps sheet.
        
        Args:
            gaps_sheet_config: Dictionary with gaps sheet configuration:
                - wb_id: Google Sheets workbook ID (optional)
                - sheet_name: Sheet name within the workbook (optional)
                - Any other gaps sheet configuration fields
        
        Returns:
            Updated configuration dictionary
        """
        if not gaps_sheet_config:
            raise ValueError("gaps_sheet_config cannot be empty")
        
        config = self._config_manager.get_config()
        
        # Ensure the structure exists
        if 'files' not in config:
            config['files'] = {}
        if 'filter' not in config['files']:
            config['files']['filter'] = {}
        if 'output' not in config['files']['filter']:
            config['files']['filter']['output'] = {}
        
        config['files']['filter']['output']['gaps_sheet'] = gaps_sheet_config
        
        self._config_manager.save_config(config)
        
        return config
    
    def get_customers_input_sheets(self, sheets_names):
        """
        Get configuration for customers input sheet (sheet_1 or sheet_2).
        
        Args:
            sheet_name: Either 'sheet_1' or 'sheet_2'
            
        Returns:
            Dictionary with sheet configuration, or None if not found
        """

        if isinstance(sheets_names, str):
            sheets_names = [sheets_names]
        elif not isinstance(sheets_names, list):
            raise ValueError(f"sheets_names must be a list of strings")
        
        if any(sheet_name not in ['sheet_1', 'sheet_2'] for sheet_name in sheets_names):
            raise ValueError(f"Invalid sheet_name: {sheets_names}. Must be 'sheet_1' or 'sheet_2'")
        
        config = self._config_manager.get_config()

        sheet_configs = {}

        for sheet_name in sheets_names:
            if sheet_name not in config.get('files', {}).get('customers', {}).get('input', {}):
                raise ValueError(f"Sheet {sheet_name} not found in config")
            sheet_config = config.get('files', {}).get('customers', {}).get('input', {}).get(sheet_name)
            if sheet_config is None:
                raise ValueError(f"Sheet {sheet_name} not found in config")
            sheet_configs[sheet_name] = sheet_config
        
        return sheet_configs
    
    def get_all_customers_input_sheets(self) -> Dict[str, Any]:
        """
        Get all customers input sheet configurations.
        
        Returns:
            Dictionary with sheet_1 and sheet_2 configurations
        """
        config = self._config_manager.get_config()
        
        return config.get('files', {}).get('customers', {}).get('input', {})

# Default singleton instance for backward compatibility
config_instance: Optional[Config] = None

def _get_default_config(path: Optional[str] = None) -> Config:
    global config_instance
    
    if config_instance is None:
        config_instance = Config(path)
    return config_instance