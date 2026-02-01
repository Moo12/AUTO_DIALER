"""
Configuration management for dial file generator.
"""

import os
import yaml
import sys
import re
from pathlib import Path
from typing import Dict, Any, Optional, Union
from urllib.parse import urlparse, parse_qs
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
                - sheet_id: Sheet ID within the workbook (required, integer)
                - content_column_letter: Column letter for content (optional, e.g., for allowed_gaps_sheet)
                - Any other sheet-specific configuration fields
        
        Returns:
            Updated configuration dictionary
            
        Raises:
            ValueError: If required fields are missing
        """

        print(f"Updating filter input sheet: {sheet_config}", file=sys.stderr)
        
        config = self._config_manager.get_config()
        
        # Ensure the structure exists
        if 'files' not in config:
            config['files'] = {}
        if 'filter' not in config['files']:
            config['files']['filter'] = {}
        if 'input' not in config['files']['filter']:
            config['files']['filter']['input'] = {}
        
        # Process each sheet configuration
        for sheet_name, sheet_config_item in sheet_config.items():
            if not isinstance(sheet_config_item, dict):
                raise ValueError(f"Sheet config for {sheet_name} must be a dictionary")
            
            # Get existing config for this sheet (if it exists)
            existing_config = config['files']['filter']['input'].get(sheet_name, {})
            if not isinstance(existing_config, dict):
                existing_config = {}
            
            # Merge existing config with new config (new values override existing)
            merged_config = existing_config.copy()
            merged_config.update(sheet_config_item)
            
            # Process URL if present (extract wb_id and sheet_id, fetch file_name and sheet_name)
            processed_config = self._process_sheet_config_with_url(sheet_name, merged_config)
            
            # Validate required fields after processing
            required_fields = ['wb_id', 'sheet_id']
            for field in required_fields:
                if field not in processed_config:
                    raise ValueError(f"Missing required field '{field}' for sheet '{sheet_name}' (after URL processing)")
            
            # Merge processed config back (preserves any additional fields added during processing)
            final_config = existing_config.copy()
            final_config.update(processed_config)
            
            config['files']['filter']['input'][sheet_name] = final_config

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

        config = self._config_manager.get_config()
        
        # Ensure the structure exists
        if 'files' not in config:
            config['files'] = {}
        if 'customers' not in config['files']:
            config['files']['customers'] = {}
        if 'input' not in config['files']['customers']:
            config['files']['customers']['input'] = {}
        
        # Process each sheet configuration
        for sheet_name, sheet_config_item in sheet_config.items():
            if not isinstance(sheet_config_item, dict):
                raise ValueError(f"Sheet config for {sheet_name} must be a dictionary")
            
            print(f"Sheet name: {sheet_name}, Sheet config item: {sheet_config_item}", file=sys.stderr)
            
            # Get existing config for this sheet (if it exists)
            existing_config = config['files']['customers']['input'].get(sheet_name, {})
            if not isinstance(existing_config, dict):
                existing_config = {}
            
            # Merge existing config with new config (new values override existing)
            merged_config = existing_config.copy()
            merged_config.update(sheet_config_item)
            
            # Process URL if present (extract wb_id and sheet_id, fetch file_name and sheet_name)
            processed_config = self._process_sheet_config_with_url(sheet_name, merged_config)
            
            # Validate required fields after processing
            required_fields_base = ['wb_id', 'sheet_name', 'asterix_column_letter']
            required_fields = required_fields_base + ['filter_column_letter'] if sheet_name == 'sheet_2' else required_fields_base
            for field in required_fields:
                if field not in processed_config:
                    raise ValueError(f"Missing required field: {field} (after URL processing)")
            
            # Merge processed config back (preserves any additional fields added during processing)
            final_config = existing_config.copy()
            final_config.update(processed_config)
            
            config['files']['customers']['input'][sheet_name] = final_config
        

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
    
    def _enhance_config_with_sheet_info(self, config_dict: Dict[str, Any], drive_service, sheets_service) -> Dict[str, Any]:
        """
        Helper method to enhance a config dictionary with file_name and sheet_name.
        
        Args:
            config_dict: Configuration dictionary that should contain wb_id and sheet_id
            drive_service: GDriveService instance
            sheets_service: Google Sheets service instance
            
        Returns:
            Enhanced config dictionary with file_name and sheet_name added (or None if failed)
        """
        wb_id = config_dict.get('wb_id')
        sheet_id = config_dict.get('sheet_id')
        
        if not wb_id or sheet_id is None:
            return config_dict
        
        try:
            # Get spreadsheet metadata from Google Sheets API (includes title and sheets)
            spreadsheet = sheets_service.spreadsheets().get(
                spreadsheetId=wb_id
            ).execute()
            
            # Get file name from spreadsheet properties (more reliable than Drive API)
            file_name = spreadsheet.get('properties', {}).get('title', '')
            if not file_name:
                # Fallback: try to get from Drive API if Sheets API doesn't have title
                try:
                    file_metadata = drive_service.drive_service.files().get(
                        fileId=wb_id,
                        fields='name'
                    ).execute()
                    file_name = file_metadata.get('name', '')
                except Exception as drive_error:
                    print(f"⚠️  Warning: Could not fetch file name from Drive API: {drive_error}", file=sys.stderr)
                    file_name = ''
            
            config_dict['file_name'] = file_name
            
            # Get sheet name from spreadsheet sheets list
            # Convert sheet_id to int for comparison (YAML might store it as string)
            sheet_id_int = int(sheet_id) if isinstance(sheet_id, str) else sheet_id
            
            sheet_name = None
            for sheet in spreadsheet.get('sheets', []):
                if sheet['properties']['sheetId'] == sheet_id_int:
                    sheet_name = sheet['properties']['title']
                    break
            
            if sheet_name:
                config_dict['sheet_name'] = sheet_name
            else:
                print(f"⚠️  Warning: Sheet ID {sheet_id} not found in spreadsheet {wb_id}", file=sys.stderr)
                config_dict['sheet_name'] = None
                
        except Exception as e:
            print(f"⚠️  Warning: Could not fetch file_name/sheet_name: {e}", file=sys.stderr)
            # Don't fail - just skip adding these fields
            config_dict['file_name'] = None
            config_dict['sheet_name'] = None
        
        return config_dict
    
    def _get_google_services(self):
        """
        Helper method to initialize Google Drive and Sheets services.
        
        Returns:
            Tuple of (drive_service, sheets_service) or (None, None) if initialization fails
        """
        try:
            from .google_drive_utils import GDriveService
            service_config = self.get_service_config()
            drive_service = GDriveService(service_config)
            sheets_service = drive_service.sheets_service
            return drive_service, sheets_service
        except Exception as e:
            print(f"⚠️  Warning: Could not initialize Google Drive service for fetching file/sheet names: {e}", file=sys.stderr)
            return None, None
    
    def _extract_ids_from_google_sheets_url(self, url: str) -> Optional[Dict[str, str]]:
        """
        Extract wb_id and sheet_id from a Google Sheets URL.
        
        Google Sheets URL formats:
        - https://docs.google.com/spreadsheets/d/{wb_id}/edit#gid={sheet_id}
        - https://docs.google.com/spreadsheets/d/{wb_id}/edit?gid={sheet_id}#gid={sheet_id}
        
        Args:
            url: Google Sheets URL
            
        Returns:
            Dictionary with 'wb_id' and 'sheet_id' keys, or None if URL is invalid
        """
        if not url or not isinstance(url, str):
            return None
        
        try:
            # Parse the URL
            parsed = urlparse(url)
            
            # Extract wb_id from path: /spreadsheets/d/{wb_id}/edit
            path_match = re.search(r'/spreadsheets/d/([a-zA-Z0-9_-]+)', parsed.path)
            if not path_match:
                print(f"⚠️  Warning: Could not extract wb_id from URL: {url}", file=sys.stderr)
                return None
            
            wb_id = path_match.group(1)
            
            # Extract sheet_id from fragment (#gid=) or query parameter (?gid=)
            sheet_id = None
            
            # Try fragment first (#gid=)
            if parsed.fragment:
                fragment_match = re.search(r'gid=(\d+)', parsed.fragment)
                if fragment_match:
                    sheet_id = fragment_match.group(1)
            
            # Try query parameter if not found in fragment
            if not sheet_id and parsed.query:
                query_params = parse_qs(parsed.query)
                if 'gid' in query_params:
                    sheet_id = query_params['gid'][0]
            
            if not sheet_id:
                print(f"⚠️  Warning: Could not extract sheet_id from URL: {url}", file=sys.stderr)
                # Return wb_id only, sheet_id will be None
                return {'wb_id': wb_id, 'sheet_id': None}
            
            return {'wb_id': wb_id, 'sheet_id': int(sheet_id)}
            
        except Exception as e:
            print(f"⚠️  Warning: Error parsing Google Sheets URL: {e}", file=sys.stderr)
            return None
    
    def _process_sheet_config_with_url(self, config_key: str, config_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a sheet config dictionary: if it contains 'url', extract wb_id and sheet_id,
        then fetch file_name and sheet_name from Google services.
        
        Args:
            config_key: The config key name (used to check if it contains "sheet")
            config_dict: Configuration dictionary that may contain 'url' key
            
        Returns:
            Updated configuration dictionary with wb_id, sheet_id, file_name, and sheet_name
        """
        # Check if key contains "sheet" and config has "url"
        if "sheet" not in config_key.lower():
            return config_dict
        
        if 'url' not in config_dict:
            return config_dict
        
        url = config_dict.get('url')
        if not url:
            return config_dict
        
        print(f"📝 Processing URL for {config_key}: {url}", file=sys.stderr)
        
        # Extract wb_id and sheet_id from URL
        extracted_ids = self._extract_ids_from_google_sheets_url(url)
        if not extracted_ids:
            print(f"⚠️  Warning: Could not extract IDs from URL for {config_key}", file=sys.stderr)
            return config_dict
        
        # Update config_dict with extracted IDs
        config_dict['wb_id'] = extracted_ids['wb_id']
        if extracted_ids.get('sheet_id') is not None:
            config_dict['sheet_id'] = extracted_ids['sheet_id']
        
        # Remove url from config_dict (we've extracted what we need)
        config_dict.pop('url', None)
        
        # Fetch file_name and sheet_name from Google services
        drive_service, sheets_service = self._get_google_services()
        if drive_service and sheets_service:
            config_dict = self._enhance_config_with_sheet_info(config_dict, drive_service, sheets_service)
        else:
            print(f"⚠️  Warning: Could not initialize Google services to fetch file_name/sheet_name for {config_key}", file=sys.stderr)
        
        return config_dict
    
    def get_output_files_config_used_display_by_name(self, module_name: str, sub_module_name: Union[str, list[str]]) -> Dict[str, Any]:
        """
        Get configuration for output files in a module.
        
        Args:
            module_name: Module name (e.g., "filter", "customers")
            sub_module_name: Sub-module name(s) - can be a string or list of strings
                           (e.g., "gaps_sheet" or ["gaps_sheet_archive", "gaps_sheet_runs"])
        
        Returns:
            Dictionary mapping sub-module names to their configurations.
            
        Raises:
            ValueError: If sub_module_name is invalid type or sub-module not found in config
        """
        # Normalize sub_module_name to a list
        if isinstance(sub_module_name, str):
            sub_module_names = [sub_module_name]
        elif isinstance(sub_module_name, list):
            sub_module_names = sub_module_name
        else:
            raise ValueError("sub_module_name must be a string or list of strings")
        
        config = self._config_manager.get_config()
        
        # Get the output section for the module
        output_config = config.get('files', {}).get(module_name, {}).get('output', {})
        
        if not output_config:
            raise ValueError(f"Module '{module_name}' output section not found in config")
        
        sub_module_configs = {}
        
        for sub_module in sub_module_names:
            if sub_module not in output_config:
                raise ValueError(f"Sub-module '{sub_module}' not found in module '{module_name}' output config")
            
            sub_module_config = output_config.get(sub_module)
            if sub_module_config is None:
                raise ValueError(f"Sub-module '{sub_module}' config is None in module '{module_name}'")
            
            # Copy config to avoid modifying original
            sub_module_configs[sub_module] = sub_module_config.copy() if isinstance(sub_module_config, dict) else sub_module_config
        
        return sub_module_configs

    def get_output_files_config(self, module_name: str) -> Dict[str, Any]:
        """
        Get configuration for gaps sheet.
        
        Returns:
            Dictionary with gaps sheet configuration
        """
        config = self.get_config()
        return config.get('files', {}).get(module_name, {}).get('output', {})
    
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
    
    def update_output_files(self, module_name: str, output_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update configuration for output files in a module.
        
        Args:
            module_name: Module name (e.g., "filter", "customers")
            output_config: Dictionary mapping sub-module names to their configuration.
                          Example: {"gaps_sheet_archive": {...}, "gaps_sheet_runs": {...}}
        
        Returns:
            Updated configuration dictionary
        """
        if not output_config:
            raise ValueError("output_config cannot be empty")
        
        if not isinstance(output_config, dict):
            raise ValueError("output_config must be a dictionary")
        
        # Extract sub-module names from output_config keys
        sub_modules_names = list(output_config.keys())
        
        if not sub_modules_names:
            raise ValueError("output_config cannot be empty")
        
        config = self._config_manager.get_config()
        
        # Ensure the structure exists
        if 'files' not in config:
            config['files'] = {}
        if module_name not in config['files']:
            config['files'][module_name] = {}
        if 'output' not in config['files'][module_name]:
            config['files'][module_name]['output'] = {}
        
        # Update each sub-module configuration
        for sub_module_name in sub_modules_names:
            sub_module_config = output_config[sub_module_name]
            if not isinstance(sub_module_config, dict):
                raise ValueError(f"Config for sub-module '{sub_module_name}' must be a dictionary")
            
            # Get existing config for this sub-module (if it exists)
            existing_config = config['files'][module_name]['output'].get(sub_module_name, {})
            if not isinstance(existing_config, dict):
                existing_config = {}
            
            # Merge existing config with new config (new values override existing)
            merged_config = existing_config.copy()
            merged_config.update(sub_module_config)
            
            # Process URL if present (extract wb_id and sheet_id, fetch file_name and sheet_name)
            processed_config = self._process_sheet_config_with_url(sub_module_name, merged_config)
            
            # Merge processed config back (preserves any additional fields added during processing)
            final_config = existing_config.copy()
            final_config.update(processed_config)
            
            config['files'][module_name]['output'][sub_module_name] = final_config
        
        self._config_manager.save_config(config)
        
        return config
    
    def get_input_user_display(self, module_name: str, sub_modules: Union[str, list[str]]) -> Dict[str, Any]:
        """
        Get configuration for input sheets in a module.
        
        Args:
            module_name: Module name (e.g., "customers", "filter")
            sub_modules: Sub-module name(s) - can be a string or list of strings
                        (e.g., "sheet_1" or ["sheet_1", "sheet_2"] for customers,
                        or "allowed_gaps_sheet" for filter)
        
        Returns:
            Dictionary mapping sub-module names to their configurations.
            
        Raises:
            ValueError: If sub_modules is invalid type or sub-module not found in config
        """
        # Normalize sub_modules to a list
        if isinstance(sub_modules, str):
            sub_modules = [sub_modules]
        elif not isinstance(sub_modules, list):
            raise ValueError("sub_modules must be a string or list of strings")
        
        config = self._config_manager.get_config()
        
        # Get the input section for the module
        input_config = config.get('files', {}).get(module_name, {}).get('input', {})
        
        if not input_config:
            raise ValueError(f"Module '{module_name}' input section not found in config")
        
        sub_module_configs = {}
        
        for sub_module in sub_modules:
            if sub_module not in input_config:
                raise ValueError(f"Sub-module '{sub_module}' not found in module '{module_name}' input config")
            
            sub_module_config = input_config.get(sub_module).copy() if input_config.get(sub_module) else None
            if sub_module_config is None:
                raise ValueError(f"Sub-module '{sub_module}' config is None in module '{module_name}'")
            
            sub_module_configs[sub_module] = sub_module_config
        
        return sub_module_configs
    
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