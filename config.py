"""
Configuration management for dial file generator.
"""

import os
import yaml
import sys
from pathlib import Path
from typing import Dict, Any, Optional


class Config:
    """
    Configuration manager for dial file generator.
    
    Handles loading and accessing configuration from YAML files.
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize Config instance.
        
        Args:
            config_path: Path to config file. If None, checks environment variable
                         CONFIG_FILE_PATH, then defaults to config.yaml in project root.
        """
        self._config: Optional[Dict[str, Any]] = None
        # Check environment variable first, then use provided path, then default
        if config_path is None:
            env_config_path = os.getenv('CONFIG_FILE_PATH')
            if env_config_path:
                config_path = env_config_path
            else:
                config_path = Path(__file__).parent / "config.yaml"
        if config_path:
            self.load(config_path)
    
    def load(self, config_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Load configuration from YAML file.
        
        Args:
            config_path: Path to config file. If None, looks for config.yaml in project root.
            
        Returns:
            Dictionary containing configuration
            
        Raises:
            FileNotFoundError: If config file doesn't exist
            yaml.YAMLError: If config file is invalid
        """
        print(f"Loading config from: {config_path}", file=sys.stderr)
        if config_path is None:
            # Look for config.yaml in project root
            project_root = Path(__file__).parent.parent
            config_path = project_root / "config.yaml"
        
        config_path = Path(config_path)
        
        if not config_path.exists():
            raise FileNotFoundError(
                f"Config file not found: {config_path}. "
                f"Please copy config.example.yaml to config.yaml and fill in your settings."
            )
        
        with open(config_path, 'r', encoding='utf-8') as f:
            self._config = yaml.safe_load(f)
        
        # Validate required config sections
        required_sections = ['files']
        for section in required_sections:
            if section not in self._config:
                raise ValueError(f"Missing required config section: {section}")
        
        return self._config
    
    def get_config(self) -> Dict[str, Any]:
        """
        Get loaded configuration.
        
        Returns:
            Dictionary containing configuration
            
        Raises:
            RuntimeError: If config hasn't been loaded yet
        """
        if self._config is None:
            raise RuntimeError(
                "Configuration not loaded. Call load() first."
            )
        return self._config
    
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
        if name_method not in self.get_config().get('files', {}):
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

# Default singleton instance for backward compatibility
_default_config: Optional[Config] = None


def _get_default_config(config_path: Optional[str] = None) -> Config:
    """
    Get or create the default config instance.
    
    Args:
        config_path: Optional path to config file. If None, checks environment variable
                     CONFIG_FILE_PATH, then defaults to "config.yaml" in project root.
    
    Returns:
        The default Config instance
    """
    global _default_config
    
    # Check environment variable first, then use provided path, then default
    if config_path is None:
        config_path = os.getenv('CONFIG_FILE_PATH', 'config.yaml')
    
    if _default_config is None:
        _default_config = Config(config_path)
    else:
        _default_config.load(config_path)
    return _default_config