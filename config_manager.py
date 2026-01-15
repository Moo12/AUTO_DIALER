"""
Configuration manager for updating config.yaml file.

This module provides functionality to update configuration values,
specifically for files.customers.input.sheet_1 and sheet_2.
"""

import yaml
import os
from pathlib import Path
from typing import Dict, Any, Optional, List
import sys


class ConfigManager:
    """
    Manages configuration file updates.
    
    Allows updating specific configuration sections, particularly
    files.customers.input.sheet_1 and sheet_2.
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize ConfigManager.
        
        Args:
            config_path: Path to config.yaml file. If None, checks environment variable
                         CONFIG_FILE_PATH, then uses default location.
        """
        if config_path is None:
            env_config_path = os.getenv('CONFIG_FILE_PATH')
            if env_config_path:
                config_path = env_config_path
            else:
                config_path = Path(__file__).parent / "config.yaml"
        
        self.config_path = Path(config_path)
        
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config file not found: {self.config_path}")
    
    def load_config(self) -> Dict[str, Any]:
        """
        Load configuration from YAML file.
        
        Returns:
            Dictionary containing the full configuration
            
        Raises:
            FileNotFoundError: If config file doesn't exist
            yaml.YAMLError: If config file is invalid
        """
        with open(self.config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        return config or {}
    
    def save_config(self, config: Dict[str, Any]) -> None:
        """
        Save configuration to YAML file.
        
        Args:
            config: Dictionary containing the full configuration
            
        Raises:
            IOError: If file cannot be written
        """
        # Create backup before saving
        # Use configurable backup directory (default: /tmp/config_backups)
        backup_dir = os.getenv('CONFIG_BACKUP_DIR', '/tmp/config_backups')
        os.makedirs(backup_dir, exist_ok=True)
        
        # Create backup filename with timestamp
        from datetime import datetime
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_filename = f"{self.config_path.stem}_{timestamp}.yaml.bak"
        backup_path = Path(backup_dir) / backup_filename
        
        if self.config_path.exists():
            import shutil
            try:
                shutil.copy2(self.config_path, backup_path)
                print(f"Backup created: {backup_path}", file=sys.stderr)
            except PermissionError as e:
                print(f"Warning: Could not create backup in {backup_dir}: {e}", file=sys.stderr)
                print(f"Attempting backup in /tmp instead...", file=sys.stderr)
                # Fallback to /tmp if configured directory fails
                backup_path = Path('/tmp') / backup_filename
                shutil.copy2(self.config_path, backup_path)
                print(f"Backup created: {backup_path}", file=sys.stderr)
        
        # Write updated config
        print(f"Config path: {self.config_path} to save: {config}", file=sys.stderr)
        with open(self.config_path, 'w', encoding='utf-8') as f:
            yaml.dump(config, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
        
        print(f"Configuration saved to {self.config_path}", file=sys.stderr)
    
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
        
        # Load current config
        config = self.load_config()
        
        # Ensure the structure exists
        if 'files' not in config:
            config['files'] = {}
        if 'customers' not in config['files']:
            config['files']['customers'] = {}
        if 'input' not in config['files']['customers']:
            config['files']['customers']['input'] = {}
        
        for sheet_name, sheet_config_item in sheet_config.items():
            config['files']['customers']['input'][sheet_name] = sheet_config_item
        

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
        
        config = self.load_config()

        sheet_configs = {}

        for sheet_name in sheets_names:
            if sheet_name not in config.get('files', {}).get('customers', {}).get('input', {}):
                raise ValueError(f"Sheet {sheet_name} not found in config")
            sheet_config = config.get('files', {}).get('customers', {}).get('input', {}).get(sheet_name)
            if sheet_config is None:
                raise ValueError(f"Sheet {sheet_name} not found in config")
            sheet_configs[sheet_name] = sheet_config
        
        return sheet_configs
    
    def update_and_save_customers_input_sheet(self, sheet_config: Dict[str, Any]) -> None:
        """
        Update and save configuration for customers input sheet.
        
        This is a convenience method that combines update and save operations.
        
        Args:
            sheet_name: Either 'sheet_1' or 'sheet_2'
            sheet_config: Dictionary with sheet configuration
        """
        config = self.update_customers_input_sheet(sheet_config)

        self.save_config(config)
    
    def get_all_customers_input_sheets(self) -> Dict[str, Any]:
        """
        Get all customers input sheet configurations.
        
        Returns:
            Dictionary with sheet_1 and sheet_2 configurations
        """
        config = self.load_config()
        return config.get('files', {}).get('customers', {}).get('input', {})


def update_customers_sheet_config( sheet_config: Dict[str, Any], config_path: Optional[str] = None) -> None:
    """
    Convenience function to update customers sheet configuration.
    
    Args:
        sheet_name: Either 'sheet_1' or 'sheet_2'
        sheet_config: Dictionary with sheet configuration
        config_path: Optional path to config file
    """
    manager = ConfigManager(config_path)
    manager.update_and_save_customers_input_sheet(sheet_config)


def get_customers_sheet_config(sheet_name: str, config_path: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Convenience function to get customers sheet configuration.
    
    Args:
        sheet_name: Either 'sheet_1' or 'sheet_2'
        config_path: Optional path to config file
        
    Returns:
        Dictionary with sheet configuration, or None if not found
    """
    manager = ConfigManager(config_path)
    return manager.get_customers_input_sheet(sheet_name)

