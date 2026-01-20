"""
Field Options Manager Module

Manages fetching distinct field values from database tables based on configuration.
Used for populating dropdown options and filters.
"""

import sys
import re
from pathlib import Path
from typing import Dict, Any, Optional, List
from common_utils.db_connection import DatabaseConnection
from common_utils.config_manager import ConfigManager


class FieldOptionsManager:
    """
    Manages fetching distinct field values from database tables.
    """
    
    def __init__(self, option_type: str, db_connection: DatabaseConnection, config_manager: ConfigManager):
        """
        Initialize Field Options Manager.
        
        Args:
            option_type: Type key that matches data_base_tables configuration (e.g., 'network_companies')
            db_connection: DatabaseConnection instance for MySQL operations
            config_manager: ConfigManager instance
        """
        self.db = db_connection
        self.option_type = option_type
        
        # Load configuration
        self.config_manager = config_manager
        self.config = self.config_manager.load()

        print(f"config: {self.config}")
        
        # Load table configurations from data_base_tables
        self.data_base_tables = self.config.get('data_base_tables', {}).get(option_type, {})
        
        if not self.data_base_tables:
            raise ValueError(
                f"Configuration not found for option_type '{option_type}'. "
                f"Please ensure it exists in data_base_tables section of config.yaml"
            )
    
    def get_field_options(self) -> Dict[str, Any]:
        """
        Get distinct values from the configured field.
        
        Returns:
            Dictionary with operation results:
                {
                    'success': bool,
                    'options': List[str],  # List of distinct field values
                    'error': Optional[str],
                    'error_type': Optional[str]
                }
        """
        try:
            # Get table_name and network_field_name from config
            table_name = self.data_base_tables.get('table_name')
            field_name = self.data_base_tables.get('network_field_name')
            
            if not table_name:
                return {
                    'success': False,
                    'options': [],
                    'error': f"table_name not found in configuration for option_type '{self.option_type}'",
                    'error_type': 'ConfigurationError'
                }
            
            if not field_name:
                return {
                    'success': False,
                    'options': [],
                    'error': f"network_field_name not found in configuration for option_type '{self.option_type}'",
                    'error_type': 'ConfigurationError'
                }
            
            # Validate table and field names contain only safe characters (alphanumeric, underscore)
            if not re.match(r'^[a-zA-Z0-9_]+$', table_name):
                return {
                    'success': False,
                    'options': [],
                    'error': f"Invalid table_name '{table_name}': must contain only alphanumeric characters and underscores",
                    'error_type': 'ConfigurationError'
                }
            
            if not re.match(r'^[a-zA-Z0-9_]+$', field_name):
                return {
                    'success': False,
                    'options': [],
                    'error': f"Invalid network_field_name '{field_name}': must contain only alphanumeric characters and underscores",
                    'error_type': 'ConfigurationError'
                }
            
            # Query distinct values from the field
            # Note: table_name and field_name are validated above and come from config, so safe to use in query
            query = f"SELECT DISTINCT `{field_name}` FROM `{table_name}` WHERE `{field_name}` IS NOT NULL ORDER BY `{field_name}`"
            
            results = self.db.execute_query(query)
            
            # Extract distinct values
            options = [row[field_name] for row in results if row.get(field_name) is not None]
            
            return {
                'success': True,
                'options': options,
                'error': None,
                'error_type': None
            }
            
        except Exception as e:
            error_type = type(e).__name__
            error_msg = str(e)
            print(f"✗ Error fetching field options: {error_msg}", file=sys.stderr)
            return {
                'success': False,
                'options': [],
                'error': error_msg,
                'error_type': error_type
            }

