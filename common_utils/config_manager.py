"""
Configuration manager for updating config.yaml file.

This module provides functionality to update configuration values,
specifically for files.customers.input.sheet_1 and sheet_2.
"""

import yaml
import os
from pathlib import Path
from typing import Dict, Any
import sys


class ConfigManager:
    """
    Manages configuration file updates.
    
    Allows updating specific configuration sections, particularly
    files.customers.input.sheet_1 and sheet_2.
    """
    
    def __init__(self, config_path: str):
        """
        Initialize ConfigManager.
        
        Args:
            config_path: Path to config.yaml file. If None, checks environment variable
                         CONFIG_FILE_PATH, then uses default location.
        """
        
        
        self.config_path = Path(config_path)
        
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config file not found: {self.config_path}")
    
    def load(self) -> Dict[str, Any]:
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
    
    




