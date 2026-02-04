"""
Gaps actions file processor for entering callers into gaps sheets.

This module contains the GapsActionsFile class that processes callers_gap
from POST requests and inserts them into configured gaps sheets.
"""

import sys
from typing import Dict, Any, Optional, List
from .google_drive_utils import BaseProcess
from .config import _get_default_config
from common_utils.config_manager import ConfigManager

from .spreadsheet_updaters.gap_spreadsheet_updater import GapSpreadsheetUpdater
from .spreadsheet_updaters.base import BaseSpreadsheetUpdater
class GapsActionsFile(BaseProcess):
    """Process for entering callers into gaps sheets from POST request."""

    def __init__(
        self,
        drive_service,
        config_manager: ConfigManager,
        name: str,
        spreadsheet_updaters: List[BaseSpreadsheetUpdater],
        mail_service = None
    ):
        super().__init__(drive_service, config_manager, name, spreadsheet_updaters=spreadsheet_updaters, mail_service=mail_service)
        
        
        # Store metadata for gaps sheet insertion
        self._metadata = None
        self._callers_gap = None

    def generate_data(self, **kwargs):
        """
        Generate data - returns empty dict as all logic is in post_process_implementation.
        
        Args:
            **kwargs: May contain metadata (caller_id, date, time, customers_input_file, nick_name, start_date, end_date)
        """
        # Store metadata for use in post_process_implementation
        self._metadata = {
            'caller_id': kwargs.get('caller_id'),
            'date_str': kwargs.get('date_str'),
            'time_str': kwargs.get('time_str'),
            'customers_input_file_name': kwargs.get('customers_input_file_name'),
            'nick_name': kwargs.get('nick_name'),
        }
        # Store callers_gap for use in post_process
        self._callers_gap = kwargs.get('callers_gap', [])
        return {}

    def post_process_implementation(self, excel_info: Dict[str, Any]):
        """
        The specific logic to post process - insert callers_gap into gaps sheets.
        
        Args:
            excel_info: Dictionary containing information about created Excel files (unused here)
        
        Returns:
            Dictionary with callers_gap and metadata
        """
        callers_gap = self._callers_gap
        print(f"Processing callers gap from POST request. len of callers gap: {len(callers_gap)}", file=sys.stderr)

        return {'callers_gap': callers_gap, "metadata": self._metadata}


def create_gaps_actions_google_manager(config_manager: ConfigManager):
    """Factory function to create GapsActionsFile instance."""
    from .gaps_actions_file import GapsActionsFile
    from .google_drive_utils import GDriveService
    from .mail_service import create_mail_service

    module_name = 'gaps_actions'
    config = _get_default_config(config_manager)
    print(f"Config created", file=sys.stderr)
    service_config = config.get_service_config()
    drive_service = GDriveService(service_config)

    print(f"Drive service created", file=sys.stderr)

    output_files_config = config.get_output_files_config(module_name)

    spreadsheet_updaters = []

    for sheet_name_key, sheet_config in output_files_config.items():
        spreadsheet_updater = GapSpreadsheetUpdater(drive_service, sheet_config, sheet_name_key)
        spreadsheet_updaters.append(spreadsheet_updater)

    print(f"Gaps actions output files config: {output_files_config}", file=sys.stderr)

    # Create mail service if mail config exists
    mail_config = config.get_mail_config_by_name(module_name)
    mail_service = create_mail_service(module_name, mail_config, service_config)

    return GapsActionsFile(drive_service, config_manager, module_name, spreadsheet_updaters, mail_service)

