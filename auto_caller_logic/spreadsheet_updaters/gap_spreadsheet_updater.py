"""
Gap spreadsheet updater implementation.

This module provides the concrete implementation for updating gaps spreadsheets
with callers_gap data and metadata.
"""

import sys
from typing import Dict, Any, List, Optional, Set
from .base import BaseSpreadsheetUpdater


class GapSpreadsheetUpdater(BaseSpreadsheetUpdater):
    """
    Concrete implementation for updating gaps spreadsheets.
    
    Handles insertion of callers_gap data into gaps sheets with metadata
    (caller_id, date, time, customers_input_file, nick_name).
    """
    
    def update_spreadsheets(self, **kwargs) -> Dict[str, Any]:
        """
        Update the gaps spreadsheet configured in spreadsheet_config.
        
        Each derived class handles its own data processing from kwargs.
        
        Args:
            **kwargs: Arguments specific to gaps spreadsheet update
        
        Returns:
            Dictionary with summary information:
            {
                'success': bool,
                'error': Optional[str]
            }
        """
        wb_id = self.spreadsheet_config.get('wb_id')
        sheet_id = self.spreadsheet_config.get('sheet_id')
        
        print(f"📝 Processing spreadsheet (wb_id: {wb_id}, sheet_id: {sheet_id})", file=sys.stderr)
        try:
            self._update_single_spreadsheet(**kwargs)
            print(f"✅ Successfully updated spreadsheet", file=sys.stderr)
            return {
                'success': True,
                'error': None
            }
        except Exception as e:
            error_msg = str(e)
            print(f"❌ Error updating spreadsheet: {error_msg}", file=sys.stderr)
            return {
                'success': False,
                'error': error_msg
            }
    
    def _update_single_spreadsheet(self, **kwargs) -> None:
        """
        Update the spreadsheet.
        
        Args:
            **kwargs: Arguments specific to gaps spreadsheet update
        
        Raises:
            ValueError: If required config fields are missing
            RuntimeError: If update fails
        """
        # Get sheet name from sheet_id for range construction
        sheet_name = self._get_sheet_name_from_id()
        
        # New behavior: insert new data at row 2 (below header) and shift existing rows down.
        insert_at_row = 2

        # Step 1: Prepare data for insertion at row 2
        batch_updates = self._prepare_batch_updates(sheet_name, insert_at_row, **kwargs)
        if not batch_updates:
            print("ℹ️  No data to insert", file=sys.stderr)
            return

        # Step 2: Insert required number of rows so we don't overwrite existing content
        rows_to_insert = len(batch_updates[0].get('values', []))
        if rows_to_insert <= 0:
            print("ℹ️  No rows to insert", file=sys.stderr)
            return

        # If space_row is enabled, insert one extra blank row AFTER the inserted block
        # (acts as a separator between the new data and the previous content).
        space_row = self.spreadsheet_config.get('space_row') is True
        total_rows_to_insert = rows_to_insert + (1 if space_row else 0)

        self._insert_rows(insert_at_row, total_rows_to_insert)
        if space_row:
            print(
                f"📝 space_row enabled: inserted {total_rows_to_insert} rows at row {insert_at_row} "
                f"({rows_to_insert} data rows + 1 separator)",
                file=sys.stderr
            )
        else:
            print(f"📝 Inserted {total_rows_to_insert} rows at row {insert_at_row}", file=sys.stderr)

        # Step 3: Execute batch update into the newly inserted rows (row 2..)
        self._execute_batch_update(batch_updates)
        
        print(f"✅ Data inserted successfully", file=sys.stderr)
    
    def _prepare_batch_updates(
        self,
        sheet_name: str,
        start_row: int,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """
        Prepare batch update data for gaps sheets.
        
        Format:
        - Column A: caller gap value
        - Column B: empty (untouched)
        - Column C (or start_column_gap_info): caller_display (nick_name or caller_id)
        - Column D (or start_column_gap_info + 1): caller_id
        - Column E (or start_column_gap_info + 2): date
        - Column F (or start_column_gap_info + 3): time
        - Column G (or start_column_gap_info + 4): customers_input_file
        
        Args:
            sheet_name: Name of the sheet
            start_row: Starting row number (1-indexed)
            **kwargs: Arguments specific to gaps spreadsheet update
        
        Returns:
            List of dictionaries for batchUpdate API
        """
        # Each derived class handles its own data extraction from kwargs
        # This implementation processes gaps-specific data
        data_items = kwargs.get('callers_gap', [])
        metadata = kwargs.get('metadata', {})
        filter_items = kwargs.get('filter_items', None)
        
        # Normalize data items
        normalized_items = [str(item).strip() for item in data_items]
        
        # Filter items if filter_items provided
        filtered_items = normalized_items
        if filter_items is not None:
            filtered_items = [
                item for item in normalized_items
                if str(item).strip() not in filter_items
            ]
        
        if not filtered_items:
            return []
        
        # Extract metadata
        date = metadata.get('date_str', '')
        time = metadata.get('time_str', '')
        customers_input_file = metadata.get('customers_input_file_name', '')
        caller_id = metadata.get('caller_id', '')
        nick_name = metadata.get('nick_name', '')
        
        # Use nick_name if available, otherwise fall back to caller_id
        caller_display = nick_name
        
        # Get start column for gap info from config (default: 'C')
        start_column_gap_info = self.spreadsheet_config.get('start_column_gap_info', 'C')
        start_col_letter = start_column_gap_info.upper()
        
        # Calculate end column for gap info (5 columns total: caller_display, caller_id, date, time, customers_input_file)
        end_col_letter = self._get_column_letter_offset(start_col_letter, 4)
        
        # Prepare column A values (caller gap)
        column_a_values = [[gap] for gap in filtered_items]
        
        # Format caller_id as text to preserve leading zeros (e.g., 0522574817)
        caller_id_text = f"'{caller_id}" if caller_id else caller_id
        
        # Format time as text to preserve leading zeros (e.g., 09:05 instead of 9:5)
        time_text = f"'{str(time)}" if time else ''
        
        # Prepare gap info columns
        gap_info_values = []
        for gap in filtered_items:
            gap_info_values.append([
                caller_display,         # First column: nick_name or caller_id
                caller_id_text,        # Second column: caller_id (formatted as text)
                date,                   # Third column: date
                time_text,              # Fourth column: time (formatted as text)
                customers_input_file    # Fifth column: customers_input_file
            ])
        
        # Calculate end row
        end_row = start_row + len(filtered_items) - 1
        
        # Prepare batch update with two ranges: A and gap info columns
        updates = [
            {
                'range': f"'{sheet_name}'!A{start_row}:A{end_row}",
                'values': column_a_values
            },
            {
                'range': f"'{sheet_name}'!{start_col_letter}{start_row}:{end_col_letter}{end_row}",
                'values': gap_info_values
            }
        ]
        
        return updates

