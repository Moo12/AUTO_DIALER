"""
Base workbook class for Excel to Google Workbook converters.
"""

from abc import ABC, abstractmethod
import os
import io
import sys
from datetime import datetime


class ExcelToGoogleWorkbook(ABC):
    """Base class for Excel to Google Workbook converters."""
    
    def __init__(self, google_sheet_folder_id: str, excel_file_pattern: str, google_wb_name: str):
        self.google_sheet_folder_id = google_sheet_folder_id
        self.excel_file_pattern = excel_file_pattern
        self.google_wb_name = google_wb_name

        print(f"Google Sheet Folder ID: {self.google_sheet_folder_id}", file=sys.stderr)
        print(f"Excel File Pattern: {self.excel_file_pattern}", file=sys.stderr)
        print(f"Google WB Name: {self.google_wb_name}", file=sys.stderr)

        date_str = datetime.now().strftime("%d.%m.%Y")
        time_str = datetime.now().strftime("%H.%M")
        
        self.output_file_name = self.excel_file_pattern.format(date=date_str, time=time_str)
    
    def post_excel_file_creation(self, **kwargs):
        """
        Post Excel file creation hook.
        
        Called after all initial Excel files have been created and uploaded to Google Drive.
        This allows workbooks to create additional Excel files based on data from post_process().
        
        Args:
            **kwargs: Data returned from BaseProcess.post_process() method.
                     This typically contains processed data from the initial Excel files
                     (e.g., {'callers_gap': [...]}).
        
        Returns:
            BytesIO buffer containing the Excel file, or None if no post-excel file should be created.
            If a buffer is returned, it will be automatically uploaded to Google Drive if
            google_sheet_folder_id is configured.
        """
        return None
    
    @abstractmethod
    def create_excel_file(self, **kwargs):
        """Create Excel file from data. Must be implemented by subclasses."""
        raise NotImplementedError("create_excel_file() must be implemented by subclass")


class BaseExcelToGoogleWorkbook(ExcelToGoogleWorkbook):
    """Default implementation for ExcelToGoogleWorkbook."""
    
    def create_excel_file(self, **kwargs):
        """Default implementation - raises NotImplementedError."""
        raise NotImplementedError("create_excel_file() not yet implemented for this workbook type")

