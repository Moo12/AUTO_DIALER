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
    
    def __init__(self, google_sheet_folder_id: str, excel_file_pattern: str, google_wb_name: str, output_folder_path: str):
        self.google_sheet_folder_id = google_sheet_folder_id
        self.excel_file_pattern = excel_file_pattern
        self.google_wb_name = google_wb_name
        self.output_local_folder_path = output_folder_path

        print(f"Google Sheet Folder ID: {self.google_sheet_folder_id}", file=sys.stderr)
        print(f"Excel File Pattern: {self.excel_file_pattern}", file=sys.stderr)
        print(f"Google WB Name: {self.google_wb_name}", file=sys.stderr)
        print(f"Output Folder Path: {self.output_local_folder_path}", file=sys.stderr)

        date_str = datetime.now().strftime("%d.%m.%Y")
        time_str = datetime.now().strftime("%H.%M")
        
        self.output_file_name = self.excel_file_pattern.format(date=date_str, time=time_str)
    
    @abstractmethod
    def create_excel_file(self, **kwargs):
        """Create Excel file from data. Must be implemented by subclasses."""
        raise NotImplementedError("create_excel_file() must be implemented by subclass")

    def save_excel_file(self, excel_buffer: io.BytesIO):
        """Save Excel buffer to local file system."""
        if self.output_local_folder_path:
            os.makedirs(self.output_local_folder_path, exist_ok=True)
            self.output_file_path = os.path.join(self.output_local_folder_path, self.output_file_name)
            with open(self.output_file_path, 'wb') as f:
                f.write(excel_buffer.getvalue())
            return self.output_file_path
        else:
            return None


class BaseExcelToGoogleWorkbook(ExcelToGoogleWorkbook):
    """Default implementation for ExcelToGoogleWorkbook."""
    
    def create_excel_file(self, **kwargs):
        """Default implementation - raises NotImplementedError."""
        raise NotImplementedError("create_excel_file() not yet implemented for this workbook type")

