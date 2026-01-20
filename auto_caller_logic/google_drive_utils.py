from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import os
import io
from pathlib import Path
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.http import MediaIoBaseUpload
import pickle
import sys
from datetime import datetime
from .config import _get_default_config

# Google API scopes - need both Drive and Sheets
SCOPES = [
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/spreadsheets'
]

class GDriveService:
    """Handles all interactions with Google Drive and Sheets APIs."""
    def __init__(self, credentials_config):
        
        token_path = credentials_config['pickle_file_path']
        credentials_path = credentials_config['credentials_file_path']

        self.credentials = self._authenticate(token_path, credentials_path)
        self.drive_service = self._get_service('drive', 'v3')
        self.sheets_service = self._get_service('sheets', 'v4')

    def _authenticate(self, token_path, credentials_path):
        # Try to load existing credentials
        creds = None
        if os.path.exists(token_path):
            with open(token_path, 'rb') as token_file:
                creds = pickle.load(token_file)
        
        # If there are no (valid) credentials available, let the user log in
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                if not os.path.exists(credentials_path):
                    raise FileNotFoundError(f"Credentials file not found at: {credentials_path}")
                flow = InstalledAppFlow.from_client_secrets_file(credentials_path, SCOPES)
                creds = flow.run_local_server(port=8080)
            
            # Save the credentials for the next run
            with open(token_path, 'wb') as token_file:
                pickle.dump(creds, token_file)
        
        return creds
    
    def _get_service(self, service_name, version):
        """Get a Google API service (Drive v3 or Sheets v4)."""
        return build(service_name, version, credentials=self.credentials, cache_discovery=False)

    def upload_excel(self, folder_id, file_name, excel_buffer):
        """
        Uploads an in-memory bytes buffer as a Google Sheet.
        
        Returns:
            str: The file ID of the uploaded Google Sheet
        """
        print(f"Uploading {file_name} to {folder_id}...", file=sys.stderr)
        
        media = MediaIoBaseUpload(
            excel_buffer, 
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            resumable=True
        )
        
        file_metadata = {
            'name': file_name,
            'parents': [folder_id],
            # This converts the Excel file into a native Google Sheet
            'mimeType': 'application/vnd.google-apps.spreadsheet' 
        }

        file = self.drive_service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        ).execute()
        
        file_id = file.get('id')
        print(f"✅ File uploaded. ID: {file_id}", file=sys.stderr)
        return file_id
    
    def get_latest_file_by_pattern(self, folder_id: str, file_name_pattern: str) -> Optional[str]:
        """
        Get the latest file from Google Drive folder that matches the filename pattern.
        
        The pattern may contain placeholders like {date} and {time}, which will be converted
        to regex patterns to match files.
        
        Args:
            folder_id: Google Drive folder ID
            file_name_pattern: Filename pattern (e.g., "CUSTOMERS_{date}_{time}.xlsx")
            
        Returns:
            File ID (str) of the latest matching file, or None if not found
        """
        import re
        from datetime import datetime
        
        print(f"Searching for files matching pattern '{file_name_pattern}' in folder {folder_id}...", file=sys.stderr)
        
        # Convert pattern to regex
        # Replace {date} with regex for date patterns (e.g., 25.12.2025 or 25-12-2025)
        # Replace {time} with regex for time patterns (e.g., 11:52 or 11-52)
        pattern_regex = file_name_pattern
        pattern_regex = re.escape(pattern_regex)  # Escape special characters first
        pattern_regex = pattern_regex.replace(r'\{date\}', r'\d{2}[.\-]\d{2}[.\-]\d{4}')  # Date pattern
        pattern_regex = pattern_regex.replace(r'\{time\}', r'\d{2}[:.\-]\d{2}')  # Time pattern
        
        # Compile regex pattern
        regex = re.compile(pattern_regex, re.IGNORECASE)
        
        # List files in folder
        query = f"'{folder_id}' in parents and trashed=false"
        results = self.drive_service.files().list(
            q=query,
            fields="files(id, name, createdTime, modifiedTime, mimeType)",
            orderBy="modifiedTime desc",
            pageSize=100
        ).execute()
        
        files = results.get('files', [])
        
        if not files:
            print(f"No files found in folder {folder_id}", file=sys.stderr)
            return None
        
        # Filter files by pattern
        matching_files = []
        for file in files:
            matching_files.append(file)
            #if regex.match(file['name']):
        
        if not matching_files:
            print(f"No files matching pattern '{file_name_pattern}' found", file=sys.stderr)
            return None
        
        # Get the latest file (already sorted by modifiedTime desc, so first one is latest)
        latest_file = matching_files[0]
        file_id = latest_file['id']
        
        
        print(f"✅ Found latest file: {latest_file['name']} (ID: {file_id})", file=sys.stderr)
        return file_id, latest_file['name']
    
    def add_formulas_to_sheet(self, spreadsheet_id, formulas):
        """
        Add formulas to a Google Sheet using Sheets API v4.
        
        Args:
            spreadsheet_id: The ID of the Google Sheet
            formulas: Dictionary mapping full ranges (sheet!cell) to formula strings
                     e.g., {'Sheet1!D2': '=ARRAYFORMULA(...)', 'Sheet1!E2': '=ARRAYFORMULA(...)'}
        """
        if not formulas:
            return
        
        print(f"📝 Adding {len(formulas)} formulas to sheet...", file=sys.stderr)
        
        # Prepare value input option
        value_input_option = 'USER_ENTERED'  # Formulas will be interpreted
        
        # Build the data for batch update
        # Formulas dictionary already contains full ranges as keys
        data = []
        for range_address, formula in formulas.items():
            data.append({
                'range': range_address,
                'values': [[formula]]
            })
        
        body = {
            'valueInputOption': value_input_option,
            'data': data
        }
        
        try:
            self.sheets_service.spreadsheets().values().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=body
            ).execute()
            print(f"✅ Formulas added successfully", file=sys.stderr)
        except Exception as e:
            print(f"⚠️  Error adding formulas: {e}", file=sys.stderr)
            raise

class BaseProcess(ABC):
    """Abstract base class for all file-creating processes."""
    def __init__(self, drive_service, config_name: str):
        self.name = config_name

        excel_workbooks_config = _get_default_config().get_excel_workbooks_config_by_name(self.name)

        # Initialize dictionary to store ExcelToGoogleWorkbook instances
        # Key is the workbook name from excel_workbooks_config (e.g., 'intermidiate', 'outo_dialer', 'filter')
        self.excel_to_google_workbook = {}
        
        # Iterate over dictionary items - workbook_name is the key from excel_workbooks_config
        for workbook_name, excel_workbook_config in excel_workbooks_config.items():
            # Use factory function to create appropriate class instance
            workbook_instance = create_excel_to_google_workbook(
                workbook_name=workbook_name,
                config=excel_workbook_config
            )


            # Store using the same key from excel_workbooks_config
            self.excel_to_google_workbook[workbook_name] = workbook_instance

        self.drive_service = drive_service
        
    @abstractmethod
    def generate_data(self, **kwargs):
        """The specific logic to create the file content."""
        pass

    def run(self, **kwargs):
        data = self.generate_data(**kwargs)
        
        # Ensure data is a dictionary
        if not isinstance(data, dict):
            raise ValueError(f"generate_data() must return a dictionary, got {type(data)}")

        file_ids = []
        for workbook_name, excel_to_google_workbook in self.excel_to_google_workbook.items():

            print(f"Creating excel file for {workbook_name}", file=sys.stderr)
            # Pass data as **kwargs to create_excel_file
            excel_buffer = excel_to_google_workbook.create_excel_file(**data)

            excel_to_google_workbook.save_excel_file(excel_buffer)

            file_id = self.drive_service.upload_excel(
                folder_id=excel_to_google_workbook.google_sheet_folder_id,
                file_name=excel_to_google_workbook.output_file_name,
                excel_buffer=excel_buffer,
            )


            if file_id is not None:
                file_ids.append(file_id)                
                # Check if workbook has formulas to add (e.g., FilterWorkbook)
                if hasattr(excel_to_google_workbook, '_formulas') and excel_to_google_workbook._formulas:
                    self.drive_service.add_formulas_to_sheet(
                        spreadsheet_id=file_id,
                        formulas=excel_to_google_workbook._formulas
                    )

        return file_ids


def create_excel_to_google_workbook(workbook_name: str, config: Dict[str, Any]):
    """
    Factory function to create appropriate ExcelToGoogleWorkbook subclass based on workbook name.
    
    Args:
        workbook_name: Name of the workbook (e.g., 'intermidiate', 'outo_dialer', 'filter')
        config: Configuration dictionary for the workbook
        
    Returns:
        Instance of appropriate ExcelToGoogleWorkbook subclass
    """

    from .workbooks.intermediate_workbook import IntermediateWorkbook
    from .workbooks.auto_dialer_workbook import AutoDialerWorkbook
    from .workbooks.filter_workbook import FilterWorkbook
    from .workbooks.base_workbook import BaseExcelToGoogleWorkbook
    # Get config values with fallbacks for optional fields
    google_folder_id = config.get('google_folder_id', '')
    excel_file_pattern = config.get('file_name_pattern')
    google_wb_name = config.get('google_wb_name', workbook_name)
    output_folder_path = config.get('output_folder_path', '')
    
    # Map workbook names to their corresponding classes
    workbook_class_map = {
        'intermidiate': IntermediateWorkbook,
        'auto_dialer': AutoDialerWorkbook,
        'filter': FilterWorkbook,
    }
    
    # Get the appropriate class, default to BaseExcelToGoogleWorkbook if not found
    workbook_class = workbook_class_map.get(workbook_name, BaseExcelToGoogleWorkbook)
    
    return workbook_class(
        google_sheet_folder_id=google_folder_id,
        excel_file_pattern=excel_file_pattern,
        google_wb_name=google_wb_name,
        output_folder_path=output_folder_path,
    )

