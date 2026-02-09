"""
Gmail service for sending emails using Google Gmail API.

This module provides a singleton GmailService class that handles authentication
and email sending functionality.
"""

import os
import pickle
import sys
import smtplib
from typing import Dict, Any, Optional, List
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from email.utils import formataddr
import base64

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# Gmail API scope
GMAIL_SCOPE = ['https://www.googleapis.com/auth/gmail.send']


class GmailService:
    """
    Singleton Gmail service for sending emails via Google Gmail API.
    
    Handles authentication on initialization and reuses the same service
    instance to avoid re-authenticating for each email send.
    """
    
    _instance: Optional['GmailService'] = None
    _initialized: bool = False
    
    def __new__(cls, credentials_config: Optional[Dict[str, Any]] = None):
        """
        Singleton pattern: return existing instance if available.
        
        Args:
            credentials_config: Dictionary with 'pickle_file_path' and 'credentials_file_path'
                              Required only on first initialization
        """
        if cls._instance is None:
            if credentials_config is None:
                raise ValueError("credentials_config is required for first initialization")
            cls._instance = super(GmailService, cls).__new__(cls)
        return cls._instance
    
    def __init__(self, credentials_config: Optional[Dict[str, Any]] = None):
        """
        Initialize Gmail service and authenticate.
        
        Args:
            credentials_config: Dictionary with:
                - 'pickle_file_path': Path to token pickle file
                - 'credentials_file_path': Path to OAuth2 credentials JSON file
        """
        # Only initialize once (singleton pattern)
        if GmailService._initialized:
            return
        
        if credentials_config is None:
            raise ValueError("credentials_config is required for initialization")
        
        token_path = credentials_config.get('pickle_file_path')
        credentials_path = credentials_config.get('credentials_file_path')
        
        if not token_path or not credentials_path:
            raise ValueError("Both 'pickle_file_path' and 'credentials_file_path' are required in credentials_config")
        
        self.credentials = self._authenticate(token_path, credentials_path)
        self.gmail_service = self._get_service('gmail', 'v1')
        
        GmailService._initialized = True
        print(f"✅ Gmail service initialized and authenticated", file=sys.stderr)
    
    def _authenticate(self, token_path: str, credentials_path: str) -> Credentials:
        """
        Authenticate with Google Gmail API.
        
        Args:
            token_path: Path to token pickle file
            credentials_path: Path to OAuth2 credentials JSON file
            
        Returns:
            Authenticated credentials object
        """
        creds = None
        
        # Try to load existing credentials
        if os.path.exists(token_path):
            try:
                with open(token_path, 'rb') as token_file:
                    creds = pickle.load(token_file)
            except Exception as e:
                print(f"⚠️  Warning: Could not load existing token: {e}", file=sys.stderr)
        
        # If there are no (valid) credentials available, let the user log in
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                print("🔄 Refreshing expired credentials...", file=sys.stderr)
                try:
                    creds.refresh(Request())
                except Exception as e:
                    print(f"⚠️  Warning: Could not refresh credentials: {e}", file=sys.stderr)
                    creds = None
            
            if not creds:
                if not os.path.exists(credentials_path):
                    raise FileNotFoundError(f"Credentials file not found at: {credentials_path}")
                
                print("🔐 Starting OAuth2 flow for Gmail API...", file=sys.stderr)
                flow = InstalledAppFlow.from_client_secrets_file(credentials_path, GMAIL_SCOPE)
                # Use port 8081 to avoid conflict with FastAPI server on 8080
                # If port is still in use, try run_console() for non-interactive auth
                try:
                    creds = flow.run_local_server(port=8081, open_browser=False)
                except OSError as e:
                    if "Address already in use" in str(e):
                        print("⚠️  Port 8081 also in use, trying console-based authentication...", file=sys.stderr)
                        creds = flow.run_console()
                    else:
                        raise
            
            # Save the credentials for the next run
            try:
                os.makedirs(os.path.dirname(token_path), exist_ok=True)
                with open(token_path, 'wb') as token_file:
                    pickle.dump(creds, token_file)
                print(f"✅ Credentials saved to {token_path}", file=sys.stderr)
            except Exception as e:
                print(f"⚠️  Warning: Could not save credentials: {e}", file=sys.stderr)
        
        return creds
    
    def _get_service(self, service_name: str, version: str):
        """
        Get a Google API service.
        
        Args:
            service_name: Name of the service (e.g., 'gmail')
            version: API version (e.g., 'v1')
            
        Returns:
            Google API service object
        """
        return build(service_name, version, credentials=self.credentials, cache_discovery=False)
    
    def send_email(
        self,
        to: str,
        subject: str,
        body: str,
        body_html: Optional[str] = None,
        cc: Optional[List[str]] = None,
        bcc: Optional[List[str]] = None,
        attachments: Optional[List[Dict[str, Any]]] = None,
        from_email: Optional[str] = None,
        from_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Send an email via Gmail API.
        
        Args:
            to: Recipient email address (required)
            subject: Email subject (required)
            body: Plain text email body (required if body_html not provided)
            body_html: HTML email body (optional, will use body if not provided)
            cc: List of CC email addresses (optional)
            bcc: List of BCC email addresses (optional)
            attachments: List of attachment dictionaries with:
                - 'filename': Name of the file
                - 'content': File content as bytes
                - 'mime_type': MIME type (optional, defaults to 'application/octet-stream')
            from_email: Sender email address (optional, uses authenticated user's email if not provided)
            
        Returns:
            Dictionary with 'message_id' and 'thread_id' from Gmail API response
            
        Raises:
            ValueError: If required parameters are missing
            RuntimeError: If email sending fails
        """
        if not to:
            raise ValueError("'to' email address is required")
        if not subject:
            raise ValueError("'subject' is required")
        if not body and not body_html:
            raise ValueError("Either 'body' or 'body_html' is required")
        
        try:
            # Create message
            message = MIMEMultipart('alternative')
            message['to'] = to
            message['subject'] = subject
            
            if cc:
                message['cc'] = ', '.join(cc)
            if bcc:
                message['bcc'] = ', '.join(bcc)
            if from_email:
                message['from'] = formataddr((from_name, from_email))
            
            # Add plain text body
            if body:
                text_part = MIMEText(body, 'plain', 'utf-8')
                message.attach(text_part)
            
            # Add HTML body
            if body_html:
                html_part = MIMEText(body_html, 'html', 'utf-8')
                message.attach(html_part)
            elif not body:
                # If only HTML provided, use it as plain text too
                html_part = MIMEText(body_html, 'html', 'utf-8')
                message.attach(html_part)
            
            # Add attachments
            if attachments:
                for attachment in attachments:
                    filename = attachment.get('filename')
                    content = attachment.get('content')
                    mime_type = attachment.get('mime_type', 'application/octet-stream')
                    
                    if not filename or content is None:
                        print(f"⚠️  Warning: Skipping attachment with missing filename or content", file=sys.stderr)
                        continue
                    
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(content)
                    encoders.encode_base64(part)
                    part.add_header(
                        'Content-Disposition',
                        f'attachment; filename= {filename}'
                    )
                    message.attach(part)
            
            # Encode message
            raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode('utf-8')
            
            # Send message
            send_message = {
                'raw': raw_message
            }
            
            result = self.gmail_service.users().messages().send(
                userId='me',
                body=send_message
            ).execute()
            
            message_id = result.get('id')
            thread_id = result.get('threadId')
            
            print(f"✅ Email sent successfully to {to} (Message ID: {message_id})", file=sys.stderr)
            
            return {
                'message_id': message_id,
                'thread_id': thread_id,
                'success': True
            }
            
        except Exception as e:
            error_msg = f"Failed to send email to {to}: {str(e)}"
            print(f"❌ {error_msg}", file=sys.stderr)
            raise RuntimeError(error_msg) from e
    
    @classmethod
    def reset_instance(cls):
        """
        Reset the singleton instance (useful for testing or re-authentication).
        """
        cls._instance = None
        cls._initialized = False


class SMTPService:
    """
    SMTP service for sending emails via SMTP protocol.
    
    Provides the same interface as GmailService but uses SMTP instead of Gmail API.
    Useful for cases where SMTP is preferred or when Gmail API is not available.
    """
    
    def __init__(self, smtp_config: Dict[str, Any]):
        """
        Initialize SMTP service.
        
        Args:
            smtp_config: Dictionary with SMTP configuration:
                - 'smtp_server': SMTP server address (e.g., 'smtp.gmail.com')
                - 'smtp_port': SMTP port (e.g., 587 for TLS, 465 for SSL)
                - 'smtp_username': SMTP username/email
                - 'smtp_password': SMTP password or app password
                - 'use_tls': Boolean, whether to use TLS (default: True)
                - 'use_ssl': Boolean, whether to use SSL (default: False, for port 465)
        """
        self.smtp_server = smtp_config.get('smtp_server')
        self.smtp_port = smtp_config.get('smtp_port', 587)
        self.smtp_username = smtp_config.get('smtp_username')
        self.smtp_password = os.getenv(smtp_config.get('smtp_password_env_var'), None)
        self.use_tls = smtp_config.get('use_tls', True)
        self.use_ssl = smtp_config.get('use_ssl', False)
        
        if not self.smtp_server:
            raise ValueError("'smtp_server' is required in smtp_config")
        if not self.smtp_username:
            raise ValueError("'smtp_username' is required in smtp_config")
        if not self.smtp_password:
            raise ValueError("'smtp_password' is required in smtp_config")
        
        print(f"✅ SMTP service initialized for {self.smtp_server}:{self.smtp_port}", file=sys.stderr)
    
    def send_email(
        self,
        to: str,
        subject: str,
        body: Optional[str] = None,
        body_html: Optional[str] = None,
        cc: Optional[List[str]] = None,
        bcc: Optional[List[str]] = None,
        attachments: Optional[List[Dict[str, Any]]] = None,
        from_email: Optional[str] = None,
        from_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Send an email via SMTP.
        
        Args:
            to: Recipient email address (required)
            subject: Email subject (required)
            body: Plain text email body (required if body_html not provided)
            body_html: HTML email body (optional, will use body if not provided)
            cc: List of CC email addresses (optional)
            bcc: List of BCC email addresses (optional)
            attachments: List of attachment dictionaries with:
                - 'filename': Name of the file
                - 'content': File content as bytes
                - 'mime_type': MIME type (optional, defaults to 'application/octet-stream')
            from_email: Sender email address (optional, uses smtp_username if not provided)
            from_name: Sender display name (optional)
            
        Returns:
            Dictionary with 'success': True and 'message': Success message
            
        Raises:
            ValueError: If required parameters are missing
            RuntimeError: If email sending fails
        """
        if not to:
            raise ValueError("'to' email address is required")
        if not subject:
            raise ValueError("'subject' is required")
        if not body and not body_html:
            raise ValueError("Either 'body' or 'body_html' is required")
        
        # Use smtp_username as default from_email if not provided
        sender_email = from_email or self.smtp_username
        
        try:
            # Create message
            message = MIMEMultipart('alternative')
            message['To'] = to
            message['Subject'] = subject
            
            if cc:
                message['Cc'] = ', '.join(cc)
            if bcc:
                message['Bcc'] = ', '.join(bcc)
            
            # Set From header with optional display name
            if from_name:
                message['From'] = formataddr((from_name, sender_email))
            else:
                message['From'] = sender_email
            
            # Add plain text body
            if body:
                text_part = MIMEText(body, 'plain', 'utf-8')
                message.attach(text_part)
            
            # Add HTML body
            if body_html:
                html_part = MIMEText(body_html, 'html', 'utf-8')
                message.attach(html_part)
            elif not body:
                # If only HTML provided, use it as plain text too
                html_part = MIMEText(body_html, 'html', 'utf-8')
                message.attach(html_part)
            
            # Add attachments
            if attachments:
                for attachment in attachments:
                    filename = attachment.get('filename')
                    content = attachment.get('content')
                    mime_type = attachment.get('mime_type', 'application/octet-stream')
                    
                    if not filename or content is None:
                        print(f"⚠️  Warning: Skipping attachment with missing filename or content", file=sys.stderr)
                        continue
                    
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(content)
                    encoders.encode_base64(part)
                    part.add_header(
                        'Content-Disposition',
                        f'attachment; filename= {filename}'
                    )
                    message.attach(part)
            
            # Collect all recipients (to + cc + bcc)
            recipients = [to]
            if cc:
                recipients.extend(cc)
            if bcc:
                recipients.extend(bcc)
            
            # Connect to SMTP server and send
            if self.use_ssl:
                # Use SSL (for port 465)
                server = smtplib.SMTP_SSL(self.smtp_server, self.smtp_port)
            else:
                # Use TLS (for port 587)
                server = smtplib.SMTP(self.smtp_server, self.smtp_port)
                if self.use_tls:
                    server.starttls()
            
            # Login and send
            server.login(self.smtp_username, self.smtp_password)
            server.sendmail(sender_email, recipients, message.as_string())
            server.quit()
            
            print(f"✅ Email sent successfully via SMTP to {to} (from {sender_email})", file=sys.stderr)
            
            return {
                'success': True,
                'message': f'Email sent successfully to {to}',
                'from': sender_email,
                'to': to
            }
            
        except Exception as e:
            error_msg = f"Failed to send email via SMTP to {to}: {str(e)}"
            print(f"❌ {error_msg}", file=sys.stderr)
            raise RuntimeError(error_msg) from e

