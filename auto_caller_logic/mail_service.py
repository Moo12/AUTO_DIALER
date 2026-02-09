"""
Mail service wrapper for email services.

This module provides a base MailService class and derived classes for Gmail API and SMTP.
"""

import sys
import re
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List, Union
from common_utils.gmail_service import GmailService
from common_utils.gmail_service import SMTPService


class MailService(ABC):
    """
    Abstract base class for mail services.
    
    Provides common functionality for sending emails with placeholder replacement.
    Derived classes implement the actual email sending logic.
    """
    
    def __init__(self, service_config: Dict[str, Any], name: str, mail_config: Dict[str, Any]):
        """
        Initialize MailService base class.
        
        Args:
            service_config: Service configuration dictionary
            name: Process name (e.g., 'filter', 'gaps_actions')
            mail_config: Mail configuration dictionary from config
        """
        self.mail_config = mail_config
        self.name = name
    
    def _replace_placeholders(self, text: str, data: Dict[str, Any]) -> str:
        """
        Replace placeholders in text with values from data dictionary.
        Placeholders are in the format {{key_name}} (double curly braces).
        
        Args:
            text: Text with placeholders (e.g., "Hello {{name}}, date: {{date}}")
            data: Dictionary with values to replace placeholders
            
        Returns:
            Text with placeholders replaced
        """
        if not text:
            return text
        
        def replace_func(match):
            key = match.group(1)
            return str(data.get(key, match.group(0)))  # Return original if key not found
        
        return re.sub(r'\{\{([^}]+)\}\}', replace_func, text)
    
    def _prepare_email_data(self, mail_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepare email data from mail_config and mail_data.
        
        Args:
            mail_data: Dictionary with values for placeholder replacement
            
        Returns:
            Dictionary with prepared email data:
                - to_email: str
                - subject: str
                - body: str
                - body_html: Optional[str]
                - cc_emails: Optional[List[str]]
                - from_email: str
                - from_name: str
        """
        # Extract and replace placeholders in mail config
        # Support both old format (title, subtitle, body) and new format (mail_subject, mail_body)
        mail_subject = self.mail_config.get('subject', '')
        mail_body = self.mail_config.get('body', '')
        title = self.mail_config.get('title', mail_subject)  # Fallback to mail_subject if title not present
        subtitle = self.mail_config.get('subtitle', '')
        body = self.mail_config.get('body', mail_body)  # Fallback to mail_body if body not present
        body_html = self.mail_config.get('body_html', None)
        recipients = self.mail_config.get('recipients', [])
        cc_recipients = self.mail_config.get('cc_recipients', [])

        # Replace placeholders
        title = self._replace_placeholders(title, mail_data)
        subtitle = self._replace_placeholders(subtitle, mail_data)
        body = self._replace_placeholders(body, mail_data)
        if body_html:
            body_html = self._replace_placeholders(body_html, mail_data)
        
        # Parse recipients (can be comma-separated string or list)
        if isinstance(recipients, str):
            recipient_list = [r.strip() for r in recipients.split(',') if r.strip()]
        elif isinstance(recipients, list):
            recipient_list = [r.strip() if isinstance(r, str) else str(r) for r in recipients if r]
        else:
            raise ValueError("Invalid recipients format")
        
        if not recipient_list:
            raise ValueError("No recipients specified")
        
        # Parse cc_recipients (can be comma-separated string or list)
        cc_emails = None
        if cc_recipients:
            if isinstance(cc_recipients, str):
                cc_emails = [r.strip() for r in cc_recipients.split(',') if r.strip()]
            elif isinstance(cc_recipients, list):
                cc_emails = [r.strip() if isinstance(r, str) else str(r) for r in cc_recipients if r]
        
        # Use first recipient as 'to' (send_email expects a string, not a list)
        to_email = recipient_list[0]
        
        # Combine title and subtitle for subject, or use mail_subject if available
        if mail_subject:
            subject = self._replace_placeholders(mail_subject, mail_data)
        elif subtitle:
            subject = f"{title} - {subtitle}"
        else:
            subject = title
        
        from_email = self.mail_config.get('from_email', None)
        from_name = self.mail_config.get('from_name', None)

        if not from_email or not from_name:
            raise ValueError("From email or from name is not set")
        
        return {
            'to_email': to_email,
            'subject': subject,
            'body': body,
            'body_html': body_html,
            'cc_emails': cc_emails,
            'from_email': from_email,
            'from_name': from_name
        }
    
    @abstractmethod
    def _send_email_impl(self, to_email: str, subject: str, body: str, body_html: Optional[str], 
                        cc_emails: Optional[List[str]], from_email: str, from_name: str) -> None:
        """
        Abstract method to send email. Must be implemented by derived classes.
        
        Args:
            to_email: Recipient email address
            subject: Email subject
            body: Plain text email body
            body_html: HTML email body (optional)
            cc_emails: List of CC email addresses (optional)
            from_email: Sender email address
            from_name: Sender display name
        """
        pass
    
    def send_mail(self, **kwargs) -> None:
        """
        Send email using mail config and mail data.
        
        Args:
            **kwargs: Should contain 'mail_data' dictionary with values for placeholder replacement
        """
        if not self.mail_config:
            return

        if self.mail_config.get('should_send_mail', True) is False:
            print(f"ℹ️  Mail sending is disabled (should_send_mail: false), skipping email", file=sys.stderr)
            return
        
        # Get mail data from kwargs
        mail_data = kwargs.get('mail_data', {})
        
        try:
            # Prepare email data
            email_data = self._prepare_email_data(mail_data)
            
            # Send email using derived class implementation
            self._send_email_impl(
                to_email=email_data['to_email'],
                subject=email_data['subject'],
                body=email_data['body'],
                body_html=email_data['body_html'],
                cc_emails=email_data['cc_emails'],
                from_email=email_data['from_email'],
                from_name=email_data['from_name']
            )
            
            print(f"✅ Email sent successfully to {email_data['to_email']}", file=sys.stderr)
        except ValueError as e:
            print(f"⚠️  Warning: {e}, skipping email", file=sys.stderr)
        except Exception as e:
            print(f"❌ Failed to send email: {e}", file=sys.stderr)
            raise


class GmailMailService(MailService):
    """
    Mail service implementation using Gmail API.
    
    Wraps GmailService and provides a clean interface for sending emails via Gmail API.
    """
    
    def __init__(self, service_config: Dict[str, Any], name: str, mail_config: Dict[str, Any]):
        """
        Initialize GmailMailService.
        
        Args:
            service_config: Service configuration dictionary (credentials and token paths)
            name: Process name (e.g., 'filter', 'gaps_actions')
            mail_config: Mail configuration dictionary from config
        """
        super().__init__(service_config, name, mail_config)
        self.gmail_service = None
        
        if not mail_config:
            return
        
        try:
            # Use a separate token file for Gmail to avoid scope conflicts
            # Change token file path from token_sheets.pickle to token_gmail.pickle
            gmail_service_config = service_config.copy() if service_config else {}
            if 'pickle_file_path' in gmail_service_config:
                token_path = gmail_service_config['pickle_file_path']
                # Replace token_sheets.pickle with token_gmail.pickle
                if 'token_sheets.pickle' in token_path:
                    gmail_token_path = token_path.replace('token_sheets.pickle', 'token_gmail.pickle')
                else:
                    # If different naming, append _gmail before .pickle
                    base_path = token_path.rsplit('.', 1)[0]
                    gmail_token_path = f"{base_path}_gmail.pickle"
                gmail_service_config['pickle_file_path'] = gmail_token_path
            
            self.gmail_service = GmailService(gmail_service_config)
            print(f"✅ Gmail service initialized for {name}", file=sys.stderr)
        except Exception as e:
            print(f"⚠️  Warning: Could not initialize Gmail service: {e}", file=sys.stderr)
    
    def _send_email_impl(self, to_email: str, subject: str, body: str, body_html: Optional[str], 
                         cc_emails: Optional[List[str]], from_email: str, from_name: str) -> None:
        """Send email using Gmail API."""
        if not self.gmail_service:
            raise RuntimeError("Gmail service not initialized")
        
        self.gmail_service.send_email(
            to=to_email,
            subject=subject,
            body=body,
            body_html=body_html,
            cc=cc_emails,
            from_email=from_email,
            from_name=from_name
        )


class SMTPMailService(MailService):
    """
    Mail service implementation using SMTP.
    
    Wraps SMTPService and provides a clean interface for sending emails via SMTP.
    """
    
    def __init__(self, service_config: Dict[str, Any], name: str, mail_config: Dict[str, Any]):
        """
        Initialize SMTPMailService.
        
        Args:
            service_config: Service configuration dictionary with SMTP settings
            name: Process name (e.g., 'filter', 'gaps_actions')
            mail_config: Mail configuration dictionary from config
        """
        super().__init__(service_config, name, mail_config)
        self.smtp_service = None
        
        if not mail_config:
            return
        
        try:
            # Extract SMTP config from service_config
            smtp_config = service_config.get('smtp_config', service_config) if service_config else {}
            self.smtp_service = SMTPService(smtp_config)
            print(f"✅ SMTP service initialized for {name}", file=sys.stderr)
        except Exception as e:
            print(f"⚠️  Warning: Could not initialize SMTP service: {e}", file=sys.stderr)
    
    def _send_email_impl(self, to_email: str, subject: str, body: str, body_html: Optional[str], 
                         cc_emails: Optional[List[str]], from_email: str, from_name: str) -> None:
        """Send email using SMTP."""
        if not self.smtp_service:
            raise RuntimeError("SMTP service not initialized")
        
        self.smtp_service.send_email(
            to=to_email,
            subject=subject,
            body=body,
            body_html=body_html,
            cc=cc_emails,
            from_email=from_email,
            from_name=from_name
        )


def create_mail_service(module_name: str, mail_config: Dict[str, Any], service_config: Dict[str, Any] = None) -> Optional[MailService]:
    """
    Create MailService instance with provided mail configuration.
    
    Supports both Gmail API and SMTP services based on service_config.
    If service_config contains 'smtp_server' or 'smtp_config', SMTP service will be used.
    Otherwise, Gmail API service will be used.
    
    Args:
        module_name: Module name (e.g., 'filter', 'gaps_actions')
        mail_config: Mail configuration dictionary with keys:
            - should_send_mail: bool
            - subject: str
            - body: str
            - recipients: List[str]
            - cc_recipients: List[str]
            - from_email: str (optional)
            - from_name: str (optional)
        service_config: Service configuration dictionary:
            - For SMTP: 'smtp_server', 'smtp_port', 'smtp_username', 'smtp_password_env_var', 'use_tls', 'use_ssl'
            - For Gmail API: 'pickle_file_path', 'credentials_file_path'
    
    Returns:
        MailService instance (GmailMailService or SMTPMailService) if mail_config is provided and valid, None otherwise.
    """
    if not mail_config or mail_config.get('should_send_mail') is False:
        print(f"ℹ️  Mail service for '{module_name}' is disabled or config is empty, skipping initialization.", file=sys.stderr)
        return None
    
    # Parse recipients and cc_recipients (can be string, list, or None)
    def parse_recipients(recipients_value: Optional[Union[str, List[str]]]) -> List[str]:
        """Parse recipients from text field (comma-separated) or return list as-is."""
        if not recipients_value:
            return []
        
        # If already a list, return it (with string cleaning)
        if isinstance(recipients_value, list):
            return [r.strip() if isinstance(r, str) else str(r) for r in recipients_value if r]
        
        # If string, parse comma-separated values
        if isinstance(recipients_value, str):
            recipients_text = recipients_value.strip()
            if not recipients_text:
                return []
            return [r.strip() for r in recipients_text.split(',') if r.strip()]
        
        # Fallback: convert to string and parse
        return [str(recipients_value).strip()]
    
    # Ensure recipients and cc_recipients are lists
    mail_config['recipients'] = parse_recipients(mail_config.get('recipients'))
    mail_config['cc_recipients'] = parse_recipients(mail_config.get('cc_recipients'))
    
    try:
        # Check if SMTP config is provided
        if service_config and ('smtp_server' in service_config or 'smtp_config' in service_config):
            return SMTPMailService(service_config, module_name, mail_config)
        else:
            return GmailMailService(service_config, module_name, mail_config)
    except Exception as e:
        print(f"⚠️  Warning: Failed to create mail service: {e}", file=sys.stderr)
        return None
