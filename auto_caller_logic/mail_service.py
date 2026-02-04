"""
Mail service wrapper for GmailService.

This module provides a MailService class that wraps GmailService
and provides a clean interface for sending emails.
"""

import sys
import re
from typing import Dict, Any, Optional, List
from common_utils.gmail_service import GmailService


class MailService:
    """
    Wrapper class for GmailService that provides a clean interface for sending emails.
    
    This class handles initialization of GmailService with proper token file management
    and provides a send_mail method that implements the email sending logic.
    """
    
    def __init__(self, service_config: Dict[str, Any], name: str, mail_config: Dict[str, Any]):
        """
        Initialize MailService.
        
        Args:
            service_config: Service configuration dictionary (credentials and token paths)
            name: Process name (e.g., 'filter', 'gaps_actions')
            mail_config: Mail configuration dictionary from config
        """
        self.mail_config = mail_config
        self.gmail_service = None
        
        if not mail_config:
            return
        
        try:
            # Use a separate token file for Gmail to avoid scope conflicts
            # Change token file path from token_sheets.pickle to token_gmail.pickle
            gmail_service_config = service_config.copy()
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
    
    def send_mail(self, **kwargs) -> None:
        """
        Send email using mail config and mail data.
        
        Args:
            **kwargs: Should contain 'mail_data' dictionary with values for placeholder replacement
        """
        if not self.mail_config:
            return
        
        if not self.gmail_service:
            print(f"⚠️  Warning: Gmail service not initialized, skipping email", file=sys.stderr)
            return

        if self.mail_config['should_send_mail'] is False:
            print(f"ℹ️  Mail sending is disabled (should_send_mail: false), skipping email", file=sys.stderr)
            return
        
        # Get mail data from kwargs
        mail_data = kwargs.get('mail_data', {})
        
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
            print(f"⚠️  Warning: Invalid recipients format, skipping email", file=sys.stderr)
            return
        
        if not recipient_list:
            print(f"⚠️  Warning: No recipients specified, skipping email", file=sys.stderr)
            return
        
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
        
        # Check if mail should be sent
        should_send_mail = self.mail_config.get('should_send_mail', True)
        if not should_send_mail:
            print(f"ℹ️  Mail sending is disabled (should_send_mail: false), skipping email", file=sys.stderr)
            return
        
        from_email = self.mail_config.get('from_email', None)
        from_name = self.mail_config.get('from_name', None)

        if not from_email or not from_name:
            print(f"⚠️  Warning: From email or from name is not set, skipping email", file=sys.stderr)
            return
        
        # Send email
        try:
            result = self.gmail_service.send_email(
                to=to_email,
                subject=subject,
                body=body,
                body_html=body_html,
                cc=cc_emails,
                from_email=from_email,
                from_name=from_name
            )
            print(f"✅ Email sent successfully to {to_email}", file=sys.stderr)
        except Exception as e:
            print(f"❌ Failed to send email: {e}", file=sys.stderr)
            raise


def create_mail_service(module_name: str, mail_config: Dict[str, Any], service_config: Dict[str, Any] = None) -> Optional[MailService]:
    """
    Create MailService instance with provided mail configuration.
    
    Args:
        module_name: Module name (e.g., 'filter', 'gaps_actions')
        mail_config: Mail configuration dictionary with keys:
            - should_send_mail: bool
            - subject: str
            - body: str
            - recipients: List[str]
            - cc_recipients: List[str]
        service_config: Service configuration dictionary (credentials and token paths)
    
    Returns:
        MailService instance if mail_config is provided and valid, None otherwise.
    """
    if not mail_config:
        return None
    
    try:
        return MailService(service_config, module_name, mail_config)
    except Exception as e:
        print(f"⚠️  Warning: Failed to create mail service: {e}", file=sys.stderr)
        return None

