import logging
from typing import Dict, Optional
import os
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import msal

logger = logging.getLogger(__name__)

class EmailValidator:
    """Service for sending Excel files for validation and handling responses."""

    def __init__(self):
        """Initialize email client with Microsoft Graph API."""
        self.client_id = os.getenv('CLIENT_ID')
        self.client_secret = os.getenv('CLIENT_SECRET')
        self.tenant_id = os.getenv('TENANT_ID')
        self.validator_email = os.getenv('VALIDATOR_EMAIL')  # Email of the employee who validates
        self.scope = ['https://graph.microsoft.com/.default']
        self.access_token = None

        # Reference IDs to track validation requests
        self.pending_validations = {}

    def send_for_validation(self, 
                          excel_path: str, 
                          process_id: str,
                          metadata: Optional[Dict] = None) -> Dict:
        """
        Send Excel file to validator for review.
        
        Args:
            excel_path: Path to Excel file
            process_id: Process identifier
            metadata: Additional process information
            
        Returns:
            Dict containing email status and tracking info
        """
        try:
            # Ensure we have a token
            if not self.access_token:
                self._authenticate()

            # Create email
            msg = MIMEMultipart()
            msg['Subject'] = f'Data Validation Required - Process {process_id}'
            msg['To'] = self.validator_email

            # Create email body
            body = self._create_email_body(process_id, metadata)
            msg.attach(MIMEText(body, 'html'))

            # Attach Excel file
            with open(excel_path, 'rb') as f:
                excel_attachment = MIMEApplication(f.read(), _subtype='xlsx')
                excel_attachment.add_header(
                    'Content-Disposition', 
                    'attachment', 
                    filename=os.path.basename(excel_path)
                )
                msg.attach(excel_attachment)

            # Send email using Microsoft Graph API
            self._send_email(msg)

            # Store validation request
            validation_id = f"VAL_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            self.pending_validations[validation_id] = {
                'process_id': process_id,
                'excel_path': excel_path,
                'sent_at': datetime.now(),
                'status': 'pending',
                'metadata': metadata
            }

            return {
                'status': 'sent',
                'validation_id': validation_id,
                'sent_to': self.validator_email
            }

        except Exception as e:
            logger.error(f"Failed to send validation email: {str(e)}")
            return {
                'status': 'error',
                'error': str(e)
            }

    def _authenticate(self) -> None:
        """Authenticate with Microsoft Graph API."""
        try:
            app = msal.ConfidentialClientApplication(
                client_id=self.client_id,
                client_credential=self.client_secret,
                authority=f"https://login.microsoftonline.com/{self.tenant_id}"
            )
            
            result = app.acquire_token_for_client(scopes=self.scope)
            
            if "access_token" in result:
                self.access_token = result["access_token"]
            else:
                raise Exception("Failed to acquire token")

        except Exception as e:
            logger.error(f"Authentication failed: {str(e)}")
            raise

    def _send_email(self, msg: MIMEMultipart) -> None:
        """Send email using Microsoft Graph API."""
        import requests
        
        endpoint = "https://graph.microsoft.com/v1.0/users/@/sendMail"
        
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
        
        email_data = {
            'message': {
                'subject': msg['Subject'],
                'body': {
                    'contentType': 'HTML',
                    'content': msg.get_payload(0).get_payload()
                },
                'toRecipients': [
                    {
                        'emailAddress': {
                            'address': msg['To']
                        }
                    }
                ],
                'attachments': [
                    {
                        '@odata.type': '#microsoft.graph.fileAttachment',
                        'name': att.get_filename(),
                        'contentBytes': att.get_payload()
                    }
                    for att in msg.get_payload()[1:]
                ]
            }
        }
        
        response = requests.post(endpoint, headers=headers, json=email_data)
        response.raise_for_status()

    def _create_email_body(self, process_id: str, metadata: Optional[Dict] = None) -> str:
        """Create HTML email body for validation request."""
        body = f"""
        <html>
        <body>
            <h2>Data Validation Required</h2>
            <p>Please review the attached Excel file for Process ID: {process_id}</p>
            
            <h3>Instructions:</h3>
            <ol>
                <li>Open the attached Excel file</li>
                <li>Review all entered data for accuracy</li>
                <li>Make any necessary corrections directly in the file</li>
                <li>Reply to this email with one of the following:
                    <ul>
                        <li>APPROVED - if all data is correct</li>
                        <li>REJECTED - if there are issues that need to be addressed</li>
                    </ul>
                </li>
                <li>If rejected, please specify the issues in your reply</li>
            </ol>
        """
        
        if metadata:
            body += "<h3>Process Details:</h3><ul>"
            for key, value in metadata.items():
                body += f"<li>{key}: {value}</li>"
            body += "</ul>"
        
        body += """
            <p>Thank you for your help!</p>
        </body>
        </html>
        """
        
        return body

    def check_validation_status(self, validation_id: str) -> Dict:
        """Check status of a validation request."""
        if validation_id not in self.pending_validations:
            return {
                'status': 'not_found',
                'error': 'Validation request not found'
            }
            
        return self.pending_validations[validation_id]

    def process_validation_response(self, email_data: Dict) -> Dict:
        """
        Process validation response email.
        
        Args:
            email_data: Email data containing response
            
        Returns:
            Dict containing validation result
        """
        # TODO: Implement email response processing
        # This will be implemented when we have the email monitoring system ready
        pass