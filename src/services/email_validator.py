import os
import logging
from typing import Dict, Optional
from datetime import datetime
import msal
import requests
import base64
from urllib.parse import quote

logger = logging.getLogger(__name__)

class EmailValidator:
    """Service for sending Excel files for validation and handling responses."""

    def __init__(self):
        """Initialize email client with Microsoft Graph API."""
        self.client_id = os.getenv('CLIENT_ID')
        self.client_secret = os.getenv('CLIENT_SECRET')
        self.tenant_id = os.getenv('TENANT_ID')
        self.validator_email = os.getenv('VALIDATOR_EMAIL')
        self.sender_email = os.getenv('USER_EMAIL')
        self.scope = ['https://graph.microsoft.com/.default']
        self.access_token = None
        self.graph_endpoint = "https://graph.microsoft.com/v1.0"

        # Reference IDs to track validation requests
        self.pending_validations = {}

        # Get initial token
        self._authenticate()

    def _authenticate(self) -> None:
        """Authenticate with Microsoft Graph API using application permissions."""
        try:
            # Use client credentials flow for application permissions
            app = msal.ConfidentialClientApplication(
                client_id=self.client_id,
                client_credential=self.client_secret,
                authority=f"https://login.microsoftonline.com/{self.tenant_id}"
            )
            
            result = app.acquire_token_for_client(scopes=self.scope)
            
            if "access_token" in result:
                self.access_token = result["access_token"]
                logger.info("Successfully acquired access token")
            else:
                raise Exception(f"Failed to acquire token: {result.get('error_description')}")

        except Exception as e:
            logger.error(f"Authentication failed: {str(e)}")
            raise

    def send_for_validation(self, 
                          excel_path: str, 
                          process_id: str,
                          metadata: Optional[Dict] = None) -> Dict:
        """Send Excel file to validator for review."""
        try:
            # Ensure we have a token
            if not self.access_token:
                self._authenticate()

            # Read Excel file
            with open(excel_path, 'rb') as f:
                excel_content = f.read()

            # Prepare email data
            email_data = {
                'message': {
                    'subject': f'Data Validation Required - Process {process_id}',
                    'body': {
                        'contentType': 'HTML',
                        'content': self._create_email_body(process_id, metadata)
                    },
                    'toRecipients': [
                        {
                            'emailAddress': {
                                'address': self.validator_email
                            }
                        }
                    ],
                    'attachments': [
                        {
                            '@odata.type': '#microsoft.graph.fileAttachment',
                            'name': os.path.basename(excel_path),
                            'contentBytes': base64.b64encode(excel_content).decode(),
                            'contentType': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                        }
                    ]
                }
            }

            # Send email using Microsoft Graph API
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json',
            }

            # Use the correct endpoint for application permissions
            endpoints = [
                f"{self.graph_endpoint}/users/{quote(self.sender_email)}/sendMail",
                f"{self.graph_endpoint}/groups/{os.getenv('GROUP_ID', '')}/sendMail",
                f"{self.graph_endpoint}/users/{quote(self.sender_email)}/messages"
            ]
            
            success = False
            last_error = None
            
            for endpoint in endpoints:
                try:
                    if endpoint.endswith('/messages'):
                        # Create draft and send
                         response = requests.post(endpoint, headers=headers, json={'message': email_data['message']})
                         if response.status_code == 201:
                             message_id = response.json()['id']
                             send_endpoint = f"{endpoint}/{message_id}/send"
                             response = requests.post(send_endpoint, headers=headers)
                             success = response.status_code in [200, 202]
                    else:
                        response = requests.post(endpoint, headers=headers, json=email_data)
                        success = response.status_code in [200, 202]

                    if success:
                        break
                except Exception as e:
                    last_error = e
                    continue
                
            if not success:
                error_msg = f"All send attempts failed. Last error: {last_error if last_error else 'Unknown error'}"
                logger.error(error_msg)
                raise Exception(error_msg)

            # Store validation request
            validation_id = f"VAL_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            self.pending_validations[validation_id] = {
                'process_id': process_id,
                'excel_path': excel_path,
                'sent_at': datetime.now(),
                'status': 'pending',
                'metadata': metadata
            }

            logger.info(f"Successfully sent validation email to {self.validator_email}")
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