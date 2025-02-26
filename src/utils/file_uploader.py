import os
import requests
import json
import logging
from typing import Optional

logger = logging.getLogger(__name__)

class SharePointUploader:
    """Upload files to SharePoint or OneDrive and get a sharing link."""
    
    def __init__(self, tenant_id=None, client_id=None, client_secret=None):
        """Initialize with Microsoft Graph API credentials."""
        self.tenant_id = tenant_id or os.getenv('TENANT_ID')
        self.client_id = client_id or os.getenv('CLIENT_ID')
        self.client_secret = client_secret or os.getenv('CLIENT_SECRET')
        self.site_id = os.getenv('SHAREPOINT_SITE_ID')
        self.drive_id = os.getenv('SHAREPOINT_DRIVE_ID')
        self.folder_path = os.getenv('SHAREPOINT_FOLDER_PATH', 'MedicalBot')
        
        self.access_token = None
    
    def get_access_token(self):
        """Get Microsoft Graph API access token."""
        url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        data = {
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'scope': 'https://graph.microsoft.com/.default'
        }
        
        response = requests.post(url, data=data)
        response.raise_for_status()
        
        token_data = response.json()
        self.access_token = token_data['access_token']
        return self.access_token
    
    def upload_file(self, file_path: str) -> Optional[str]:
        """Upload file to SharePoint and return sharing link."""
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return None
            
        try:
            # Get token
            token = self.get_access_token()
            
            # Upload file to SharePoint/OneDrive
            file_name = os.path.basename(file_path)
            upload_url = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/root:/{self.folder_path}/{file_name}:/content"
            
            with open(file_path, 'rb') as file_data:
                headers = {
                    'Authorization': f'Bearer {token}',
                    'Content-Type': 'application/octet-stream'
                }
                
                upload_response = requests.put(upload_url, headers=headers, data=file_data)
                upload_response.raise_for_status()
                
                # Get file ID
                file_info = upload_response.json()
                file_id = file_info['id']
                
                # Create sharing link
                share_url = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{file_id}/createLink"
                share_payload = {
                    "type": "view",
                    "scope": "organization"
                }
                
                share_response = requests.post(
                    share_url, 
                    headers={
                        'Authorization': f'Bearer {token}',
                        'Content-Type': 'application/json'
                    },
                    data=json.dumps(share_payload)
                )
                share_response.raise_for_status()
                
                # Get sharing URL
                share_info = share_response.json()
                sharing_url = share_info['link']['webUrl']
                
                logger.info(f"File uploaded and shared: {sharing_url}")
                return sharing_url
                
        except Exception as e:
            logger.error(f"Error uploading file to SharePoint: {str(e)}")
            return None