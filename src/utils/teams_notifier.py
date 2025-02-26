import os
import zipfile
import logging
import json
import requests
from datetime import datetime
from typing import List, Dict, Optional
from src.utils.file_uploader import SharePointUploader  # Import SharePointUploader

logger = logging.getLogger(__name__)

class TeamsNotifier:
    """Utility to send notifications and files to Microsoft Teams."""
    
    def __init__(self, webhook_url=None):
        """Initialize the Teams notifier.
        
        Args:
            webhook_url: Teams webhook URL (defaults to environment variable)
        """
        self.webhook_url = webhook_url or os.getenv('TEAMS_WEBHOOK_URL')
        self.uploader = SharePointUploader()
        if not self.webhook_url:
            logger.warning("TEAMS_WEBHOOK_URL not set. Teams notifications will not be available.")
    
    def create_zip(self, folder_path: str, zip_name: Optional[str] = None) -> str:
        """Create a ZIP file from a folder.
        
        Args:
            folder_path: Path to the folder to zip
            zip_name: Optional name for the ZIP file (defaults to folder name + timestamp)
            
        Returns:
            Path to the created ZIP file
        """
        if not os.path.exists(folder_path):
            raise FileNotFoundError(f"Folder not found: {folder_path}")
            
        # If no zip name provided, use folder name + timestamp
        if not zip_name:
            folder_basename = os.path.basename(folder_path)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            zip_name = f"{folder_basename}_{timestamp}.zip"
            
        # Create ZIP file
        zip_path = os.path.join(os.path.dirname(folder_path), zip_name)
        
        try:
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                # Walk through all files in the directory
                for root, _, files in os.walk(folder_path):
                    for file in files:
                        file_path = os.path.join(root, file)
                        
                        # Calculate path inside the ZIP file
                        rel_path = os.path.relpath(file_path, os.path.dirname(folder_path))
                        
                        # Add file to ZIP
                        zipf.write(file_path, rel_path)
                        
            logger.info(f"Created ZIP file: {zip_path}")
            return zip_path
            
        except Exception as e:
            logger.error(f"Error creating ZIP file: {str(e)}")
            raise
    
    def send_notification(self, message: str, title: Optional[str] = None) -> bool:
        """Send a simple notification to Teams.
        
        Args:
            message: Message text
            title: Optional message title
            
        Returns:
            True if successful, False otherwise
        """
        if not self.webhook_url:
            logger.warning("Teams webhook URL not set. Cannot send notification.")
            return False
            
        try:
            # Create Teams message card
            card = {
                "@type": "MessageCard",
                "@context": "http://schema.org/extensions",
                "summary": title or "Medical Bot Notification",
                "themeColor": "0078D7",
                "title": title or "Medical Bot Notification",
                "text": message
            }
            
            # Send to Teams
            response = requests.post(
                self.webhook_url,
                headers={"Content-Type": "application/json"},
                data=json.dumps(card)
            )
            
            response.raise_for_status()
            logger.info(f"Sent Teams notification: {title}")
            return True
            
        except Exception as e:
            logger.error(f"Error sending Teams notification: {str(e)}")
            return False
    
    def send_file(self, file_path, message, title=None):
        """Send file by uploading to SharePoint and sharing link in Teams."""
        if not self.webhook_url:
            logger.warning("Teams webhook URL not set. Cannot send file.")
            return False
            
        try:
            # Upload file to SharePoint
            sharing_url = self.uploader.upload_file(file_path)
            
            if not sharing_url:
                logger.error("Failed to upload file to SharePoint")
                return False
                
            # Create message with file link
            file_name = os.path.basename(file_path)
            full_message = f"{message}\n\n"
            full_message += f"**File:** [{file_name}]({sharing_url})\n\n"
            full_message += "Click the link above to download the file."
            
            # Create Teams message card
            card = {
                "@type": "MessageCard",
                "@context": "http://schema.org/extensions",
                "summary": title or f"Medical Bot File: {file_name}",
                "themeColor": "0078D7",
                "title": title or f"Medical Bot File: {file_name}",
                "text": full_message
            }
            
            # Send to Teams
            response = requests.post(
                self.webhook_url,
                headers={"Content-Type": "application/json"},
                data=json.dumps(card)
            )
            
            response.raise_for_status()
            logger.info(f"Sent Teams file notification with link for: {file_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error sending Teams file notification: {str(e)}")
            return False