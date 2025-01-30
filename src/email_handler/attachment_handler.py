import os
import base64
from typing import Dict, List
import logging
from datetime import datetime

from config.settings import RAW_DATA_DIR, ATTACHMENT_TYPES
from config.constants import FILE_NAME_PATTERN
from src.utils.exceptions import AttachmentError

logger = logging.getLogger(__name__)

class AttachmentHandler:
    def __init__(self):
        self.download_dir = RAW_DATA_DIR

    def is_valid_attachment(self, attachment: Dict) -> bool:
        """
        Validate attachment type and name.
        
        Args:
            attachment: Attachment dictionary from Graph API
            
        Returns:
            bool: Whether attachment is valid
        """
        name = attachment.get("name", "")
        return (
            any(name.lower().endswith(ext) for ext in ATTACHMENT_TYPES) and
            FILE_NAME_PATTERN.match(name) is not None
        )

    def save_attachment(self, attachment: Dict, email_id: str) -> str:
        """
        Save attachment to disk.
        
        Args:
            attachment: Attachment dictionary from Graph API
            email_id: ID of the email
            
        Returns:
            str: Path where attachment was saved
        """
        try:
            # Create directory for this email's attachments
            email_dir = os.path.join(
                self.download_dir,
                f"{email_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            )
            os.makedirs(email_dir, exist_ok=True)

            # Save attachment
            file_path = os.path.join(email_dir, attachment["name"])
            content = base64.b64decode(attachment["contentBytes"])
            
            with open(file_path, "wb") as f:
                f.write(content)
                
            logger.info(f"Saved attachment: {file_path}")
            return file_path

        except Exception as e:
            logger.error(f"Failed to save attachment: {str(e)}")
            raise AttachmentError(f"Failed to save attachment: {str(e)}")

    def process_attachments(self, attachments: List[Dict], email_id: str) -> List[str]:
        """
        Process and save all valid attachments from an email.
        
        Args:
            attachments: List of attachment dictionaries
            email_id: ID of the email
            
        Returns:
            List[str]: Paths of saved attachments
        """
        saved_paths = []
        for attachment in attachments:
            if self.is_valid_attachment(attachment):
                try:
                    path = self.save_attachment(attachment, email_id)
                    saved_paths.append(path)
                except AttachmentError as e:
                    logger.error(f"Skipping attachment due to error: {str(e)}")
                    continue
                    
        return saved_paths