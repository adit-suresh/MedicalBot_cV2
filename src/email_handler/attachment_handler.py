import os
import base64
import logging
from datetime import datetime
from typing import Dict, List

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
        file_ext = os.path.splitext(name)[1].lower()
        
        # Log validation steps
        logger.debug(f"Validating attachment: {name}")
        logger.debug(f"File extension: {file_ext}")
        
        # Check file extension
        valid_extension = any(file_ext.endswith(ext.lower()) for ext in ATTACHMENT_TYPES)
        if not valid_extension:
            logger.debug(f"Extension {file_ext} not in allowed types: {ATTACHMENT_TYPES}")
            return False

        # Check filename pattern
        valid_pattern = FILE_NAME_PATTERN.match(name) is not None
        if not valid_pattern:
            logger.debug(f"Filename {name} doesn't match pattern {FILE_NAME_PATTERN.pattern}")
            return False

        logger.debug("Attachment passed all validation checks")
        return True

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
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            email_dir = os.path.join(self.download_dir, f"{email_id}_{timestamp}")
            os.makedirs(email_dir, exist_ok=True)

            # Clean filename
            original_name = attachment["name"]
            safe_name = original_name.replace(' ', '_')
            file_path = os.path.join(email_dir, safe_name)
            
            logger.info(f"Saving attachment {original_name} to {file_path}")
            
            # Save the file
            content = base64.b64decode(attachment["contentBytes"])
            with open(file_path, "wb") as f:
                f.write(content)
            
            file_size = os.path.getsize(file_path)
            logger.info(f"Successfully saved {original_name} ({file_size} bytes)")
            
            return file_path

        except Exception as e:
            logger.error(f"Failed to save attachment {attachment.get('name', 'unknown')}: {str(e)}")
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
            name = attachment.get("name", "unknown")
            logger.info(f"Processing attachment: {name}")
            
            if self.is_valid_attachment(attachment):
                try:
                    path = self.save_attachment(attachment, email_id)
                    saved_paths.append(path)
                    logger.info(f"Successfully processed: {name}")
                except AttachmentError as e:
                    logger.error(f"Failed to save {name}: {str(e)}")
                    continue
            else:
                logger.info(f"Skipping invalid attachment: {name}")
                
        logger.info(f"Successfully processed {len(saved_paths)} attachments")
        return saved_paths