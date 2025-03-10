import os
import json
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class EmailTracker:
    def __init__(self, storage_file="processed_emails.json"):
        self.storage_file = storage_file
        self.tracker_file = storage_file  # Add this line to fix the error
        self.processed_emails = self._load_processed_emails()

    def _load_processed_emails(self):
        if os.path.exists(self.storage_file):
            try:
                with open(self.storage_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error loading processed emails: {str(e)}")
                return {}
        return {}

    def _save_processed_emails(self):
        try:
            with open(self.storage_file, 'w') as f:
                json.dump(self.processed_emails, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving processed emails: {str(e)}")

    def is_processed(self, email_id: str) -> bool:
        """Check if email has been processed."""
        return email_id in self.processed_emails

    def mark_processed(self, email_id: str, metadata: dict = None):
        self.processed_emails[email_id] = {
            'timestamp': datetime.now().isoformat(),
            'metadata': metadata or {}
        }
        self._save_processed_emails()
        
    def reset_tracker(self) -> bool:
        """Reset the email tracker."""
        try:
            # Backup existing file
            if os.path.exists(self.storage_file):
                backup = f"{self.storage_file}.bak.{int(datetime.now().timestamp())}"
                try:
                    os.rename(self.storage_file, backup)
                    logger.info(f"Backed up tracker to {backup}")
                except Exception:
                    pass
            
            # Clear tracking data
            self.processed_emails = {}
            
            # Create new empty file
            with open(self.storage_file, 'w') as f:
                json.dump({}, f)
                
            logger.info("Email tracker has been reset")
            return True
        except Exception as e:
            logger.error(f"Failed to reset email tracker: {str(e)}")
            return False