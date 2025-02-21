import os
import json
import logging
from datetime import datetime
from typing import List, Dict, Set, Optional
import threading

logger = logging.getLogger(__name__)

class EmailTracker:
    """Tracks processed emails to avoid duplicates."""
    
    def __init__(self, storage_path: str = None):
        """
        Initialize email tracker.
        
        Args:
            storage_path: Optional path to store tracked emails
        """
        self.storage_path = storage_path or os.path.join('data', 'processed_emails.json')
        self._tracked_emails: Dict[str, Dict] = {}
        self._lock = threading.RLock()
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(self.storage_path), exist_ok=True)
        
        # Load existing tracking data
        self._load_tracking_data()
    
    def _load_tracking_data(self) -> None:
        """Load tracking data from file."""
        with self._lock:
            if os.path.exists(self.storage_path):
                try:
                    with open(self.storage_path, 'r') as f:
                        self._tracked_emails = json.load(f)
                    logger.info(f"Loaded {len(self._tracked_emails)} tracked emails from {self.storage_path}")
                except Exception as e:
                    logger.error(f"Error loading tracked emails: {str(e)}")
                    self._tracked_emails = {}
    
    def _save_tracking_data(self) -> None:
        """Save tracking data to file."""
        with self._lock:
            try:
                with open(self.storage_path, 'w') as f:
                    json.dump(self._tracked_emails, f, indent=2)
                logger.info(f"Saved {len(self._tracked_emails)} tracked emails to {self.storage_path}")
            except Exception as e:
                logger.error(f"Error saving tracked emails: {str(e)}")
    
    def is_email_processed(self, email_id: str) -> bool:
        """
        Check if an email has already been processed.
        
        Args:
            email_id: Email ID to check
            
        Returns:
            True if email has been processed, False otherwise
        """
        with self._lock:
            return email_id in self._tracked_emails
    
    def mark_email_processed(self, email_id: str, metadata: Optional[Dict] = None) -> None:
        """
        Mark an email as processed.
        
        Args:
            email_id: Email ID to mark
            metadata: Optional metadata about processing
        """
        with self._lock:
            self._tracked_emails[email_id] = {
                'timestamp': datetime.now().isoformat(),
                'metadata': metadata or {}
            }
            # Save after each update to prevent data loss
            self._save_tracking_data()
    
    def get_processed_emails(self) -> List[str]:
        """
        Get list of processed email IDs.
        
        Returns:
            List of processed email IDs
        """
        with self._lock:
            return list(self._tracked_emails.keys())
    
    def get_email_metadata(self, email_id: str) -> Optional[Dict]:
        """
        Get metadata for a processed email.
        
        Args:
            email_id: Email ID to get metadata for
            
        Returns:
            Metadata dictionary or None if email hasn't been processed
        """
        with self._lock:
            if email_id in self._tracked_emails:
                return self._tracked_emails[email_id]
            return None
    
    def remove_tracked_email(self, email_id: str) -> bool:
        """
        Remove an email from tracking.
        
        Args:
            email_id: Email ID to remove
            
        Returns:
            True if email was removed, False if it wasn't tracked
        """
        with self._lock:
            if email_id in self._tracked_emails:
                del self._tracked_emails[email_id]
                self._save_tracking_data()
                return True
            return False
    
    def filter_unprocessed_emails(self, emails: List[Dict]) -> List[Dict]:
        """
        Filter out already processed emails.
        
        Args:
            emails: List of email dictionaries with 'id' keys
            
        Returns:
            List of unprocessed email dictionaries
        """
        with self._lock:
            unprocessed = [email for email in emails if email['id'] not in self._tracked_emails]
            filtered_count = len(emails) - len(unprocessed)
            if filtered_count > 0:
                logger.info(f"Filtered out {filtered_count} already processed emails")
            return unprocessed