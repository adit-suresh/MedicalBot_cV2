import os
import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime
import time

logger = logging.getLogger(__name__)

class EmailTracker:
    """Track processed emails to avoid reprocessing."""
    
    def __init__(self, tracker_file: str = "processed_emails.json"):
        """Initialize email tracker.
        
        Args:
            tracker_file: Path to email tracker file
        """
        self.tracker_file = tracker_file
        self._processed_emails = {}
        self._load_tracker()
        
    def _load_tracker(self) -> None:
        """Load email tracker from file."""
        try:
            if os.path.exists(self.tracker_file):
                with open(self.tracker_file, 'r') as f:
                    try:
                        self._processed_emails = json.load(f)
                        logger.info(f"Loaded {len(self._processed_emails)} processed emails from {self.tracker_file}")
                    except json.JSONDecodeError:
                        logger.error(f"Error parsing {self.tracker_file}, creating new one")
                        # Backup corrupted file
                        backup = f"{self.tracker_file}.bak.{int(time.time())}"
                        os.rename(self.tracker_file, backup)
                        logger.info(f"Backed up corrupted tracker to {backup}")
                        # Initialize empty tracker
                        self._processed_emails = {}
                        self._save_tracker()
            else:
                logger.info(f"No tracker file found at {self.tracker_file}, creating new one")
                self._processed_emails = {}
                self._save_tracker()
        except Exception as e:
            logger.error(f"Error loading email tracker: {str(e)}")
            self._processed_emails = {}
    
    def _save_tracker(self) -> None:
        """Save email tracker to file."""
        try:
            with open(self.tracker_file, 'w') as f:
                json.dump(self._processed_emails, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving email tracker: {str(e)}")
    
    def is_processed(self, email_id: str) -> bool:
        """Check if email has been processed.
        
        Args:
            email_id: Email ID to check
            
        Returns:
            bool: Whether email has been processed
        """
        return email_id in self._processed_emails
    
    def mark_processed(self, email_id: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Mark email as processed.
        
        Args:
            email_id: Email ID to mark
            metadata: Optional metadata about the email
        """
        if metadata is None:
            metadata = {}
            
        # Add timestamp if not present
        if 'processed_at' not in metadata:
            metadata['processed_at'] = datetime.now().isoformat()
            
        # Update tracker
        self._processed_emails[email_id] = metadata
        
        # Save to file
        self._save_tracker()
        
        logger.info(f"Marked email {email_id} as processed")
    
    def get_processed_emails(self) -> Dict[str, Dict[str, Any]]:
        """Get all processed emails.
        
        Returns:
            Dict of email IDs to metadata
        """
        return self._processed_emails
    
    def get_email_metadata(self, email_id: str) -> Optional[Dict[str, Any]]:
        """Get metadata for a processed email.
        
        Args:
            email_id: Email ID to get metadata for
            
        Returns:
            Dict of metadata or None if email not processed
        """
        return self._processed_emails.get(email_id)
    
    def reset_tracker(self) -> bool:
        """Reset the email tracker.
        
        Returns:
            bool: Whether reset was successful
        """
        try:
            # Backup existing file
            if os.path.exists(self.tracker_file):
                backup = f"{self.tracker_file}.bak.{int(time.time())}"
                os.rename(self.tracker_file, backup)
                logger.info(f"Backed up tracker to {backup}")
            
            # Clear tracking data
            self._processed_emails = {}
            
            # Create new empty file
            with open(self.tracker_file, 'w') as f:
                json.dump({}, f)
                
            logger.info("Email tracker has been reset")
            return True
        except Exception as e:
            logger.error(f"Failed to reset email tracker: {str(e)}")
            return False