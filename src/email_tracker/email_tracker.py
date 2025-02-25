import os
import json
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class EmailTracker:
    def __init__(self, storage_file="processed_emails.json"):
        self.storage_file = storage_file
        self.processed_emails = self._load_processed_emails()

    def _load_processed_emails(self):
        if os.path.exists(self.storage_file):
            try:
                with open(self.storage_file, 'r') as f:
                    return json.load(f)
            except:
                return {}
        return {}

    def _save_processed_emails(self):
        with open(self.storage_file, 'w') as f:
            json.dump(self.processed_emails, f, indent=2)

    def is_processed(self, email_id: str) -> bool:
        return email_id in self.processed_emails

    def mark_processed(self, email_id: str, metadata: dict = None):
        self.processed_emails[email_id] = {
            'timestamp': datetime.now().isoformat(),
            'metadata': metadata or {}
        }
        self._save_processed_emails()