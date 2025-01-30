import os
import shutil
import logging
from datetime import datetime, timedelta
from typing import Optional

from config.settings import RAW_DATA_DIR, PROCESSED_DATA_DIR
logger = logging.getLogger(__name__)

class FileCleanup:
    def __init__(self, retention_days: int = 7):
        self.retention_days = retention_days
        self.raw_dir = RAW_DATA_DIR
        self.processed_dir = PROCESSED_DATA_DIR

    def cleanup_old_files(self) -> None:
        """Remove files older than retention_days."""
        logger.info(f"Starting cleanup of files older than {self.retention_days} days")
        
        cutoff_date = datetime.now() - timedelta(days=self.retention_days)
        
        # Clean raw directory
        self._cleanup_directory(self.raw_dir, cutoff_date)
        # Clean processed directory
        self._cleanup_directory(self.processed_dir, cutoff_date)

    def _cleanup_directory(self, directory: str, cutoff_date: datetime) -> None:
        """Clean up a specific directory."""
        try:
            if not os.path.exists(directory):
                return

            for item in os.listdir(directory):
                item_path = os.path.join(directory, item)
                
                # Get the modification time of the directory
                mtime = datetime.fromtimestamp(os.path.getmtime(item_path))
                
                if mtime < cutoff_date:
                    if os.path.isdir(item_path):
                        shutil.rmtree(item_path)
                        logger.info(f"Removed directory: {item_path}")
                    else:
                        os.remove(item_path)
                        logger.info(f"Removed file: {item_path}")

        except Exception as e:
            logger.error(f"Error during cleanup of {directory}: {str(e)}")

# Add to your main processing loop or run as a scheduled task
def cleanup_files(retention_days: Optional[int] = None) -> None:
    cleanup = FileCleanup(retention_days=retention_days or 7)
    cleanup.cleanup_old_files()