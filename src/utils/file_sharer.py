import os
import shutil
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class FileSharer:
    """Simple file sharing via network folder."""
    
    def __init__(self, shared_folder=None):
        """Initialize with path to shared folder."""
        self.shared_folder = shared_folder or os.getenv('SHARED_FOLDER_PATH')
        if not self.shared_folder:
            logger.warning("SHARED_FOLDER_PATH not set. Using local 'shared_files' folder.")
            self.shared_folder = os.path.join(os.getcwd(), 'shared_files')
            
        # Ensure shared folder exists
        os.makedirs(self.shared_folder, exist_ok=True)
    
    def copy_to_shared(self, file_path, new_name=None):
        """Copy file to shared folder with optional new name."""
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return None
            
        try:
            # Generate new name if not provided
            if not new_name:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                file_basename = os.path.basename(file_path)
                new_name = f"{timestamp}_{file_basename}"
                
            # Copy to shared folder
            dest_path = os.path.join(self.shared_folder, new_name)
            shutil.copy2(file_path, dest_path)
            
            logger.info(f"File copied to shared folder: {dest_path}")
            return dest_path
            
        except Exception as e:
            logger.error(f"Error copying file to shared folder: {str(e)}")
            return None