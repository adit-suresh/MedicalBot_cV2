# src/folder_processor.py
# (We'll put this directly in the src directory for simplicity)

import os
import logging
import shutil
import re
from datetime import datetime
from typing import Dict, List, Any
import uuid

logger = logging.getLogger(__name__)

class FolderProcessor:
    """Process documents from a local folder instead of email attachments."""
    
    def __init__(self, watch_folder="input_documents"):
        """
        Initialize the folder processor.
        
        Args:
            watch_folder: Path to the folder to watch for documents
        """
        self.watch_folder = watch_folder
        self.processed_folder = os.path.join(watch_folder, "processed")
        
        # Create folders if they don't exist
        os.makedirs(self.watch_folder, exist_ok=True)
        os.makedirs(self.processed_folder, exist_ok=True)
        
        logger.info(f"Initialized FolderProcessor watching folder: {self.watch_folder}")
    
    def check_for_documents(self) -> List[Dict]:
        """
        Check for document folders ready for processing.
        
        Returns:
            List of dicts containing folder info that needs processing
        """
        result = []
        
        # Check if the watch folder exists
        if not os.path.exists(self.watch_folder):
            logger.warning(f"Watch folder does not exist: {self.watch_folder}")
            return []
    
        # Check for immediate subfolders (each represents a set of documents)
        for item in os.listdir(self.watch_folder):
            folder_path = os.path.join(self.watch_folder, item)
            
            # Skip the processed folder and non-directories
            if item == "processed" or not os.path.isdir(folder_path):
                continue
                
            # Check if folder has a marker file indicating it's ready for processing
            ready_marker = os.path.join(folder_path, "READY_FOR_PROCESSING.txt")
            if os.path.exists(ready_marker):
                # Get info about this folder
                folder_info = self._get_folder_info(folder_path)
                result.append(folder_info)
                logger.info(f"Found folder ready for processing: {folder_path}")
        
        return result
    
    def _get_folder_info(self, folder_path: str) -> Dict:
        """Get information about a document folder."""
        # Count files by type
        excel_files = []
        pdf_files = []
        image_files = []
        other_files = []
        
        for file in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file)
            if not os.path.isfile(file_path):
                continue
                
            if file.lower().endswith(('.xlsx', '.xls')):
                excel_files.append(file_path)
            elif file.lower().endswith('.pdf'):
                pdf_files.append(file_path)
            elif file.lower().endswith(('.jpg', '.jpeg', '.png')):
                image_files.append(file_path)
            elif file != "READY_FOR_PROCESSING.txt":
                other_files.append(file_path)
        
        # Try to get folder metadata
        metadata = {}
        metadata_file = os.path.join(folder_path, "metadata.txt")
        if os.path.exists(metadata_file):
            try:
                with open(metadata_file, 'r') as f:
                    for line in f:
                        if ':' in line:
                            key, value = line.split(':', 1)
                            metadata[key.strip()] = value.strip()
            except Exception as e:
                logger.error(f"Error reading metadata file: {str(e)}")
        
        # Generate a unique ID for this folder
        folder_id = str(uuid.uuid4())[:8]
        
        return {
            'id': folder_id,
            'folder_path': folder_path,
            'folder_name': os.path.basename(folder_path),
            'subject': metadata.get('subject', f"Documents from folder {os.path.basename(folder_path)}"),
            'excel_files': excel_files,
            'pdf_files': pdf_files,
            'image_files': image_files,
            'other_files': other_files,
            'total_files': len(excel_files) + len(pdf_files) + len(image_files) + len(other_files),
            'metadata': metadata,
            'timestamp': datetime.now().isoformat()
        }
    
    def process_folder(self, folder_info: Dict) -> List[str]:
        """
        Process a folder of documents, copying them to a temp location.
        
        Args:
            folder_info: Dictionary with folder information
            
        Returns:
            List of paths to copied files ready for processing
        """
        folder_path = folder_info['folder_path']
        data_dir = os.path.join("data", "raw")
        os.makedirs(data_dir, exist_ok=True)
        
        temp_folder = os.path.join(data_dir, f"{folder_info['id']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        os.makedirs(temp_folder, exist_ok=True)
        
        logger.info(f"Processing folder: {folder_path}")
        logger.info(f"Temporary folder: {temp_folder}")
        
        # Copy all files to temp folder
        copied_files = []
        
        # Gather all files from the folder info
        all_files = (
            folder_info.get('excel_files', []) + 
            folder_info.get('pdf_files', []) + 
            folder_info.get('image_files', []) + 
            folder_info.get('other_files', [])
        )
        
        for file_path in all_files:
            try:
                filename = os.path.basename(file_path)
                dest_path = os.path.join(temp_folder, filename)
                shutil.copy2(file_path, dest_path)
                copied_files.append(dest_path)
                logger.info(f"Copied file: {filename}")
            except Exception as e:
                logger.error(f"Error copying file {file_path}: {str(e)}")
        
        logger.info(f"Copied {len(copied_files)} files to temporary folder")
        return copied_files
    
    def mark_as_processed(self, folder_info: Dict, status: str, results: Dict = None) -> None:
        """
        Mark a folder as processed by moving it to the processed folder.
        
        Args:
            folder_info: Dictionary with folder information
            status: Processing status ("success" or "error")
            results: Optional dictionary with processing results
        """
        folder_path = folder_info['folder_path']
        folder_name = folder_info['folder_name']
        
        # Create timestamped folder name to avoid conflicts
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        status_prefix = "SUCCESS" if status == "success" else "ERROR"
        new_folder_name = f"{status_prefix}_{timestamp}_{folder_name}"
        
        # Create path in processed folder
        processed_path = os.path.join(self.processed_folder, new_folder_name)
        
        # Create results file
        try:
            with open(os.path.join(folder_path, "PROCESSING_RESULTS.txt"), 'w') as f:
                f.write(f"Status: {status}\n")
                f.write(f"Processed: {datetime.now().isoformat()}\n")
                f.write(f"Folder: {folder_path}\n\n")
                
                if results:
                    f.write("Results:\n")
                    for key, value in results.items():
                        f.write(f"{key}: {value}\n")
        except Exception as e:
            logger.error(f"Error creating results file: {str(e)}")
        
        # Move the folder to processed location
        try:
            shutil.move(folder_path, processed_path)
            logger.info(f"Moved folder to processed location: {processed_path}")
        except Exception as e:
            logger.error(f"Error moving folder to processed location: {str(e)}")
            # Try to rename the ready marker instead
            try:
                ready_marker = os.path.join(folder_path, "READY_FOR_PROCESSING.txt")
                if os.path.exists(ready_marker):
                    os.rename(ready_marker, os.path.join(folder_path, f"PROCESSED_{status_prefix}_{timestamp}.txt"))
                    logger.info(f"Renamed ready marker to indicate processing status")
            except Exception as e2:
                logger.error(f"Error renaming ready marker: {str(e2)}")