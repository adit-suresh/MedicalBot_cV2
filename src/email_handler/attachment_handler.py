import os
import base64
import logging
from datetime import datetime
from typing import Dict, List, Optional, Set
import hashlib
import shutil
import re
import threading
import mimetypes
import magic  # python-magic package for better file type detection

from config.settings import RAW_DATA_DIR, ATTACHMENT_TYPES
from config.constants import FILE_NAME_PATTERN
from src.utils.exceptions import AttachmentError
from src.utils.error_handling import handle_errors, ErrorCategory, ErrorSeverity

logger = logging.getLogger(__name__)

class AttachmentHandler:
    """Enhanced attachment handler with improved validation and file handling."""
    
    def __init__(self, download_dir: Optional[str] = None):
        """Initialize attachment handler.
        
        Args:
            download_dir: Optional custom directory for saving attachments
        """
        self.download_dir = download_dir or RAW_DATA_DIR
        self._processed_files: Set[str] = set()  # Track processed files
        self._lock = threading.RLock()  # Thread safety
        
        # Ensure directory exists
        os.makedirs(self.download_dir, exist_ok=True)
        
        # Initialize magic for file type detection
        self.mime_detector = magic.Magic(mime=True)
        
    def is_valid_attachment(self, attachment: Dict) -> bool:
        """
        Validate attachment type and name with enhanced security checks.
        
        Args:
            attachment: Attachment dictionary from Graph API
            
        Returns:
            bool: Whether attachment is valid
        """
        name = attachment.get("name", "")
        if not name:
            logger.warning("Attachment has no name")
            return False
            
        # Get file extension
        file_ext = os.path.splitext(name)[1].lower()
        
        # Check attachment size
        size = attachment.get("size", 0)
        if size == 0:
            logger.warning(f"Attachment {name} has zero size")
            return False
        elif size > 25 * 1024 * 1024:  # 25MB limit
            logger.warning(f"Attachment {name} exceeds size limit (25MB)")
            return False
            
        # Log validation steps
        logger.debug(f"Validating attachment: {name}")
        logger.debug(f"File extension: {file_ext}")
        logger.debug(f"File size: {size/1024:.1f} KB")
        
        # Check file extension against allowed types
        valid_extension = any(file_ext.endswith(ext.lower()) for ext in ATTACHMENT_TYPES)
        if not valid_extension:
            logger.debug(f"Extension {file_ext} not in allowed types: {ATTACHMENT_TYPES}")
            return False

        # Check filename pattern for security
        valid_pattern = bool(FILE_NAME_PATTERN.match(name))
        if not valid_pattern:
            logger.debug(f"Filename {name} doesn't match security pattern {FILE_NAME_PATTERN.pattern}")
            return False
            
        # Check for potentially malicious file names
        if self._is_potentially_dangerous_filename(name):
            logger.warning(f"Potentially dangerous filename detected: {name}")
            return False

        logger.debug("Attachment passed all validation checks")
        return True
        
    def _is_potentially_dangerous_filename(self, filename: str) -> bool:
        """Check if filename appears potentially dangerous."""
        # Check for double extensions (e.g. doc.exe)
        if re.match(r'.*\.(jpg|pdf|png|txt|xlsx?)\.(exe|bat|cmd|vbs|ps1|sh)$', filename, re.IGNORECASE):
            return True
            
        # Check for unusual Unicode characters that might be used to deceive
        if re.search(r'[\u202E\u200E\u200F\u061C]', filename):  # RTL/LTR override characters
            return True
            
        # Check for common suspicious patterns
        suspicious_patterns = [
            r'\.exe$', r'\.bat$', r'\.cmd$', r'\.ps1$', r'\.vbs$', r'\.js$',
            r'_vir', r'hack', r'crack', r'keygen', r'patch'
        ]
        
        for pattern in suspicious_patterns:
            if re.search(pattern, filename, re.IGNORECASE):
                return True
                
        return False

    @handle_errors(ErrorCategory.PROCESSING, ErrorSeverity.MEDIUM)
    def save_attachment(self, attachment: Dict, email_id: str) -> str:
        """
        Save attachment to disk with improved security and error handling.
        
        Args:
            attachment: Attachment dictionary from Graph API
            email_id: ID of the email
            
        Returns:
            str: Path where attachment was saved
            
        Raises:
            AttachmentError: If saving attachment fails
        """
        try:
            # Create directory with timestamp to prevent collisions
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
            email_dir = os.path.join(self.download_dir, f"{email_id}_{timestamp}")
            os.makedirs(email_dir, exist_ok=True)

            # Clean and sanitize filename
            original_name = attachment.get("name", "unknown_file")
            safe_name = self._sanitize_filename(original_name)
            file_path = os.path.join(email_dir, safe_name)
            
            logger.info(f"Saving attachment {original_name} to {file_path}")
            
            # Get content bytes
            content_bytes = attachment.get("contentBytes")
            if not content_bytes:
                raise AttachmentError(f"No content bytes in attachment {original_name}")
                
            # Decode content
            try:
                content = base64.b64decode(content_bytes)
            except Exception as e:
                raise AttachmentError(f"Failed to decode attachment content: {str(e)}")
                
            # Verify file isn't empty
            if len(content) == 0:
                raise AttachmentError(f"Attachment {original_name} has empty content")
                
            # Check actual file type with magic
            detected_mime = self.mime_detector.from_buffer(content)
            expected_mime = mimetypes.guess_type(original_name)[0] or 'application/octet-stream'
            
            # Verify mime type is consistent with extension
            if not self._is_mimetype_valid(detected_mime, expected_mime, original_name):
                raise AttachmentError(
                    f"File type mismatch for {original_name}: "
                    f"expected {expected_mime}, got {detected_mime}"
                )
            
            # Write content to file
            with open(file_path, "wb") as f:
                f.write(content)
            
            # Verify file was written correctly
            file_size = os.path.getsize(file_path)
            if file_size != len(content):
                raise AttachmentError(
                    f"File size mismatch: expected {len(content)} bytes, got {file_size} bytes"
                )
                
            # Calculate and log file hash for traceability
            file_hash = hashlib.md5(content).hexdigest()
            logger.info(f"Successfully saved {original_name} ({file_size} bytes, MD5: {file_hash})")
            
            # Track this file as processed
            with self._lock:
                self._processed_files.add(file_path)
                
            return file_path

        except Exception as e:
            if 'file_path' in locals() and os.path.exists(file_path):
                # Clean up partially written file
                try:
                    os.remove(file_path)
                except:
                    pass
                    
            logger.error(f"Failed to save attachment {attachment.get('name', 'unknown')}: {str(e)}")
            raise AttachmentError(f"Failed to save attachment: {str(e)}")
            
    def _sanitize_filename(self, filename: str) -> str:
        """Sanitize filename for security."""
        # Remove control characters and common problematic characters
        sanitized = re.sub(r'[\\/*?:"<>|]', '_', filename)
        # Replace runs of spaces and underscores with a single underscore
        sanitized = re.sub(r'[\s_]+', '_', sanitized)
        # Add a timestamp if name gets too short from sanitization
        if len(sanitized) < 5:
            sanitized = f"{sanitized}_{datetime.now().strftime('%H%M%S')}"
        return sanitized
        
    def _is_mimetype_valid(self, detected_mime: str, expected_mime: str, filename: str) -> bool:
        """
        Validate if the detected mimetype is compatible with the expected type.
        
        Args:
            detected_mime: MIME type detected from file content
            expected_mime: Expected MIME type based on file extension
            filename: Original filename
            
        Returns:
            bool: Whether the mimetype is valid
        """
        # Handle special cases
        file_ext = os.path.splitext(filename)[1].lower()
        
        # PDF validation
        if file_ext == '.pdf':
            return detected_mime == 'application/pdf'
            
        # Image validation
        if file_ext in ['.jpg', '.jpeg', '.png']:
            return detected_mime.startswith('image/')
            
        # Excel validation
        if file_ext in ['.xlsx', '.xls']:
            valid_excel_types = [
                'application/vnd.ms-excel',
                'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                'application/octet-stream'  # Sometimes Excel files are detected as binary
            ]
            return detected_mime in valid_excel_types
            
        # For other types, check for general compatibility
        if expected_mime and detected_mime:
            # Compare major type (e.g., 'image', 'application')
            expected_major = expected_mime.split('/')[0]
            detected_major = detected_mime.split('/')[0]
            return expected_major == detected_major
            
        # Default to permissive for unrecognized types
        return True

    @handle_errors(ErrorCategory.PROCESSING, ErrorSeverity.MEDIUM)
    def process_attachments(self, attachments: List[Dict], email_id: str) -> List[str]:
        """
        Process and save all valid attachments from an email with enhanced error handling.
        
        Args:
            attachments: List of attachment dictionaries
            email_id: ID of the email
            
        Returns:
            List[str]: Paths of saved attachments
            
        Raises:
            AttachmentError: If processing attachments fails
        """
        saved_paths = []
        skipped = 0
        errors = 0
        
        for attachment in attachments:
            name = attachment.get("name", "unknown")
            try:
                logger.info(f"Processing attachment: {name}")
                
                # Skip attachments that are inline images (often signatures)
                if attachment.get("isInline", False):
                    logger.info(f"Skipping inline attachment: {name}")
                    skipped += 1
                    continue
                
                if self.is_valid_attachment(attachment):
                    path = self.save_attachment(attachment, email_id)
                    saved_paths.append(path)
                    logger.info(f"Successfully processed: {name}")
                else:
                    logger.info(f"Skipping invalid attachment: {name}")
                    skipped += 1
                    
            except AttachmentError as e:
                logger.error(f"Failed to save {name}: {str(e)}")
                errors += 1
                continue
            except Exception as e:
                logger.error(f"Unexpected error processing {name}: {str(e)}")
                errors += 1
                continue
                
        # Log summary
        logger.info(f"Attachment processing complete for email {email_id}:")
        logger.info(f"  Successfully saved: {len(saved_paths)}")
        logger.info(f"  Skipped (invalid): {skipped}")
        logger.info(f"  Errors: {errors}")
        
        # If we couldn't process any attachments, that's a problem
        if not saved_paths and (skipped > 0 or errors > 0):
            logger.warning(f"No attachments were successfully saved from email {email_id}")
            
        return saved_paths
        
    def get_file_info(self, file_path: str) -> Dict:
        """
        Get detailed information about a file.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Dictionary with file information
        """
        if not os.path.exists(file_path):
            return {'exists': False, 'path': file_path}
            
        try:
            file_size = os.path.getsize(file_path)
            file_modified = datetime.fromtimestamp(os.path.getmtime(file_path))
            file_created = datetime.fromtimestamp(os.path.getctime(file_path))
            
            # Get first 4KB of file to determine type
            with open(file_path, 'rb') as f:
                header = f.read(4096)
                mime_type = self.mime_detector.from_buffer(header)
                
            return {
                'exists': True,
                'path': file_path,
                'name': os.path.basename(file_path),
                'size': file_size,
                'size_human': self._format_size(file_size),
                'modified': file_modified,
                'created': file_created,
                'mime_type': mime_type,
                'is_processed': file_path in self._processed_files
            }
        except Exception as e:
            logger.error(f"Error getting file info for {file_path}: {str(e)}")
            return {
                'exists': True,
                'path': file_path,
                'name': os.path.basename(file_path),
                'error': str(e)
            }
            
    def _format_size(self, size_bytes: int) -> str:
        """Format file size in human-readable format."""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024 or unit == 'GB':
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024
            
    def cleanup_old_files(self, days: int = 7) -> int:
        """
        Remove attachments older than specified days.
        
        Args:
            days: Number of days to keep files
            
        Returns:
            Number of files removed
        """
        cutoff_time = datetime.now().timestamp() - (days * 86400)
        removed_count = 0
        
        for root, dirs, files in os.walk(self.download_dir):
            for dir_name in dirs:
                dir_path = os.path.join(root, dir_name)
                try:
                    dir_mtime = os.path.getmtime(dir_path)
                    if dir_mtime < cutoff_time:
                        # Check if directory is empty
                        if not os.listdir(dir_path):
                            os.rmdir(dir_path)
                            logger.info(f"Removed empty directory: {dir_path}")
                except Exception as e:
                    logger.error(f"Error cleaning up directory {dir_path}: {str(e)}")
                    
            for file_name in files:
                file_path = os.path.join(root, file_name)
                try:
                    file_mtime = os.path.getmtime(file_path)
                    if file_mtime < cutoff_time:
                        os.remove(file_path)
                        logger.info(f"Removed old file: {file_path}")
                        removed_count += 1
                        
                        # Remove from processed files tracking
                        with self._lock:
                            if file_path in self._processed_files:
                                self._processed_files.remove(file_path)
                                
                except Exception as e:
                    logger.error(f"Error removing file {file_path}: {str(e)}")
                    
        return removed_count