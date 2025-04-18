#!/usr/bin/env python3
# test_complete_workflow.py

import os
import sys
import logging
import pandas as pd
import re
from datetime import datetime
from typing import Dict, Optional, List
import json
import argparse
import shutil
import time
import copy 

# Add project root to Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_root)

# Import original components
from src.email_handler.outlook_client import OutlookClient
from src.email_handler.attachment_handler import AttachmentHandler
from src.document_processor.textract_processor import TextractProcessor
from src.document_processor.deepseek_processor import DeepseekProcessor
from src.services.enhanced_document_processor import EnhancedDocumentProcessorService
from src.utils.file_sharer import FileSharer
from src.utils.teams_notifier import TeamsNotifier
from src.utils.email_sender import EmailSender
from src.document_processor.gpt_processor import GPTProcessor

# Import original workflow components
from src.utils.process_tracker import ProcessTracker
from src.services.data_combiner import DataCombiner
from src.document_processor.excel_processor import EnhancedExcelProcessor as ExcelProcessor
from src.folder_processor import FolderProcessor

class FolderProcessor:
    def __init__(self, *args, **kwargs):
        """Initialize folder processor with watch folder configuration."""
        self.watch_folder = "input_documents"
        self.processed_folders_file = "processed_folders.json"
        
        # Create the watch folder if it doesn't exist
        os.makedirs(self.watch_folder, exist_ok=True)
        
        # Create a subfolder for your_documents if it doesn't exist
        self.your_documents_folder = os.path.join(self.watch_folder, "your_documents")
        os.makedirs(self.your_documents_folder, exist_ok=True)
        
        logger.info(f"FolderProcessor initialized with watch folder: {self.watch_folder}")
        logger.info(f"Place documents in: {self.your_documents_folder}")
            
    def check_for_documents(self):
        """
        Check for new document folders in the input_documents/your_documents directory.
        
        Returns:
            List of dictionaries with folder information for processing
        """
        try:
            logger.info(f"Checking for documents in {self.your_documents_folder}")
            
            # Check if the directory exists
            if not os.path.exists(self.your_documents_folder):
                logger.warning(f"Directory does not exist: {self.your_documents_folder}")
                return []
                
            # Get list of all subdirectories in the your_documents folder
            # Each subdirectory is considered a separate submission
            folders = []
            
            # First check immediate files in your_documents
            files_in_root = [f for f in os.listdir(self.your_documents_folder) 
                        if os.path.isfile(os.path.join(self.your_documents_folder, f))
                        and not f.startswith('.')]
                        
            if files_in_root:
                # If there are files directly in your_documents, treat as one submission
                logger.info(f"Found {len(files_in_root)} files in root of your_documents")
                folder_info = {
                    'folder_name': 'your_documents',
                    'folder_path': self.your_documents_folder,
                    'is_root': True,
                    'total_files': len(files_in_root),
                    'files': files_in_root,
                    'id': f"local_{datetime.now().strftime('%Y%m%d%H%M%S')}_{len(files_in_root)}",
                    'subject': f"Local Submission - {len(files_in_root)} files - {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                }
                
                # Check if already processed
                if not self._is_folder_processed(folder_info['id']):
                    folders.append(folder_info)
                else:
                    logger.info(f"Skipping already processed root folder: {folder_info['id']}")
            
            # Then check subdirectories
            subdirs = [d for d in os.listdir(self.your_documents_folder) 
                    if os.path.isdir(os.path.join(self.your_documents_folder, d))
                    and not d.startswith('.')]
                    
            for subdir in subdirs:
                dir_path = os.path.join(self.your_documents_folder, subdir)
                files = [f for f in os.listdir(dir_path) 
                        if os.path.isfile(os.path.join(dir_path, f))
                        and not f.startswith('.')]
                        
                if files:
                    folder_info = {
                        'folder_name': subdir,
                        'folder_path': dir_path,
                        'is_root': False,
                        'total_files': len(files),
                        'files': files,
                        'id': f"local_{subdir}_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                        'subject': f"Local Submission - {subdir} - {len(files)} files",
                    }
                    
                    # Check if already processed
                    if not self._is_folder_processed(folder_info['id']):
                        folders.append(folder_info)
                    else:
                        logger.info(f"Skipping already processed subfolder: {folder_info['id']}")
            
            logger.info(f"Found {len(folders)} unprocessed folders/submissions")
            return folders
            
        except Exception as e:
            logger.error(f"Error checking for documents: {str(e)}", exc_info=True)
            return []
            
    def process_folder(self, folder_info):
        """
        Process a folder by copying its files to a temporary location.
        
        Args:
            folder_info: Dictionary with folder metadata
            
        Returns:
            List of paths to copied files
        """
        try:
            logger.info(f"Processing folder: {folder_info['folder_name']} with {folder_info['total_files']} files")
            
            # Create a temporary directory for this submission
            temp_dir = os.path.join("temp_submissions", folder_info['id'])
            os.makedirs(temp_dir, exist_ok=True)
            
            saved_files = []
            
            # Copy all files to the temporary directory
            for file_name in folder_info['files']:
                if folder_info.get('is_root', False):
                    source_path = os.path.join(self.your_documents_folder, file_name)
                else:
                    source_path = os.path.join(folder_info['folder_path'], file_name)
                    
                # Skip .DS_Store and other hidden files
                if file_name.startswith('.'):
                    continue
                    
                # Get file extension
                _, ext = os.path.splitext(file_name)
                
                # Check if it's a valid file type
                if ext.lower() not in ['.pdf', '.jpg', '.jpeg', '.png', '.xlsx', '.xls']:
                    logger.warning(f"Skipping unsupported file type: {file_name}")
                    continue
                
                # Copy file to temp directory
                dest_path = os.path.join(temp_dir, file_name)
                shutil.copy2(source_path, dest_path)
                logger.info(f"Copied {file_name} to {temp_dir}")
                
                saved_files.append(dest_path)
            
            logger.info(f"Processed {len(saved_files)} files from folder {folder_info['folder_name']}")
            return saved_files
            
        except Exception as e:
            logger.error(f"Error processing folder {folder_info['folder_name']}: {str(e)}", exc_info=True)
            return []
            
    def mark_as_processed(self, folder_info, status, result=None):
        """
        Mark a folder as processed to avoid reprocessing.
        
        Args:
            folder_info: Dictionary with folder metadata
            status: Processing status ('success' or 'error')
            result: Optional result information
        """
        try:
            # Load existing processed folders
            processed_folders = {}
            if os.path.exists(self.processed_folders_file):
                try:
                    with open(self.processed_folders_file, 'r') as f:
                        processed_folders = json.load(f)
                except json.JSONDecodeError:
                    logger.error("Error parsing processed_folders.json, treating as empty")
                    processed_folders = {}
            
            # Add this folder with processing information
            processed_folders[folder_info['id']] = {
                'folder_name': folder_info['folder_name'],
                'folder_path': folder_info['folder_path'],
                'total_files': folder_info['total_files'],
                'status': status,
                'processed_at': datetime.now().isoformat(),
                'result': result
            }
            
            # Save updated processed folders
            with open(self.processed_folders_file, 'w') as f:
                json.dump(processed_folders, f, indent=2)
                
            logger.info(f"Marked folder {folder_info['folder_name']} as {status}")
            
            # If successful, move files to a "processed" subdirectory
            if status == 'success':
                try:
                    # Create processed subdirectory if it doesn't exist
                    processed_dir = os.path.join(self.your_documents_folder, "processed")
                    os.makedirs(processed_dir, exist_ok=True)
                    
                    # Create a subfolder for this specific submission
                    submission_processed_dir = os.path.join(processed_dir, folder_info['id'])
                    os.makedirs(submission_processed_dir, exist_ok=True)
                    
                    # Move or copy files to processed directory
                    for file_name in folder_info['files']:
                        if folder_info.get('is_root', False):
                            source_path = os.path.join(self.your_documents_folder, file_name)
                        else:
                            source_path = os.path.join(folder_info['folder_path'], file_name)
                            
                        if os.path.exists(source_path):
                            dest_path = os.path.join(submission_processed_dir, file_name)
                            # Copy instead of move to avoid disrupting any ongoing processes
                            shutil.copy2(source_path, dest_path)
                            logger.info(f"Copied processed file to: {dest_path}")
                    
                    logger.info(f"Files for {folder_info['folder_name']} copied to processed directory")
                except Exception as e:
                    logger.error(f"Error copying files to processed directory: {str(e)}", exc_info=True)
                    
        except Exception as e:
            logger.error(f"Error marking folder as processed: {str(e)}", exc_info=True)
    
    def _is_folder_processed(self, folder_id):
        """
        Check if a folder has already been processed.
        
        Args:
            folder_id: Unique identifier for the folder
            
        Returns:
            Boolean indicating if the folder has been processed
        """
        try:
            # Load existing processed folders
            if not os.path.exists(self.processed_folders_file):
                return False
                
            with open(self.processed_folders_file, 'r') as f:
                processed_folders = json.load(f)
                
            return folder_id in processed_folders
        except Exception as e:
            logger.error(f"Error checking if folder is processed: {str(e)}", exc_info=True)
            return False

def is_email_processed(email_id):
    """Simple check if email has been processed."""
    processed_file = "processed_emails_simple.txt"
    
    # Check if file exists and contains the email ID
    if os.path.exists(processed_file):
        with open(processed_file, 'r') as f:
            processed_ids = [line.strip() for line in f.readlines()]
            return email_id in processed_ids
    return False

def mark_email_processed(email_id):
    """Mark email as processed in simple tracking file."""
    processed_file = "processed_emails_simple.txt"
    
    # Append email ID to file
    with open(processed_file, 'a') as f:
        f.write(f"{email_id}\n")
    logger.info(f"Marked email {email_id} as processed")

# Create WorkflowTester class from the original file
class CompletedSubmission:
    def __init__(self, process_id: str, documents: Dict[str, str], final_excel: str):
        self.process_id = process_id
        self.documents = documents  # Map of document types to file paths
        self.final_excel = final_excel
        self.timestamp = datetime.now()
        self.status = 'completed'

class WorkflowTester:
    def __init__(self):
        # Initialize services
        self.outlook = OutlookClient()
        self.attachment_handler = AttachmentHandler()
        self.textract = TextractProcessor()
        self.DEFAULT_VALUE = "."  
        
        # Initialize folder processor
        self.folder_processor = FolderProcessor()
        logger.info("Initialized folder processor")
        
        try:
            self.gpt = GPTProcessor()  
            logger.info("GPT processor initialized")
        except Exception as e:
            logger.warning(f"Failed to initialize GPT: {str(e)}")
            self.gpt = None
            
        self.document_processor = EnhancedDocumentProcessorService(self.textract, self.gpt)
        self.file_sharer = FileSharer()
        
        self.excel_processor = ExcelProcessor()
        self.data_combiner = DataCombiner(self.textract, self.excel_processor, self.gpt)
        self.process_tracker = ProcessTracker()
        self.teams_notifier = TeamsNotifier()
        self.email_sender = EmailSender()
        
        # Validate templates
        self._validate_templates()
        
        # Storage for completed submissions
        self.completed_submissions: List[CompletedSubmission] = []
        
        # Create necessary directories
        os.makedirs("processed_submissions", exist_ok=True)
        os.makedirs("templates", exist_ok=True)

        
    def _select_template_for_company(self, subject: str) -> str:
        """Select the appropriate template based on the email subject."""
        subject_lower = subject.lower()
        
        # Check for Al Madallah first
        if any(term in subject_lower for term in ['al madallah', 'almadallah', 'madallah', 'al-madallah']):
            template = 'templates/al_madallah.xlsx'
            logger.info(f"Selected Al Madallah template: {template}")
            return template
            
        # Default to NAS template
        template = 'templates/nas.xlsx'
        logger.info(f"Selected NAS template (default): {template}")
        return template
    
    def _validate_templates(self):
        """Ensure all required templates exist."""
        templates = [
            'templates/nas.xlsx',
            'templates/al_madallah.xlsx',
            'templates/union.xlsx',
            'templates/al_sagar.xlsx',
            'templates/dic.xlsx',
            'templates/dni.xlsx',
            'templates/ngi.xlsx',
            'templates/qic.xlsx',
            'templates/orient.xlsx',
            'templates/takaful.xlsx'
        ]
        
        missing = []
        for template in templates:
            if not os.path.exists(template):
                missing.append(template)
        
        if missing:
            logger.warning(f"Missing templates: {', '.join(missing)}")
            logger.warning("Default template will be used for missing templates")
            
            
    def _is_document_processed(self, file_path: str) -> bool:
        """Check if a document has already been processed by computing a hash of the file."""
        import hashlib
        
        # Generate a hash of the file content to uniquely identify it regardless of name
        try:
            with open(file_path, 'rb') as f:
                file_hash = hashlib.md5(f.read()).hexdigest()
                
            processed_docs_file = "processed_documents.json"
            processed_docs = {}
            
            # Load existing processed documents
            if os.path.exists(processed_docs_file):
                try:
                    with open(processed_docs_file, 'r') as f:
                        processed_docs = json.load(f)
                except json.JSONDecodeError:
                    logger.error("Error parsing processed_documents.json, treating as empty")
                    # Create a backup of the corrupted file
                    backup_name = f"processed_documents.json.corrupted.{int(time.time())}"
                    shutil.copy2(processed_docs_file, backup_name)
                    logger.info(f"Created backup of corrupted file: {backup_name}")
                    
                    # Create a new empty file
                    with open(processed_docs_file, "w") as f_new:
                        f_new.write("{}")
                    processed_docs = {}
                    
            # Check if file hash exists in processed documents
            if file_hash in processed_docs:
                logger.info(f"Document already processed: {file_path} (hash: {file_hash})")
                return True
                
            # Not processed yet
            return False
        except Exception as e:
            logger.error(f"Error checking document processed status: {str(e)}")
            # In case of error, assume not processed to be safe
            return False

    def _mark_document_processed(self, file_path: str) -> None:
        """Mark a document as processed by saving its hash."""
        import hashlib
        
        try:
            # Generate a hash of the file content
            with open(file_path, 'rb') as f:
                file_hash = hashlib.md5(f.read()).hexdigest()
                
            processed_docs_file = "processed_documents.json"
            processed_docs = {}
            
            # Load existing processed documents
            if os.path.exists(processed_docs_file):
                try:
                    with open(processed_docs_file, 'r') as f:
                        processed_docs = json.load(f)
                except json.JSONDecodeError:
                    logger.error("Error parsing processed_documents.json, treating as empty")
                    processed_docs = {}
            
            # Add this document hash with file path and timestamp
            processed_docs[file_hash] = {
                "path": file_path,
                "processed_at": datetime.now().isoformat(),
                "file_name": os.path.basename(file_path)
            }
            
            # Save updated processed documents
            with open(processed_docs_file, 'w') as f:
                json.dump(processed_docs, f, indent=2)
                
            logger.info(f"Marked document as processed: {file_path} (hash: {file_hash})")
        except Exception as e:
            logger.error(f"Error marking document as processed: {str(e)}")
           
    def run_complete_workflow(self, bypass_dedup=False) -> Dict:
        """Run complete workflow from email to final Excel."""
        try:
            # Check for documents in local folder first
            if hasattr(self, 'folder_processor'):
                try:
                    folder_documents = self.folder_processor.check_for_documents()
                    
                    if folder_documents:
                        logger.info(f"Found {len(folder_documents)} folders with documents to process")
                        
                        for folder_info in folder_documents:
                            try:
                                logger.info(f"Processing folder: {folder_info['folder_name']} with {folder_info['total_files']} files")
                                
                                # Process the folder (copy files to temp location)
                                saved_files = self.folder_processor.process_folder(folder_info)
                                
                                if saved_files:
                                    # Create a synthetic email object to reuse existing processing logic
                                    synthetic_email = {
                                        'id': folder_info['id'],
                                        'subject': folder_info['subject'],
                                        'receivedDateTime': datetime.now().isoformat(),
                                        'from': {'emailAddress': {'address': 'local_folder@system.internal'}},
                                        'source': 'folder'
                                    }
                                    
                                    # Process this "email" with existing logic
                                    logger.info(f"Processing local folder as synthetic email with ID: {synthetic_email['id']}")
                                    result = self._process_folder_as_email(synthetic_email, saved_files)
                                    
                                    # Mark the folder as processed
                                    self.folder_processor.mark_as_processed(
                                        folder_info, 
                                        "success" if result['status'] == 'success' else "error", 
                                        result
                                    )
                                    
                                    # If we processed at least one folder successfully, return result
                                    if result['status'] == 'success':
                                        return {
                                            "status": "success",
                                            "source": "folder",
                                            "folders_processed": 1,
                                            "successful": 1,
                                            "skipped": 0,
                                            "failed": 0,
                                            "emails_processed": 0,
                                            "details": result
                                        }
                            except Exception as e:
                                logger.error(f"Error processing folder {folder_info['folder_name']}: {str(e)}", exc_info=True)
                                if hasattr(self, 'folder_processor'):
                                    self.folder_processor.mark_as_processed(folder_info, "error", {"error": str(e)})
                except Exception as e:
                    logger.error(f"Error in folder processing: {str(e)}", exc_info=True)
            
            # Step 1: Fetch and process new emails
            logger.info("Step 1: Fetching emails...")
            emails = self.outlook.fetch_emails()
            logger.info(f"Fetched {len(emails)} emails before filtering")
            
            # Force deduplication
            if not bypass_dedup:
                emails = force_deduplication(emails)
                logger.info(f"After force deduplication: {len(emails)} emails remain")
            
            # Initialize email tracker
            from src.email_tracker.email_tracker import EmailTracker
            email_tracker = EmailTracker()
            
            # Check tracker file integrity
            processed_ids = set()
            try:
                if os.path.exists("processed_emails.json"):
                    with open("processed_emails.json", "r") as f:
                        try:
                            processed_data = json.load(f)
                            for email_id in processed_data:
                                processed_ids.add(email_id)
                            logger.info(f"Loaded {len(processed_ids)} processed email IDs from tracking file")
                        except json.JSONDecodeError:
                            logger.error("Error parsing processed_emails.json, treating as empty")
                            # Create a backup of the corrupted file
                            import shutil
                            backup_name = f"processed_emails.json.corrupted.{int(time.time())}"
                            shutil.copy2("processed_emails.json", backup_name)
                            logger.info(f"Created backup of corrupted file: {backup_name}")
                            # Create a new empty file
                            with open("processed_emails.json", "w") as f_new:
                                f_new.write("{}")
            except Exception as e:
                logger.error(f"Error loading processed email IDs: {str(e)}")
            
            # Examine subjects for debugging
            subjects_found = [email.get('subject', '').strip() for email in emails]
            logger.info(f"Found emails with subjects: {subjects_found}")

            # Log each email we're going to process
            for idx, email in enumerate(emails, 1):
                email_id = email['id']
                subject = email.get('subject', '').strip()
                # Check both methods
                is_processed_tracker = email_tracker.is_processed(email_id)
                is_processed_direct = email_id in processed_ids
                
                if is_processed_tracker != is_processed_direct:
                    logger.warning(f"Inconsistent tracking state for email {email_id}: tracker={is_processed_tracker}, direct={is_processed_direct}")
                    
                is_processed = is_processed_tracker or is_processed_direct
                
                logger.info(f"Email {idx}: ID={email_id}, Subject={subject}")
                logger.info(f"  - Is processed (tracker): {is_processed_tracker}")
                logger.info(f"  - Is processed (direct): {is_processed_direct}")
                
                if not bypass_dedup and is_processed:
                    logger.info(f"  - Status: Will be skipped (already processed)")
                else:
                    logger.info(f"  - Status: Will be processed")
            
            # Check if --reset flag was used but didn't clear the tracker
            if bypass_dedup and len(processed_ids) > 0:
                logger.warning(f"Reset flag was used but {len(processed_ids)} processed emails still in tracker")
                
            results = []
            successful = 0
            skipped = 0
            failed = 0
            failed_emails = []  # Track details of failed emails
            
            for email in emails:
                try:
                    email_id = email['id']
                    subject = email.get('subject', '').strip()
                    
                    # Double-check processing status
                    is_processed_tracker = email_tracker.is_processed(email_id)
                    is_processed_direct = email_id in processed_ids
                    is_processed = is_processed_tracker or is_processed_direct
                    
                    # Skip if already processed (unless bypassing)
                    if not bypass_dedup and is_email_processed(email_id):
                        logger.info(f"Skipping already processed email: {subject}")
                        skipped += 1
                        continue
                        
                    # Process email
                    logger.info(f"Processing email: {subject}")
                    try:
                        result = self._process_single_email(email)
                        results.append(result)
                        
                        # Record result
                        if result['status'] == 'success':
                            logger.info(f"Successfully processed email: {subject}")
                            successful += 1
                            # Record as processed if successful
                            if not bypass_dedup:
                                mark_email_processed(email_id)
                                # Also update our direct tracking set
                                processed_ids.add(email_id)
                        else:
                            logger.error(f"Failed to process email: {subject}, Error: {result.get('error', 'Unknown error')}")
                            failed += 1
                            failed_emails.append({
                                'id': email_id,
                                'subject': subject,
                                'error': result.get('error', 'Unknown error'),
                                'received': email.get('receivedDateTime', 'Unknown')
                            })
                            
                    except Exception as e:
                        logger.error(f"Uncaught exception processing email {subject}: {str(e)}", exc_info=True)
                        failed += 1
                        failed_emails.append({
                            'id': email_id,
                            'subject': subject,
                            'error': str(e),
                            'received': email.get('receivedDateTime', 'Unknown')
                        })
                        
                except Exception as e:
                    logger.error(f"Error accessing email information: {str(e)}", exc_info=True)
                    failed += 1
                    failed_emails.append({
                        'id': email.get('id', 'Unknown'),
                        'subject': email.get('subject', 'Unknown'),
                        'error': str(e),
                        'received': email.get('receivedDateTime', 'Unknown')
                    })
                    continue
            
            # Save failed emails info to file for inspection
            if failed_emails:
                with open('failed_emails_debug.json', 'w') as f:
                    json.dump(failed_emails, f, indent=2, default=str)
                logger.info(f"Saved details of {len(failed_emails)} failed emails to failed_emails_debug.json")
            
            logger.info(f"Email processing summary:")
            logger.info(f"  - Total emails: {len(emails)}")
            logger.info(f"  - Successfully processed: {successful}")
            logger.info(f"  - Skipped (already processed): {skipped}")
            logger.info(f"  - Failed: {failed}")
            
            return {
                "status": "success",
                "emails_processed": len(emails),
                "successful": successful,
                "skipped": skipped,
                "failed": failed
            }

        except Exception as e:
            logger.error(f"Workflow failed: {str(e)}")
            return {
                "status": "error",
                "error": str(e)
            }

        except Exception as e:
            logger.error(f"Workflow failed: {str(e)}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    def _process_folder_as_email(self, synthetic_email: Dict, saved_files: List[str]) -> Dict:
        """
        Process a folder of documents as if it were an email submission.
        
        Args:
            synthetic_email: Dictionary with synthetic email information
            saved_files: List of paths to files already copied to temp location
            
        Returns:
            Dictionary with processing results
        """
        email_id = synthetic_email['id']
        subject = re.sub(r'[<>:"/\\|?*]', '', synthetic_email.get('subject', 'Local Folder Documents'))
        process_id = f"{subject}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        try:
            # Create submission directory
            submission_dir = os.path.join("processed_submissions", re.sub(r'[^a-zA-Z0-9]', '_', process_id)[:50])
            os.makedirs(submission_dir, exist_ok=True)
            
            logger.info(f"Processing folder documents with ID: {email_id}")
            logger.info(f"Subject: {subject}")
            logger.info(f"Saved files: {len(saved_files)}")
            
            if not saved_files:
                logger.warning(f"No valid files found in folder {email_id}")
                raise Exception("No valid files found")

            # Step 3: Categorize and process files
            document_paths = {}
            excel_files = []
            processed_docs = []

            for file_path in saved_files:
                file_type = self._determine_file_type(file_path)
                
                if file_type == 'excel':
                    excel_files.append(file_path)
                else:
                    doc_path = os.path.join(submission_dir, os.path.basename(file_path))
                    shutil.copy2(file_path, doc_path)
                    
                    # Store multiple documents of the same type
                    if file_type not in document_paths:
                        document_paths[file_type] = []
                    document_paths[file_type].append(doc_path)
                    
                    processed_docs.append({
                        'type': file_type,
                        'original_name': os.path.basename(file_path),
                        'path': doc_path
                    })
            
            logger.info(f"Categorized files: {len(excel_files)} Excel files, {len(document_paths)} documents")
            
            # Process documents
            extracted_data = {}
            if self.gpt:
                for doc_type, paths in document_paths.items():
                    if isinstance(paths, list):
                        for file_path in paths:
                            try:
                                # Check if document already processed
                                if hasattr(self, '_is_document_processed') and self._is_document_processed(file_path):
                                    logger.info(f"Skipping already processed document: {file_path}")
                                    # Try to get cached data if available
                                    if hasattr(self.gpt, '_extracted_cache'):
                                        for key, value in self.gpt._extracted_cache.items():
                                            if key not in extracted_data or value != self.DEFAULT_VALUE:
                                                extracted_data[key] = value
                                                logger.info(f"Using cached data for {key}: {value}")
                                    continue
                                
                                # Process with GPT
                                gpt_data = self.gpt.process_document(file_path, doc_type)
                                
                                # Mark as processed if successful
                                if gpt_data and 'error' not in gpt_data and hasattr(self, '_mark_document_processed'):
                                    self._mark_document_processed(file_path)
                                    
                                    # Update extracted data with GPT results
                                    for key, value in gpt_data.items():
                                        if key not in extracted_data or value != self.DEFAULT_VALUE:
                                            extracted_data[key] = value
                            except Exception as e:
                                logger.error(f"Error processing {doc_type} with GPT: {str(e)}")
            
            # Process Excel files
            all_excel_rows = []
            for excel_path in excel_files:
                try:
                    df, errors = self.excel_processor.process_excel(excel_path, dayfirst=True)
                    if not df.empty:
                        for _, row in df.iterrows():
                            row_dict = copy.deepcopy(row.to_dict())
                            
                            # Split names according to requirements if needed
                            if 'First Name' in row_dict and row_dict['First Name']:
                                full_name = str(row_dict['First Name']).strip()
                                name_parts = full_name.split()
                                
                                if len(name_parts) >= 3:
                                    # First word as first name, second as middle, third+ as last
                                    first_name = name_parts[0]
                                    middle_name = name_parts[1]
                                    last_name = ' '.join(name_parts[2:])
                                    
                                    row_dict['First Name'] = first_name
                                    row_dict['Middle Name'] = middle_name
                                    row_dict['Last Name'] = last_name
                                    
                                    logger.info(f"Split name '{full_name}' into First='{first_name}', Middle='{middle_name}', Last='{last_name}'")
                                elif len(name_parts) == 2:
                                    # First word as first name, '.' as middle, second as last
                                    first_name = name_parts[0]
                                    middle_name = '.'
                                    last_name = name_parts[1]
                                    
                                    row_dict['First Name'] = first_name
                                    row_dict['Middle Name'] = middle_name
                                    row_dict['Last Name'] = last_name
                                    
                                    logger.info(f"Split name '{full_name}' into First='{first_name}', Middle='{middle_name}', Last='{last_name}'")
                            
                            all_excel_rows.append(row_dict)
                except Exception as e:
                    logger.error(f"Error processing Excel file {excel_path}: {str(e)}")
            
            # Select template
            template_path = self._select_template_for_company(subject)
                
            # Combine data
            output_path = os.path.join(
                submission_dir,
                f"final_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            )
            
            if all_excel_rows is None or len(all_excel_rows) == 0:
                all_excel_rows = [{
                    "First Name": "",
                    "Middle Name": ".",
                    "Last Name": "",
                    "Effective Date": datetime.now().strftime('%d/%m/%Y'),
                    "DOB": "",
                    "Gender": ""
                }]
            
            # Apply Emirates ID and nationality formatting
            if 'emirates_id' in extracted_data:
                # Check if we have the _process_emirates_id method in data_combiner
                if hasattr(self.data_combiner, '_process_emirates_id'):
                    extracted_data['emirates_id'] = self.data_combiner._process_emirates_id(extracted_data['emirates_id'])
            
            if 'nationality' in extracted_data:
                # Check if we have the _standardize_nationality method in data_combiner
                if hasattr(self.data_combiner, '_standardize_nationality'):
                    extracted_data['nationality'] = self.data_combiner._standardize_nationality(extracted_data['nationality'])
            
            # Combine data using template
            result = self.data_combiner.combine_and_populate_template(
                template_path,
                output_path,
                extracted_data,
                all_excel_rows,
                document_paths
            )
            
            # Create a submission object
            submission = CompletedSubmission(
                process_id=process_id,
                documents=document_paths,
                final_excel=output_path
            )
            self.completed_submissions.append(submission)
            
            # Create a ZIP file
            zip_path = None
            try:
                zip_path = self._create_zip(submission_dir)
                logger.info(f"Created ZIP file: {zip_path}")
            except Exception as e:
                logger.error(f"Error creating ZIP file: {str(e)}")
                
            # Send email notification with results - ADDED EMAIL SENDING FUNCTIONALITY
            try:
                # Prepare email content
                email_subject = f"Medical Bot: Local Folder Submission - {subject} - Complete"
                
                email_body = f"""
                Dear Team,
                
                The Medical Bot has completed processing a local folder submission.
                
                Details:
                - Subject: {subject}
                - Files Processed: {len(saved_files)}
                - Documents: {list(document_paths.keys())}
                - Excel Files: {[os.path.basename(f) for f in excel_files]}
                - Rows Processed: {len(all_excel_rows) if all_excel_rows else 0}
                
                The processed files are attached as a ZIP archive.
                They are also available locally at: {submission_dir}
                
                Regards,
                Medical Bot
                """
                
                # Check if we have email sender
                if hasattr(self, 'email_sender') and self.email_sender:
                    try:
                        # Send email with ZIP attachment if available
                        if zip_path and os.path.exists(zip_path):
                            email_sent = self.email_sender.send_email(
                                subject=email_subject,
                                body=email_body,
                                attachment_path=zip_path
                            )
                        else:
                            # Send without attachment
                            email_sent = self.email_sender.send_email(
                                subject=email_subject,
                                body=email_body
                            )
                        
                        if email_sent:
                            logger.info("Email notification sent successfully for local folder submission")
                        else:
                            logger.warning("Failed to send email notification for local folder submission")
                    except Exception as e:
                        logger.error(f"Error sending email for local folder: {str(e)}", exc_info=True)
                else:
                    logger.warning("Email sender not available, skipping notification email")
                    
                # Also try sending via Teams if available
                if hasattr(self, 'teams_notifier') and self.teams_notifier:
                    try:
                        teams_message = f"🔔 Local folder submission processed: {subject} - {len(saved_files)} files, {len(all_excel_rows)} rows"
                        self.teams_notifier.send_notification(teams_message)
                        logger.info("Teams notification sent for local folder submission")
                    except Exception as e:
                        logger.error(f"Error sending Teams notification: {str(e)}")
                    
            except Exception as e:
                logger.error(f"Error sending notifications: {str(e)}", exc_info=True)
            
            return {
                "status": "success",
                "process_id": process_id,
                "submission_dir": submission_dir,
                "documents": document_paths,
                "excel_files": excel_files,
                "documents_processed": processed_docs,
                "rows_processed": len(all_excel_rows) if all_excel_rows else 0,
                "output_path": output_path,
                "zip_path": zip_path
            }
        
        except Exception as e:
            logger.error(f"Error processing folder {email_id}: {str(e)}", exc_info=True)
            return {
                "status": "error",
                "process_id": process_id,
                "error": str(e)
            }
    

    def _process_single_email(self, email: Dict) -> Dict:
        """Process a single email submission with improved error handling."""
        email_id = email['id']
        # Clean subject for folder name
        subject = re.sub(r'[<>:"/\\|?*]', '', email.get('subject', 'No Subject'))
        process_id = f"{subject}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        try:
            # Create submission directory
            submission_dir = os.path.join("processed_submissions", re.sub(r'[^a-zA-Z0-9]', '_', process_id)[:50])
            os.makedirs(submission_dir, exist_ok=True)
            
            # Step 2: Get and save attachments
            logger.info(f"Processing email {email_id} with subject: {subject}")
            try:
                attachments = self.outlook.get_attachments(email_id)
                logger.info(f"Retrieved {len(attachments)} attachments")
            except Exception as e:
                logger.error(f"Error getting attachments: {str(e)}", exc_info=True)
                raise Exception(f"Failed to get attachments: {str(e)}")
                    
            try:
                saved_files = self.attachment_handler.process_attachments(attachments, email_id)
                logger.info(f"Saved {len(saved_files)} attachment files")
            except Exception as e:
                logger.error(f"Error processing attachments: {str(e)}", exc_info=True)
                raise Exception(f"Failed to process attachments: {str(e)}")
            
            if not saved_files:
                logger.warning(f"No valid attachments found for email {email_id}")
                raise Exception("No valid attachments found")

            # Step 3: Categorize and process files
            try:
                document_paths = {}
                excel_files = []
                processed_docs = []
                all_excel_rows = []  # Initialize this variable here

                for file_path in saved_files:
                    file_type = self._determine_file_type(file_path)
                    
                    if file_type == 'excel':
                        excel_files.append(file_path)
                    else:
                        doc_path = os.path.join(submission_dir, os.path.basename(file_path))
                        shutil.copy2(file_path, doc_path)
                        
                        # Store multiple documents of the same type
                        if file_type not in document_paths:
                            document_paths[file_type] = []
                        document_paths[file_type].append(doc_path)
                        
                        processed_docs.append({
                            'type': file_type,
                            'original_name': os.path.basename(file_path),
                            'path': doc_path
                        })
                
                logger.info(f"Categorized files: {len(excel_files)} Excel files, {len(document_paths)} documents")
            except Exception as e:
                logger.error(f"Error categorizing files: {str(e)}", exc_info=True)
                raise Exception(f"Failed to categorize files: {str(e)}")
            
            # Check if we have just one Excel file and no documents (special case for large client Excel)
            if len(saved_files) == 1 and saved_files[0].lower().endswith(('.xlsx', '.xls')):
                try:
                    excel_path = saved_files[0]
                    
                    # Check if this is a large client Excel file
                    try:
                        df = pd.read_excel(excel_path)
                        logger.info(f"Examining Excel file with {len(df)} rows and {len(df.columns)} columns")
                        
                        # Log column names for debugging
                        logger.info(f"Excel columns: {list(df.columns)}")
                        
                        # Check for specific columns or large number of rows
                        client_excel_indicators = ['StaffNo', 'FirstName', 'Country', 'EIDNumber']
                        
                        # Check if any of the indicator columns exist (flexible matching)
                        columns_found = [col for col in client_excel_indicators if any(col.lower() in c.lower() for c in df.columns)]
                        
                        # Consider it a large client Excel if it has several matching columns or just a lot of rows
                        is_large_client = (len(columns_found) >= 2 and len(df) > 10) or len(df) > 100
                        
                        if is_large_client:
                            logger.info(f"Detected large client Excel file with {len(df)} rows")
                            logger.info(f"Matching columns found: {columns_found}")
                            
                            # Process as large client Excel
                            output_path = os.path.join(
                                submission_dir,
                                f"final_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                            )
                            
                            template_path = self._select_template_for_company(subject)
                            logger.info(f"Selected template: {template_path}")
                            
                            # Make sure to use self. to call it as a method
                            result = self._process_large_client_excel(excel_path, template_path, output_path)
                            
                            if result['status'] == 'success':
                                logger.info(f"Successfully processed large client Excel: {result['rows_processed']} rows")
                                
                                # Create a zip file for easy download
                                try:
                                    zip_path = self._create_zip(submission_dir)
                                    logger.info(f"Created ZIP file: {zip_path}")
                                except Exception as e:
                                    logger.error(f"Error creating ZIP file: {str(e)}")
                                    zip_path = None
                                
                                # Skip the rest of the document processing, use the processed output directly
                                return {
                                    "status": "success",
                                    "process_id": process_id,
                                    "submission_dir": submission_dir,
                                    "documents": {},
                                    "excel_files": [excel_path],
                                    "documents_processed": [],
                                    "rows_processed": result['rows_processed'],
                                    "output_path": output_path,
                                    "zip_path": zip_path
                                }
                            else:
                                logger.error(f"Failed to process large client Excel: {result.get('error', 'Unknown error')}")
                                # Continue with normal processing
                        else:
                            logger.info("Not a large client Excel file, processing normally")
                    except Exception as e:
                        logger.warning(f"Error checking for large client Excel: {str(e)}")
                        # Continue with normal processing
                except Exception as e:
                    logger.error(f"Error in large client Excel detection: {str(e)}")
                    # Continue with normal processing

            # Step 4: Process all documents
            try:
                logger.info(f"Processing {len(document_paths)} documents")
                extracted_data = {}
                
                # Process documents with GPT only
                if self.gpt:
                    for doc_type, paths in document_paths.items():
                        if isinstance(paths, list):
                            # Handle list of paths (new structure)
                            for file_path in paths:
                                try:
                                    # Check if document already processed
                                    if self._is_document_processed(file_path):
                                        logger.info(f"Skipping already processed document: {file_path}")
                                        continue
                                        
                                    logger.info(f"Processing {doc_type} with GPT: {file_path}")
                                    gpt_data = self.gpt.process_document(file_path, doc_type)
                                    
                                    # Mark as processed if successful
                                    if gpt_data and 'error' not in gpt_data:
                                        self._mark_document_processed(file_path)
                                    
                                    # Check if GPT extraction was successful
                                    if gpt_data and 'error' not in gpt_data:
                                        logger.info(f"GPT successfully extracted data from {doc_type}")
                                        # Update extracted data, giving priority to GPT results
                                        for key, value in gpt_data.items():
                                            if key not in extracted_data or value != self.DEFAULT_VALUE:
                                                extracted_data[key] = value
                                    else:
                                        logger.warning(f"GPT extraction failed for {doc_type}")
                                except Exception as e:
                                    logger.error(f"Error processing {doc_type} with GPT: {str(e)}", exc_info=True)
                        else:
                            # Handle single path (old structure)
                            try:
                                # Check if document already processed
                                if self._is_document_processed(paths):
                                    logger.info(f"Skipping already processed document: {paths}")
                                    continue
                                    
                                logger.info(f"Processing {doc_type} with GPT: {paths}")
                                gpt_data = self.gpt.process_document(paths, doc_type)
                                
                                # Mark as processed if successful
                                if gpt_data and 'error' not in gpt_data:
                                    self._mark_document_processed(paths)
                                
                                # Check if GPT extraction was successful
                                if gpt_data and 'error' not in gpt_data:
                                    logger.info(f"GPT successfully extracted data from {doc_type}")
                                    # Update extracted data, giving priority to GPT results
                                    for key, value in gpt_data.items():
                                        if key not in extracted_data or value != self.DEFAULT_VALUE:
                                            extracted_data[key] = value
                                else:
                                    logger.warning(f"GPT extraction failed for {doc_type}")
                            except Exception as e:
                                logger.error(f"Error processing {doc_type} with GPT: {str(e)}", exc_info=True)
                else:
                    logger.warning("GPT processor not available. Document processing will be limited.")
                
                # Log the combined extracted data
                logger.info(f"Combined extracted data contains {len(extracted_data)} fields: {list(extracted_data.keys())}")
                
            except Exception as e:
                logger.error(f"Error processing documents: {str(e)}", exc_info=True)
                raise Exception(f"Failed to process documents: {str(e)}")

            # Step 5: Process all Excel files
            try:
                logger.info(f"Processing {len(excel_files)} Excel files")
                all_excel_rows = []  # Initialize again to be safe
                
                for excel_path in excel_files:
                    try:
                        df, errors = self.excel_processor.process_excel(excel_path, dayfirst=True)
                        if not df.empty:
                            # Process all rows - create DEEP COPIES to ensure no shared references
                            for _, row in df.iterrows():
                                # Create a deep copy of each row to prevent reference sharing
                                row_dict = copy.deepcopy(row.to_dict())
                                
                                # NEW CODE: Split names according to requirements
                                if 'First Name' in row_dict and row_dict['First Name']:
                                    full_name = str(row_dict['First Name']).strip()
                                    name_parts = full_name.split()
                                    
                                    if len(name_parts) >= 3:
                                        # First word as first name, second as middle, third+ as last
                                        first_name = name_parts[0]
                                        middle_name = name_parts[1]
                                        last_name = ' '.join(name_parts[2:])
                                        
                                        row_dict['First Name'] = first_name
                                        row_dict['Middle Name'] = middle_name
                                        row_dict['Last Name'] = last_name
                                        
                                        logger.info(f"Split name '{full_name}' into First='{first_name}', Middle='{middle_name}', Last='{last_name}'")
                                    elif len(name_parts) == 2:
                                        # First word as first name, '.' as middle, second as last
                                        first_name = name_parts[0]
                                        middle_name = '.'
                                        last_name = name_parts[1]
                                        
                                        row_dict['First Name'] = first_name
                                        row_dict['Middle Name'] = middle_name
                                        row_dict['Last Name'] = last_name
                                        
                                        logger.info(f"Split name '{full_name}' into First='{first_name}', Middle='{middle_name}', Last='{last_name}'")
                                
                                all_excel_rows.append(row_dict)
                            
                            logger.info(f"Processed Excel file with {len(df)} rows")
                        if errors:
                            logger.warning(f"Excel validation errors: {errors}")
                    except Exception as e:
                        logger.error(f"Error processing Excel file {excel_path}: {str(e)}", exc_info=True)
                        logger.warning(f"Continuing with partial data due to Excel processing error")
                
                logger.info(f"Extracted {len(all_excel_rows)} rows from Excel files")
            except Exception as e:
                logger.error(f"Error processing Excel files: {str(e)}", exc_info=True)
                raise Exception(f"Failed to process Excel files: {str(e)}")

            # Match documents to specific employees
            if document_paths and all_excel_rows:
                logger.info(f"Attempting to match {len(document_paths)} documents to {len(all_excel_rows)} employees")
                
                # Process each document type to extract data
                doc_data_by_type = {}
                for doc_type, paths in document_paths.items():
                    if isinstance(paths, list):
                        # Handle list of paths (new structure)
                        for file_path in paths:
                            try:
                                # Check if document already processed
                                if self._is_document_processed(file_path):
                                    logger.info(f"Skipping already processed document: {file_path}")
                                    continue
                                    
                                # Process with GPT first if available
                                if self.gpt:
                                    try:
                                        doc_data = self.gpt.process_document(file_path, doc_type)
                                        
                                        # Mark as processed if successful
                                        if doc_data and 'error' not in doc_data:
                                            self._mark_document_processed(file_path)
                                            
                                            if doc_type not in doc_data_by_type:
                                                doc_data_by_type[doc_type] = {}
                                            # Merge with existing data
                                            for key, value in doc_data.items():
                                                doc_data_by_type[doc_type][key] = value
                                            continue
                                    except Exception as e:
                                        logger.warning(f"GPT extraction failed for {doc_type}, using Textract: {str(e)}")

                                # Fallback to Textract
                                doc_data = self.textract.process_document(file_path, doc_type)
                                # Mark as processed after Textract (if not marked already by GPT)
                                self._mark_document_processed(file_path)
                                if doc_type not in doc_data_by_type:
                                    doc_data_by_type[doc_type] = {}
                                # Merge with existing data
                                for key, value in doc_data.items():
                                    doc_data_by_type[doc_type][key] = value
                            except Exception as e:
                                logger.error(f"Error processing {doc_type} document: {str(e)}")
                    else:
                        # Handle single path (old structure)
                        try:
                            # Check if document already processed
                            if self._is_document_processed(paths):
                                logger.info(f"Skipping already processed document: {paths}")
                                continue
                                
                            # Process with GPT first if available
                            if self.gpt:
                                try:
                                    doc_data = self.gpt.process_document(paths, doc_type)
                                    
                                    # Mark as processed if successful
                                    if doc_data and 'error' not in doc_data:
                                        self._mark_document_processed(paths)
                                        doc_data_by_type[doc_type] = doc_data
                                        continue
                                except Exception as e:
                                    logger.warning(f"GPT extraction failed for {doc_type}, using Textract: {str(e)}")

                            # Fallback to Textract
                            doc_data = self.textract.process_document(paths, doc_type)
                            # Mark as processed after Textract (if not marked already by GPT)
                            self._mark_document_processed(paths)
                            doc_data_by_type[doc_type] = doc_data
                        except Exception as e:
                            logger.error(f"Error processing {doc_type} document: {str(e)}")
                
                # Process each Excel row INDIVIDUALLY with better matching logic
                all_excel_rows = []
                doc_data_by_type = {}

                # First extract data from all documents
                for doc_type, paths in document_paths.items():
                    if isinstance(paths, list):
                        # Handle list of paths (new structure)
                        for file_path in paths:
                            try:
                                # Check if document already processed
                                if self._is_document_processed(file_path):
                                    logger.info(f"Skipping already processed document: {file_path}")
                                    continue
                                    
                                # Process with GPT first if available (prioritize GPT as mentioned)
                                if self.gpt:
                                    try:
                                        doc_data = self.gpt.process_document(file_path, doc_type)
                                        if doc_data and 'error' not in doc_data:
                                            # Mark as processed if successful
                                            self._mark_document_processed(file_path)
                                            
                                            if doc_type not in doc_data_by_type:
                                                doc_data_by_type[doc_type] = {}
                                            # Merge with existing data
                                            for key, value in doc_data.items():
                                                doc_data_by_type[doc_type][key] = value
                                            logger.info(f"GPT successfully extracted data from {doc_type}")
                                            continue  # Skip Textract if GPT succeeded
                                    except Exception as e:
                                        logger.warning(f"GPT extraction failed for {doc_type}, using Textract: {str(e)}")

                                # Fallback to Textract
                                doc_data = self.textract.process_document(file_path, doc_type)
                                # Mark as processed after Textract (if not marked already by GPT)
                                self._mark_document_processed(file_path)
                                if doc_type not in doc_data_by_type:
                                    doc_data_by_type[doc_type] = {}
                                # Merge with existing data
                                for key, value in doc_data.items():
                                    doc_data_by_type[doc_type][key] = value
                                logger.info(f"Textract extracted data from {doc_type}")
                            except Exception as e:
                                logger.error(f"Error processing {doc_type} document: {str(e)}")
                    else:
                        # Handle single path (old structure)
                        try:
                            # Check if document already processed
                            if self._is_document_processed(paths):
                                logger.info(f"Skipping already processed document: {paths}")
                                continue
                                
                            # Process with GPT first if available (prioritize GPT as mentioned)
                            if self.gpt:
                                try:
                                    doc_data = self.gpt.process_document(paths, doc_type)
                                    if doc_data and 'error' not in doc_data:
                                        # Mark as processed if successful
                                        self._mark_document_processed(paths)
                                        
                                        doc_data_by_type[doc_type] = doc_data
                                        logger.info(f"GPT successfully extracted data from {doc_type}")
                                        continue  # Skip Textract if GPT succeeded
                                except Exception as e:
                                    logger.warning(f"GPT extraction failed for {doc_type}, using Textract: {str(e)}")

                            # Fallback to Textract
                            doc_data = self.textract.process_document(paths, doc_type)
                            # Mark as processed after Textract (if not marked already by GPT)
                            self._mark_document_processed(paths)
                            doc_data_by_type[doc_type] = doc_data
                            logger.info(f"Textract extracted data from {doc_type}")
                        except Exception as e:
                            logger.error(f"Error processing {doc_type} document: {str(e)}")

                # Combine all document data into one dictionary for template population
                extracted_data = {}
                for doc_data in doc_data_by_type.values():
                    for key, value in doc_data.items():
                        if key not in extracted_data or (value != self.DEFAULT_VALUE and extracted_data[key] == self.DEFAULT_VALUE):
                            extracted_data[key] = value

                # Process each Excel row with individual document matching
                for excel_file in excel_files:
                    try:
                        df, errors = self.excel_processor.process_excel(excel_file, dayfirst=True)
                        if not df.empty:
                            # Create document matching diagnostics
                            self._log_document_matches(document_paths, df.to_dict('records'), doc_data_by_type)
                            
                            # Process each row individually
                            for _, row in df.iterrows():
                                # Create a deep copy of row to prevent reference issues
                                row_dict = copy.deepcopy(row.to_dict())
                                
                                # Check if we need to split names
                                if 'First Name' in row_dict and row_dict['First Name']:
                                    full_name = str(row_dict['First Name']).strip()
                                    name_parts = full_name.split()
                                    
                                    if len(name_parts) >= 3:
                                        # First word = first name, second = middle, third+ = last
                                        first_name = name_parts[0]
                                        middle_name = name_parts[1]
                                        last_name = ' '.join(name_parts[2:])
                                        
                                        row_dict['First Name'] = first_name
                                        row_dict['Middle Name'] = middle_name
                                        row_dict['Last Name'] = last_name
                                        
                                        logger.info(f"Split name '{full_name}' into First='{first_name}', Middle='{middle_name}', Last='{last_name}'")
                                    elif len(name_parts) == 2:
                                        # First word = first name, '.' = middle, second = last
                                        first_name = name_parts[0]
                                        middle_name = '.'
                                        last_name = name_parts[1]
                                        
                                        row_dict['First Name'] = first_name
                                        row_dict['Middle Name'] = middle_name
                                        row_dict['Last Name'] = last_name
                                        
                                        logger.info(f"Split name '{full_name}' into First='{first_name}', Middle='{middle_name}', Last='{last_name}'")
                                
                                # Find best matching document for this row
                                best_match_doc_type = None
                                best_match_score = 0
                                match_reason = []
                                
                                # Extract row identifiers for matching
                                row_passport = None
                                row_emirates_id = None
                                row_name = None
                                
                                for field in ['Passport No', 'passport_no']:
                                    if field in row_dict and row_dict[field] and str(row_dict[field]).strip() != '':
                                        row_passport = str(row_dict[field]).strip()
                                        break
                                        
                                for field in ['Emirates Id', 'emirates_id']:
                                    if field in row_dict and row_dict[field] and str(row_dict[field]).strip() != '':
                                        row_emirates_id = str(row_dict[field]).strip()
                                        break
                                        
                                first_name = str(row_dict.get('First Name', '')).strip()
                                last_name = str(row_dict.get('Last Name', '')).strip()
                                if first_name or last_name:
                                    row_name = f"{first_name} {last_name}".strip()
                                
                                # Match with each document
                                for doc_type, doc_data in doc_data_by_type.items():
                                    current_score = 0
                                    current_reason = []
                                    
                                    # Get document identifiers
                                    doc_passport = None
                                    doc_emirates_id = None
                                    doc_name = None
                                    
                                    for field in ['passport_number', 'passport_no']:
                                        if field in doc_data and doc_data[field] != self.DEFAULT_VALUE:
                                            doc_passport = doc_data[field]
                                            break
                                            
                                    for field in ['emirates_id', 'eid']:
                                        if field in doc_data and doc_data[field] != self.DEFAULT_VALUE:
                                            doc_emirates_id = doc_data[field]
                                            break
                                            
                                    for field in ['full_name', 'name']:
                                        if field in doc_data and doc_data[field] != self.DEFAULT_VALUE:
                                            doc_name = doc_data[field]
                                            break
                                    
                                    # Check passport match (strong)
                                    if doc_passport and row_passport:
                                        if doc_passport.lower() == row_passport.lower():
                                            current_score += 100
                                            current_reason.append(f"Passport match: {doc_passport}")
                                    
                                    # Check Emirates ID match (strong)
                                    if doc_emirates_id and row_emirates_id:
                                        # Clean IDs for comparison
                                        clean_doc_id = re.sub(r'[^0-9]', '', str(doc_emirates_id))
                                        clean_row_id = re.sub(r'[^0-9]', '', str(row_emirates_id))
                                        
                                        if clean_doc_id == clean_row_id:
                                            current_score += 100
                                            current_reason.append(f"Emirates ID match: {doc_emirates_id}")
                                    
                                    # Check name similarity (medium)
                                    if doc_name and row_name:
                                        doc_words = set(doc_name.lower().split())
                                        row_words = set(row_name.lower().split())
                                        common_words = doc_words.intersection(row_words)
                                        
                                        if common_words:
                                            similarity = len(common_words) / max(len(doc_words), len(row_words))
                                            name_score = int(similarity * 50)
                                            current_score += name_score
                                            current_reason.append(f"Name similarity: {similarity:.2f}")
                                    
                                    # Update best match if better score
                                    if current_score > best_match_score:
                                        best_match_score = current_score
                                        best_match_doc_type = doc_type
                                        match_reason = current_reason
                                
                                # Apply document data if good match found
                                if best_match_doc_type and best_match_score >= 50:
                                    logger.info(f"Row match found: {row_name} matches {best_match_doc_type} (score: {best_match_score})")
                                    for reason in match_reason:
                                        logger.info(f"  - {reason}")
                                        
                                    # Apply document fields to row
                                    doc_data = doc_data_by_type[best_match_doc_type]
                                    
                                    # Apply key fields from document
                                    field_mapping = {
                                        'passport_number': 'Passport No',
                                        'emirates_id': 'Emirates Id',
                                        'unified_no': 'Unified No',
                                        'visa_file_number': 'Visa File Number',
                                        'nationality': 'Nationality',
                                        'date_of_birth': 'DOB',
                                        'gender': 'Gender',
                                        'mobile_no': 'Mobile No',
                                        'email': 'Email'
                                    }
                                    
                                    for doc_field, row_field in field_mapping.items():
                                        if doc_field in doc_data and doc_data[doc_field] != self.DEFAULT_VALUE:
                                            # Special handling for gender
                                            if doc_field == 'gender':
                                                gender_val = doc_data[doc_field].upper()
                                                if gender_val in ['M', 'MALE']:
                                                    row_dict[row_field] = 'Male'
                                                elif gender_val in ['F', 'FEMALE']:
                                                    row_dict[row_field] = 'Female'
                                            else:
                                                row_dict[row_field] = doc_data[doc_field]
                                else:
                                    logger.info(f"No document match for row with name: {row_name}")
                                
                                all_excel_rows.append(row_dict)
                            
                            logger.info(f"Processed {len(df)} rows from Excel file")
                            
                            # Debug code to check document extraction and conversion
                            logger.info("=" * 80)
                            logger.info("DOCUMENT PROCESSING DIAGNOSTICS")
                            logger.info("=" * 80)

                            # Check which documents were found and converted
                            logger.info(f"Documents found: {len(document_paths)}")
                            for doc_type, doc_path in document_paths.items():
                                if isinstance(doc_path, list):
                                    for path in doc_path:
                                        logger.info(f"Document: {doc_type} - {os.path.basename(path)}")
                                else:
                                    logger.info(f"Document: {doc_type} - {os.path.basename(doc_path)}")
                                
                                # Check if it was converted from PDF to JPG
                                converted_dir = os.path.join(os.path.dirname(doc_path), "converted")
                                if os.path.exists(converted_dir):
                                    converted_files = [f for f in os.listdir(converted_dir) if f.endswith(".jpg")]
                                    logger.info(f"  - Converted to {len(converted_files)} JPG files in {converted_dir}")
                                else:
                                    logger.info(f"  - No conversion directory found")

                            # Log extracted data from documents
                            logger.info("Extracted data from documents:")
                            if doc_data_by_type:
                                for doc_type, doc_data in doc_data_by_type.items():
                                    logger.info(f"Data from {doc_type}:")
                                    for key, value in doc_data.items():
                                        if value != self.DEFAULT_VALUE:
                                            logger.info(f"  - {key}: {value}")
                            else:
                                logger.error("NO DOCUMENT DATA EXTRACTED! This will cause empty fields in final output.")
                                
                            # Check if we're using GPT or Textract
                            logger.info(f"OCR processors: GPT available: {self.gpt is not None}, Textract available: {self.textract is not None}")

                            # Add this fix to ensure document data is properly processed
                            # If no document data was extracted and documents exist, force processing with Textract
                            if not doc_data_by_type and document_paths:
                                logger.warning("No document data extracted but documents exist. Forcing extraction with Textract...")
                                
                                # Force processing with Textract
                                for doc_type, paths in document_paths.items():
                                    if isinstance(paths, list):
                                        # Handle list of paths (new structure)
                                        for file_path in paths:
                                            try:
                                                # Check if document already processed
                                                if self._is_document_processed(file_path):
                                                    logger.info(f"Skipping already processed document: {file_path}")
                                                    continue
                                                    
                                                logger.info(f"Force processing {doc_type} with Textract: {file_path}")
                                                textract_data = self.textract.process_document(file_path, doc_type)
                                                
                                                # Mark as processed after Textract
                                                self._mark_document_processed(file_path)
                                                if doc_type not in doc_data_by_type:
                                                    doc_data_by_type[doc_type] = {}
                                                # Merge with existing data
                                                for key, value in textract_data.items():
                                                    doc_data_by_type[doc_type][key] = value
                                                
                                                # Update the combined extracted data
                                                for key, value in textract_data.items():
                                                    if key not in extracted_data or (value != self.DEFAULT_VALUE and extracted_data[key] == self.DEFAULT_VALUE):
                                                        extracted_data[key] = value
                                            except Exception as e:
                                                logger.error(f"Error in forced processing of {doc_type} document: {str(e)}", exc_info=True)
                                    else:
                                        # Handle single path (old structure)
                                        try:
                                            # Check if document already processed
                                            if self._is_document_processed(paths):
                                                logger.info(f"Skipping already processed document: {paths}")
                                                continue
                                                
                                            logger.info(f"Force processing {doc_type} with Textract: {paths}")
                                            textract_data = self.textract.process_document(paths, doc_type)
                                            
                                            # Mark as processed after Textract
                                            self._mark_document_processed(paths)
                                            doc_data_by_type[doc_type] = textract_data
                                            
                                            # Update the combined extracted data
                                            for key, value in textract_data.items():
                                                if key not in extracted_data or (value != self.DEFAULT_VALUE and extracted_data[key] == self.DEFAULT_VALUE):
                                                    extracted_data[key] = value
                                        except Exception as e:
                                            logger.error(f"Error in forced processing of {doc_type} document: {str(e)}", exc_info=True)
                                
                                # Log the updated extracted data
                                if doc_data_by_type:
                                    logger.info("Updated document data after forced extraction:")
                                    for doc_type, doc_data in doc_data_by_type.items():
                                        logger.info(f"Data from {doc_type}:")
                                        for key, value in doc_data.items():
                                            if value != self.DEFAULT_VALUE:
                                                logger.info(f"  - {key}: {value}")
                                else:
                                    logger.error("STILL NO DOCUMENT DATA AFTER FORCED EXTRACTION!")

                            # Also check Excel data
                            logger.info("Excel data diagnostics:")
                            logger.info(f"Excel files found: {len(excel_files)}")
                            for excel_file in excel_files:
                                logger.info(f"Excel file: {os.path.basename(excel_file)}")
                                try:
                                    df = pd.read_excel(excel_file)
                                    logger.info(f"  - Rows: {len(df)}")
                                    logger.info(f"  - Columns: {list(df.columns)[:10]}...")
                                except Exception as e:
                                    logger.error(f"  - Error reading Excel: {str(e)}")

                            # Check what's being passed to the data combiner
                            logger.info("Data being passed to the combiner:")
                            logger.info(f"Extracted data keys: {list(extracted_data.keys())}")
                            logger.info(f"Excel rows: {len(all_excel_rows)}")
                            logger.info(f"Document paths: {document_paths}")
                        
                        if errors:
                            logger.warning(f"Excel validation errors: {errors}")
                            
                    except Exception as e:
                        logger.error(f"Error processing Excel file {excel_file}: {str(e)}", exc_info=True)


            # Rename client files based on Excel data
            if all_excel_rows:
                # document_paths = self._rename_client_files(document_paths, all_excel_rows)
                logger.info("Skipping file renaming to avoid duplicate processing")

            # Step 6: Select template based on company in subject
            template_path = self._select_template_for_company(subject)
                
            # Step 7: Combine data
            try:
                logger.info(f"Combining data using template: {template_path}")
                output_path = os.path.join(
                    submission_dir,
                    f"final_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                )
                
                if all_excel_rows is None or len(all_excel_rows) == 0:
                    logger.warning("No Excel data found. Creating default Excel data...")
                    # Create default Excel data with at least basic structure
                    all_excel_rows = [{
                        "First Name": "",
                        "Middle Name": ".",
                        "Last Name": "",
                        "Effective Date": datetime.now().strftime('%d/%m/%Y'),
                        "DOB": "",
                        "Gender": ""
                    }]

                # Use selected template instead of hardcoded "template.xlsx"
                result = self.data_combiner.combine_and_populate_template(
                    template_path,
                    output_path,
                    extracted_data,  # This now contains combined GPT and Textract data
                    all_excel_rows if all_excel_rows else None,
                    document_paths
                )
                
                logger.info(f"Data combination result: {result['status']}, rows processed: {result.get('rows_processed', 0)}")
            except Exception as e:
                logger.error(f"Error combining data: {str(e)}", exc_info=True)
                raise Exception(f"Failed to combine data: {str(e)}")


            # Step 8: Save submission
            try:
                submission = CompletedSubmission(
                    process_id=process_id,
                    documents=document_paths,
                    final_excel=output_path
                )
                self.completed_submissions.append(submission)
                
                # Try creating and sending a ZIP file
                try:
                    zip_path = self._create_zip(submission_dir)
                    
                    # Create message body with local file path
                    email_body = (
                        f"New submission processed: {subject}\n\n"
                        f"Please find the attached ZIP file containing all processed documents.\n\n"
                        f"Local file location: {zip_path}\n"
                    )
                    
                    logger.info("Attempting to send email with submission results")
    
                    # Prepare email content
                    email_subject = f"Medical Bot: {subject} - Submission Complete"
                    email_body = f"""
                    Dear Team,
                    
                    The Medical Bot has completed processing a submission.
                    
                    Details:
                    - Subject: {subject}
                    - Files Processed: {len(saved_files)}
                    - Documents: {list(document_paths.keys())}
                    - Excel Files: {[os.path.basename(f) for f in excel_files]}
                    - Rows Processed: {len(all_excel_rows) if all_excel_rows else 0}
                    
                    The processed files are attached as a ZIP archive.
                    They are also available locally at: {submission_dir}
                    
                    Regards,
                    Medical Bot
                    """
                    
                    # Create ZIP file with all processed files
                    zip_path = None
                    try:
                        zip_path = self._create_zip(submission_dir)
                        logger.info(f"Created ZIP file for email attachment: {zip_path}")
                    except Exception as e:
                        logger.error(f"Error creating ZIP file for email: {str(e)}", exc_info=True)
                    
                    # Try to send email
                    try:
                        # Import inside the function to avoid circular dependencies
                        from src.utils.email_sender import EmailSender
                        
                        # Create a fresh EmailSender instance
                        email_sender = EmailSender()
                        
                        # Log email configuration without accessing attributes directly
                        logger.info("Email sender configuration:")
                        
                        # Safely check if attributes exist
                        for attr in ['smtp_server', 'smtp_port', 'from_email', 'to_email']:
                            if hasattr(email_sender, attr):
                                logger.info(f"  - {attr}: {getattr(email_sender, attr)}")
                        
                        # Send email with attachment if available
                        email_sent = False
                        if zip_path and os.path.exists(zip_path):
                            logger.info(f"Sending email with ZIP attachment: {os.path.basename(zip_path)}")
                            email_sent = email_sender.send_email(
                                subject=email_subject,
                                body=email_body,
                                attachment_path=zip_path
                            )
                        else:
                            # Send without attachment
                            logger.info("Sending email without attachment (ZIP creation failed)")
                            email_sent = email_sender.send_email(
                                subject=email_subject,
                                body=email_body
                            )
                        
                        if email_sent:
                            logger.info("Email sent successfully")
                        else:
                            logger.warning("Email sending returned False")
                    except Exception as e:
                        logger.error(f"Error sending email: {str(e)}", exc_info=True)
                except Exception as e:
                    logger.error(f"Error in email preparation: {str(e)}", exc_info=True)      
            except Exception as e:
                logger.error(f"Error saving submission: {str(e)}", exc_info=True)
                # Continue without failing the whole process
                logger.warning("Continuing despite submission saving error")

            # Copy all files to submission directory for reference
            try:
                for excel_path in excel_files:
                    new_path = os.path.join(submission_dir, os.path.basename(excel_path))
                    shutil.copy2(excel_path, new_path)
            except Exception as e:
                logger.error(f"Error copying files to submission directory: {str(e)}", exc_info=True)
                # Continue without failing the whole process
                logger.warning("Continuing despite file copying error")

            return {
                "status": "success",
                "process_id": process_id,
                "submission_dir": submission_dir,
                "documents": document_paths,
                "excel_files": excel_files,
                "documents_processed": processed_docs,
                "rows_processed": len(all_excel_rows) if all_excel_rows else 0
            }

        except Exception as e:
            logger.error(f"Error processing email {email_id}: {str(e)}", exc_info=True)
            return {
                "status": "error",
                "process_id": process_id,
                "error": str(e)
            }
    
    def _rename_client_files(self, document_paths: Dict[str, str], excel_data: List[Dict]) -> Dict[str, str]:
        """Rename files based on staff ID from Excel with improved matching."""
        if not excel_data:
            logger.warning("No Excel data provided, cannot rename files")
            return document_paths
            
        try:
            # Build a map of staff ID to name for reference
            staff_id_map = {}
            for row in excel_data:
                # Check both lowercase and proper case field names
                staff_id = None
                # Try different field name variations for staff ID
                for field_name in ['staff_id', 'Staff ID', 'StaffID', 'staff id']:
                    if field_name in row and row[field_name]:
                        val = str(row.get(field_name, '')).strip()
                        if val and val != '.':
                            staff_id = val
                            break
                            
                if staff_id:
                    # Try different field name variations for first name
                    first_name = ''
                    for field_name in ['first_name', 'First Name', 'firstname', 'FirstName']:
                        if field_name in row:
                            val = str(row.get(field_name, '')).strip()
                            if val and val != '.':
                                first_name = val
                                break
                                
                    # Try different field name variations for last name
                    last_name = ''
                    for field_name in ['last_name', 'Last Name', 'lastname', 'LastName']:
                        if field_name in row:
                            val = str(row.get(field_name, '')).strip()
                            if val and val != '.':
                                last_name = val
                                break
                                
                    if first_name or last_name:
                        staff_id_map[staff_id] = {
                            'first_name': first_name,
                            'last_name': last_name,
                            'full_name': f"{first_name} {last_name}".strip()
                        }
                        
            logger.info(f"Found {len(staff_id_map)} staff IDs in Excel data")
            
            # Create a copy of document paths to update
            updated_paths = {}
            
            # Create direct one-to-one mapping if appropriate
            if len(document_paths) == len(excel_data) and len(document_paths) > 0:
                logger.info(f"Equal number of documents ({len(document_paths)}) and rows ({len(excel_data)}), using direct mapping")
                sorted_docs = sorted(document_paths.items())
                sorted_excel = sorted(excel_data, key=lambda x: x.get('staff_id', ''))
                
                for i, ((doc_type, file_path), row_data) in enumerate(zip(sorted_docs, sorted_excel)):
                    staff_id = str(row_data.get('staff_id', '')).strip()
                    if not staff_id or staff_id == '.':
                        logger.warning(f"Missing staff_id in row {i+1}, using row index")
                        staff_id = f"UNKNOWN_{i+1}"
                    
                    file_dir = os.path.dirname(file_path)
                    file_ext = os.path.splitext(file_path)[1]
                    
                    # Determine document type suffix
                    if 'passport' in doc_type.lower():
                        doc_suffix = "PASSPORT"
                    elif 'emirates' in doc_type.lower() or 'eid' in doc_type.lower():
                        doc_suffix = "EMIRATES_ID"
                    elif 'visa' in doc_type.lower() or 'permit' in doc_type.lower():
                        doc_suffix = "VISA"
                    else:
                        doc_suffix = doc_type.upper().replace(' ', '_')
                    
                    # Create new filename with staff ID and doc type
                    safe_id = re.sub(r'[<>:"/\\|?*]', '_', staff_id)
                    new_name = f"{safe_id}_{doc_suffix}{file_ext}"
                    new_path = os.path.join(file_dir, new_name)
                    
                    # Handle potential conflicts
                    if os.path.exists(new_path) and os.path.abspath(new_path) != os.path.abspath(file_path):
                        import uuid
                        unique = str(uuid.uuid4())[:8]
                        new_name = f"{safe_id}_{doc_suffix}_{unique}{file_ext}"
                        new_path = os.path.join(file_dir, new_name)
                    
                    # Rename file
                    os.rename(file_path, new_path)
                    logger.info(f"Renamed file: {os.path.basename(file_path)} -> {new_name}")
                    updated_paths[doc_type] = new_path
                
                return updated_paths
            
            # Process each document using the regular matching approach
            updated_paths = {}
            for doc_type, paths in document_paths.items():
                if isinstance(paths, list):
                    # Handle list of paths (new structure)
                    updated_paths[doc_type] = []
                    for file_path in paths:
                        try:
                            # Skip if file doesn't exist
                            if not os.path.exists(file_path):
                                logger.warning(f"File not found: {file_path}")
                                updated_paths[doc_type].append(file_path)
                                continue
                                
                            # Extract file info
                            file_dir = os.path.dirname(file_path)
                            file_ext = os.path.splitext(file_path)[1]
                            file_name = os.path.basename(file_path).lower()
                            
                            # Find matching staff ID
                            matched_staff_id = None
                            
                            # Strategy 1: Check if staff ID is already in the filename
                            for staff_id in staff_id_map:
                                if staff_id.lower() in file_name:
                                    matched_staff_id = staff_id
                                    logger.info(f"Found staff ID {staff_id} in filename {file_name}")
                                    break
                            
                            # Strategy 2: Try to match by name if no staff ID match
                            if not matched_staff_id:
                                best_match = None
                                best_score = 0
                                
                                for staff_id, info in staff_id_map.items():
                                    score = 0
                                    
                                    # Check for first name match
                                    if info['first_name'] and info['first_name'].lower() in file_name:
                                        score += 5
                                        
                                    # Check for last name match (weighted higher)
                                    if info['last_name'] and info['last_name'].lower() in file_name:
                                        score += 10
                                        
                                    # Check for full name match
                                    if info['full_name'] and info['full_name'].lower() in file_name:
                                        score += 15
                                        
                                    # Update best match if better score
                                    if score > best_score:
                                        best_score = score
                                        best_match = staff_id
                                
                                # Use match if score is high enough
                                if best_match and best_score >= 5:
                                    matched_staff_id = best_match
                                    logger.info(f"Matched file {file_name} to staff ID {matched_staff_id} by name (score: {best_score})")
                            
                            # If no match found, try the first staff ID as a fallback
                            if not matched_staff_id and staff_id_map:
                                matched_staff_id = list(staff_id_map.keys())[0]
                                logger.warning(f"No match found for {file_name}, using first staff ID: {matched_staff_id}")
                            
                            # If no staff ID available, keep original name
                            if not matched_staff_id:
                                logger.warning(f"No staff ID available, keeping original name for {file_name}")
                                updated_paths[doc_type].append(file_path)
                                continue
                            
                            # Determine document type suffix
                            if 'passport' in doc_type.lower():
                                doc_suffix = "PASSPORT"
                            elif 'emirates' in doc_type.lower() or 'eid' in doc_type.lower():
                                doc_suffix = "EMIRATES_ID"
                            elif 'visa' in doc_type.lower() or 'permit' in doc_type.lower():
                                doc_suffix = "VISA"
                            else:
                                # Use doc_type as is for other document types
                                doc_suffix = doc_type.upper()
                            
                            # Create new filename with staff ID and doc type
                            import re
                            safe_id = re.sub(r'[<>:"/\\|?*]', '_', matched_staff_id)
                            new_name = f"{safe_id}_{doc_suffix}{file_ext}"
                            new_path = os.path.join(file_dir, new_name)
                            
                            # Ensure no filename conflicts
                            if os.path.exists(new_path) and os.path.abspath(new_path) != os.path.abspath(file_path):
                                import uuid
                                unique = str(uuid.uuid4())[:8]
                                new_name = f"{safe_id}_{doc_suffix}_{unique}{file_ext}"
                                new_path = os.path.join(file_dir, new_name)
                            
                            # Rename file
                            os.rename(file_path, new_path)
                            logger.info(f"Renamed file: {os.path.basename(file_path)} -> {new_name}")
                            
                            # Update path in result
                            updated_paths[doc_type].append(new_path)
                            
                        except Exception as e:
                            logger.error(f"Error renaming file {file_path}: {str(e)}")
                            updated_paths[doc_type].append(file_path)
                            
                else:
                    # Handle single path (old structure)
                    try:
                        # Skip if file doesn't exist
                        if not os.path.exists(paths):
                            logger.warning(f"File not found: {paths}")
                            updated_paths[doc_type] = paths
                            continue
                            
                        # Extract file info
                        file_dir = os.path.dirname(paths)
                        file_ext = os.path.splitext(paths)[1]
                        file_name = os.path.basename(paths).lower()
                        
                        # Find matching staff ID
                        matched_staff_id = None
                        
                        # Strategy 1: Check if staff ID is already in the filename
                        for staff_id in staff_id_map:
                            if staff_id.lower() in file_name:
                                matched_staff_id = staff_id
                                logger.info(f"Found staff ID {staff_id} in filename {file_name}")
                                break
                        
                        # Strategy 2: Try to match by name if no staff ID match
                        if not matched_staff_id:
                            best_match = None
                            best_score = 0
                            
                            for staff_id, info in staff_id_map.items():
                                score = 0
                                
                                # Check for first name match
                                if info['first_name'] and info['first_name'].lower() in file_name:
                                    score += 5
                                    
                                # Check for last name match (weighted higher)
                                if info['last_name'] and info['last_name'].lower() in file_name:
                                    score += 10
                                    
                                # Check for full name match
                                if info['full_name'] and info['full_name'].lower() in file_name:
                                    score += 15
                                    
                                # Update best match if better score
                                if score > best_score:
                                    best_score = score
                                    best_match = staff_id
                            
                            # Use match if score is high enough
                            if best_match and best_score >= 5:
                                matched_staff_id = best_match
                                logger.info(f"Matched file {file_name} to staff ID {matched_staff_id} by name (score: {best_score})")
                        
                        # If no match found, try the first staff ID as a fallback
                        if not matched_staff_id and staff_id_map:
                            matched_staff_id = list(staff_id_map.keys())[0]
                            logger.warning(f"No match found for {file_name}, using first staff ID: {matched_staff_id}")
                        
                        # If no staff ID available, keep original name
                        if not matched_staff_id:
                            logger.warning(f"No staff ID available, keeping original name for {file_name}")
                            updated_paths[doc_type] = paths
                            continue
                        
                        # Determine document type suffix
                        if 'passport' in doc_type.lower():
                            doc_suffix = "PASSPORT"
                        elif 'emirates' in doc_type.lower() or 'eid' in doc_type.lower():
                            doc_suffix = "EMIRATES_ID"
                        elif 'visa' in doc_type.lower() or 'permit' in doc_type.lower():
                            doc_suffix = "VISA"
                        else:
                            # Use doc_type as is for other document types
                            doc_suffix = doc_type.upper()
                        
                        # Create new filename with staff ID and doc type
                        import re
                        safe_id = re.sub(r'[<>:"/\\|?*]', '_', matched_staff_id)
                        new_name = f"{safe_id}_{doc_suffix}{file_ext}"
                        new_path = os.path.join(file_dir, new_name)
                        
                        # Ensure no filename conflicts
                        if os.path.exists(new_path) and os.path.abspath(new_path) != os.path.abspath(paths):
                            import uuid
                            unique = str(uuid.uuid4())[:8]
                            new_name = f"{safe_id}_{doc_suffix}_{unique}{file_ext}"
                            new_path = os.path.join(file_dir, new_name)
                        
                        # Rename file
                        os.rename(paths, new_path)
                        logger.info(f"Renamed file: {os.path.basename(paths)} -> {new_name}")
                        
                        # Update path in result
                        updated_paths[doc_type] = new_path
                        
                    except Exception as e:
                        logger.error(f"Error renaming file {paths}: {str(e)}")
                        updated_paths[doc_type] = paths

            return updated_paths
        
        except Exception as e:
            logger.error(f"Error in rename_client_files: {str(e)}")
            return document_paths
    
    def _create_zip(self, folder_path: str) -> str:
        """Create a ZIP file from a folder.
        
        Args:
            folder_path: Path to the folder to zip
            
        Returns:
            Path to the created ZIP file
        """
        import zipfile
        
        if not os.path.exists(folder_path):
            raise FileNotFoundError(f"Folder not found: {folder_path}")
            
        # Create ZIP filename
        folder_basename = os.path.basename(folder_path)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        zip_name = f"{folder_basename}_{timestamp}.zip"
        
        # Create ZIP file path
        zip_path = os.path.join(os.path.dirname(folder_path), zip_name)
        
        # Create ZIP file
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            # Walk through all files in the directory
            for root, _, files in os.walk(folder_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    
                    # Calculate path inside the ZIP file
                    rel_path = os.path.relpath(file_path, os.path.dirname(folder_path))
                    
                    # Add file to ZIP
                    zipf.write(file_path, rel_path)
                    
        logger.info(f"Created ZIP file: {zip_path}")
        return zip_path
            
    def _validate_output_excel(self, excel_path: str) -> Dict:
        """Validate the output Excel file."""
        try:
            df = pd.read_excel(excel_path)
            if df.empty:
                return {
                    "is_valid": False,
                    "issues": ["Output file is empty"]
                }

            issues = []
            
            # Check required columns have data
            required_fields = [
                'first_name', 'last_name', 'nationality', 
                'emirates_id', 'passport_no', 'date_of_birth'
            ]
            
            for field in required_fields:
                if field not in df.columns:
                    issues.append(f"Missing required column: {field}")
                elif df[field].iloc[0] in ['.', 'nan', '']:
                    issues.append(f"Missing required data in {field}")

            # Validate data formats
            first_row = df.iloc[0]
            
            # Emirates ID format
            eid = str(first_row.get('emirates_id', ''))
            if not eid.startswith('.'):
                if not re.match(r'^\d{3}-\d{4}-\d{7}-\d{1}$', eid):
                    issues.append("Invalid Emirates ID format")

            # Date format
            date_fields = ['date_of_birth', 'passport_expiry_date', 'visa_expiry_date']
            for field in date_fields:
                if field in df.columns:
                    date_val = str(first_row.get(field, ''))
                    if date_val not in ['.', 'nan', '']:
                        try:
                            # Check if date is in DD-MM-YYYY format
                            if not re.match(r'^\d{2}-\d{2}-\d{4}$', date_val):
                                issues.append(f"Invalid date format in {field}: {date_val}")
                        except ValueError:
                            issues.append(f"Invalid date format in {field}")

            return {
                "is_valid": len(issues) == 0,
                "issues": issues
            }

        except Exception as e:
            return {
                "is_valid": False,
                "issues": [f"Validation error: {str(e)}"]
            }

    def _determine_file_type(self, file_path: str) -> str:
        """Determine file type from filename and content if possible."""
        name = file_path.lower()
        
        # First check extensions for common file types
        if name.endswith(('.xlsx', '.xls')):
            return 'excel'
            
        # If content-based detection is available, use it here
        # For now, fallback to filename-based classification with improvements
        if 'passport' in name:
            return 'passport'
        elif 'emirates' in name or 'eid' in name or 'id card' in name:
            return 'emirates_id'
        elif 'visa' in name or 'permit' in name or 'residence' in name:
            return 'visa'
            
        # Try additional checks for content if unable to determine from filename
        # For example, check file size, basic content patterns, etc.
        
        return 'unknown'
    
    def get_completed_submissions(self) -> List[CompletedSubmission]:
        """Get list of completed submissions."""
        return self.completed_submissions
    
    def check_data_transfer(self, extracted_data, output_path):
        """Check if extracted data was properly transferred to Excel."""
        logger.info(f"Checking data transfer to Excel file: {output_path}")
        
        if not os.path.exists(output_path):
            logger.error(f"Output file does not exist: {output_path}")
            return False
            
        try:
            df = pd.read_excel(output_path)
            
            # Print key extracted fields
            logger.info("Key extracted fields:")
            key_fields = ['unified_no', 'nationality', 'passport_no', 'passport_number', 
                        'visa_file_number', 'dob', 'date_of_birth']
            
            extracted_values = {}
            for field in key_fields:
                if field in extracted_data:
                    value = extracted_data[field]
                    if value != '.':
                        extracted_values[field] = value
                        logger.info(f"  Extracted {field}: {value}")
                        
            # Print corresponding values in Excel
            logger.info("Corresponding fields in Excel:")
            excel_values = {}
            for field in key_fields:
                excel_field = field
                # Handle field name variants
                if field == 'passport_number':
                    excel_field = 'passport_no'
                elif field == 'date_of_birth':
                    excel_field = 'dob'
                    
                if excel_field in df.columns:
                    value = df[excel_field].iloc[0]
                    if pd.notna(value) and str(value).strip() != '':
                        excel_values[excel_field] = value
                        logger.info(f"  Excel {excel_field}: {value}")
            
            # Check if fields match
            for ext_field, ext_value in extracted_values.items():
                excel_field = ext_field
                if ext_field == 'passport_number':
                    excel_field = 'passport_no'
                elif ext_field == 'date_of_birth':
                    excel_field = 'dob'
                    
                if excel_field in excel_values:
                    excel_value = str(excel_values[excel_field])
                    ext_value = str(ext_value)
                    
                    if excel_value != ext_value:
                        logger.warning(f"Mismatch for {ext_field}: Extracted={ext_value}, Excel={excel_value}")
            
            return True
        except Exception as e:
            logger.error(f"Error checking data transfer: {str(e)}")
            return False
    
    def _match_documents_to_employees(self, document_paths: Dict[str, str], all_excel_rows: List[Dict]) -> Dict[int, Dict[str, str]]:
        """Match documents to specific employees based on name matching."""
        if not document_paths or not all_excel_rows:
            return {}
            
        # Extract employee names from Excel
        employee_names = []
        for idx, row in enumerate(all_excel_rows):
            first_name = str(row.get('First Name', '')).strip()
            last_name = str(row.get('Last Name', '')).strip()
            full_name = f"{first_name} {last_name}".strip()
            employee_names.append({
                'index': idx,
                'first_name': first_name,
                'last_name': last_name,
                'full_name': full_name
            })
        
        # For each document, try to extract name and match to employee
        document_matches = {}
        for doc_type, paths in document_paths.items():
            if isinstance(paths, list):
                # Handle list of paths (new structure)
                for file_path in paths:
                    try:
                        # Check if document already processed
                        if self._is_document_processed(file_path):
                            logger.info(f"Skipping already processed document: {file_path}")
                            continue
                            
                        # Process document to extract data
                        doc_data = self.textract.process_document(file_path, doc_type)

                        # Mark as processed
                        self._mark_document_processed(file_path)
                        
                        # Look for name in extracted data
                        extracted_name = None
                        for field in ['name', 'full_name']:
                            if field in doc_data and doc_data[field] != '.':
                                extracted_name = doc_data[field]
                                break
                        
                        if not extracted_name:
                            continue
                            
                        # Find best employee match
                        best_match = None
                        best_score = 0
                        for emp in employee_names:
                            score = self._calculate_name_match_score(extracted_name, emp['full_name'])
                            if score > best_score:
                                best_score = score
                                best_match = emp
                        
                        # If good match found, assign document to employee
                        if best_match and best_score > 50:
                            idx = best_match['index']
                            if idx not in document_matches:
                                document_matches[idx] = {}
                            document_matches[idx][doc_type] = file_path
                            logger.info(f"Matched {doc_type} to employee {best_match['full_name']} (score: {best_score})")
                            
                    except Exception as e:
                        logger.error(f"Error matching document {doc_type} ({file_path}): {str(e)}")
            else:
                # Handle single path (old structure)
                try:
                    # Check if document already processed
                    if self._is_document_processed(file_path):
                        logger.info(f"Skipping already processed document: {file_path}")
                        continue
                        
                    # Process document to extract data
                    doc_data = self.textract.process_document(file_path, doc_type)

                    # Mark as processed
                    self._mark_document_processed(file_path)
                    
                    # Look for name in extracted data
                    extracted_name = None
                    for field in ['name', 'full_name']:
                        if field in doc_data and doc_data[field] != '.':
                            extracted_name = doc_data[field]
                            break
                    
                    if not extracted_name:
                        continue
                        
                    # Find best employee match
                    best_match = None
                    best_score = 0
                    for emp in employee_names:
                        score = self._calculate_name_match_score(extracted_name, emp['full_name'])
                        if score > best_score:
                            best_score = score
                            best_match = emp
                    
                    # If good match found, assign document to employee
                    if best_match and best_score > 50:
                        idx = best_match['index']
                        if idx not in document_matches:
                            document_matches[idx] = {}
                        document_matches[idx][doc_type] = paths
                        logger.info(f"Matched {doc_type} to employee {best_match['full_name']} (score: {best_score})")
                        
                except Exception as e:
                    logger.error(f"Error matching document {doc_type} ({paths}): {str(e)}")

        return document_matches

    def _calculate_name_match_score(self, name1: str, name2: str) -> int:
        """Calculate similarity score between two names."""
        if not name1 or not name2:
            return 0
            
        # Normalize names
        name1 = name1.lower().strip()
        name2 = name2.lower().strip()
        
        # Exact match
        if name1 == name2:
            return 100
            
        # Split into parts
        parts1 = set(name1.split())
        parts2 = set(name2.split())
        
        # Calculate intersection
        common_parts = parts1.intersection(parts2)
        
        # Calculate score based on common parts
        if not common_parts:
            return 0
            
        return int(100 * len(common_parts) / max(len(parts1), len(parts2)))
        
    def debug_data_flow(self, extracted_data, excel_data, document_paths, final_excel_path):
        """Create detailed debug report of data flow from extraction to final Excel."""
        try:
            debug_dir = "data_flow_debug"
            os.makedirs(debug_dir, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            debug_file = os.path.join(debug_dir, f"data_flow_{timestamp}.txt")
            
            with open(debug_file, 'w') as f:
                # Document paths section
                f.write("=" * 80 + "\n")
                f.write("DOCUMENT PATHS:\n")
                f.write("=" * 80 + "\n")
                for doc_type, path in document_paths.items():
                    f.write(f"{doc_type}: {path}\n")
                f.write("\n\n")
                
                # Extracted data section
                f.write("=" * 80 + "\n")
                f.write("EXTRACTED DATA FROM DOCUMENTS:\n")
                f.write("=" * 80 + "\n")
                for key, value in sorted(extracted_data.items()):
                    f.write(f"{key}: {value}\n")
                f.write("\n\n")
                
                # Excel data section
                f.write("=" * 80 + "\n")
                f.write("INPUT EXCEL DATA:\n")
                f.write("=" * 80 + "\n")
                if isinstance(excel_data, list):
                    for i, row in enumerate(excel_data):
                        f.write(f"ROW {i+1}:\n")
                        for key, value in sorted(row.items()):
                            f.write(f"  {key}: {value}\n")
                        f.write("\n")
                elif isinstance(excel_data, dict):
                    for key, value in sorted(excel_data.items()):
                        f.write(f"{key}: {value}\n")
                f.write("\n\n")
                
                # Final Excel output
                f.write("=" * 80 + "\n")
                f.write("FINAL EXCEL OUTPUT:\n")
                f.write("=" * 80 + "\n")
                try:
                    df = pd.read_excel(final_excel_path)
                    for i, row in df.iterrows():
                        f.write(f"ROW {i+1}:\n")
                        for col in df.columns:
                            f.write(f"  {col}: {row[col]}\n")
                        f.write("\n")
                except Exception as e:
                    f.write(f"Error reading final Excel: {str(e)}\n")
                
            logger.info(f"Created data flow debug report: {debug_file}")
            return debug_file
        except Exception as e:
            logger.error(f"Error creating debug report: {str(e)}")
            return None
        
    def _combine_data_for_row(self, row_data, extracted_data, document_matches):
        """Combine Excel row data with extracted document data."""
        combined = row_data.copy()
        
        # Priority fields from different document types
        passport_fields = ['passport_number', 'surname', 'given_names', 'nationality', 'date_of_birth', 'sex', 'gender']
        visa_fields = ['entry_permit_no', 'unified_no', 'visa_file_number', 'sponsor_name', 'profession']
        
        # Apply document data based on priority
        for field, value in extracted_data.items():
            if value == self.DEFAULT_VALUE:
                continue
                
            # Handle passport fields with highest priority
            if field in passport_fields:
                # Map to Excel column names
                if field == 'passport_number':
                    combined['passport_no'] = value
                    combined['Passport No'] = value
                elif field == 'surname':
                    combined['last_name'] = value
                    combined['Last Name'] = value
                elif field == 'given_names':
                    combined['first_name'] = value
                    combined['First Name'] = value
                elif field == 'nationality':
                    combined['nationality'] = value
                    combined['Nationality'] = value
                elif field == 'date_of_birth':
                    combined['dob'] = value
                    combined['DOB'] = value
                elif field in ['gender', 'sex']:
                    combined['Gender'] = value
            
            # Handle visa fields
            elif field in visa_fields:
                if field == 'entry_permit_no' or field == 'visa_file_number':
                    combined['visa_file_number'] = value
                    combined['Visa File Number'] = value
                elif field == 'unified_no':
                    combined['unified_no'] = value
                    combined['Unified No'] = value
                elif field == 'sponsor_name':
                    combined['sponsor'] = value
                    combined['Sponsor'] = value
                elif field == 'profession':
                    combined['profession'] = value
                    combined['Profession'] = value
                    combined['Occupation'] = value
        
        return combined
    
    def _process_large_client_excel(self, excel_path: str, template_path: str, output_path: str) -> Dict:
        """
        Special handler for large client Excel files (400+ employees) with custom mapping.
        
        Args:
            excel_path: Path to the client Excel file
            template_path: Path to the template Excel file
            output_path: Path for the output Excel file
            
        Returns:
            Dictionary with processing result
        """
        try:
            logger.info(f"Processing large client Excel: {excel_path}")
            
            # Read the client Excel file
            client_df = pd.read_excel(excel_path)
            logger.info(f"Read {len(client_df)} rows from client Excel")
            
            # Read the template to understand required columns
            template_df = pd.read_excel(template_path)
            template_columns = template_df.columns.tolist()
            logger.info(f"Template has {len(template_columns)} columns")
            
            # Determine which template we're using
            template_name = os.path.basename(template_path).lower()
            is_nas = 'nas' in template_name
            is_almadallah = 'madallah' in template_name or 'almadallah' in template_name
            
            logger.info(f"Using {'NAS' if is_nas else 'Al Madallah' if is_almadallah else 'Unknown'} template")
            
            # Create a new DataFrame with template columns
            result_df = pd.DataFrame(columns=template_columns)
            
            # Log client Excel columns and some sample values for debugging
            logger.info("Client Excel columns:")
            for col in client_df.columns:
                sample_values = client_df[col].dropna().head(2).tolist()
                sample_str = str(sample_values)[:50] + "..." if len(str(sample_values)) > 50 else sample_values
                logger.info(f"  - {col}: {sample_str}")
            
            # Process each row in the client Excel
            for idx, row in client_df.iterrows():
                # Create a dictionary for this row with template column names
                template_row = {}
                for col in template_columns:
                    template_row[col] = ""  # Initialize with empty strings
                
                # UNIVERSAL FIELD MAPPING - works for any client Excel format
                # Try multiple variations of column names
                
                # Map Staff ID / Employee ID
                for client_field in ['StaffNo', 'Staff No', 'Staff ID', 'Employee No', 'EmpNo', 'Employee ID']:
                    if client_field in client_df.columns and pd.notna(row[client_field]):
                        if is_nas:
                            template_row['Staff ID'] = str(row[client_field]).strip()
                            template_row['Family No.'] = str(row[client_field]).strip()
                        elif is_almadallah:
                            template_row['Employee ID'] = str(row[client_field]).strip()
                        break
                
                # Map Nationality
                for client_field in ['Country', 'Nationality', 'Nation']:
                    if client_field in client_df.columns and pd.notna(row[client_field]):
                        template_row['Nationality'] = str(row[client_field]).strip()
                        break
                
                # Map Emirates ID
                for client_field in ['EIDNumber', 'Emirates ID', 'EmiratesID', 'EID', "Emirates Id"]:
                    if client_field in client_df.columns and pd.notna(row[client_field]):
                        eid = str(row[client_field]).strip()
                        # Format Emirates ID if needed
                        if eid and '-' not in eid and len(eid.replace(' ', '')) == 15:
                            digits = eid.replace(' ', '')
                            eid = f"{digits[:3]}-{digits[3:7]}-{digits[7:14]}-{digits[14]}"
                        
                        if is_nas or is_almadallah:
                            template_row['Emirates Id'] = eid
                        # Format Emirates ID - Replace any that don't start with '784'
                        if is_nas and 'Emirates Id' in template_row and template_row['Emirates Id']:
                            eid_value = template_row['Emirates Id']
                            # Remove any non-digits or hyphens to check the start
                            clean_eid = ''.join(filter(lambda c: c.isdigit() or c == '-', eid_value))
                            
                            # Check if it starts with 784 (either with or without hyphens)
                            if not (clean_eid.startswith('784') or clean_eid.startswith('784-')):
                                logger.warning(f"Emirates ID {eid_value} doesn't start with 784, replacing with default")
                                template_row['Emirates Id'] = '111-1111-1111111-1'
                        elif is_almadallah and 'Emirates Id' in template_row and template_row['Emirates Id']:
                            eid_value = template_row['Emirates Id']
                            # Remove any non-digits or hyphens to check the start
                            clean_eid = ''.join(filter(lambda c: c.isdigit() or c == '-', eid_value))
                            
                            # Check if it starts with 784 (either with or without hyphens)
                            if not (clean_eid.startswith('784') or clean_eid.startswith('784-')):
                                logger.warning(f"Emirates ID {eid_value} doesn't start with 784, replacing with default")
                                template_row['Emirates Id'] = '111-1111-1111111-1'

                        break
                
                # Map Passport Number
                for client_field in ['PassportNum', 'Passport No', 'Passport', 'PassportNumber']:
                    if client_field in client_df.columns and pd.notna(row[client_field]):
                        template_row['Passport No'] = str(row[client_field]).strip()
                        break
                
                # Map Unified Number
                for client_field in ['UIDNo', 'UID', 'Unified No', 'UnifiedNumber']:
                    if client_field in client_df.columns and pd.notna(row[client_field]):
                        template_row['Unified No'] = str(row[client_field]).strip()
                        break
                
                # Map Visa File Number
                for client_field in ['ResisdentFileNumber', 'ResidentFileNumber', 'VisaFileNumber', 'Visa File No', 'Visa File Number']:
                    if client_field in client_df.columns and pd.notna(row[client_field]):
                        template_row['Visa File Number'] = str(row[client_field]).strip()
                        break
                
                # Map Mobile Number
                for client_field in ['EntityContactNumber', 'Mobile', 'Mobile No', 'Phone', 'Contact', 'ContactNo', 'Contact Number']:
                    if client_field in client_df.columns and pd.notna(row[client_field]):
                        mobile = str(row[client_field]).strip()
                        if is_nas:
                            template_row['Mobile No'] = mobile
                            template_row['Company Phone'] = mobile
                        elif is_almadallah:
                            template_row['MOBILE'] = mobile
                            template_row['COMPANYPHONENUMBER'] = mobile
                            template_row['LANDLINENO'] = mobile
                        break
                
                # Map Email - added more variations and debug logging
                email_found = False
                for client_field in ['EmailID', 'Email', 'EmailAddress', 'EMAIL', 'Mail', 'email', 'Email Address', 'E-mail', 'Email ID', 'mail']:
                    if client_field in client_df.columns and pd.notna(row[client_field]):
                        email = str(row[client_field]).strip()
                        logger.info(f"Found email '{email}' in column '{client_field}' for row {idx}")
                        if is_nas:
                            template_row['Email'] = email
                            template_row['Company Mail'] = email
                        elif is_almadallah:
                            template_row['EMAIL'] = email
                            template_row['COMPANYEMAILID'] = email
                        email_found = True
                        break
                        
                if not email_found:
                    logger.warning(f"No email found for row {idx}. Available columns: {list(client_df.columns)}")
                
                # Map Gender
                for client_field in ['Gender', 'Sex']:
                    if client_field in client_df.columns and pd.notna(row[client_field]):
                        gender = str(row[client_field]).strip().upper()
                        if gender in ['M', 'MALE']:
                            if is_nas:
                                template_row['Gender'] = 'Male'
                            else:
                                template_row['Gender'] = 'Male'
                        elif gender in ['F', 'FEMALE']:
                            if is_nas:
                                template_row['Gender'] = 'Female'
                            else:
                                template_row['Gender'] = 'Female'
                        break
                
                # Map Department or Subgroup
                for client_field in ['Department', 'Dept', 'Division']:
                    if client_field in client_df.columns and pd.notna(row[client_field]):
                        if is_nas:
                            template_row['Department'] = str(row[client_field]).strip()
                        elif is_almadallah:
                            template_row['Subgroup Name'] = str(row[client_field]).strip()
                        break
                
                # Map Designation or Rank
                for client_field in ['Designation', 'JobTitle', 'Occupation', 'Position']:
                    if client_field in client_df.columns and pd.notna(row[client_field]):
                        if is_nas:
                            template_row['Occupation'] = str(row[client_field]).strip()
                        elif is_almadallah:
                            template_row['RANK'] = str(row[client_field]).strip()
                            template_row['Occupation'] = str(row[client_field]).strip()
                        break
                
                # Map Marital Status
                for client_field in ['MaritalStatus', 'Marital', 'MarriageStatus', 'Marital Status']:
                    if client_field in client_df.columns and pd.notna(row[client_field]):
                        if is_nas:
                            template_row['Marital Status'] = str(row[client_field]).strip()
                        break
                
                # Map Category
                for client_field in ['Category', 'EmpCategory', 'EmployeeCategory', 'EmpType', 'Type']:
                    if client_field in client_df.columns and pd.notna(row[client_field]):
                        if is_nas:
                            template_row['Category'] = str(row[client_field]).strip()
                        break
                
                # Map Relation
                for client_field in ['Relation', 'Relationship', 'RelationshipToSponsor']:
                    if client_field in client_df.columns and pd.notna(row[client_field]):
                        if is_nas:
                            template_row['Relation'] = str(row[client_field]).strip()
                        break
                
                # Map DOB or Date of Birth
                for client_field in ['DOB', 'DateOfBirth', 'BirthDate', 'Date of Birth']:
                    if client_field in client_df.columns and pd.notna(row[client_field]):
                        try:
                            dob = row[client_field]
                            if isinstance(dob, pd.Timestamp):
                                formatted_dob = dob.strftime('%d-%m-%Y')
                            else:
                                # Try to parse as date
                                formatted_dob = pd.to_datetime(dob).strftime('%d-%m-%Y')
                            
                            template_row['DOB'] = formatted_dob
                        except:
                            template_row['DOB'] = str(row[client_field]).strip()
                        break
                
                # First check if there's a Salary Band field in the client Excel
                for salary_band_field in ['Salary Band', 'SalaryBand', 'Salary_Band', 'salary band', 'salary_band']:
                    if salary_band_field in client_df.columns and pd.notna(row[salary_band_field]):
                        # Directly copy the value
                        template_row['Salary Band'] = str(row[salary_band_field]).strip()
                        logger.info(f"Copied Salary Band directly: '{template_row['Salary Band']}'")
                        break
                else:
                    # If no direct Salary Band field, fallback to default
                    template_row['Salary Band'] = 'less than 4000'
                    logger.info(f"No Salary Band field found, using default: '{template_row['Salary Band']}'")

                
                # SPECIAL HANDLING FOR NAME FIELDS
                # Check if the FirstName column contains full names that need splitting
                name_found = False
                first_name = ""
                middle_name = "."
                last_name = ""

                # Look for explicit first, middle, last name fields
                for fn_field in ['FirstName', 'First Name', 'FName', 'GivenName']:
                    if fn_field in client_df.columns and pd.notna(row[fn_field]):
                        first_name = str(row[fn_field]).strip()
                        name_found = True
                        
                        # If FirstName contains multiple words, split it immediately
                        full_name = first_name
                        name_parts = full_name.split()
                        
                        if len(name_parts) >= 2:  # If there are at least 2 words, split it
                            logger.info(f"Splitting multi-word name '{full_name}' for row {idx}")
                            if len(name_parts) >= 3:
                                # If 3+ words: first word is first name, second is middle name, rest is last name
                                first_name = name_parts[0]
                                middle_name = name_parts[1]
                                last_name = ' '.join(name_parts[2:])
                            else:  # Exactly 2 words
                                # First word is first name, middle is ".", second word is last name
                                first_name = name_parts[0]
                                middle_name = "."
                                last_name = name_parts[1]
                            logger.info(f"Split into First='{first_name}', Middle='{middle_name}', Last='{last_name}'")
                        break

                # Check for middle and last name fields only if we haven't already handled them above
                if not len(first_name.split()) >= 2:  # Skip if we already split a multi-word name
                    for mn_field in ['MiddleName', 'Middle Name', 'MName']:
                        if mn_field in client_df.columns and pd.notna(row[mn_field]):
                            middle_name = str(row[mn_field]).strip()
                            if not middle_name:
                                middle_name = "."
                            break
                    
                    for ln_field in ['LastName', 'Last Name', 'LName', 'Surname', 'FamilyName']:
                        if ln_field in client_df.columns and pd.notna(row[ln_field]):
                            last_name = str(row[ln_field]).strip()
                            break

                # Check for full name field if individual components weren't found
                if not name_found or not first_name:
                    for name_field in ['Name', 'FullName', 'Full Name', 'EmployeeName']:
                        if name_field in client_df.columns and pd.notna(row[name_field]):
                            full_name = str(row[name_field]).strip()
                            logger.info(f"Processing name from '{name_field}' column: '{full_name}'")
                            
                            # Split name into components
                            name_parts = full_name.split()
                            
                            if len(name_parts) >= 3:
                                # If 3+ words: first = first, middle = second, last = rest
                                first_name = name_parts[0]
                                middle_name = name_parts[1]
                                last_name = ' '.join(name_parts[2:])
                            elif len(name_parts) == 2:
                                # If 2 words: first = first, middle = ".", last = second
                                first_name = name_parts[0]
                                middle_name = "."
                                last_name = name_parts[1]
                            elif len(name_parts) == 1:
                                # If 1 word: first = that word, middle = ".", last = ""
                                first_name = name_parts[0]
                                middle_name = "."
                                last_name = ""
                            
                            logger.info(f"Split into First='{first_name}', Middle='{middle_name}', Last='{last_name}'")
                            name_found = True
                            break

                # Log final name components
                logger.info(f"Final name components for row {idx}: First='{first_name}', Middle='{middle_name}', Last='{last_name}'")

                # Apply the name fields to the template
                if is_nas:
                    template_row['First Name'] = first_name
                    template_row['Middle Name'] = middle_name if middle_name else "."
                    template_row['Last Name'] = last_name
                    # Double check the fields were actually set
                    logger.info(f"Set template fields: First='{template_row['First Name']}', Middle='{template_row['Middle Name']}', Last='{template_row['Last Name']}'")
                elif is_almadallah:
                    # For Al Madallah, we need to store in temp variables since 
                    # it doesn't have the same name fields as NAS
                    full_name = f"{first_name} {middle_name} {last_name}".replace(" . ", " ").strip()
                    # We'll use this for other matching fields later

                # TEMPLATE-SPECIFIC FIELDS
                
                # NAS Template specific fields
                if is_nas:
                    # Default fields if not set above
                    if not template_row.get('Contract Name'):
                        template_row['Contract Name'] = ""
                    
                    # Only set defaults for Category and Relation if not already set
                    if not template_row.get('Category'):
                        template_row['Category'] = ""
                    
                    if not template_row.get('Relation'):
                        template_row['Relation'] = ""
                    
                    # Copy Staff ID to Family No. if not already set
                    if template_row.get('Staff ID') and not template_row.get('Family No.'):
                        template_row['Family No.'] = template_row['Staff ID']
                    
                    # Default countries
                    template_row['Work Country'] = "United Arab Emirates"
                    template_row['Residence Country'] = "United Arab Emirates"
                    
                    # Default commission
                    template_row['Commission'] = "NO"
                    
                    # Make sure Middle Name is "." if empty
                    if not template_row.get('Middle Name'):
                        template_row['Middle Name'] = "."
                
                # Al Madallah Template specific fields
                elif is_almadallah:
                    # Policy category
                    template_row['POLICYCATEGORY'] = "Standard"
                    
                    # Establishment type
                    template_row['ESTABLISHMENTTYPE'] = "Establishment"
                    
                    # Commission
                    template_row['Commission'] = "NO"
                    
                    # Set Subgroup Name if empty
                    if not template_row.get('Subgroup Name'):
                        template_row['Subgroup Name'] = "GENERAL"
                    
                    # VIP status
                    template_row['VIP'] = "NO"
                    
                    # Waiting period days
                    template_row['WPDAYS'] = "0"
                    
                    # Set POLICYSEQUENCE if needed
                    if 'POLICYSEQUENCE' in template_columns and not template_row.get('POLICYSEQUENCE'):
                        template_row['POLICYSEQUENCE'] = "1"
                
                # COMMON FIELDS FOR BOTH TEMPLATES
                
                # Set Effective Date (today's date)
                today = datetime.now().strftime('%d/%m/%Y')
                for field in ['Effective Date', 'Effective Date ']:  # Note the space after Date in second field
                    if field in template_columns:
                        template_row[field] = today
                
                # Handle emirate-based fields based on visa file number
                visa_file = template_row.get('Visa File Number')
                if visa_file:
                    digits = ''.join(filter(str.isdigit, str(visa_file)))
                    
                    if digits.startswith('10'):  # Abu Dhabi
                        if is_almadallah:
                            template_row['Work Emirate'] = 'Abu Dhabi'
                            template_row['Residence Emirate'] = 'Abu Dhabi'
                            template_row['Work Region'] = 'Abu Dhabi - Abu Dhabi'
                            template_row['Residence Region'] = 'Abu Dhabi - Abu Dhabi'
                            template_row['Visa Issuance Emirate'] = 'Abu Dhabi'
                            template_row['Member Type'] = 'Expat whose residence issued other than Dubai'
                        elif is_nas:
                            template_row['Work Emirate'] = 'Abu Dhabi'
                            template_row['Residence Emirate'] = 'Abu Dhabi'
                            template_row['Work Region'] = 'Al Ain City'
                            template_row['Residence Region'] = 'Al Ain City'
                            template_row['Visa Issuance Emirate'] = 'Abu Dhabi'
                            template_row['Member Type'] = 'Expat whose residence issued other than Dubai'
                    elif digits.startswith('20'):  # Dubai
                        if is_almadallah:
                            template_row['Work Emirate'] = 'Dubai'
                            template_row['Residence Emirate'] = 'Dubai'
                            template_row['Work Region'] = 'Dubai - Abu Hail'
                            template_row['Residence Region'] = 'Dubai - Abu Hail'
                            template_row['Visa Issuance Emirate'] = 'Dubai'
                            template_row['Member Type'] = 'Expat whose residence issued in Dubai'
                        elif is_nas:
                            template_row['Work Emirate'] = 'Dubai'
                            template_row['Residence Emirate'] = 'Dubai'
                            template_row['Work Region'] = 'DUBAI (DISTRICT UNKNOWN)'
                            template_row['Residence Region'] = 'DUBAI (DISTRICT UNKNOWN)'
                            template_row['Visa Issuance Emirate'] = 'Dubai'
                            template_row['Member Type'] = 'Expat whose residence issued in Dubai'
                    else:
                        # Default to Dubai for any other visa number pattern
                        if is_nas:
                            template_row['Work Emirate'] = 'Dubai'
                            template_row['Residence Emirate'] = 'Dubai'
                            template_row['Work Region'] = 'Dubai - Abu Hail'
                            template_row['Residence Region'] = 'Dubai - Abu Hail'
                            template_row['Visa Issuance Emirate'] = 'Dubai'
                            template_row['Member Type'] = 'Expat whose residence issued in Dubai'
                else:
                    # Default to Dubai if no visa file number
                    if is_nas:
                        template_row['Work Emirate'] = 'Dubai'
                        template_row['Residence Emirate'] = 'Dubai'
                        template_row['Work Region'] = 'Dubai - Abu Hail'
                        template_row['Residence Region'] = 'Dubai - Abu Hail'
                        template_row['Visa Issuance Emirate'] = 'Dubai'
                        template_row['Member Type'] = 'Expat whose residence issued in Dubai'
                    elif is_almadallah:
                        template_row['Work Emirate'] = 'Dubai'
                        template_row['Residence Emirate'] = 'Dubai'
                        template_row['Work Region'] = 'DUBAI (DISTRICT UNKNOWN)'
                        template_row['Residence Region'] = 'DUBAI (DISTRICT UNKNOWN)'
                        template_row['Visa Issuance Emirate'] = 'Dubai'
                        template_row['Member Type'] = 'Expat whose residence issued in Dubai'
                
                # Add row to result DataFrame
                result_rows = []
                for col in template_columns:
                    if col in template_row:
                        # If we have a value for this column, use it
                        result_rows.append(template_row[col])
                    else:
                        # Otherwise, use empty string
                        result_rows.append("")
                
                # Add the new row to the result DataFrame
                result_df.loc[len(result_df)] = result_rows
                
                # Provide progress updates for large files
                if (idx + 1) % 100 == 0 or idx == len(client_df) - 1:
                    logger.info(f"Processed {idx + 1}/{len(client_df)} rows")
            
            # Final data cleaning
            # Make sure all text fields are strings
            for col in result_df.columns:
                result_df[col] = result_df[col].astype(str)
                # Replace "nan" with empty string
                result_df[col] = result_df[col].replace("nan", "")
            
            # Make sure Middle Name is "." for NAS template
            if is_nas and 'Middle Name' in result_df.columns:
                result_df['Middle Name'] = result_df['Middle Name'].apply(lambda x: "." if not x or x == "" else x)
            
            # Write the result to the output file
            result_df.to_excel(output_path, index=False)
            
            logger.info(f"Successfully processed large client Excel with {len(result_df)} rows")
            
            # Try to send email with the processed file
            try:
                email_subject = "Medical Bot - Large Client Excel Processing Complete"
                email_body = f"""
                Dear Team,
                
                The medical bot has completed processing a large client Excel file.
                
                File: {os.path.basename(excel_path)}
                Output: {os.path.basename(output_path)}
                Rows Processed: {len(result_df)}
                
                Please find the processed file attached.
                
                Regards,
                Medical Bot
                """
                
                # Try to send email with attachment
                logger.info(f"Attempting to send email with attachment: {output_path}")
                from src.utils.email_sender import EmailSender
                email_sender = EmailSender()
                
                # Log email configuration without accessing attributes directly
                logger.info("Email sender configuration:")
                
                # Safely check if attributes exist
                for attr in ['smtp_server', 'smtp_port', 'from_email', 'to_email']:
                    if hasattr(email_sender, attr):
                        logger.info(f"  - {attr}: {getattr(email_sender, attr)}")
                
                email_sent = email_sender.send_email(
                    subject=email_subject,
                    body=email_body,
                    attachment_path=output_path
                )
                
                if email_sent:
                    logger.info("Email sent successfully with processed Excel file")
                else:
                    logger.warning("Failed to send email with processed Excel file")
            except Exception as e:
                logger.error(f"Error sending email: {str(e)}", exc_info=True)
                # Continue processing even if email fails
            
            return {
                "status": "success",
                "rows_processed": len(result_df),
                "output_path": output_path
            }
        
        except Exception as e:
            logger.error(f"Error processing large client Excel: {str(e)}", exc_info=True)
            return {
                "status": "error",
                "error": str(e)
            }
            
            
    # This function needs to be fixed in test_complete_workflow.py to prevent the TypeError

    def _log_document_matches(self, document_paths, excel_data, extracted_data_by_document):
        """
        Create detailed debug logs for document to employee matching.
        
        Args:
            document_paths: Dict of document paths by type
            excel_data: List of dictionaries with Excel data
            extracted_data_by_document: Dict of extracted data by document type
        
        Returns:
            Dict with matching statistics
        """
        logger.info("=" * 80)
        logger.info("DOCUMENT MATCHING DIAGNOSTICS")
        logger.info("=" * 80)
        
        # Create diagnostic info
        diagnostics = {
            "documents": [],
            "excel_rows": [],
            "matches": []
        }
        
        # Document information - handle new list-based structure properly
        for doc_type, paths in document_paths.items():
            if isinstance(paths, list):
                # Handle list of paths (new structure)
                for path in paths:
                    try:
                        # Log document info
                        logger.info(f"Document: {doc_type} - {os.path.basename(path)}")
                        
                        doc_info = {
                            "type": doc_type,
                            "file_path": path,
                            "key_identifiers": {}
                        }
                        
                        # Extract key identifiers from document
                        doc_data = {}
                        if doc_type in extracted_data_by_document:
                            doc_data = extracted_data_by_document[doc_type]
                        
                        # Add key identifiers
                        for id_type, field_names in [
                            ("name", ['full_name', 'name']), 
                            ("passport", ['passport_number', 'passport_no']),
                            ("emirates_id", ['emirates_id', 'eid'])
                        ]:
                            for field in field_names:
                                if field in doc_data and doc_data[field] != self.DEFAULT_VALUE:
                                    doc_info["key_identifiers"][id_type] = doc_data[field]
                                    logger.info(f"  - {id_type}: {doc_data[field]}")
                                    break
                        
                        diagnostics["documents"].append(doc_info)
                    except Exception as e:
                        logger.error(f"Error processing document path: {str(e)}")
            else:
                # Handle single path (old structure)
                try:
                    # Log document info
                    logger.info(f"Document: {doc_type} - {os.path.basename(paths)}")
                    
                    doc_info = {
                        "type": doc_type,
                        "file_path": paths,
                        "key_identifiers": {}
                    }
                    
                    # Extract key identifiers from document
                    doc_data = {}
                    if doc_type in extracted_data_by_document:
                        doc_data = extracted_data_by_document[doc_type]
                    
                    # Add key identifiers
                    for id_type, field_names in [
                        ("name", ['full_name', 'name']), 
                        ("passport", ['passport_number', 'passport_no']),
                        ("emirates_id", ['emirates_id', 'eid'])
                    ]:
                        for field in field_names:
                            if field in doc_data and doc_data[field] != self.DEFAULT_VALUE:
                                doc_info["key_identifiers"][id_type] = doc_data[field]
                                logger.info(f"  - {id_type}: {doc_data[field]}")
                                break
                    
                    diagnostics["documents"].append(doc_info)
                except Exception as e:
                    logger.error(f"Error processing document path: {str(e)}")
        
        # Excel row information
        try:
            if isinstance(excel_data, list):
                for idx, row in enumerate(excel_data):
                    row_info = {
                        "row_index": idx,
                        "key_identifiers": {}
                    }
                    
                    # Row name
                    first_name = str(row.get('First Name', '')).strip()
                    last_name = str(row.get('Last Name', '')).strip()
                    row_name = f"{first_name} {last_name}".strip()
                    if row_name:
                        row_info["key_identifiers"]["name"] = row_name
                        
                    # Passport number
                    for field in ['passport_no', 'Passport No', 'PassportNo']:
                        if field in row and row[field] and row[field] != self.DEFAULT_VALUE:
                            row_info["key_identifiers"]["passport"] = str(row[field]).strip()
                            break
                            
                    # Emirates ID
                    for field in ['emirates_id', 'Emirates Id', 'EID']:
                        if field in row and row[field] and row[field] != self.DEFAULT_VALUE:
                            row_info["key_identifiers"]["emirates_id"] = str(row[field]).strip()
                            break
                    
                    # Log row info
                    logger.info(f"Excel Row {idx+1}:")
                    for id_type, value in row_info["key_identifiers"].items():
                        logger.info(f"  - {id_type}: {value}")
                        
                    diagnostics["excel_rows"].append(row_info)
            elif isinstance(excel_data, pd.DataFrame):
                for idx, row in excel_data.iterrows():
                    row_dict = row.to_dict()
                    row_info = {
                        "row_index": idx,
                        "key_identifiers": {}
                    }
                    
                    # Row name
                    first_name = str(row_dict.get('First Name', '')).strip()
                    last_name = str(row_dict.get('Last Name', '')).strip()
                    row_name = f"{first_name} {last_name}".strip()
                    if row_name:
                        row_info["key_identifiers"]["name"] = row_name
                        
                    # Passport number
                    for field in ['passport_no', 'Passport No', 'PassportNo']:
                        if field in row_dict and row_dict[field] and row_dict[field] != self.DEFAULT_VALUE:
                            row_info["key_identifiers"]["passport"] = str(row_dict[field]).strip()
                            break
                            
                    # Emirates ID
                    for field in ['emirates_id', 'Emirates Id', 'EID']:
                        if field in row_dict and row_dict[field] and row_dict[field] != self.DEFAULT_VALUE:
                            row_info["key_identifiers"]["emirates_id"] = str(row_dict[field]).strip()
                            break
                    
                    # Log row info
                    logger.info(f"Excel Row {idx+1}:")
                    for id_type, value in row_info["key_identifiers"].items():
                        logger.info(f"  - {id_type}: {value}")
                        
                    diagnostics["excel_rows"].append(row_info)
            else:
                logger.warning(f"Unknown excel_data type: {type(excel_data)}")
        except Exception as e:
            logger.error(f"Error processing Excel rows: {str(e)}")
        
        # Calculate potential matches
        try:
            for doc_idx, doc in enumerate(diagnostics["documents"]):
                for row_idx, row in enumerate(diagnostics["excel_rows"]):
                    match_score = 0
                    match_reasons = []
                    
                    # Name matching
                    doc_name = doc["key_identifiers"].get("name")
                    row_name = row["key_identifiers"].get("name")
                    
                    if doc_name and row_name:
                        # Calculate name similarity
                        doc_words = set(doc_name.lower().split())
                        row_words = set(row_name.lower().split())
                        
                        # Find common words
                        common_words = doc_words.intersection(row_words)
                        
                        if common_words:
                            # Calculate similarity percentage
                            similarity = len(common_words) / max(len(doc_words), len(row_words))
                            name_score = int(similarity * 50)  # Max 50 points for name matching
                            match_score += name_score
                            match_reasons.append(f"Name similarity: {similarity:.2f} ({', '.join(common_words)})")
                    
                    # Passport matching
                    doc_passport = doc["key_identifiers"].get("passport")
                    row_passport = row["key_identifiers"].get("passport")
                    
                    if doc_passport and row_passport:
                        if doc_passport.lower() == row_passport.lower():
                            match_score += 100
                            match_reasons.append(f"Passport match: {doc_passport}")
                    
                    # Emirates ID matching
                    doc_eid = doc["key_identifiers"].get("emirates_id")
                    row_eid = row["key_identifiers"].get("emirates_id")
                    
                    if doc_eid and row_eid:
                        # Clean both for comparison (remove spaces, hyphens)
                        clean_doc_id = re.sub(r'[^0-9]', '', str(doc_eid))
                        clean_row_id = re.sub(r'[^0-9]', '', str(row_eid))
                        
                        if clean_doc_id == clean_row_id:
                            match_score += 100
                            match_reasons.append(f"Emirates ID match: {doc_eid}")
                    
                    # Record match if score is high enough
                    if match_score >= 50:  # Threshold for considering a match
                        match_info = {
                            "document_index": doc_idx,
                            "document_type": doc["type"],
                            "row_index": row_idx,
                            "score": match_score,
                            "reasons": match_reasons
                        }
                        
                        logger.info(f"MATCH: Document {doc['type']} with Row {row_idx+1}")
                        logger.info(f"  - Score: {match_score}")
                        for reason in match_reasons:
                            logger.info(f"  - {reason}")
                            
                        diagnostics["matches"].append(match_info)
        except Exception as e:
            logger.error(f"Error calculating matches: {str(e)}")
        
        # Save diagnostics to file
        try:
            with open('document_matches.json', 'w') as f:
                json.dump(diagnostics, f, indent=2, default=str)
            logger.info(f"Saved document matching diagnostics to document_matches.json")
        except Exception as e:
            logger.error(f"Error saving document matching diagnostics: {str(e)}")
        
        # Summary
        logger.info("=" * 80)
        logger.info(f"MATCHING SUMMARY: {len(diagnostics['matches'])} matches found")
        logger.info("=" * 80)
        
        return diagnostics

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s'
)
logger = logging.getLogger(__name__)

def print_separator():
    """Print a separator line for better readability."""
    logger.info("=" * 80)

def run_test(reset=False):
    """Run the workflow test."""
    print_separator()
    logger.info("STARTING WORKFLOW TEST")
    print_separator()
    
    # Initialize workflow tester
    tester = WorkflowTester()
    
    # Reset email tracker if requested
    if reset:
        from src.email_tracker.email_tracker import EmailTracker
        tracker = EmailTracker()
        if tracker.reset_tracker():
            logger.info("Successfully reset email tracker")
        else:
            logger.warning("Failed to reset email tracker")
    
    # Run workflow
    result = tester.run_complete_workflow(bypass_dedup=reset)
    
    # Print results
    print_separator()
    logger.info("WORKFLOW TEST COMPLETED")
    
    if result['status'] == 'success':
        # Handle different sources (email or folder)
        if result.get('source') == 'folder':
            logger.info(f"Successfully processed {result['successful']} folders")
        else:
            # Default to email processing
            logger.info(f"Successfully processed {result['successful']} out of {result.get('emails_processed', 0)} emails")
        
        # Show completed submissions
        submissions = tester.get_completed_submissions()
        for sub in submissions:
            logger.info(f"\nSubmission {sub.process_id}:")
            logger.info(f"Status: {sub.status}")
            logger.info(f"Timestamp: {sub.timestamp}")
            logger.info(f"Documents: {list(sub.documents.keys())}")
            logger.info(f"Final Excel: {sub.final_excel}")
    else:
        logger.error(f"Workflow failed: {result.get('error')}")

def validate_excel_file(file_path: str) -> Dict:
    """
    Validate an output Excel file.
    
    Args:
        file_path: Path to Excel file
        
    Returns:
        Validation result dictionary
    """
    try:
        logger.info(f"Validating {file_path}")
        df = pd.read_excel(file_path)
        
        if df.empty:
            return {
                'file': os.path.basename(file_path),
                'is_valid': False,
                'row_count': 0,
                'issues': ["File is empty"]
            }
            
        issues = []
        
        # Check required columns
        required_fields = [
            'first_name', 'last_name', 'nationality', 
            'passport_no', 'dob', 'gender'
        ]
        
        missing_columns = [field for field in required_fields if field not in df.columns]
        if missing_columns:
            issues.append(f"Missing required columns: {', '.join(missing_columns)}")
            
        # Check for empty values in required fields
        for field in [f for f in required_fields if f in df.columns]:
            empty_rows = df[df[field].isin(['', '.', 'nan'])].index.tolist()
            if empty_rows:
                if len(empty_rows) <= 3:  # Show only first few for readability
                    issues.append(f"Empty values in {field} at rows: {[i+2 for i in empty_rows]}")  # +2 for Excel row numbers (header + 1-indexing)
                else:
                    issues.append(f"Empty values in {field} at {len(empty_rows)} rows")
                    
        # Validate date formats
        date_fields = ['dob', 'passport_expiry_date', 'visa_expiry_date']
        for field in [f for f in date_fields if f in df.columns]:
            # Check if dates are in DD-MM-YYYY format
            invalid_format = []
            for idx, val in enumerate(df[field]):
                if pd.notna(val) and val not in ['.', '']:
                    if not re.match(r'^\d{2}-\d{2}-\d{4}$', str(val)):
                        invalid_format.append(idx)
                        
            if invalid_format:
                if len(invalid_format) <= 3:
                    issues.append(f"Invalid date format in {field} at rows: {[i+2 for i in invalid_format]}")
                else:
                    issues.append(f"Invalid date format in {field} at {len(invalid_format)} rows")
        
        # Check Emirates ID format if present
        if 'emirates_id' in df.columns:
            invalid_eid = []
            for idx, val in enumerate(df['emirates_id']):
                if pd.notna(val) and val not in ['.', '']:
                    if not re.match(r'^\d{3}-\d{4}-\d{7}-\d{1}$', str(val)):
                        invalid_eid.append(idx)
                        
            if invalid_eid:
                if len(invalid_eid) <= 3:
                    issues.append(f"Invalid Emirates ID format at rows: {[i+2 for i in invalid_eid]}")
                else:
                    issues.append(f"Invalid Emirates ID format at {len(invalid_eid)} rows")
                    
        return {
            'file': os.path.basename(file_path),
            'is_valid': len(issues) == 0,
            'row_count': len(df),
            'issues': issues
        }
    
    except Exception as e:
        logger.error(f"Error validating {file_path}: {str(e)}")
        return {
            'file': os.path.basename(file_path),
            'is_valid': False,
            'row_count': 0,
            'issues': [f"Validation error: {str(e)}"]
        }

def run_validation(output_dir: str):
    """
    Validate output files in a directory.
    
    Args:
        output_dir: Directory containing output files
    """
    print_separator()
    logger.info(f"VALIDATING OUTPUT FILES IN {output_dir}")
    print_separator()
    
    if not os.path.exists(output_dir):
        logger.error(f"Output directory does not exist: {output_dir}")
        return False
        
    # Find all Excel files in output directory and subdirectories
    excel_files = []
    for root, _, files in os.walk(output_dir):
        for file in files:
            if file.endswith(('.xlsx', '.xls')) and 'template' not in file.lower():
                excel_files.append(os.path.join(root, file))
                
    if not excel_files:
        logger.warning("No output Excel files found")
        return False
        
    logger.info(f"Found {len(excel_files)} output Excel files")
    
    # Validate each Excel file
    validation_results = []
    for excel_file in excel_files:
        result = validate_excel_file(excel_file)
        validation_results.append(result)
        
    # Print summary
    valid_count = sum(1 for r in validation_results if r['is_valid'])
    logger.info(f"Validation results: {valid_count}/{len(validation_results)} files are valid")
    
    for idx, result in enumerate(validation_results, 1):
        logger.info(f"File {idx}: {result['file']}")
        if result['is_valid']:
            logger.info(f"  Valid: Yes - {result['row_count']} rows")
        else:
            logger.info(f"  Valid: No - {len(result['issues'])} issues")
            for issue in result['issues']:
                logger.warning(f"  - {issue}")
                
    return valid_count == len(validation_results)

def reset_processed_emails():
    """Simple reset for email tracking."""
    processed_file = "processed_emails_simple.txt"
    
    # Delete and recreate the file
    if os.path.exists(processed_file):
        os.remove(processed_file)
        logger.info(f"Removed {processed_file}")
    
    # Create empty file
    with open(processed_file, 'w') as f:
        pass
    logger.info("Reset processed emails tracking")
        
def run_diagnostics():
    """Run diagnostics on document processing."""
    print_separator()
    logger.info("RUNNING TEXTRACT DIAGNOSTICS")
    print_separator()
    
    # Ask for a document to test
    import tkinter as tk
    from tkinter import filedialog
    
    root = tk.Tk()
    root.withdraw()
    
    file_path = filedialog.askopenfilename(
        title="Select Document to Test",
        filetypes=[("PDF Files", "*.pdf"), ("Image Files", "*.jpg;*.jpeg;*.png")]
    )
    
    if not file_path:
        logger.info("No file selected, exiting diagnostics")
        return
    
    logger.info(f"Testing document: {file_path}")
    
    # Initialize components
    textract = TextractProcessor()
    
    # Run diagnostics
    doc_type = None
    if "passport" in file_path.lower():
        doc_type = "passport"
    elif "emirates" in file_path.lower() or "eid" in file_path.lower():
        doc_type = "emirates_id"
    elif "visa" in file_path.lower():
        doc_type = "visa"
    
    logger.info(f"Detected document type: {doc_type}")
    result = textract.diagnostic_extract(file_path, doc_type)
    
    logger.info("Diagnostic summary:")
    for key, value in result.items():
        logger.info(f"  {key}: {value}")
    
    print_separator()
    logger.info(f"Diagnostics complete. Check {result.get('diagnostic_dir', 'logs')} for details")
    
    
def force_deduplication(emails, processed_file="processed_emails.json"):
    """Force deduplication by directly checking the file."""
    if not os.path.exists(processed_file):
        return emails
        
    try:
        with open(processed_file, 'r') as f:
            processed = json.load(f)
            processed_ids = set(processed.keys())
            
        original_count = len(emails)
        emails = [e for e in emails if e.get('id') not in processed_ids]
        
        logger.info(f"Force deduplication: Removed {original_count - len(emails)} emails")
        logger.info(f"Remaining emails: {[e.get('subject', 'No Subject') for e in emails]}")
        return emails
    except Exception as e:
        logger.error(f"Error in force deduplication: {str(e)}")
        return emails
    
if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Test complete workflow')
    parser.add_argument('--reset', action='store_true', help='Reset processed emails tracking')
    parser.add_argument('--validate', metavar='DIR', help='Validate output files in directory')
    parser.add_argument('--diagnose', action='store_true', help='Run diagnostics on document processing')
    args = parser.parse_args()
    
    if args.reset:
        reset_processed_emails()
    
    if args.diagnose:
        run_diagnostics()
    elif args.validate:
        run_validation(args.validate)
    else:
        run_test()