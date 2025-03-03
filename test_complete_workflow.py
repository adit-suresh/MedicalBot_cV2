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

# Import original workflow components
from src.utils.process_tracker import ProcessTracker
from src.services.data_combiner import DataCombiner
from src.document_processor.excel_processor import EnhancedExcelProcessor as ExcelProcessor

def get_processed_emails():
    """Get list of processed email IDs and subjects."""
    if os.path.exists("processed_emails_manual.txt"):
        try:
            with open("processed_emails_manual.txt", "r") as f:
                lines = [line.strip() for line in f.readlines()]
                logger.info(f"Loaded {len(lines)} processed emails from tracking file")
                return lines
        except Exception as e:
            logger.error(f"Error reading processed_emails_manual.txt: {str(e)}")
            return []
    else:
        logger.info("No processed emails tracking file found, creating new one")
        # Create empty file
        with open("processed_emails_manual.txt", "w") as f:
            pass
        return []

def add_processed_email(email_id, subject):
    """Add email ID and subject to processed list."""
    try:
        identifier = f"{email_id}:{subject}"
        with open("processed_emails_manual.txt", "a") as f:
            f.write(f"{identifier}\n")
        logger.info(f"Added to processed emails: {identifier}")
    except Exception as e:
        logger.error(f"Error adding email to processed list: {str(e)}")

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
        try:
            self.deepseek = DeepseekProcessor()
            logger.info("DeepSeek processor initialized")
        except Exception as e:
            logger.warning(f"Failed to initialize DeepSeek: {str(e)}")
            self.deepseek = None
            
        self.document_processor = EnhancedDocumentProcessorService(self.textract, self.deepseek)
        self.file_sharer = FileSharer()
        
        self.excel_processor = ExcelProcessor()
        self.data_combiner = DataCombiner(self.textract, self.excel_processor, self.deepseek)
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
        
        # Map of company names to template files
        company_templates = {
            'nas': 'templates/nas.xlsx',
            'al madallah': 'templates/al_madallah.xlsx',
            'almadallah': 'templates/al_madallah.xlsx',
            'union': 'templates/union.xlsx',
            'al sagar': 'templates/al_sagar.xlsx',
            'alsagar': 'templates/al_sagar.xlsx',
            'dic': 'templates/dic.xlsx',
            'dni': 'templates/dni.xlsx',
            'ngi': 'templates/ngi.xlsx',
            'qic': 'templates/qic.xlsx',
            'orient': 'templates/orient.xlsx',
            'takaful': 'templates/takaful.xlsx'
        }
        
        # Look for company names in the subject
        for company, template in company_templates.items():
            if company in subject_lower:
                logger.info(f"Selected template for {company}: {template}")
                return template
        
        # Default to Nas template if no match found
        logger.warning(f"No company match found in subject '{subject}', defaulting to Nas template")
        return 'templates/nas.xlsx'
    
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
           
    def run_complete_workflow(self, bypass_dedup=False) -> Dict:
        """Run complete workflow from email to final Excel."""
        try:
            # Step 1: Fetch and process new emails
            logger.info("Step 1: Fetching emails...")
            emails = self.outlook.fetch_emails()
            logger.info(f"Fetched {len(emails)} emails before filtering")
            
            # Initialize email tracker
            from src.email_tracker.email_tracker import EmailTracker
            email_tracker = EmailTracker()
            
            # Log each email we're going to process
            for idx, email in enumerate(emails, 1):
                email_id = email['id']
                subject = email.get('subject', '').strip()
                is_processed = email_tracker.is_processed(email_id)
                logger.info(f"Email {idx}: ID={email_id}, Subject={subject}")
                
                if not bypass_dedup and is_processed:
                    logger.info(f"  - Status: Will be skipped (already processed)")
                else:
                    logger.info(f"  - Status: Will be processed")
            
            results = []
            successful = 0
            skipped = 0
            failed = 0
            
            for email in emails:
                try:
                    email_id = email['id']
                    subject = email.get('subject', '').strip()
                    
                    # Skip if already processed (unless bypassing)
                    if not bypass_dedup and email_tracker.is_processed(email_id):
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
                                metadata = {
                                    'process_id': result.get('process_id'),
                                    'submission_dir': result.get('submission_dir'),
                                    'processed_at': datetime.now().isoformat(),
                                    'subject': subject,
                                    'received_time': email.get('receivedDateTime', ''),
                                    'has_attachments': email.get('hasAttachments', False),
                                    'documents_processed': list(result.get('documents', {}).keys())
                                }
                                email_tracker.mark_processed(email_id, metadata)
                        else:
                            logger.error(f"Failed to process email: {subject}, Error: {result.get('error', 'Unknown error')}")
                            failed += 1
                            
                    except Exception as e:
                        logger.error(f"Uncaught exception processing email {subject}: {str(e)}", exc_info=True)
                        failed += 1
                        
                except Exception as e:
                    logger.error(f"Error accessing email information: {str(e)}", exc_info=True)
                    failed += 1
                    continue
            
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

    def _process_single_email(self, email: Dict) -> Dict:
        """Process a single email submission with improved error handling."""
        email_id = email['id']
        # Clean subject for folder name
        subject = re.sub(r'[<>:"/\\|?*]', '', email.get('subject', 'No Subject'))
        process_id = f"{subject}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        try:
            # Create submission directory
            submission_dir = os.path.join("processed_submissions", process_id)
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

                # First pass: identify Excel files and documents
                for file_path in saved_files:
                    file_type = self._determine_file_type(file_path)
                    logger.debug(f"Determined file type for {file_path}: {file_type}")
                    
                    if file_type == 'excel':
                        excel_files.append(file_path)
                    else:
                        doc_path = os.path.join(submission_dir, os.path.basename(file_path))
                        shutil.copy2(file_path, doc_path)
                        document_paths[file_type] = doc_path
                        processed_docs.append({
                            'type': file_type,
                            'original_name': os.path.basename(file_path),
                            'path': doc_path
                        })
                
                logger.info(f"Categorized files: {len(excel_files)} Excel files, {len(document_paths)} documents")
            except Exception as e:
                logger.error(f"Error categorizing files: {str(e)}", exc_info=True)
                raise Exception(f"Failed to categorize files: {str(e)}")

            # Step 4: Process all documents
            try:
                logger.info(f"Processing {len(document_paths)} documents")
                extracted_data = {}
                for doc_type, file_path in document_paths.items():
                    try:
                        data = self.textract.process_document(file_path, doc_type)
                        logger.info(f"Extracted data from {doc_type}: {list(data.keys())}")
                        extracted_data.update(data)
                    except Exception as e:
                        logger.error(f"Error processing {doc_type} document: {str(e)}", exc_info=True)
                        # Continue with other documents instead of failing
                        logger.warning(f"Continuing with partial data due to document processing error")
            except Exception as e:
                logger.error(f"Error processing documents: {str(e)}", exc_info=True)
                raise Exception(f"Failed to process documents: {str(e)}")

            # Step 5: Process all Excel files
            try:
                logger.info(f"Processing {len(excel_files)} Excel files")
                all_excel_rows = []
                for excel_path in excel_files:
                    try:
                        df, errors = self.excel_processor.process_excel(excel_path, dayfirst=True)
                        if not df.empty:
                            # Process all rows
                            for _, row in df.iterrows():
                                all_excel_rows.append(row.to_dict())
                            logger.info(f"Processed Excel file with {len(df)} rows")
                        if errors:
                            logger.warning(f"Excel validation errors: {errors}")
                    except Exception as e:
                        logger.error(f"Error processing Excel file {excel_path}: {str(e)}", exc_info=True)
                        # Continue with other Excel files instead of failing
                        logger.warning(f"Continuing with partial data due to Excel processing error")
                
                logger.info(f"Extracted {len(all_excel_rows)} rows from Excel files")
            except Exception as e:
                logger.error(f"Error processing Excel files: {str(e)}", exc_info=True)
                raise Exception(f"Failed to process Excel files: {str(e)}")

            # Rename client files based on Excel data
            if all_excel_rows:
                try:
                    document_paths = self._rename_client_files(document_paths, all_excel_rows)
                except Exception as e:
                    logger.error(f"Error renaming client files: {str(e)}", exc_info=True)
                    # Continue without failing the whole process
                    logger.warning("Continuing with original filenames")

            # Step 6: Select template based on company in subject
            template_path = self._select_template_for_company(subject)
                

            # Step 7: Combine data
            try:
                logger.info(f"Combining data using template: {template_path}")
                output_path = os.path.join(
                    submission_dir,
                    f"final_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                )

                # Use selected template instead of hardcoded "template.xlsx"
                result = self.data_combiner.combine_and_populate_template(
                    template_path,
                    output_path,
                    extracted_data,
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
                    
                    # Send email with attachment
                    email_sent = self.email_sender.send_email(
                        subject=f"Medical Bot: {subject} - Submission Complete",
                        body=f"New submission processed: {subject}\n\nPlease find the attached ZIP file containing all processed documents.",
                        attachment_path=zip_path
                    )
                    
                    if email_sent:
                        logger.info(f"Email sent with submission ZIP: {zip_path}")
                    else:
                        logger.error("Failed to send email with submission")
                        
                except Exception as e:
                    logger.error(f"Error creating or sending submission: {str(e)}", exc_info=True)
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
        """Rename client files based on Staff ID from Excel or name if ID not available.
        
        Args:
            document_paths: Dictionary mapping document types to file paths
            excel_data: List of dictionaries with Excel data
            
        Returns:
            Updated dictionary with new file paths
        """
        # Try to get staff ID from Excel data first
        staff_id = None
        
        if excel_data and len(excel_data) > 0:
            # Get staff_id and make sure it's valid
            if 'staff_id' in excel_data[0] and excel_data[0]['staff_id'] not in ['', '.', 'nan', None]:
                staff_id = str(excel_data[0]['staff_id']).strip()
                logger.info(f"Using Staff ID from Excel: {staff_id}")
        
        # If no staff ID, try to use Emirates ID as identifier
        emirates_id = None
        if not staff_id and excel_data and len(excel_data) > 0:
            if 'emirates_id' in excel_data[0] and excel_data[0]['emirates_id'] not in ['', '.', 'nan', None]:
                # Use just the final 7 digits of Emirates ID
                eid = str(excel_data[0]['emirates_id']).strip()
                digits = ''.join(filter(str.isdigit, eid))
                if len(digits) >= 7:
                    emirates_id = digits[-7:]
                    logger.info(f"Using last 7 digits of Emirates ID: {emirates_id}")
        
        # If no staff ID or Emirates ID, try to create a name-based reference
        name_reference = None
        if not staff_id and not emirates_id and excel_data and len(excel_data) > 0:
            first_name = excel_data[0].get('first_name', '')
            last_name = excel_data[0].get('last_name', '')
            
            if first_name and first_name not in ['', '.', 'nan', None]:
                first_name = str(first_name).strip()
                if last_name and last_name not in ['', '.', 'nan', None]:
                    last_name = str(last_name).strip()
                    name_reference = f"{first_name}_{last_name}"
                else:
                    name_reference = first_name
                    
                logger.info(f"Using name as reference: {name_reference}")
        
        # If we have nothing, use a timestamp
        if not staff_id and not emirates_id and not name_reference:
            import time
            random_id = f"UNKNOWN_{int(time.time())}"
            logger.warning(f"No ID or name found, using random ID: {random_id}")
            file_reference = random_id
        else:
            # Determine the reference to use (priority: staff_id > emirates_id > name)
            file_reference = staff_id or emirates_id or name_reference
            
        # Sanitize the file reference to ensure it's safe for filenames
        import re
        file_reference = re.sub(r'[<>:"/\\|?*]', '_', str(file_reference))
        
        # Create a copy of the paths dictionary to update
        updated_paths = {}
        
        # Process each document
        for doc_type, file_path in document_paths.items():
            try:
                # Skip if file doesn't exist
                if not os.path.exists(file_path):
                    logger.warning(f"File not found: {file_path}")
                    updated_paths[doc_type] = file_path
                    continue
                    
                # Determine new filename based on document type
                file_dir = os.path.dirname(file_path)
                file_ext = os.path.splitext(file_path)[1]
                
                if 'passport' in doc_type.lower():
                    new_name = f"{file_reference}_PASSPORT{file_ext}"
                elif 'emirates' in doc_type.lower() or 'eid' in doc_type.lower():
                    new_name = f"{file_reference}_EMIRATES_ID{file_ext}"
                elif 'visa' in doc_type.lower() or 'permit' in doc_type.lower():
                    new_name = f"{file_reference}_VISA{file_ext}"
                else:
                    # Keep original name for other document types
                    updated_paths[doc_type] = file_path
                    continue
                    
                # Create the new path
                new_path = os.path.join(file_dir, new_name)
                
                # If the new path already exists but is different from current path, create a unique name
                if os.path.exists(new_path) and os.path.abspath(new_path) != os.path.abspath(file_path):
                    logger.warning(f"File {new_name} already exists, creating a unique name")
                    import uuid
                    unique_id = str(uuid.uuid4())[:8]
                    new_name = f"{file_reference}_{unique_id}_{doc_type.upper()}{file_ext}"
                    new_path = os.path.join(file_dir, new_name)
                    
                # Rename the file
                os.rename(file_path, new_path)
                logger.info(f"Renamed file: {os.path.basename(file_path)} -> {new_name}")
                
                # Update the path in the dictionary
                updated_paths[doc_type] = new_path
                
            except Exception as e:
                logger.error(f"Error renaming file {file_path}: {str(e)}")
                updated_paths[doc_type] = file_path
                
        return updated_paths
    
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

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s'
)
logger = logging.getLogger(__name__)

def print_separator():
    """Print a separator line for better readability."""
    logger.info("=" * 80)

def run_test():
    """Run the workflow test."""
    print_separator()
    logger.info("STARTING WORKFLOW TEST")
    print_separator()
    
    # Initialize workflow tester
    tester = WorkflowTester()
    
    # Run workflow
    result = tester.run_complete_workflow()
    
    # Print results
    print_separator()
    logger.info("WORKFLOW TEST COMPLETED")
    
    if result['status'] == 'success':
        logger.info(f"Successfully processed {result['successful']} out of {result['emails_processed']} emails")
        
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
    """Reset the processed emails tracking file."""
    if os.path.exists("processed_emails.json"):
        os.remove("processed_emails.json")
        logger.info("Removed processed_emails.json file")

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Test complete workflow')
    parser.add_argument('--reset', action='store_true', help='Reset processed emails tracking')
    parser.add_argument('--validate', metavar='DIR', help='Validate output files in directory')
    args = parser.parse_args()
    
    if args.reset:
        reset_processed_emails()
    
    if args.validate:
        run_validation(args.validate)
    else:
        run_test()