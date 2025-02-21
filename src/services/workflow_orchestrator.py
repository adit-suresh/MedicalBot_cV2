import os
import sys
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import shutil
import re
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add project root to Python path if needed
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.append(project_root)

from src.email_handler.outlook_client import OutlookClient
from src.email_handler.attachment_handler import AttachmentHandler
from src.document_processor.textract_processor import TextractProcessor
from src.document_processor.document_classifier import DocumentClassifier
from src.document_processor.data_extractor import EnhancedDataExtractor
from src.document_processor.excel_processor import EnhancedExcelProcessor
from src.email_tracker import EmailTracker

class ImprovedWorkflowOrchestrator:
    """
    Enhanced workflow orchestrator that integrates all improvements.
    """
    
    def __init__(self, template_path: str = "template.xlsx"):
        """
        Initialize the workflow orchestrator.
        
        Args:
            template_path: Path to template Excel file
        """
        # Initialize components
        self.outlook_client = OutlookClient()
        self.attachment_handler = AttachmentHandler()
        self.textract_processor = TextractProcessor()
        self.document_classifier = DocumentClassifier()
        self.data_extractor = EnhancedDataExtractor()
        self.excel_processor = EnhancedExcelProcessor()
        self.email_tracker = EmailTracker()
        
        # Template path
        self.template_path = template_path
        
        # Create necessary directories
        os.makedirs("processed_submissions", exist_ok=True)
        
    def run_workflow(self) -> Dict:
        """
        Run the complete workflow and return results.
        
        Returns:
            Dictionary with workflow results
        """
        try:
            # Step 1: Fetch emails
            logger.info("Fetching emails...")
            emails = self.outlook_client.fetch_emails()
            
            # Filter out already processed emails
            unprocessed_emails = self.email_tracker.filter_unprocessed_emails(emails)
            
            if not unprocessed_emails:
                logger.info("No new emails to process")
                return {
                    "status": "success",
                    "emails_processed": 0,
                    "message": "No new emails to process"
                }
                
            logger.info(f"Found {len(unprocessed_emails)} new emails to process")
            
            # Process each email
            results = []
            for email in unprocessed_emails:
                try:
                    result = self._process_single_email(email)
                    results.append(result)
                    
                    # Mark email as processed
                    if result['status'] == 'success':
                        self.email_tracker.mark_email_processed(email['id'], {
                            'process_id': result.get('process_id'),
                            'submission_dir': result.get('submission_dir'),
                            'timestamp': datetime.now().isoformat()
                        })
                        
                except Exception as e:
                    logger.error(f"Error processing email {email['id']}: {str(e)}")
                    results.append({
                        "status": "error",
                        "email_id": email['id'],
                        "error": str(e)
                    })
            
            return {
                "status": "success",
                "emails_processed": len(unprocessed_emails),
                "successful": len([r for r in results if r['status'] == 'success']),
                "failed": len([r for r in results if r['status'] == 'error']),
                "results": results
            }
            
        except Exception as e:
            logger.error(f"Workflow failed: {str(e)}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    def _process_single_email(self, email: Dict) -> Dict:
        """
        Process a single email with improved document handling.
        
        Args:
            email: Email dictionary from Outlook client
            
        Returns:
            Dictionary with processing results
        """
        email_id = email['id']
        process_id = f"PROC_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        logger.info(f"Processing email {email_id} as {process_id}")
        
        try:
            # Step 2: Get attachments
            attachments = self.outlook_client.get_attachments(email_id)
            if not attachments:
                logger.warning(f"No attachments found in email {email_id}")
                return {
                    "status": "error",
                    "process_id": process_id,
                    "email_id": email_id,
                    "error": "No attachments found"
                }
                
            # Step 3: Save attachments
            saved_files = self.attachment_handler.process_attachments(attachments, email_id)
            if not saved_files:
                logger.warning(f"No valid attachments saved from email {email_id}")
                return {
                    "status": "error",
                    "process_id": process_id,
                    "email_id": email_id,
                    "error": "No valid attachments saved"
                }
                
            # Create submission directory
            submission_dir = os.path.join("processed_submissions", process_id)
            os.makedirs(submission_dir, exist_ok=True)
            
            # Step 4: Classify documents and extract data
            document_data = []
            excel_files = []
            document_paths = {}
            
            for file_path in saved_files:
                file_type = self._get_file_type(file_path)
                
                if file_type == 'excel':
                    excel_files.append(file_path)
                    continue
                    
                if file_type in ['pdf', 'image']:
                    # Process with textract
                    logger.info(f"Processing document {file_path}")
                    ocr_text = self.textract_processor.extract_text(file_path)
                    
                    # Classify document
                    doc_type = self.document_classifier.classify_document(ocr_text, os.path.basename(file_path))
                    logger.info(f"Classified {file_path} as {doc_type}")
                    
                    # Extract data based on document type
                    if doc_type == 'passport':
                        extracted_data = self.data_extractor.extract_passport_data(ocr_text)
                    elif doc_type == 'emirates_id':
                        extracted_data = self.data_extractor.extract_emirates_id_data(ocr_text)
                    elif doc_type == 'visa':
                        extracted_data = self.data_extractor.extract_visa_data(ocr_text)
                    else:
                        logger.warning(f"Unknown document type {doc_type} for {file_path}")
                        continue
                    
                    # Add document type to extracted data
                    extracted_data['_doc_type'] = doc_type
                    document_data.append(extracted_data)
                    
                    # Store document path
                    document_paths[doc_type] = file_path
                    
                    logger.info(f"Extracted {len(extracted_data)} fields from {file_path}")
            
            # Step 5: Process Excel if available
            excel_rows = []
            if excel_files:
                for excel_path in excel_files:
                    logger.info(f"Processing Excel {excel_path}")
                    df, errors = self.excel_processor.process_excel(excel_path, dayfirst=True)
                    
                    if not df.empty:
                        # Process all rows, not just the first one
                        for _, row in df.iterrows():
                            excel_rows.append(row.to_dict())
                            
                    if errors:
                        logger.warning(f"Excel validation errors in {excel_path}: {errors}")
            
            # Step 6: Combine data for each row
            if not excel_rows and not document_data:
                logger.warning(f"No data extracted from email {email_id}")
                return {
                    "status": "error",
                    "process_id": process_id,
                    "email_id": email_id,
                    "error": "No data extracted from attachments"
                }
                
            # If no Excel rows but we have document data, create a single row
            if not excel_rows and document_data:
                excel_rows = [{}]
                
            # Process each Excel row with document data
            output_rows = []
            for excel_row in excel_rows:
                # Consolidate document data
                consolidated_doc_data = self.data_extractor.consolidate_data(document_data)
                
                # Merge Excel and document data (Excel takes precedence for fields it has)
                merged_data = {**consolidated_doc_data, **excel_row}
                
                # Ensure date fields are correctly formatted
                date_fields = ['date_of_birth', 'dob', 'passport_expiry_date', 
                              'emirates_id_expiry', 'visa_expiry_date', 'effective_date']
                              
                for field in date_fields:
                    if field in merged_data and merged_data[field]:
                        try:
                            # Parse date and format as DD-MM-YYYY
                            date_val = merged_data[field]
                            if isinstance(date_val, str) and date_val not in ['.', 'nan', '']:
                                dt = pd.to_datetime(date_val)
                                merged_data[field] = dt.strftime('%d-%m-%Y')
                        except:
                            # Keep original if parse fails
                            pass
                
                # Map fields to template fields if needed
                if 'date_of_birth' in merged_data and 'dob' not in merged_data:
                    merged_data['dob'] = merged_data['date_of_birth']
                    
                if 'passport_number' in merged_data and 'passport_no' not in merged_data:
                    merged_data['passport_no'] = merged_data['passport_number']
                
                output_rows.append(merged_data)
            
            # Step 7: Create final Excel with template
            output_path = os.path.join(
                submission_dir,
                f"final_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            )
            
            result = self.excel_processor.populate_template(
                self.template_path,
                output_path,
                output_rows
            )
            
            if result['status'] != 'success':
                logger.error(f"Error creating output Excel: {result.get('error')}")
                return {
                    "status": "error",
                    "process_id": process_id,
                    "email_id": email_id,
                    "error": f"Error creating output Excel: {result.get('error')}"
                }
                
            # Step 8: Move all files to submission directory
            for doc_type, file_path in document_paths.items():
                new_path = os.path.join(submission_dir, os.path.basename(file_path))
                shutil.copy2(file_path, new_path)
                document_paths[doc_type] = new_path
                
            for excel_path in excel_files:
                new_path = os.path.join(submission_dir, os.path.basename(excel_path))
                shutil.copy2(excel_path, new_path)
            
            logger.info(f"Successfully processed email {email_id}")
            return {
                "status": "success",
                "process_id": process_id,
                "email_id": email_id,
                "submission_dir": submission_dir,
                "output_file": output_path,
                "document_count": len(document_paths),
                "excel_count": len(excel_files),
                "rows_processed": len(output_rows)
            }
            
        except Exception as e:
            logger.error(f"Error processing email {email_id}: {str(e)}")
            return {
                "status": "error",
                "process_id": process_id,
                "email_id": email_id,
                "error": str(e)
            }
    
    def _get_file_type(self, file_path: str) -> str:
        """
        Get file type from file path.
        
        Args:
            file_path: Path to file
            
        Returns:
            File type: 'excel', 'pdf', 'image', or 'other'
        """
        lower_path = file_path.lower()
        if lower_path.endswith(('.xlsx', '.xls')):
            return 'excel'
        elif lower_path.endswith('.pdf'):
            return 'pdf'
        elif lower_path.endswith(('.jpg', '.jpeg', '.png', '.tiff', '.tif', '.bmp')):
            return 'image'
        else:
            return 'other'