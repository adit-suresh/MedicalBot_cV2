import os
import sys
import logging
from datetime import datetime
import re
from typing import Dict, Optional

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.email_handler.outlook_client import OutlookClient
from src.email_handler.attachment_handler import AttachmentHandler
from src.document_processor.textract_processor import TextractProcessor
from src.document_processor.excel_processor import ExcelProcessor
from src.services.data_combiner import DataCombiner
from src.utils.process_tracker import ProcessTracker
from src.services.data_integrator import DataIntegrator

logger = logging.getLogger(__name__)

class WorkflowRunner:
    """Runs the complete workflow from email fetching to final Excel generation."""

    def __init__(self):
        self.outlook_client = OutlookClient()
        self.attachment_handler = AttachmentHandler()
        self.textract_processor = TextractProcessor()
        self.excel_processor = ExcelProcessor()
        self.data_combiner = DataCombiner(self.textract_processor, self.excel_processor)
        self.process_tracker = ProcessTracker()

    def run_workflow(self, process_id: Optional[str] = None) -> Dict:
        """
        Run complete workflow.
        
        Args:
            process_id: Optional process identifier
            
        Returns:
            Dict containing process results
        """
        try:
            if not process_id:
                process_id = f"PROC_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

            logger.info(f"Starting workflow process: {process_id}")
            
            # Step 1: Fetch new emails
            logger.info("Fetching emails...")
            emails = self.outlook_client.fetch_emails()
            
            for email in emails:
                try:
                    email_id = email['id']
                    logger.info(f"Processing email {email_id}")
                    
                    # Step 2: Get attachments
                    attachments = self.outlook_client.get_attachments(email_id)
                    
                    # Step 3: Save attachments
                    saved_files = self.attachment_handler.process_attachments(
                        attachments,
                        email_id
                    )
                    
                    if not saved_files:
                        logger.warning(f"No valid attachments in email {email_id}")
                        continue

                    # Categorize files
                    document_paths = {}
                    excel_path = None
                    
                    for file_path in saved_files:
                        file_type = self._determine_file_type(file_path)
                        if file_type == 'excel':
                            excel_path = file_path
                        else:
                            document_paths[file_type] = file_path

                    # Step 4: Process documents
                    logger.info("Processing documents...")
                    extracted_data = {}
                    for doc_type, file_path in document_paths.items():
                        try:
                            data = self.textract_processor.process_document(file_path, doc_type)
                            extracted_data.update(data)
                        except Exception as e:
                            logger.error(f"Error processing {doc_type}: {str(e)}")

                    # Step 5: Process Excel if available
                    excel_data = None
                    if excel_path:
                        logger.info("Processing Excel data...")
                        df, errors = self.excel_processor.process_excel(excel_path, dayfirst=True)
                        if not df.empty:
                            excel_data = df.iloc[0].to_dict()

                    # Step 6: Combine data
                    logger.info("Combining data...")
                    output_dir = os.path.join('data', 'processed', email_id)
                    os.makedirs(output_dir, exist_ok=True)
                    
                    output_path = os.path.join(
                        output_dir,
                        f"combined_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                    )

                    result = self.data_combiner.combine_and_populate_template(
                        "template.xlsx",  # Your template path
                        output_path,
                        extracted_data,
                        excel_data
                    )

                    # Step 7: Save to database and upload files
                    submission_id = self._save_submission(
                        email_id=email_id,
                        document_paths=document_paths,
                        excel_path=excel_path,
                        output_path=output_path
                    )

                    logger.info(f"Successfully processed email {email_id}")
                    logger.info(f"Submission ID: {submission_id}")

                except Exception as e:
                    logger.error(f"Error processing email {email_id}: {str(e)}")
                    continue

            return {
                "status": "success",
                "process_id": process_id,
                "emails_processed": len(emails)
            }

        except Exception as e:
            logger.error(f"Workflow failed: {str(e)}")
            return {
                "status": "error",
                "process_id": process_id,
                "error": str(e)
            }

    def _determine_file_type(self, file_path: str, content_text: Optional[str] = None) -> str:
        """Determine file type from both filename and content when available."""
        name = file_path.lower()
        
        # First check file extension
        if name.endswith(('.xlsx', '.xls')):
            return 'excel'
            
        # If we have OCR text, use it for better classification
        if content_text:
            content_lower = content_text.lower()
            
            # Passport indicators
            if any(term in content_lower for term in ['passport no', 'surname', 'given names', 'nationality', 'date of birth', 'place of issue']):
                return 'passport'
                
            # Emirates ID indicators
            if any(term in content_lower for term in ['emirates id', 'id number', 'united arab emirates', 'id card']) or re.search(r'\d{3}-\d{4}-\d{7}-\d{1}', content_text):
                return 'emirates_id'
                
            # Visa indicators
            if any(term in content_lower for term in ['visa', 'permit no', 'entry permit', 'sponsor']):
                return 'visa'
        
        # Fallback to filename-based classification
        if 'passport' in name:
            return 'passport'
        elif 'emirates' in name or 'eid' in name or 'id card' in name:
            return 'emirates_id'
        elif 'visa' in name or 'permit' in name or 'residence' in name:
            return 'visa'
            
        return 'unknown'
    
    def run(self):
        # Wrap the sequence of operations
        from src.email_handler.outlook_client import OutlookClient
        from src.document_processor.ocr_processor import OCRProcessor
        from src.document_processor.excel_processor import ExcelProcessor
        from src.services.data_integrator import DataIntegrator

        # Fetch emails
        outlook = OutlookClient()
        emails = outlook.fetch_emails()

        # Process documents (placeholders for actual processing)
        textract_processor = TextractProcessor()
        excel_processor = ExcelProcessor()
        # You might iterate over emails and process attachments here

        # For demonstration, assume we get some data:
        excel_data = {}  # Replace with actual call to process an Excel file
        ocr_data = {}    # Replace with actual OCR processing call

        integrator = DataIntegrator(textract_processor, excel_processor)
        final_data = integrator.combine_data(excel_data, ocr_data)

        # Save final report (e.g., final_report.xlsx)
        # Your code to generate and save the Excel file goes here
        logging.info("Workflow executed and final report generated.")

    def _save_submission(self, email_id: str, document_paths: Dict, 
                        excel_path: Optional[str], output_path: str) -> str:
        """Save submission details to database."""
        # Implement database saving logic here
        submission_id = f"SUB_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        return submission_id

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run workflow
    runner = WorkflowRunner()
    result = runner.run_workflow()
    
    # Print results
    if result['status'] == 'success':
        logger.info(f"Successfully processed {result['emails_processed']} emails")
    else:
        logger.error(f"Workflow failed: {result['error']}")