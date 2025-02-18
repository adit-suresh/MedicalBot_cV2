# test_complete_workflow.py

import os
import sys
import logging
import pandas as pd
import re
from datetime import datetime
from typing import Dict, Optional, List

# Add project root to Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_root)

from src.email_handler.outlook_client import OutlookClient
from src.email_handler.attachment_handler import AttachmentHandler
from src.document_processor.textract_processor import TextractProcessor
from src.document_processor.excel_processor import ExcelProcessor
from src.services.data_combiner import DataCombiner
from src.utils.process_tracker import ProcessTracker

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
        self.excel_processor = ExcelProcessor()
        self.data_combiner = DataCombiner(self.textract, self.excel_processor)
        self.process_tracker = ProcessTracker()
        
        # Storage for completed submissions
        self.completed_submissions: List[CompletedSubmission] = []
        
        # Create necessary directories
        os.makedirs("processed_submissions", exist_ok=True)

    def run_complete_workflow(self) -> Dict:
        """Run complete workflow from email to final Excel."""
        try:
            # Step 1: Fetch and process new emails
            logger.info("Step 1: Fetching emails...")
            emails = self.outlook.fetch_emails()
            
            results = []
            for email in emails:
                try:
                    result = self._process_single_email(email)
                    results.append(result)
                except Exception as e:
                    logger.error(f"Error processing email {email['id']}: {str(e)}")
                    continue
            
            return {
                "status": "success",
                "emails_processed": len(emails),
                "successful": len([r for r in results if r['status'] == 'success']),
                "failed": len([r for r in results if r['status'] == 'error'])
            }

        except Exception as e:
            logger.error(f"Workflow failed: {str(e)}")
            return {
                "status": "error",
                "error": str(e)
            }

    def _process_single_email(self, email: Dict) -> Dict:
        """Process a single email submission."""
        email_id = email['id']
        process_id = f"PROC_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        try:
            # Step 2: Get and save attachments
            logger.info(f"Processing email {email_id}")
            attachments = self.outlook.get_attachments(email_id)
            saved_files = self.attachment_handler.process_attachments(attachments, email_id)
            
            if not saved_files:
                raise Exception("No valid attachments found")

            # Step 3: Categorize files
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
                data = self.textract.process_document(file_path, doc_type)
                extracted_data.update(data)

            # Step 5: Process Excel
            excel_data = None
            if excel_path:
                logger.info("Processing Excel data...")
                df, errors = self.excel_processor.process_excel(excel_path, dayfirst=True)
                if not df.empty:
                    excel_data = df.iloc[0].to_dict()
                    if errors:
                        logger.warning(f"Excel validation errors: {errors}")

            # Step 6: Combine data
            logger.info("Combining data...")
            submission_dir = os.path.join(
                "processed_submissions",
                process_id
            )
            os.makedirs(submission_dir, exist_ok=True)
            
            output_path = os.path.join(
                submission_dir,
                f"final_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            )

            result = self.data_combiner.combine_and_populate_template(
                "template.xlsx",
                output_path,
                extracted_data,
                excel_data
            )

            # Step 7: Validate output
            if result['status'] == 'success':
                validation_result = self._validate_output_excel(output_path)
                if not validation_result['is_valid']:
                    logger.warning(f"Output validation issues: {validation_result['issues']}")

            # Step 8: Save submission
            submission = CompletedSubmission(
                process_id=process_id,
                documents=document_paths,
                final_excel=output_path
            )
            self.completed_submissions.append(submission)

            # Copy all files to submission directory
            for doc_type, file_path in document_paths.items():
                new_path = os.path.join(submission_dir, os.path.basename(file_path))
                os.rename(file_path, new_path)
                document_paths[doc_type] = new_path

            if excel_path:
                new_path = os.path.join(submission_dir, os.path.basename(excel_path))
                os.rename(excel_path, new_path)

            return {
                "status": "success",
                "process_id": process_id,
                "submission_dir": submission_dir
            }

        except Exception as e:
            logger.error(f"Error processing email {email_id}: {str(e)}")
            return {
                "status": "error",
                "process_id": process_id,
                "error": str(e)
            }

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
                            datetime.strptime(date_val, '%Y-%m-%d')
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
        """Determine file type from filename."""
        name = file_path.lower()
        if name.endswith(('.xlsx', '.xls')):
            return 'excel'
        elif 'passport' in name:
            return 'passport'
        elif 'emirates' in name or 'eid' in name:
            return 'emirates_id'
        elif 'visa' in name:
            return 'visa'
        return 'unknown'

    def get_completed_submissions(self) -> List[CompletedSubmission]:
        """Get list of completed submissions."""
        return self.completed_submissions

if __name__ == "__main__":
    # Run complete workflow test
    tester = WorkflowTester()
    result = tester.run_complete_workflow()
    
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