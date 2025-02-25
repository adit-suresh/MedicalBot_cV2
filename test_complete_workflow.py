#!/usr/bin/env python3
# test_complete_workflow.py

import os
import sys
import logging
import pandas as pd
import re
from datetime import datetime
from typing import Dict, Optional, List
import argparse
import shutil

# Add project root to Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_root)

# Import original components
from src.email_handler.outlook_client import OutlookClient
from src.email_handler.attachment_handler import AttachmentHandler
from src.document_processor.textract_processor import TextractProcessor

# Import original workflow components
from src.utils.process_tracker import ProcessTracker
from src.services.data_combiner import DataCombiner
from src.document_processor.excel_processor import EnhancedExcelProcessor as ExcelProcessor

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
            # Initialize email tracker
            from src.email_tracker.email_tracker import EmailTracker
            email_tracker = EmailTracker()
            
            # Step 1: Fetch and process new emails
            logger.info("Step 1: Fetching emails...")
            emails = self.outlook.fetch_emails()
            
            from src.email_tracker.email_tracker import EmailTracker
            email_tracker = EmailTracker()
            
            results = []
            for email in emails:
                try:
                    # Check if email was already processed
                    if email_tracker.is_processed(email['id']):
                        logger.info(f"Email {email['id']} already processed, skipping.")
                        continue
                        
                    result = self._process_single_email(email)
                    results.append(result)
                    
                    # If processing was successful, mark as processed with metadata
                    if result['status'] == 'success':
                        metadata = {
                            'process_id': result.get('process_id'),
                            'submission_dir': result.get('submission_dir'),
                            'processed_at': datetime.now().isoformat(),
                            'subject': email.get('subject', 'No Subject'),
                            'received_time': email.get('receivedDateTime', ''),
                            'has_attachments': email.get('hasAttachments', False),
                            'documents_processed': list(result.get('documents', {}).keys())
                        }
                        email_tracker.mark_processed(email['id'], metadata)
                        
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
        """Process a single email submission with improved document tracking."""
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
            attachments = self.outlook.get_attachments(email_id)
            saved_files = self.attachment_handler.process_attachments(attachments, email_id)
            
            if not saved_files:
                raise Exception("No valid attachments found")

            # Step 3: Categorize and process all files
            document_paths = {}
            excel_files = []
            processed_docs = []

            # First pass: identify Excel files and documents
            for file_path in saved_files:
                file_type = self._determine_file_type(file_path)
                if file_type == 'excel':
                    excel_files.append(file_path)
                else:
                    doc_path = os.path.join(submission_dir, os.path.basename(file_path))
                    shutil.copy2(file_path, doc_path)  # Use copy2 to preserve metadata
                    document_paths[file_type] = doc_path
                    processed_docs.append({
                        'type': file_type,
                        'original_name': os.path.basename(file_path),
                        'path': doc_path
                    })

            # Step 4: Process all documents
            logger.info(f"Processing {len(document_paths)} documents")
            extracted_data = {}
            for doc_type, file_path in document_paths.items():
                try:
                    data = self.textract.process_document(file_path, doc_type)
                    logger.info(f"Extracted data from {doc_type}: {list(data.keys())}")
                    extracted_data.update(data)
                except Exception as e:
                    logger.error(f"Error processing {doc_type} document: {str(e)}")

            # Step 5: Process all Excel files
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
                    logger.error(f"Error processing Excel file {excel_path}: {str(e)}")

            # Step 6: Combine data
            logger.info("Combining data...")
            output_path = os.path.join(
                submission_dir,
                f"final_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            )

            # Use all Excel rows with document data
            result = self.data_combiner.combine_and_populate_template(
                "template.xlsx",
                output_path,
                extracted_data,
                all_excel_rows if all_excel_rows else None
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
            for excel_path in excel_files:
                new_path = os.path.join(submission_dir, os.path.basename(excel_path))
                shutil.copy2(excel_path, new_path)  # Use copy2 instead of rename

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

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Test complete workflow')
    parser.add_argument('--validate', metavar='DIR', help='Validate output files in directory')
    args = parser.parse_args()
    
    if args.validate:
        run_validation(args.validate)
    else:
        run_test()