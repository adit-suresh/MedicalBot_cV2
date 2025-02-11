import logging
from typing import Dict, List, Optional, Tuple
import os
from datetime import datetime

from src.document_processor.textract_processor import TextractProcessor
from src.document_processor.excel_processor import ExcelProcessor
from src.services.data_integrator import DataIntegrator
from src.email_handler.attachment_handler import AttachmentHandler
from src.utils.error_handling import ServiceError, handle_errors, ErrorCategory, ErrorSeverity

logger = logging.getLogger(__name__)

class WorkflowOrchestrator:
    """Orchestrates the entire document processing workflow."""

    def __init__(self):
        """Initialize required services."""
        self.textract_processor = TextractProcessor()
        self.excel_processor = ExcelProcessor()
        self.data_integrator = DataIntegrator(
            self.textract_processor,
            self.excel_processor
        )
        self.attachment_handler = AttachmentHandler()

    @handle_errors(ErrorCategory.PROCESS, ErrorSeverity.HIGH)
    def process_email_submission(self, 
                               email_id: str,
                               attachments: List[Dict],
                               output_dir: str) -> Dict:
        """
        Process complete email submission workflow.
        
        Args:
            email_id: Email identifier
            attachments: List of attachment metadata
            output_dir: Directory for output files
            
        Returns:
            Dict containing process results and status
        """
        try:
            # Create process directory
            process_dir = self._create_process_directory(output_dir, email_id)
            
            # Download and save attachments
            saved_files = self.attachment_handler.process_attachments(
                attachments,
                email_id
            )
            
            # Categorize files
            document_paths = {}
            excel_path = None
            
            for file_path in saved_files:
                file_type = self._determine_file_type(file_path)
                if file_type == 'excel':
                    excel_path = file_path
                else:
                    document_paths[file_type] = file_path

            # Check for missing documents
            missing_docs = self.data_integrator.get_missing_documents(document_paths)
            if missing_docs:
                return {
                    'status': 'incomplete',
                    'missing_documents': missing_docs,
                    'process_id': email_id
                }

            # Process and combine data
            combined_df, errors = self.data_integrator.process_documents(
                document_paths,
                excel_path
            )

            # Create output file
            output_path = os.path.join(
                process_dir,
                f"processed_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            )
            self.data_integrator.create_output_excel(combined_df, output_path)

            return {
                'status': 'success' if not errors else 'completed_with_errors',
                'process_id': email_id,
                'output_file': output_path,
                'errors': errors
            }

        except Exception as e:
            logger.error(f"Workflow failed for email {email_id}: {str(e)}")
            return {
                'status': 'failed',
                'process_id': email_id,
                'error': str(e)
            }

    def retry_failed_process(self, 
                           process_id: str,
                           document_paths: Dict[str, str],
                           excel_path: Optional[str] = None,
                           output_dir: str = None) -> Dict:
        """
        Retry a failed process.
        
        Args:
            process_id: Process identifier
            document_paths: Dict of document paths
            excel_path: Optional Excel file path
            output_dir: Output directory
            
        Returns:
            Dict containing retry results
        """
        try:
            if output_dir is None:
                output_dir = os.path.join('data', 'processed', process_id)

            # Process and combine data
            combined_df, errors = self.data_integrator.process_documents(
                document_paths,
                excel_path
            )

            # Create output file
            output_path = os.path.join(
                output_dir,
                f"retry_processed_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            )
            self.data_integrator.create_output_excel(combined_df, output_path)

            return {
                'status': 'success' if not errors else 'completed_with_errors',
                'process_id': process_id,
                'output_file': output_path,
                'errors': errors
            }

        except Exception as e:
            logger.error(f"Retry failed for process {process_id}: {str(e)}")
            return {
                'status': 'failed',
                'process_id': process_id,
                'error': str(e)
            }

    def _create_process_directory(self, base_dir: str, process_id: str) -> str:
        """Create and return process-specific directory."""
        process_dir = os.path.join(base_dir, process_id)
        os.makedirs(process_dir, exist_ok=True)
        return process_dir

    def _determine_file_type(self, file_path: str) -> str:
        """Determine file type from extension and content."""
        ext = os.path.splitext(file_path)[1].lower()
        
        if ext in ['.xlsx', '.xls']:
            return 'excel'
        
        # Try to determine document type from filename
        name_lower = file_path.lower()
        if 'passport' in name_lower:
            return 'passport'
        elif 'emirates' in name_lower or 'eid' in name_lower:
            return 'emirates_id'
        elif 'visa' in name_lower:
            return 'visa'
        
        return 'unknown'

    def validate_process_requirements(self, attachments: List[Dict]) -> List[str]:
        """
        Validate if all required attachments are present.
        
        Args:
            attachments: List of attachment metadata
            
        Returns:
            List of missing required documents
        """
        required_docs = {'passport', 'emirates_id'}
        found_docs = set()
        
        for attachment in attachments:
            doc_type = self._determine_file_type(attachment['name'])
            if doc_type in required_docs:
                found_docs.add(doc_type)
        
        return list(required_docs - found_docs)