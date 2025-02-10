from typing import Dict, Optional, List
import logging
from datetime import datetime

from src.utils.process_tracker import ProcessTracker, ProcessStatus
from src.utils.service_monitor import ServiceMonitor
from src.document_processor.textract_processor import TextractProcessor
from src.email_handler.outlook_client import OutlookClient
from src.email_handler.attachment_handler import AttachmentHandler
from src.utils.dependency_container import inject
from src.services.process_manager import ProcessManager

logger = logging.getLogger(__name__)

@inject(ProcessManager, ServiceMonitor, OutlookClient, AttachmentHandler, TextractProcessor)
class MainHandler:
    """Main process handler coordinating all services."""

    def process_email(self, email_id: str) -> Dict:
        """
        Process a single email through the entire workflow.
        
        Args:
            email_id: Email identifier
            
        Returns:
            Dict containing process results and status
        """
        try:
            # Check required services
            if not self._check_required_services():
                return {
                    "status": "error",
                    "message": "Required services unavailable"
                }

            # Start process tracking
            process_id = f"PROC_{email_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Delegate to process manager
            self._process_manager.handle_process(process_id)
            
            # Get email attachments
            attachments = self._outlook_client.get_attachments(email_id)
            
            # Download and save attachments
            saved_files = self._attachment_handler.process_attachments(
                attachments, 
                email_id
            )

            # Process each document with Vision API
            extracted_data = {}
            for file_path in saved_files:
                try:
                    doc_data = self._vision_processor.process_document(file_path)
                    extracted_data.update(doc_data)
                except Exception as e:
                    logger.error(f"Error processing document {file_path}: {str(e)}")
                    continue

            # Validate results
            if not self._validate_extracted_data(extracted_data):
                return {
                    "status": "incomplete",
                    "process_id": process_id,
                    "missing_fields": self._get_missing_fields(extracted_data)
                }

            return {
                "status": "success",
                "process_id": process_id,
                "extracted_data": extracted_data
            }

        except Exception as e:
            logger.error(f"Error processing email {email_id}: {str(e)}")
            return {
                "status": "error",
                "message": str(e)
            }

    def _check_required_services(self) -> bool:
        """Check if all required services are available."""
        required_services = [
            "email_service",
            "vision_api",
            "document_processor"
        ]
        
        for service in required_services:
            if not self._service_monitor.is_service_available(service):
                logger.error(f"Required service unavailable: {service}")
                return False
        
        return True

    def _validate_extracted_data(self, data: Dict) -> bool:
        """Validate extracted data for completeness."""
        required_fields = {
            'passport_number',
            'full_name',
            'nationality',
            'date_of_birth',
            'emirates_id'
        }
        
        return all(field in data and data[field] for field in required_fields)

    def _get_missing_fields(self, data: Dict) -> List[str]:
        """Get list of missing required fields."""
        required_fields = {
            'passport_number',
            'full_name',
            'nationality',
            'date_of_birth',
            'emirates_id'
        }
        
        return [
            field for field in required_fields 
            if field not in data or not data[field]
        ]

    def retry_failed_process(self, process_id: str) -> Dict:
        """Retry a failed process."""
        try:
            self._process_manager.handle_process(process_id)
            return {
                "status": "success",
                "message": "Process retry initiated"
            }
        except Exception as e:
            logger.error(f"Error retrying process {process_id}: {str(e)}")
            return {
                "status": "error",
                "message": str(e)
            }

    def get_process_status(self, process_id: str) -> Dict:
        """Get current status of a process."""
        return self._process_manager.get_process_status(process_id)