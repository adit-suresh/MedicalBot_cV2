# src/services/main_handler.py
from typing import Dict, Optional, List
import logging
from datetime import datetime

from src.utils.process_tracker import ProcessTracker, ProcessStatus
from src.utils.service_monitor import ServiceMonitor
from src.document_processor.vision_processor import VisionProcessor
from src.email_handler.outlook_client import OutlookClient
from src.email_handler.attachment_handler import AttachmentHandler

logger = logging.getLogger(__name__)

class ProcessHandler:
    def __init__(self):
        self.process_tracker = ProcessTracker()
        self.service_monitor = ServiceMonitor()
        self.vision_processor = VisionProcessor()
        self.outlook_client = OutlookClient()
        self.attachment_handler = AttachmentHandler()

    def process_email(self, email_id: str) -> Dict:
        """
        Process a single email through the entire workflow.
        
        Args:
            email_id: Email identifier
            
        Returns:
            Dict containing process results and status
        """
        # Start tracking this process
        process_id = self.process_tracker.start_process(email_id)
        
        try:
            # Check required services
            if not self._check_required_services():
                self.process_tracker.update_status(
                    process_id, 
                    ProcessStatus.ERROR,
                    {"error": "Required services unavailable"}
                )
                return {"status": "error", "message": "Required services unavailable"}

            # Get email attachments
            self.process_tracker.update_status(
                process_id, 
                ProcessStatus.EMAIL_RECEIVED
            )
            
            attachments = self.outlook_client.get_attachments(email_id)
            
            # Download and save attachments
            saved_files = self.attachment_handler.process_attachments(
                attachments, 
                email_id
            )
            
            self.process_tracker.update_status(
                process_id,
                ProcessStatus.DOCUMENTS_DOWNLOADED,
                {"files": saved_files}
            )

            # Process each document with OCR
            extracted_data = {}
            for file_path in saved_files:
                try:
                    doc_data = self.vision_processor.process_document(file_path)
                    extracted_data.update(doc_data)
                except Exception as e:
                    logger.error(f"Error processing document {file_path}: {str(e)}")
                    self.process_tracker.log_error(
                        process_id,
                        f"Document processing failed: {file_path}",
                        {"error": str(e)}
                    )

            self.process_tracker.update_status(
                process_id,
                ProcessStatus.OCR_COMPLETED,
                {"extracted_data": extracted_data}
            )

            # Validate extracted data
            missing_fields = self._validate_required_fields(extracted_data)
            if missing_fields:
                self.process_tracker.update_status(
                    process_id,
                    ProcessStatus.ERROR,
                    {"missing_fields": missing_fields}
                )
                return {
                    "status": "incomplete",
                    "missing_fields": missing_fields
                }

            self.process_tracker.update_status(
                process_id,
                ProcessStatus.DATA_VALIDATED
            )

            return {
                "status": "success",
                "process_id": process_id,
                "extracted_data": extracted_data
            }

        except Exception as e:
            logger.error(f"Error processing email {email_id}: {str(e)}")
            self.process_tracker.log_error(
                process_id,
                "Process failed",
                {"error": str(e)}
            )
            return {"status": "error", "message": str(e)}

    def _check_required_services(self) -> bool:
        """Check if all required services are available."""
        required_services = ["nas_portal", "google_vision"]
        
        for service in required_services:
            if not self.service_monitor.is_service_available(service):
                logger.error(f"Required service unavailable: {service}")
                return False
        
        return True

    def _validate_required_fields(self, data: Dict) -> List[str]:
        """Check for required fields in extracted data."""
        required_fields = [
            'full_name',
            'passport_number',
            'emirates_id',
            'nationality',
            'date_of_birth'
        ]
        
        missing = []
        for field in required_fields:
            if not data.get(field):
                missing.append(field)
        
        return missing

    def get_process_status(self, process_id: int) -> Dict:
        """Get current status of a process."""
        return self.process_tracker.get_process_status(process_id)