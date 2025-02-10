import pytest
import os
import logging
from typing import List
from datetime import datetime

from src.config.app_config import configure_dependencies
from src.utils.dependency_container import container
from src.services.document_processor_service import DocumentProcessorService
from src.services.main_handler import MainHandler
from src.document_processor.textract_processor import TextractProcessor
from src.services.process_manager import ProcessManager
from src.document_processor.enhanced_ocr import EnhancedOCRProcessor
from src.email_handler.outlook_client import OutlookClient
from src.database.db_manager import DatabaseManager

logger = logging.getLogger(__name__)

@pytest.fixture(scope='module', autouse=True)
def setup_dependencies():
    """Set up dependencies for integration tests."""
    configure_dependencies()
    yield

@pytest.fixture
def document_processor():
    """Get document processor service."""
    return container.resolve(DocumentProcessorService)

@pytest.fixture
def main_handler():
    """Get main handler service."""
    return container.resolve(MainHandler)

@pytest.fixture
def process_manager():
    """Get process manager service."""
    return container.resolve(ProcessManager)

@pytest.mark.integration
class TestIntegrationFlow:
    """Integration tests for the entire system flow."""

    def test_email_processing(self, main_handler, test_files):
        """Test processing email with attachments."""
        # Create test email data
        email_id = f"TEST_EMAIL_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Process email
        result = main_handler.process_email(email_id)
        
        assert result['status'] in ['success', 'incomplete']
        if result['status'] == 'success':
            assert 'process_id' in result
            assert 'extracted_data' in result
        
        return result.get('process_id')

    def test_document_processing(self, document_processor, test_files):
        """Test document processing with OCR."""
        email_id = f"TEST_EMAIL_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Create test documents list
        documents = [
            {"file_path": file_path, "type": self._get_doc_type(file_path)}
            for file_path in test_files
        ]
        
        # Process documents
        success, message = document_processor.process_new_documents(email_id, documents)
        
        assert success is True or "missing documents" in message
        return success

    def test_end_to_end_flow(self, main_handler, process_manager, test_files):
        """Test complete end-to-end processing flow."""
        # Start with email processing
        email_id = f"TEST_EMAIL_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        result = main_handler.process_email(email_id)
        
        if result['status'] == 'success':
            process_id = result['process_id']
            
            # Check process status
            status = process_manager.get_process_status(process_id)
            assert status is not None
            
            # If process requires attention, handle it
            if status.get('manual_input_required'):
                process_manager.resume_process(process_id)
            
            # Verify final status
            final_status = process_manager.get_process_status(process_id)
            assert final_status['status'] in ['completed', 'failed']

    def test_ocr_accuracy(self, document_processor, test_files):
        """Test OCR accuracy with known documents."""
        # Skip if AWS or DeepSeek credentials not configured
        if not (os.getenv('AWS_ACCESS_KEY_ID') and os.getenv('DEEPSEEK_API_KEY')):
            pytest.skip("OCR service credentials not configured")

        # Process each test file
        for file_path in test_files:
            doc_type = self._get_doc_type(file_path)
            if doc_type in ['passport', 'emirates_id', 'visa']:
                processor = EnhancedOCRProcessor()
                processed_path, results = processor.process_document(file_path, doc_type)
                
                # Verify results structure
                assert isinstance(results, dict)
                assert len(results) > 0
                
                # Verify required fields
                self._verify_required_fields(results, doc_type)

    @staticmethod
    def _get_doc_type(file_path: str) -> str:
        """Determine document type from file path."""
        file_name = os.path.basename(file_path).lower()
        if 'passport' in file_name:
            return 'passport'
        elif 'emirates' in file_name:
            return 'emirates_id'
        elif 'visa' in file_name:
            return 'visa'
        return 'unknown'

    @staticmethod
    def _verify_required_fields(results: dict, doc_type: str) -> None:
        """Verify required fields are present in OCR results."""
        required_fields = {
            'passport': ['passport_number', 'surname', 'given_names'],
            'emirates_id': ['emirates_id', 'name_en'],
            'visa': ['entry_permit', 'full_name']
        }
        
        if doc_type in required_fields:
            for field in required_fields[doc_type]:
                assert field in results, f"Missing required field: {field}"

@pytest.mark.integration
class TestErrorHandling:
    """Test error handling and recovery."""

    def test_service_errors(self, main_handler):
        """Test handling of service errors."""
        # Test with invalid email ID
        result = main_handler.process_email("INVALID_ID")
        assert result['status'] == 'error'
        
        # Test with missing documents
        result = main_handler.process_email("NO_ATTACHMENTS")
        assert result['status'] in ['error', 'incomplete']

    def test_process_recovery(self, process_manager):
        """Test process recovery after failure."""
        process_id = f"TEST_PROCESS_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Simulate failed process
        process_manager._handle_failure(process_id, "Test failure")
        
        # Attempt recovery
        process_manager.handle_process(process_id)
        
        # Check final status
        status = process_manager.get_process_status(process_id)
        assert status is not None

@pytest.mark.integration
class TestPerformance:
    """Test system performance."""

    def test_concurrent_processing(self, main_handler):
        """Test handling multiple processes concurrently."""
        import concurrent.futures
        
        # Create multiple test emails
        email_ids = [
            f"TEST_EMAIL_{i}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            for i in range(3)
        ]
        
        # Process concurrently
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = [
                executor.submit(main_handler.process_email, email_id)
                for email_id in email_ids
            ]
            
            results = [future.result() for future in futures]
        
        # Verify results
        assert all(result['status'] in ['success', 'error', 'incomplete'] 
                  for result in results)