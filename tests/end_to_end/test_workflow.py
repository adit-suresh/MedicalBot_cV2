import os
import sys
import pytest
import logging
from datetime import datetime
from typing import Dict

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

from src.services.workflow_orchestrator import WorkflowOrchestrator
from src.services.main_handler import MainHandler
from src.services.email_validator import EmailValidator
from src.document_processor.textract_processor import TextractProcessor
from src.utils.process_tracker import ProcessTracker

logger = logging.getLogger(__name__)

@pytest.mark.end_to_end
class TestCompleteWorkflow:
    """End-to-end tests covering the complete bot workflow."""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test environment."""
        self.workflow = WorkflowOrchestrator()
        self.main_handler = MainHandler()
        self.process_tracker = ProcessTracker()
        self.test_files = self._get_test_files()
        yield
        self._cleanup()

    def _get_test_files(self) -> Dict[str, str]:
        """Get test file paths."""
        test_dir = "test_files"
        test_files = {}
        for filename in os.listdir(test_dir):
            filepath = os.path.join(test_dir, filename)
            if 'passport' in filename.lower():
                test_files['passport'] = filepath
            elif 'emirates' in filename.lower():
                test_files['emirates_id'] = filepath
            elif 'visa' in filename.lower():
                test_files['visa'] = filepath
            elif filename.endswith('.xlsx') and 'template' not in filename:
                test_files['excel'] = filepath
        return test_files

    def test_1_email_processing(self):
        """Test email processing and attachment handling."""
        # Create test email ID
        email_id = f"TEST_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Process email
        result = self.main_handler.process_email(email_id)
        assert result['status'] in ['success', 'incomplete']
        
        if result['status'] == 'success':
            assert 'process_id' in result
            assert 'extracted_data' in result

    def test_2_document_processing(self):
        """Test document extraction and validation."""
        for doc_type, file_path in self.test_files.items():
            if doc_type != 'excel':
                processor = TextractProcessor()
                result = processor.process_document(file_path)
                assert isinstance(result, dict)
                assert len(result) > 0

    def test_3_complete_workflow(self):
        """Test complete workflow from email to validation."""
        # Step 1: Process email and documents
        email_id = f"TEST_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        result = self.workflow.process_email_submission(
            email_id,
            self.test_files,
            "test_output"
        )
        
        assert result['status'] in ['success', 'completed_with_errors']
        assert os.path.exists(result['output_file'])
        
        # Step 2: Send for validation
        if result['status'] == 'success':
            validation = EmailValidator()
            val_result = validation.send_for_validation(
                result['output_file'],
                result['process_id']
            )
            assert val_result['status'] == 'sent'

    def _cleanup(self):
        """Clean up test artifacts."""
        if os.path.exists("test_output"):
            import shutil
            shutil.rmtree("test_output")