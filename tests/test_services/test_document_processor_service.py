import unittest
import os
import tempfile
import shutil
from unittest.mock import MagicMock, patch
from datetime import datetime

from src.services.document_processor_service import DocumentProcessorService
from src.utils.exceptions import OCRError

class TestDocumentProcessorService(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        
        # Create test files
        self.passport_path = os.path.join(self.test_dir, "test_passport.pdf")
        self.emirates_id_path = os.path.join(self.test_dir, "test_emirates_id.pdf")
        self.visa_path = os.path.join(self.test_dir, "test_visa.pdf")
        self.excel_path = os.path.join(self.test_dir, "test_details.xlsx")
        
        # Create dummy files
        self._create_test_files()
        
        # Create service with mocked dependencies
        self.mock_ocr = MagicMock()
        self.mock_extractor = MagicMock()
        self.mock_db = MagicMock()
        
        # Initialize service with mocks
        self.service = DocumentProcessorService()
        self.service.ocr_processor = self.mock_ocr
        self.service.data_extractor = self.mock_extractor
        self.service.db_manager = self.mock_db
        
        # Mock data
        self.mock_passport_data = {
            "passport_number": "A1234567",
            "first_name": "John",
            "last_name": "Doe",
            "nationality": "USA",
            "date_of_birth": "1990-01-01"
        }

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def _create_test_files(self):
        """Create dummy test files."""
        for path in [self.passport_path, self.emirates_id_path, 
                    self.visa_path, self.excel_path]:
            with open(path, 'wb') as f:
                f.write(b'dummy content')

    def test_process_new_documents_success(self):
        """Test successful processing of all documents."""
        # Setup mocks
        self.mock_ocr.process_document.return_value = ("processed_path", "extracted text")
        self.mock_extractor.extract_passport_data.return_value = self.mock_passport_data
        self.mock_db.client_exists.return_value = False
        self.mock_db.add_client.return_value = 1
        self.mock_db.get_missing_documents.return_value = []
        
        documents = [
            {"file_path": self.passport_path, "type": "passport"},
            {"file_path": self.emirates_id_path, "type": "emirates_id"},
            {"file_path": self.visa_path, "type": "visa"},
            {"file_path": self.excel_path, "type": "excel_sheet"}
        ]
        
        success, message = self.service.process_new_documents("test_email_1", documents)
        
        self.assertTrue(success)
        self.assertIn("processed successfully", message)

    def test_process_new_documents_no_passport(self):
        """Test processing without passport."""
        documents = [
            {"file_path": self.emirates_id_path, "type": "emirates_id"},
            {"file_path": self.visa_path, "type": "visa"}
        ]
        
        success, message = self.service.process_new_documents("test_email_2", documents)
        
        self.assertFalse(success)
        self.assertIn("No passport found", message)

    def test_duplicate_client(self):
        """Test handling of duplicate client."""
        # Setup mocks
        self.mock_ocr.process_document.return_value = ("processed_path", "extracted text")
        self.mock_extractor.extract_passport_data.return_value = self.mock_passport_data
        self.mock_db.client_exists.return_value = True
        
        documents = [{"file_path": self.passport_path, "type": "passport"}]
        success, message = self.service.process_new_documents("test_email_3", documents)
        
        self.assertFalse(success)
        self.assertIn("already exists", message)

    def test_ocr_failure(self):
        """Test handling of OCR failure."""
        # Mock OCR failure
        self.mock_ocr.process_document.side_effect = OCRError("Error processing document")
        
        documents = [{"file_path": self.passport_path, "type": "passport"}]
        
        success, message = self.service.process_new_documents("test_email_4", documents)
        
        self.assertFalse(success)
        self.assertIn("Error processing", message)

    def test_determine_document_type(self):
        """Test document type determination."""
        test_cases = [
            ("passport.pdf", "passport"),
            ("emirates_id.jpg", "emirates_id"),
            ("visa_document.png", "visa"),
            ("details.xlsx", "excel_sheet"),
            ("unknown.txt", "unknown")
        ]
        
        for filename, expected_type in test_cases:
            doc_type = self.service._determine_document_type(filename)
            self.assertEqual(doc_type, expected_type)