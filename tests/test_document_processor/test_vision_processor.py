import unittest
import os
from unittest.mock import Mock, patch
from google.cloud import vision

from src.document_processor.vision_processor import VisionProcessor

class TestVisionProcessor(unittest.TestCase):
    def setUp(self):
        """Set up test environment."""
        self.test_files_dir = os.path.join(os.path.dirname(__file__), 'test_files')
        os.makedirs(self.test_files_dir, exist_ok=True)
        
        # Initialize the processor
        self.processor = VisionProcessor()
        
        # Test file paths
        self.test_files = {
            'emirates_id': os.path.join(self.test_files_dir, 'test_emirates_id.jpg'),
            'passport': os.path.join(self.test_files_dir, 'test_passport.jpg'),
            'visa': os.path.join(self.test_files_dir, 'test_visa.jpg'),
            'work_permit': os.path.join(self.test_files_dir, 'test_work_permit.jpg')
        }

    def test_work_permit_extraction(self):
        """Test extraction from work permit."""
        # Process work permit
        result = self.processor.process_document(self.test_files['work_permit'])
        
        # Verify required fields
        self.assertIn('full_name', result)
        self.assertIn('expiry_date', result)
        self.assertIn('personal_no', result)
        
        # Verify data format
        self.assertTrue(len(result['personal_no']) > 0)
        self.assertTrue(len(result['full_name']) > 0)

    def test_emirates_id_extraction(self):
        """Test extraction from Emirates ID."""
        result = self.processor.process_document(self.test_files['emirates_id'])
        
        # Verify Emirates ID specific fields
        self.assertIn('emirates_id', result)
        self.assertIn('name_en', result)
        self.assertIn('nationality', result)

    def test_passport_extraction(self):
        """Test extraction from passport."""
        result = self.processor.process_document(self.test_files['passport'])
        
        # Verify passport specific fields
        self.assertIn('passport_number', result)
        self.assertIn('surname', result)
        self.assertIn('given_names', result)
        self.assertIn('nationality', result)

    def test_visa_extraction(self):
        """Test extraction from visa."""
        result = self.processor.process_document(self.test_files['visa'])
        
        # Verify visa specific fields
        self.assertIn('entry_permit', result)
        self.assertIn('full_name', result)
        self.assertIn('nationality', result)

    @patch('google.cloud.vision.ImageAnnotatorClient')
    def test_error_handling(self, mock_client):
        """Test error handling."""
        # Mock API error
        mock_client.return_value.text_detection.side_effect = Exception("API Error")
        
        with self.assertRaises(Exception):
            self.processor.process_document(self.test_files['passport'])

    def test_document_type_detection(self):
        """Test document type detection."""
        # Test each document type
        doc_types = {
            'emirates_id': 'emirates_id',
            'passport': 'passport',
            'visa': 'visa',
            'work_permit': 'work_permit'
        }
        
        for file_type, expected_type in doc_types.items():
            detected_type = self.processor._determine_document_type(
                self.test_files[file_type],
                "Sample text for " + file_type
            )
            self.assertEqual(detected_type, expected_type)

    def test_data_validation(self):
        """Test data validation for extracted fields."""
        # Process work permit
        result = self.processor.process_document(self.test_files['work_permit'])
        
        # Test date format
        if 'expiry_date' in result:
            from datetime import datetime
            try:
                datetime.strptime(result['expiry_date'], '%d/%m/%Y')
            except ValueError:
                self.fail("Incorrect date format")

        # Test ID number format
        if 'emirates_id' in result:
            import re
            pattern = r'^\d{3}-\d{4}-\d{7}-\d{1}$'
            self.assertTrue(re.match(pattern, result['emirates_id']))

    def test_invalid_file(self):
        """Test handling of invalid file."""
        invalid_file = os.path.join(self.test_files_dir, 'nonexistent.jpg')
        
        with self.assertRaises(FileNotFoundError):
            self.processor.process_document(invalid_file)

if __name__ == '__main__':
    unittest.main()