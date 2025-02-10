import unittest
import os
from unittest.mock import Mock, patch
import boto3
from botocore.exceptions import ClientError

from src.document_processor.textract_processor import VisionProcessor

class TestTextractProcessor(unittest.TestCase):
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

    @patch('boto3.client')
    def test_work_permit_extraction(self, mock_boto):
        """Test extraction from work permit."""
        # Mock Textract response
        mock_textract = Mock()
        mock_boto.return_value = mock_textract
        mock_textract.analyze_document.return_value = {
            'Blocks': [
                {
                    'BlockType': 'LINE',
                    'Text': 'WORK PERMIT',
                    'Confidence': 99.0
                },
                {
                    'BlockType': 'LINE',
                    'Text': 'Name: John Smith',
                    'Confidence': 98.0
                },
                {
                    'BlockType': 'LINE',
                    'Text': 'Permit No: 12345',
                    'Confidence': 97.0
                }
            ]
        }

        # Process work permit
        result = self.processor.process_document(self.test_files['work_permit'])
        
        # Verify required fields
        self.assertIn('full_name', result)
        self.assertIn('personal_no', result)
        
        # Verify data format
        self.assertTrue(len(result['personal_no']) > 0)
        self.assertTrue(len(result['full_name']) > 0)

    @patch('boto3.client')
    def test_emirates_id_extraction(self, mock_boto):
        """Test extraction from Emirates ID."""
        # Mock Textract response
        mock_textract = Mock()
        mock_boto.return_value = mock_textract
        mock_textract.analyze_document.return_value = {
            'Blocks': [
                {
                    'BlockType': 'LINE',
                    'Text': 'ID Number: 784-1234-1234567-1',
                    'Confidence': 99.0
                },
                {
                    'BlockType': 'LINE',
                    'Text': 'Name: John Smith',
                    'Confidence': 98.0
                }
            ]
        }

        result = self.processor.process_document(self.test_files['emirates_id'])
        
        # Verify Emirates ID specific fields
        self.assertIn('emirates_id', result)
        self.assertIn('name_en', result)

    @patch('boto3.client')
    def test_error_handling(self, mock_boto):
        """Test error handling."""
        # Mock Textract error
        mock_textract = Mock()
        mock_boto.return_value = mock_textract
        mock_textract.analyze_document.side_effect = ClientError(
            {'Error': {'Code': 'InvalidRequest', 'Message': 'Invalid image'}},
            'AnalyzeDocument'
        )
        
        with self.assertRaises(Exception):
            self.processor.process_document(self.test_files['passport'])

    def test_invalid_file(self):
        """Test handling of invalid file."""
        invalid_file = os.path.join(self.test_files_dir, 'nonexistent.jpg')
        
        with self.assertRaises(FileNotFoundError):
            self.processor.process_document(invalid_file)

if __name__ == '__main__':
    unittest.main()