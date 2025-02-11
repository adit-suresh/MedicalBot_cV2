import pytest
from unittest.mock import Mock, patch
import os
import boto3
from botocore.exceptions import ClientError
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.document_processor.textract_processor import TextractProcessor
from src.utils.error_handling import ServiceError

@pytest.fixture
def mock_textract_response():
    """Mock successful Textract response."""
    return {
        'Blocks': [
            {
                'BlockType': 'LINE',
                'Text': 'REPUBLIC OF TEST',
                'Confidence': 99.0
            },
            {
                'BlockType': 'LINE',
                'Text': 'Passport No: A1234567',
                'Confidence': 98.0
            },
            {
                'BlockType': 'LINE',
                'Text': 'Surname: SMITH',
                'Confidence': 97.0
            },
            {
                'BlockType': 'LINE',
                'Text': 'Given Names: JOHN JAMES',
                'Confidence': 96.0
            }
        ]
    }

@pytest.fixture
def mock_emirates_id_response():
    """Mock Emirates ID Textract response."""
    return {
        'Blocks': [
            {
                'BlockType': 'LINE',
                'Text': 'UNITED ARAB EMIRATES',
                'Confidence': 99.0
            },
            {
                'BlockType': 'LINE',
                'Text': 'ID Number: 784-1234-1234567-1',
                'Confidence': 98.0
            },
            {
                'BlockType': 'LINE',
                'Text': 'Name: MOHAMMAD AHMAD',
                'Confidence': 97.0
            },
            {
                'BlockType': 'LINE',
                'Text': 'Nationality: UAE',
                'Confidence': 96.0
            }
        ]
    }

@pytest.fixture
def processor():
    """Create Textract processor instance."""
    return TextractProcessor()

def test_process_passport(processor, mock_textract_response):
    """Test passport processing."""
    with patch('boto3.client') as mock_boto:
        # Configure mock
        mock_client = Mock()
        mock_boto.return_value = mock_client
        mock_client.analyze_document.return_value = mock_textract_response

        # Create test file
        with open('test_passport.pdf', 'wb') as f:
            f.write(b'test content')

        try:
            # Process document
            result = processor.process_document('test_passport.pdf', 'passport')

            # Verify extracted data
            assert result['passport_number'] == 'A1234567'
            assert result['surname'] == 'SMITH'
            assert result['given_names'] == 'JOHN JAMES'

            # Verify Textract was called correctly
            mock_client.analyze_document.assert_called_once()
            
        finally:
            # Cleanup
            if os.path.exists('test_passport.pdf'):
                os.remove('test_passport.pdf')

def test_process_emirates_id(processor, mock_emirates_id_response):
    """Test Emirates ID processing."""
    with patch('boto3.client') as mock_boto:
        mock_client = Mock()
        mock_boto.return_value = mock_client
        mock_client.analyze_document.return_value = mock_emirates_id_response

        with open('test_eid.pdf', 'wb') as f:
            f.write(b'test content')

        try:
            result = processor.process_document('test_eid.pdf', 'emirates_id')

            assert result['emirates_id'] == '784-1234-1234567-1'
            assert result['name_en'] == 'MOHAMMAD AHMAD'
            assert result['nationality'] == 'UAE'
            
        finally:
            if os.path.exists('test_eid.pdf'):
                os.remove('test_eid.pdf')

def test_textract_error(processor):
    """Test handling of Textract errors."""
    with patch('boto3.client') as mock_boto:
        mock_client = Mock()
        mock_boto.return_value = mock_client
        mock_client.analyze_document.side_effect = ClientError(
            {'Error': {'Code': 'InvalidRequest', 'Message': 'Test error'}},
            'AnalyzeDocument'
        )

        with pytest.raises(ServiceError) as exc_info:
            processor.process_document('test.pdf', 'passport')
        
        assert 'Textract processing failed' in str(exc_info.value)

def test_document_type_detection(processor, mock_textract_response):
    """Test automatic document type detection."""
    with patch('boto3.client') as mock_boto:
        mock_client = Mock()
        mock_boto.return_value = mock_client
        mock_client.analyze_document.return_value = mock_textract_response

        with open('test_doc.pdf', 'wb') as f:
            f.write(b'test content')

        try:
            result = processor.process_document('test_doc.pdf')  # No doc_type specified
            assert 'passport_number' in result  # Should detect it's a passport
            
        finally:
            if os.path.exists('test_doc.pdf'):
                os.remove('test_doc.pdf')

def test_missing_required_fields(processor):
    """Test validation of missing required fields."""
    with patch('boto3.client') as mock_boto:
        mock_client = Mock()
        mock_boto.return_value = mock_client
        mock_client.analyze_document.return_value = {
            'Blocks': [
                {
                    'BlockType': 'LINE',
                    'Text': 'PASSPORT',
                    'Confidence': 99.0
                }
                # Missing required fields
            ]
        }

        with open('test_passport.pdf', 'wb') as f:
            f.write(b'test content')

        try:
            result = processor.process_document('test_passport.pdf', 'passport')
            assert not result.get('passport_number')  # Should be missing
            
        finally:
            if os.path.exists('test_passport.pdf'):
                os.remove('test_passport.pdf')

@pytest.mark.integration
def test_real_document_processing(processor):
    """Integration test with real AWS Textract."""
    # Skip if no AWS credentials
    if not os.getenv('AWS_ACCESS_KEY_ID'):
        pytest.skip("AWS credentials not configured")

    # Test file should be in test_files directory
    test_file = os.path.join(
        os.path.dirname(__file__),
        'test_files',
        'sample_passport.pdf'
    )

    if not os.path.exists(test_file):
        pytest.skip("Test file not found")

    result = processor.process_document(test_file, 'passport')
    assert result  # Should return some data
    assert 'passport_number' in result