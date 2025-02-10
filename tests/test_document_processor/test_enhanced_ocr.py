import pytest
from unittest.mock import Mock, patch
import boto3
import json
import os
from botocore.exceptions import ClientError

from src.document_processor.enhanced_ocr import EnhancedOCRProcessor
from src.utils.error_handling import ServiceError

@pytest.fixture
def mock_textract():
    """Create mock Textract client."""
    with patch('boto3.client') as mock_client:
        mock_textract = Mock()
        mock_client.return_value = mock_textract
        
        # Setup sample response
        mock_textract.analyze_document.return_value = {
            'Blocks': [
                {
                    'BlockType': 'KEY_VALUE_SET',
                    'EntityTypes': ['KEY'],
                    'Id': 'key1',
                    'Confidence': 95.0,
                    'Text': 'Passport No',
                    'Relationships': [
                        {
                            'Type': 'VALUE',
                            'Ids': ['value1']
                        }
                    ]
                },
                {
                    'BlockType': 'KEY_VALUE_SET',
                    'EntityTypes': ['VALUE'],
                    'Id': 'value1',
                    'Confidence': 98.0,
                    'Text': 'A1234567',
                    'Relationships': [
                        {
                            'Type': 'CHILD',
                            'Ids': ['child1']
                        }
                    ]
                },
                {
                    'BlockType': 'WORD',
                    'Id': 'child1',
                    'Text': 'A1234567'
                }
            ]
        }
        yield mock_textract

@pytest.fixture
def mock_deepseek_response():
    """Create mock DeepSeek response."""
    return {
        'text': json.dumps({
            'passport_number': 'A1234567',
            'surname': 'Smith',
            'given_names': 'John James',
            'nationality': 'USA'
        })
    }

@pytest.fixture
def processor(mock_textract):
    """Create OCR processor with mocked services."""
    return EnhancedOCRProcessor()

def test_process_document_success(processor, mock_textract, mock_deepseek_response):
    """Test successful document processing."""
    # Mock DeepSeek API call
    with patch('requests.post') as mock_post:
        mock_post.return_value.json.return_value = mock_deepseek_response
        mock_post.return_value.raise_for_status = lambda: None
        
        # Create test file
        with open('test_passport.pdf', 'wb') as f:
            f.write(b'test content')
        
        try:
            # Process document
            processed_path, results = processor.process_document(
                'test_passport.pdf',
                'passport'
            )
            
            # Verify results
            assert results['passport_number'] == 'A1234567'
            assert results['surname'] == 'Smith'
            assert results['given_names'] == 'John James'
            assert results['nationality'] == 'USA'
            
            # Verify service calls
            mock_textract.analyze_document.assert_called_once()
            mock_post.assert_called_once()
            
        finally:
            # Cleanup
            if os.path.exists('test_passport.pdf'):
                os.remove('test_passport.pdf')

def test_textract_error(processor, mock_textract):
    """Test handling of Textract errors."""
    # Configure Textract to raise error
    mock_textract.analyze_document.side_effect = ClientError(
        {'Error': {'Code': 'InvalidRequest', 'Message': 'Test error'}},
        'AnalyzeDocument'
    )
    
    with pytest.raises(ServiceError) as exc_info:
        processor._process_with_textract(b'test content')
    
    assert 'Textract processing failed' in str(exc_info.value)

def test_deepseek_error(processor):
    """Test handling of DeepSeek errors."""
    with patch('requests.post') as mock_post:
        mock_post.side_effect = Exception('API error')
        
        with pytest.raises(ServiceError) as exc_info:
            processor._process_with_deepseek(b'test content', 'passport')
        
        assert 'DeepSeek processing failed' in str(exc_info.value)

def test_result_combination(processor):
    """Test combining results from both services."""
    textract_result = {
        'key_values': {
            'Passport No': {
                'value': 'A1234567',
                'confidence': 95.0
            }
        }
    }
    
    deepseek_result = {
        'text': json.dumps({
            'passport_number': 'B1234567',  # Different value
            'surname': 'Smith'  # Additional field
        })
    }
    
    combined = processor._combine_results(
        textract_result,
        deepseek_result,
        'passport'
    )
    
    # Should keep Textract value for passport number due to high confidence
    assert combined['passport_number'] == 'A1234567'
    # Should include DeepSeek's additional field
    assert combined['surname'] == 'Smith'

def test_key_normalization(processor):
    """Test key name normalization."""
    test_cases = [
        ('Passport No', 'passport_number'),
        ('Document Number', 'passport_number'),
        ('Last Name', 'surname'),
        ('Given Names', 'given_names'),
        ('Date of Birth', 'date_of_birth'),
        ('ID Number', 'emirates_id')
    ]
    
    for input_key, expected in test_cases:
        assert processor._normalize_key(input_key) == expected

@pytest.mark.integration
def test_full_integration(processor):
    """Integration test with real services."""
    # Skip if environment variables not set
    if not (os.getenv('AWS_ACCESS_KEY_ID') and os.getenv('DEEPSEEK_API_KEY')):
        pytest.skip("AWS or DeepSeek credentials not configured")
    
    # Create test file path
    test_file = os.path.join(
        os.path.dirname(__file__),
        'test_files',
        'sample_passport.jpg'
    )
    
    # Skip if test file doesn't exist
    if not os.path.exists(test_file):
        pytest.skip("Test file not found")
        
    processed_path, results = processor.process_document(test_file, 'passport')
    
    # Verify basic structure
    assert isinstance(results, dict)
    assert len(results) > 0
    # Verify expected fields
    assert 'passport_number' in results or 'document_number' in results