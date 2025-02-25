# tests/test_document_processor/test_deepseek_integration.py
import os
import sys
import pytest

# Add project root to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from unittest.mock import MagicMock, patch

class TestDeepseekIntegration:
    @pytest.fixture
    def mock_textract_processor(self):
        mock = MagicMock()
        mock.DEFAULT_VALUE = "."
        mock.process_document.return_value = {
            'passport_number': 'A1234567',
            'surname': '.',
            'given_names': '.',
        }
        return mock

    @pytest.fixture
    def mock_deepseek_processor(self):
        mock = MagicMock()
        mock.DEFAULT_VALUE = "."
        mock.api_key = "fake_key"
        mock.process_document.return_value = {
            'passport_number': 'A1234567',
            'surname': 'SMITH',
            'given_names': 'JOHN',
        }
        return mock

    def test_document_processor_service_merges_results(self, mock_textract_processor, mock_deepseek_processor):
        # Import here to avoid import errors
        from src.services.enhanced_document_processor import EnhancedDocumentProcessorService
        
        # Setup
        service = EnhancedDocumentProcessorService(mock_textract_processor, mock_deepseek_processor)
        
        # Execute
        with patch.object(service, 'use_deepseek_fallback', True):
            result = service.process_document('dummy_path', 'passport')
        
        # Verify
        assert result['passport_number'] == 'A1234567'
        assert result['surname'] == 'SMITH'
        assert result['given_names'] == 'JOHN'
        
        # Verify Textract was called
        mock_textract_processor.process_document.assert_called_once()

    def test_fallback_when_textract_fails(self, mock_textract_processor, mock_deepseek_processor):
        # Import here to avoid import errors
        from src.services.enhanced_document_processor import EnhancedDocumentProcessorService
        
        # Setup
        mock_textract_processor.process_document.side_effect = Exception("Textract failed")
        service = EnhancedDocumentProcessorService(mock_textract_processor, mock_deepseek_processor)
        
        # Execute
        with patch.object(service, 'use_deepseek_fallback', True):
            result = service.process_document('dummy_path', 'passport')
        
        # Verify DeepSeek result was used
        assert result['passport_number'] == 'A1234567'
        assert result['surname'] == 'SMITH'
        assert result['given_names'] == 'JOHN'
        
        # Verify both processors were called
        mock_textract_processor.process_document.assert_called_once()
        mock_deepseek_processor.process_document.assert_called_once()

    def test_deepseek_disabled(self, mock_textract_processor, mock_deepseek_processor):
        # Import here to avoid import errors
        from src.services.enhanced_document_processor import EnhancedDocumentProcessorService
        
        # Setup
        service = EnhancedDocumentProcessorService(mock_textract_processor, mock_deepseek_processor)
        
        # Execute with fallback disabled
        with patch.object(service, 'use_deepseek_fallback', False):
            result = service.process_document('dummy_path', 'passport')
        
        # Verify only Textract result was used
        assert result['passport_number'] == 'A1234567'
        assert result['surname'] == '.'
        assert result['given_names'] == '.'
        
        # Verify only Textract was called
        mock_textract_processor.process_document.assert_called_once()
        mock_deepseek_processor.process_document.assert_not_called()