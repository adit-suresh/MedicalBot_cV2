import pytest
from unittest.mock import Mock, patch
import os
from typing import Dict

from src.services.document_processor_service import DocumentProcessorService
from src.utils.exceptions import OCRError

@pytest.fixture
def mock_ocr_processor():
    """Create mock OCR processor."""
    mock = Mock()
    mock.process_document.return_value = ("processed_path.pdf", "extracted text")
    return mock

@pytest.fixture
def mock_data_extractor():
    """Create mock data extractor."""
    mock = Mock()
    mock.extract_passport_data.return_value = {
        "passport_number": "A1234567",
        "first_name": "John",
        "last_name": "Doe"
    }
    mock.extract_emirates_id_data.return_value = {
        "emirates_id": "784-1234-1234567-1",
        "name_en": "John Doe"
    }
    return mock

@pytest.fixture
def mock_db_manager():
    """Create mock database manager."""
    mock = Mock()
    mock.client_exists.return_value = False
    mock.add_client.return_value = 1
    mock.get_missing_documents.return_value = []
    return mock

@pytest.fixture
def service(mock_ocr_processor, mock_data_extractor, mock_db_manager):
    """Create document processor service with mocks."""
    service = DocumentProcessorService()
    service._ocr_processor = mock_ocr_processor
    service._data_extractor = mock_data_extractor
    service._db_manager = mock_db_manager
    return service

def test_process_new_documents_success(service):
    """Test successful document processing."""
    documents = [
        {"file_path": "passport.pdf", "type": "passport"},
        {"file_path": "emirates_id.pdf", "type": "emirates_id"}
    ]
    
    success, message = service.process_new_documents("test_email_123", documents)
    
    assert success is True
    assert "processed successfully" in message
    service._db_manager.add_client.assert_called_once()
    assert service._db_manager.add_document.call_count == 1

def test_process_new_documents_no_passport(service):
    """Test processing without passport."""
    documents = [
        {"file_path": "emirates_id.pdf", "type": "emirates_id"}
    ]
    
    success, message = service.process_new_documents("test_email_123", documents)
    
    assert success is False
    assert "No passport found" in message
    service._db_manager.add_client.assert_not_called()

def test_process_new_documents_existing_client(service):
    """Test processing with existing client."""
    # Set mock to indicate client exists
    service._db_manager.client_exists.return_value = True
    
    documents = [
        {"file_path": "passport.pdf", "type": "passport"}
    ]
    
    success, message = service.process_new_documents("test_email_123", documents)
    
    assert success is False
    assert "already exists" in message
    service._db_manager.add_client.assert_not_called()

def test_process_new_documents_ocr_error(service, mock_ocr_processor):
    """Test handling of OCR errors."""
    # Configure OCR mock to raise error
    mock_ocr_processor.process_document.side_effect = OCRError("OCR failed")
    
    documents = [
        {"file_path": "passport.pdf", "type": "passport"}
    ]
    
    success, message = service.process_new_documents("test_email_123", documents)
    
    assert success is False
    assert "Failed to extract passport data" in message

def test_process_new_documents_missing_documents(service):
    """Test processing with missing required documents."""
    # Configure mock to indicate missing documents
    service._db_manager.get_missing_documents.return_value = ["visa", "insurance_card"]
    
    documents = [
        {"file_path": "passport.pdf", "type": "passport"},
        {"file_path": "emirates_id.pdf", "type": "emirates_id"}
    ]
    
    success, message = service.process_new_documents("test_email_123", documents)
    
    assert success is True
    assert "missing documents" in message
    assert "visa" in message
    assert "insurance_card" in message

def test_document_type_determination(service):
    """Test document type detection from filenames."""
    test_cases = [
        ("passport_123.pdf", "passport"),
        ("emirates-id.jpg", "emirates_id"),
        ("visa_doc.png", "visa"),
        ("details.xlsx", "excel_sheet"),
        ("unknown.doc", "unknown")
    ]
    
    for filename, expected_type in test_cases:
        assert service._determine_document_type(filename) == expected_type

@pytest.mark.integration
def test_full_document_processing_flow(service, tmp_path):
    """Integration test for full document processing flow."""
    # Create test files
    passport_path = tmp_path / "passport.pdf"
    emirates_id_path = tmp_path / "emirates_id.pdf"
    
    # Write some dummy content
    passport_path.write_bytes(b"dummy passport content")
    emirates_id_path.write_bytes(b"dummy emirates id content")
    
    documents = [
        {"file_path": str(passport_path), "type": "passport"},
        {"file_path": str(emirates_id_path), "type": "emirates_id"}
    ]
    
    success, message = service.process_new_documents("test_email_123", documents)
    
    assert success is True
    assert service._db_manager.add_client.called
    assert service._db_manager.add_document.called
    assert "processed successfully" in message