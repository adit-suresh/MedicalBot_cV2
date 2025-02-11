import pytest
import pandas as pd
import os
from unittest.mock import Mock, patch
from datetime import datetime

from src.services.data_integrator import DataIntegrator
from src.services.workflow_orchestrator import WorkflowOrchestrator
from src.document_processor.textract_processor import TextractProcessor
from src.document_processor.excel_processor import ExcelProcessor

@pytest.fixture
def mock_textract_processor():
    """Create mock Textract processor."""
    mock = Mock()
    mock.process_document.return_value = {
        'emirates_id': '784-1234-1234567-1',
        'first_name': 'John',
        'last_name': 'Smith',
        'passport_number': 'A1234567'
    }
    return mock

@pytest.fixture
def mock_excel_processor():
    """Create mock Excel processor."""
    mock = Mock()
    mock.process_excel.return_value = (
        pd.DataFrame({
            'emirates_id': ['784-1234-1234567-1'],
            'mobile_number': ['+971501234567'],
            'email': ['john@example.com']
        }),
        []  # No validation errors
    )
    return mock

@pytest.fixture
def data_integrator(mock_textract_processor, mock_excel_processor):
    """Create DataIntegrator with mocked processors."""
    return DataIntegrator(mock_textract_processor, mock_excel_processor)

@pytest.fixture
def workflow_orchestrator():
    """Create WorkflowOrchestrator instance."""
    return WorkflowOrchestrator()

def test_data_integration_success(data_integrator, tmp_path):
    """Test successful data integration."""
    # Create test files
    document_paths = {
        'passport': str(tmp_path / 'passport.pdf'),
        'emirates_id': str(tmp_path / 'emirates_id.pdf')
    }
    excel_path = str(tmp_path / 'data.xlsx')
    
    # Create dummy files
    for path in [*document_paths.values(), excel_path]:
        with open(path, 'wb') as f:
            f.write(b'test content')
    
    # Process documents
    df, errors = data_integrator.process_documents(document_paths, excel_path)
    
    assert len(errors) == 0
    assert 'emirates_id' in df.columns
    assert 'passport_number' in df.columns
    assert 'mobile_number' in df.columns  # From Excel
    assert len(df) == 1  # One record

def test_data_integration_missing_docs(data_integrator, tmp_path):
    """Test integration with missing documents."""
    document_paths = {
        'passport': str(tmp_path / 'passport.pdf')
        # Missing Emirates ID
    }
    
    # Create dummy file
    with open(document_paths['passport'], 'wb') as f:
        f.write(b'test content')
    
    missing_docs = data_integrator.get_missing_documents(document_paths)
    assert 'emirates_id' in missing_docs

def test_workflow_orchestration(workflow_orchestrator, tmp_path):
    """Test complete workflow orchestration."""
    # Create test email attachments
    attachments = [
        {
            'name': 'passport.pdf',
            'contentBytes': 'dGVzdA=='  # base64 "test"
        },
        {
            'name': 'emirates_id.pdf',
            'contentBytes': 'dGVzdA=='
        },
        {
            'name': 'data.xlsx',
            'contentBytes': 'dGVzdA=='
        }
    ]
    
    output_dir = str(tmp_path / 'output')
    
    # Process submission
    result = workflow_orchestrator.process_email_submission(
        'TEST_EMAIL_123',
        attachments,
        output_dir
    )
    
    assert result['status'] in ['success', 'completed_with_errors']
    assert 'output_file' in result
    assert os.path.exists(result['output_file'])

def test_workflow_validation(workflow_orchestrator):
    """Test workflow validation."""
    # Test with missing required document
    attachments = [
        {
            'name': 'passport.pdf',
            'contentBytes': 'dGVzdA=='
        }
        # Missing Emirates ID
    ]
    
    missing_docs = workflow_orchestrator.validate_process_requirements(attachments)
    assert 'emirates_id' in missing_docs

def test_workflow_retry(workflow_orchestrator, tmp_path):
    """Test process retry functionality."""
    document_paths = {
        'passport': str(tmp_path / 'passport.pdf'),
        'emirates_id': str(tmp_path / 'emirates_id.pdf')
    }
    
    # Create dummy files
    for path in document_paths.values():
        with open(path, 'wb') as f:
            f.write(b'test content')
    
    result = workflow_orchestrator.retry_failed_process(
        'TEST_PROCESS_123',
        document_paths,
        output_dir=str(tmp_path)
    )
    
    assert result['status'] in ['success', 'completed_with_errors']
    assert os.path.exists(result['output_file'])

@pytest.mark.integration
def test_real_workflow_integration(tmp_path):
    """Integration test with real files and services."""
    # Skip if no AWS credentials
    if not os.getenv('AWS_ACCESS_KEY_ID'):
        pytest.skip("AWS credentials not configured")
    
    # Test files should be in test_files directory
    test_files_dir = os.path.join(
        os.path.dirname(__file__),
        'test_files'
    )
    
    if not os.path.exists(test_files_dir):
        pytest.skip("Test files directory not found")
    
    workflow = WorkflowOrchestrator()
    
    # Copy test files to temp directory
    document_paths = {}
    for filename in os.listdir(test_files_dir):
        if filename.endswith(('.pdf', '.jpg')):
            src_path = os.path.join(test_files_dir, filename)
            dest_path = str(tmp_path / filename)
            with open(src_path, 'rb') as src, open(dest_path, 'wb') as dest:
                dest.write(src.read())
            
            if 'passport' in filename.lower():
                document_paths['passport'] = dest_path
            elif 'emirates' in filename.lower():
                document_paths['emirates_id'] = dest_path
    
    if not document_paths:
        pytest.skip("No test documents found")
    
    # Test complete workflow
    result = workflow.retry_failed_process(
        'TEST_INTEGRATION',
        document_paths,
        output_dir=str(tmp_path)
    )
    
    assert result['status'] in ['success', 'completed_with_errors']
    assert os.path.exists(result['output_file'])