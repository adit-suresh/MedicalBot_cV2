"""
Integration test for the refactored components of the medical automation system.
This test ensures the components work together properly.
"""
import os
import sys
import logging
from datetime import datetime, timedelta
import tempfile
import json
from unittest.mock import patch, MagicMock

# Add project root to Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_root)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('integration_test.log')
    ]
)
logger = logging.getLogger(__name__)


def setup_test_environment():
    """Set up test environment with mocks and temp directories."""
    # Create temporary directories for testing
    temp_raw_dir = tempfile.mkdtemp(prefix="test_raw_")
    temp_processed_dir = tempfile.mkdtemp(prefix="test_processed_")
    temp_db_path = os.path.join(tempfile.mkdtemp(), "test_db.sqlite")
    
    # Set environment variables for testing
    os.environ.update({
        "RAW_DATA_DIR": temp_raw_dir,
        "PROCESSED_DATA_DIR": temp_processed_dir,
        "CLIENT_ID": "test_client_id",
        "CLIENT_SECRET": "test_client_secret",
        "TENANT_ID": "test_tenant_id",
        "USER_EMAIL": "test@example.com",
        "TARGET_MAILBOX": "target@example.com",
        "SLACK_BOT_TOKEN": "test_slack_token"
    })
    
    return {
        "raw_dir": temp_raw_dir,
        "processed_dir": temp_processed_dir,
        "db_path": temp_db_path
    }


def create_test_files(test_dirs):
    """Create test files mimicking the expected formats."""
    # Create a sample passport PDF
    passport_path = os.path.join(test_dirs["raw_dir"], "test_passport.pdf")
    with open(passport_path, 'wb') as f:
        f.write(b"Test passport file")
    
    # Create a sample Emirates ID
    eid_path = os.path.join(test_dirs["raw_dir"], "test_emirates_id.pdf")
    with open(eid_path, 'wb') as f:
        f.write(b"Test Emirates ID file")
    
    # Create a sample Excel file
    excel_path = os.path.join(test_dirs["raw_dir"], "test_data.xlsx")
    with open(excel_path, 'wb') as f:
        f.write(b"Test Excel file")
        
    return {
        "passport": passport_path,
        "emirates_id": eid_path,
        "excel": excel_path
    }


def test_email_to_attachment_workflow():
    """Test the workflow from email fetching to attachment processing."""
    logger.info("Testing email to attachment workflow")
    
    # Import required modules
    from src.email_handler.outlook_client import OutlookClient
    from src.email_handler.attachment_handler import AttachmentHandler
    from src.utils.dependency_container import container
    
    # Setup mock for ConfidentialClientApplication
    with patch('src.email_handler.outlook_client.ConfidentialClientApplication') as mock_app, \
         patch('src.email_handler.outlook_client.requests') as mock_requests:
         
        # Configure mock token
        mock_client = MagicMock()
        mock_app.return_value = mock_client
        mock_client.acquire_token_for_client.return_value = {
            "access_token": "test_token",
            "expires_in": 3600
        }
        
        # Configure mock emails response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "value": [
                {
                    "id": "email1",
                    "subject": "Test Addition",
                    "hasAttachments": True,
                    "receivedDateTime": datetime.now().isoformat()
                }
            ]
        }
        mock_requests.get.return_value = mock_response
        mock_requests.request.return_value = mock_response
        
        # Configure mock attachments response
        mock_attachments_response = MagicMock()
        mock_attachments_response.status_code = 200
        mock_attachments_response.json.return_value = {
            "value": [
                {
                    "id": "att1",
                    "name": "passport.pdf",
                    "contentBytes": "dGVzdA==",  # base64 for "test"
                    "size": 100,
                    "contentType": "application/pdf"
                },
                {
                    "id": "att2",
                    "name": "emirates_id.pdf",
                    "contentBytes": "dGVzdA==",  # base64 for "test"
                    "size": 100,
                    "contentType": "application/pdf"
                }
            ]
        }
        
        # Setup response sequence
        mock_requests.request.side_effect = [mock_response, mock_attachments_response]
        
        # Create clients
        outlook_client = OutlookClient()
        attachment_handler = AttachmentHandler()
        
        # Execute workflow
        emails = outlook_client.fetch_emails()
        assert len(emails) == 1, f"Expected 1 email, got {len(emails)}"
        
        email = emails[0]
        attachments = outlook_client.get_attachments(email['id'])
        assert len(attachments) == 2, f"Expected 2 attachments, got {len(attachments)}"
        
        with patch('builtins.open', MagicMock()), \
             patch.object(attachment_handler, 'is_valid_attachment', return_value=True):
            saved_paths = attachment_handler.process_attachments(attachments, email['id'])
            assert len(saved_paths) == 2, f"Expected 2 saved paths, got {len(saved_paths)}"
        
    logger.info("✓ Email to attachment workflow completed successfully")
    return True


def test_document_processing_workflow(test_dirs):
    """Test the document processing workflow."""
    logger.info("Testing document processing workflow")
    
    # Import required modules
    from src.document_processor.textract_processor import TextractProcessor
    from src.document_processor.data_extractor import DataExtractor
    from src.utils.process_control_interface import ProcessStatus
    
    # Mock Textract client
    with patch('boto3.client') as mock_boto:
        # Configure mock
        mock_textract = MagicMock()
        mock_boto.return_value = mock_textract
        
        # Mock response for passport
        passport_response = {
            'Blocks': [
                {
                    'BlockType': 'LINE',
                    'Text': 'PASSPORT',
                    'Confidence': 99.5
                },
                {
                    'BlockType': 'LINE',
                    'Text': 'Passport No: A1234567',
                    'Confidence': 95.0
                },
                {
                    'BlockType': 'LINE',
                    'Text': 'Surname: DOE',
                    'Confidence': 97.0
                },
                {
                    'BlockType': 'LINE',
                    'Text': 'Given Names: JOHN',
                    'Confidence': 96.0
                }
            ]
        }
        
        # Mock response for Emirates ID
        eid_response = {
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
                    'Text': 'Name: JOHN DOE',
                    'Confidence': 97.0
                }
            ]
        }
        
        # Set up response sequence
        mock_textract.analyze_document.side_effect = [passport_response, eid_response]
        
        # Create processor instances
        textract_processor = TextractProcessor()
        data_extractor = DataExtractor()
        
        # Process test documents
        test_passport = os.path.join("test_files", "passport.pdf")
        test_eid = os.path.join("test_files", "emirates_id.pdf")
        
        # Create test files if they don't exist
        os.makedirs("test_files", exist_ok=True)
        for path in [test_passport, test_eid]:
            if not os.path.exists(path):
                with open(path, 'wb') as f:
                    f.write(b'test content')
        
        # Process documents
        passport_data = textract_processor.process_document(test_passport, 'passport')
        eid_data = textract_processor.process_document(test_eid, 'emirates_id')
        
        # Verify process completion
        from src.utils.process_control import ProcessControl
        process_control = ProcessControl(test_dirs["db_path"])
        process_id = "test_process_id"
        final_status = process_control.get_process_status(process_id)
        assert final_status['status'] == ProcessStatus.COMPLETED.value
        
    logger.info("✓ End-to-end workflow completed successfully")
    return True


def cleanup_test_environment(test_dirs):
    """Clean up test directories and files."""
    import shutil
    
    # Remove temp directories
    for dir_name in ["raw_dir", "processed_dir"]:
        if os.path.exists(test_dirs[dir_name]):
            shutil.rmtree(test_dirs[dir_name])
    
    # Remove DB file directory
    db_dir = os.path.dirname(test_dirs["db_path"])
    if os.path.exists(db_dir):
        shutil.rmtree(db_dir)
    
    # Remove test files directory
    if os.path.exists("test_files"):
        shutil.rmtree("test_files")


def main():
    """Run all integration tests."""
    logger.info("Starting integration tests")
    start_time = datetime.now()
    
    # Setup test environment
    test_dirs = setup_test_environment()
    
    try:
        # Run tests
        tests = [
            ("Email to Attachment Workflow", test_email_to_attachment_workflow),
            ("Document Processing Workflow", lambda: test_document_processing_workflow(test_dirs)),
            ("Data Combination Workflow", test_data_combination_workflow),
            ("End-to-End Workflow", test_end_to_end_workflow)
        ]
        
        results = {}
        for name, test_func in tests:
            logger.info(f"\nRunning test: {name}")
            try:
                result = test_func()
                results[name] = result
            except Exception as e:
                logger.error(f"Test '{name}' failed with unexpected error: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())
                results[name] = False
        
        # Print summary
        print("\n" + "=" * 60)
        print("INTEGRATION TEST RESULTS".center(60))
        print("=" * 60)
        
        passed = sum(1 for result in results.values() if result)
        total = len(tests)
        
        for name, result in results.items():
            status = "✓ PASSED" if result else "✗ FAILED"
            print(f"{name.ljust(35)}: {status}")
        
        print("\n" + "-" * 60)
        print(f"SUMMARY: {passed}/{total} tests passed ({passed/total*100:.1f}%)")
        print(f"Duration: {(datetime.now() - start_time).total_seconds():.2f} seconds")
        print("=" * 60)
        
    finally:
        # Clean up test environment
        cleanup_test_environment(test_dirs)
    
    # Return success status for CI/CD integration
    return all(results.values()) if results else False


    if __name__ == "__main__":
        success = main()
        sys.exit(0 if success else 1)
        results
        assert 'passport_number' in passport_data, f"Missing passport_number in {passport_data}"
        assert 'emirates_id' in eid_data, f"Missing emirates_id in {eid_data}"
        
        # Test data extraction (simpler test as we've already mocked the raw text)
        passport_text = "Passport No: A1234567\nSurname: DOE\nGiven Names: JOHN"
        extracted_passport = data_extractor.extract_passport_data(passport_text)
        assert extracted_passport.get('passport_number') == 'A1234567', f"Incorrect extraction: {extracted_passport}"
        
    logger.info("✓ Document processing workflow completed successfully")
    return True


def test_data_combination_workflow():
    """Test the data combination workflow."""
    logger.info("Testing data combination workflow")
    
    # Import required modules
    from src.services.data_combiner import DataCombiner
    from src.document_processor.textract_processor import TextractProcessor
    from src.document_processor.excel_processor import ExcelProcessor
    import pandas as pd
    import tempfile
    
    # Mock dependencies
    mock_textract = MagicMock()
    mock_excel = MagicMock()
    
    # Create sample extracted data
    extracted_data = {
        'passport_number': 'A1234567',
        'first_name': 'John',
        'last_name': 'Doe',
        'nationality': 'USA',
        'date_of_birth': '1990-01-01'
    }
    
    # Create sample Excel data
    excel_data = pd.DataFrame([{
        'first_name': 'John',
        'last_name': 'Doe',
        'email': 'john.doe@example.com',
        'mobile_number': '+971501234567',
        'salary_band': 'A'
    }])
    
    # Create test template
    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
        template_path = tmp.name
    
    # Create test output path
    output_path = os.path.join(tempfile.mkdtemp(), "output.xlsx")
    
    # Create combiner
    data_combiner = DataCombiner(mock_textract, mock_excel)
    
    # Mock _get_template_structure
    template_structure = {
        'columns': [
            'first_name', 'last_name', 'passport_number', 
            'nationality', 'date_of_birth', 'email', 'mobile_number',
            'salary_band'
        ],
        'column_info': {},
        'column_count': 8,
        'last_modified': datetime.now().timestamp()
    }
    data_combiner._get_template_structure = MagicMock(return_value=template_structure)
    
    # Test with patched DataFrame operations
    with patch('pandas.DataFrame.to_excel'):
        result = data_combiner.combine_and_populate_template(
            template_path,
            output_path,
            extracted_data,
            excel_data
        )
        
        assert result['status'] == 'success', f"Expected status 'success', got '{result['status']}'"
        assert result['output_path'] == output_path
    
    logger.info("✓ Data combination workflow completed successfully")
    return True


def test_end_to_end_workflow():
    """Test simplified end-to-end workflow with mocks."""
    logger.info("Testing end-to-end workflow")
    
    # Import required modules
    from src.config.app_config import configure_dependencies
    from src.utils.dependency_container import container
    from src.utils.process_control import ProcessControl
    from src.utils.process_control_interface import ProcessStatus, ProcessStage
    from src.email_handler.outlook_client import OutlookClient
    from src.email_handler.attachment_handler import AttachmentHandler
    from src.document_processor.textract_processor import TextractProcessor
    from src.document_processor.excel_processor import ExcelProcessor
    from src.services.data_combiner import DataCombiner
    
    # Create temp directories and DB
    test_dirs = setup_test_environment()
    temp_db_path = test_dirs["db_path"]
    
    # Configure dependencies with mocks
    with patch('src.email_handler.outlook_client.ConfidentialClientApplication'), \
         patch('src.email_handler.outlook_client.requests'), \
         patch('boto3.client'), \
         patch('builtins.open'):
             
        # Configure dependencies
        configure_dependencies()
        
        # Create process controller
        process_control = ProcessControl(temp_db_path)
        
        # Start a test process
        process_id = f"TEST_E2E_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        process_control.start_process(process_id)
        
        # Test stage transitions
        stages = [
            (ProcessStage.EMAIL_PROCESSING, ProcessStatus.RUNNING),
            (ProcessStage.EMAIL_PROCESSING, ProcessStatus.COMPLETED),
            (ProcessStage.DOCUMENT_EXTRACTION, ProcessStatus.RUNNING),
            (ProcessStage.DOCUMENT_EXTRACTION, ProcessStatus.COMPLETED),
            (ProcessStage.DATA_VALIDATION, ProcessStatus.RUNNING),
            (ProcessStage.DATA_VALIDATION, ProcessStatus.COMPLETED),
            (ProcessStage.COMPLETION, ProcessStatus.COMPLETED)
        ]
        
        for stage, status in stages:
            process_control.update_stage(process_id, stage, status)
            current_status = process_control.get_process_status(process_id)
            assert current_status['current_stage'] == stage.value
            assert current_status['status'] == status.value
        
        # Get process timeline
        timeline = process_control.get_process_timeline(process_id)
        assert len(timeline) >= len(stages), f"Expected {len(stages)} timeline entries, got {len(timeline)}"
        
        # Verify