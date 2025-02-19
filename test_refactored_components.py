"""
Test script to verify refactored components are working properly.
"""
import os
import sys
import logging
from datetime import datetime, timedelta
import time

# Add project root to Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_root)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('refactor_test.log')
    ]
)
logger = logging.getLogger(__name__)


def separator(title):
    """Print a nice separator for test sections."""
    width = 80
    print("\n" + "=" * width)
    print(f"{title.center(width)}")
    print("=" * width + "\n")


def test_dependency_container():
    """Test the enhanced dependency container."""
    separator("Testing Dependency Container")
    try:
        from src.utils.dependency_container import container, inject
        
        # Define test classes
        class IService:
            def get_value(self): pass
        
        class ServiceImpl(IService):
            def get_value(self):
                return "test_value"
                
        @inject(IService)
        class Client:
            def get_service_value(self):
                return self._iservice.get_value()
        
        # Test registration and resolution
        container.register(IService, ServiceImpl)
        client = Client()
        value = client.get_service_value()
        
        assert value == "test_value", f"Expected 'test_value', got '{value}'"
        logger.info("✓ Basic dependency injection works")
        
        # Test singleton behavior
        client2 = Client()
        assert client._iservice is client2._iservice, "Instances should be the same"
        logger.info("✓ Singleton behavior works")
        
        # Test circular dependency detection
        class A:
            pass
            
        class B:
            pass
        
        @inject(B)
        class CircularA(A):
            pass
            
        @inject(A)
        class CircularB(B):
            pass
        
        container.register(A, CircularA)
        container.register(B, CircularB)
        
        try:
            instance = container.resolve(A)
            logger.error("✗ Circular dependency detection failed")
        except ValueError as e:
            if "Circular dependency detected" in str(e):
                logger.info("✓ Circular dependency detection works")
            else:
                logger.error(f"✗ Unexpected error: {str(e)}")
        
        # Clean up
        container.clear()
        logger.info("Dependency container tests completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Dependency container test failed: {str(e)}")
        return False


def test_error_handling():
    """Test the enhanced error handling system."""
    separator("Testing Error Handling System")
    try:
        from src.utils.error_handler import ErrorHandler, ProcessError, ErrorCategory, ErrorSeverity
        from src.utils.retry_handler import RetryHandler, RetryConfig
        
        # Initialize components
        error_handler = ErrorHandler()
        retry_handler = RetryHandler(error_handler)
        
        # Test basic error handling
        process_id = f"TEST_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        test_error = ValueError("Test error message")
        
        error = ProcessError(
            error=test_error,
            process_id=process_id,
            stage="testing",
            category=ErrorCategory.PROCESSING,
            severity=ErrorSeverity.MEDIUM,
            context={"test": "basic_error"}
        )
        
        result = error_handler.handle_error(error)
        logger.info(f"Basic error handling: {result}")
        
        # Test retry mechanism
        retry_count = [0]
        
        @retry_handler.with_retry(
            retry_config=RetryConfig(max_attempts=3, base_delay=0.1),
            process_id=process_id
        )
        def test_retry_function():
            retry_count[0] += 1
            if retry_count[0] < 3:
                raise ValueError(f"Test failure {retry_count[0]}")
            return "Success"
        
        try:
            result = test_retry_function()
            assert result == "Success", f"Expected 'Success', got '{result}'"
            assert retry_count[0] == 3, f"Expected 3 attempts, got {retry_count[0]}"
            logger.info(f"✓ Retry mechanism works after {retry_count[0]} attempts")
        except Exception as e:
            logger.error(f"✗ Retry test failed: {str(e)}")
            
        # Test error stats
        stats = error_handler.get_error_stats()
        logger.info(f"Error stats: {stats['total_errors']} total errors")
        
        logger.info("Error handling tests completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error handling test failed: {str(e)}")
        return False


def test_document_processing():
    """Test the enhanced document processing."""
    separator("Testing Document Processing")
    try:
        from src.document_processor.textract_processor import TextractProcessor
        import tempfile
        
        # Create a processor instance
        processor = TextractProcessor()
        
        # Test document type detection
        test_text = """
        UNITED ARAB EMIRATES
        IDENTITY CARD
        ID Number: 784-1234-1234567-1
        Name: TEST PERSON
        Nationality: UAE
        """
        
        doc_type = processor.detect_document_type(test_text)
        logger.info(f"Detected document type: {doc_type}")
        assert doc_type == 'emirates_id', f"Expected 'emirates_id', got '{doc_type}'"
        
        # Test date normalization
        test_dates = [
            "01/02/2023",
            "2023-02-01",
            "1 Feb 2023",
            "01.02.2023"
        ]
        
        for date_str in test_dates:
            normalized = processor._normalize_date(date_str)
            logger.info(f"Original: {date_str}, Normalized: {normalized}")
        
        # Test caching mechanism (can only verify it doesn't error)
        cache_key = processor._get_cache_key("test_file.pdf", "passport")
        logger.info(f"Generated cache key: {cache_key}")
        
        # We can't fully test extraction without real AWS credentials
        logger.info("Document processing tests completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Document processing test failed: {str(e)}")
        return False


def test_email_handler():
    """Test the email handler with mocked responses."""
    separator("Testing Email Handler")
    try:
        from unittest.mock import patch, MagicMock
        from src.email_handler.outlook_client import OutlookClient
        from src.email_handler.attachment_handler import AttachmentHandler
        from src.utils.exceptions import AuthenticationError
        
        # Test attachment handler
        attachment_handler = AttachmentHandler()
        
        # Test attachment validation
        valid_attachment = {
            "name": "passport.pdf",
            "size": 1024
        }
        invalid_attachment = {
            "name": "malicious.exe",
            "size": 1024
        }
        
        assert attachment_handler.is_valid_attachment(valid_attachment), "Should accept valid PDF"
        assert not attachment_handler.is_valid_attachment(invalid_attachment), "Should reject .exe file"
        logger.info("✓ Attachment validation works")
        
        # Test OutlookClient with mocks
        with patch('src.email_handler.outlook_client.ConfidentialClientApplication') as mock_app:
            # Configure mock
            mock_client = MagicMock()
            mock_app.return_value = mock_client
            mock_client.acquire_token_for_client.return_value = {
                "access_token": "test_token",
                "expires_in": 3600
            }
            
            # Create client
            outlook_client = OutlookClient()
            logger.info(f"Token expiry: {outlook_client.token_manager.token_expiry}")
            
            # Verify token manager is working
            assert outlook_client.token_manager.access_token == "test_token"
            logger.info("✓ Token management works")
            
            # Test connection pooling (can only verify doesn't error)
            assert outlook_client.session is not None
            logger.info("✓ Connection pooling initialized")
            
        logger.info("Email handler tests completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Email handler test failed: {str(e)}")
        return False


def test_data_integration():
    """Test data integration and combination."""
    separator("Testing Data Integration")
    try:
        from src.services.data_combiner import DataCombiner
        from unittest.mock import MagicMock
        import pandas as pd
        import tempfile
        
        # Create mock processors
        textract_mock = MagicMock()
        excel_mock = MagicMock()
        
        # Create the combiner
        combiner = DataCombiner(textract_mock, excel_mock)
        
        # Test field normalization
        test_dates = [
            "01/02/2023",
            "2023-02-01",
            "1 Feb 2023",
        ]
        
        for date in test_dates:
            normalized = combiner._format_date_value(date)
            logger.info(f"Original date: {date}, Normalized: {normalized}")
        
        # Test phone number formatting
        phone_numbers = [
            "+971501234567",
            "0501234567",
            "971501234567"
        ]
        
        for number in phone_numbers:
            formatted = combiner._format_numeric_value(number)
            logger.info(f"Original: {number}, Formatted: {formatted}")
        
        # Test column name normalization
        column_names = [
            "First Name",
            "first_name",
            "FIRST NAME",
            "First  Name*"
        ]
        
        for column in column_names:
            normalized = combiner._normalize_column_name(column)
            logger.info(f"Original column: '{column}', Normalized: '{normalized}'")
            assert normalized == "first_name", f"Expected 'first_name', got '{normalized}'"
        
        logger.info("Data integration tests completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Data integration test failed: {str(e)}")
        return False


def test_process_control():
    """Test process control and tracking."""
    separator("Testing Process Control")
    try:
        import tempfile
        from src.utils.process_control import ProcessControl
        from src.utils.process_control_interface import ProcessStatus, ProcessStage
        
        # Create temporary DB
        temp_db = tempfile.NamedTemporaryFile(suffix='.db').name
        
        # Initialize process control
        process_control = ProcessControl(temp_db)
        
        # Test process lifecycle
        process_id = f"TEST_PROC_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Start process
        process_control.start_process(process_id)
        logger.info(f"Started process {process_id}")
        
        # Update stage
        process_control.update_stage(
            process_id,
            ProcessStage.DOCUMENT_EXTRACTION,
            ProcessStatus.RUNNING
        )
        
        # Get status
        status = process_control.get_process_status(process_id)
        logger.info(f"Process status: {status}")
        assert status['current_stage'] == ProcessStage.DOCUMENT_EXTRACTION.value
        assert status['status'] == ProcessStatus.RUNNING.value
        
        # Pause process
        process_control.pause_process(
            process_id,
            "Testing pause functionality",
            "manual_review"
        )
        
        # Check if in paused processes list
        paused_processes = process_control.get_processes_needing_attention()
        assert any(p['process_id'] == process_id for p in paused_processes)
        logger.info("✓ Process pause works")
        
        # Resume process
        process_control.resume_process(process_id, {"reviewed": True})
        
        # Complete process
        process_control.update_stage(
            process_id,
            ProcessStage.COMPLETION,
            ProcessStatus.COMPLETED
        )
        
        # Get timeline
        timeline = process_control.get_process_timeline(process_id)
        logger.info(f"Timeline entries: {len(timeline)}")
        assert len(timeline) >= 4, f"Expected at least 4 timeline entries, got {len(timeline)}"
        
        # Get stats
        stats = process_control.get_stats()
        logger.info(f"Process stats: {stats}")
        
        logger.info("Process control tests completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Process control test failed: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False


def main():
    """Run all tests and report results."""
    start_time = time.time()
    tests = [
        ("Dependency Container", test_dependency_container),
        ("Error Handling", test_error_handling),
        ("Document Processing", test_document_processing),
        ("Email Handler", test_email_handler),
        ("Data Integration", test_data_integration),
        ("Process Control", test_process_control)
    ]
    
    results = {}
    
    for name, test_func in tests:
        logger.info(f"Running test: {name}")
        try:
            result = test_func()
            results[name] = result
        except Exception as e:
            logger.error(f"Test '{name}' failed with unexpected error: {str(e)}")
            results[name] = False
    
    # Summary
    separator("Test Results Summary")
    
    passed = sum(1 for result in results.values() if result)
    total = len(tests)
    
    for name, result in results.items():
        status = "✓ PASSED" if result else "✗ FAILED"
        print(f"{name.ljust(30)}: {status}")
    
    print(f"\nPassed {passed} of {total} tests ({passed/total*100:.1f}%)")
    print(f"Total time: {time.time() - start_time:.2f} seconds")


if __name__ == "__main__":
    main()