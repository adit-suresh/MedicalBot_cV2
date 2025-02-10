import pytest
import logging
import json
import os
from datetime import datetime

from src.utils.error_handling import (
    ApplicationError, ProcessingError, ValidationError,
    ErrorCategory, ErrorSeverity, handle_errors, retry_on_error
)
from src.utils.logging_config import (
    ApplicationLogger, JsonFormatter,
    log_process_event, log_document_event
)

def test_application_error():
    """Test ApplicationError creation and properties."""
    error = ApplicationError(
        "Test error",
        ErrorCategory.PROCESS,
        ErrorSeverity.MEDIUM,
        {"test": "detail"}
    )
    
    assert str(error) == "Test error"
    assert error.category == ErrorCategory.PROCESS
    assert error.severity == ErrorSeverity.MEDIUM
    assert error.details == {"test": "detail"}
    assert isinstance(error.timestamp, datetime)
    assert error.traceback is not None

def test_specific_errors():
    """Test specific error types."""
    # Test ProcessingError
    proc_error = ProcessingError("Processing failed", {"file": "test.pdf"})
    assert proc_error.category == ErrorCategory.PROCESS
    assert proc_error.severity == ErrorSeverity.MEDIUM
    
    # Test ValidationError
    val_error = ValidationError("Invalid data", {"field": "name"})
    assert val_error.category == ErrorCategory.VALIDATION
    assert val_error.severity == ErrorSeverity.LOW

@handle_errors(ErrorCategory.PROCESS, ErrorSeverity.MEDIUM)
def function_that_fails():
    raise ValueError("Test error")

def test_error_handler_decorator():
    """Test error handler decorator."""
    with pytest.raises(ApplicationError) as exc_info:
        function_that_fails()
    
    error = exc_info.value
    assert error.category == ErrorCategory.PROCESS
    assert error.severity == ErrorSeverity.MEDIUM
    assert "Test error" in str(error)

@retry_on_error(max_attempts=2)
def failing_function():
    raise ValueError("Temporary error")

def test_retry_decorator():
    """Test retry decorator."""
    with pytest.raises(ValueError):
        failing_function()

class TestJsonFormatter:
    """Test JSON formatter for logging."""
    
    def test_format_basic_record(self):
        """Test basic log record formatting."""
        formatter = JsonFormatter()
        logger = logging.getLogger("test")
        
        # Create a log record
        record = logger.makeRecord(
            "test", logging.INFO, "test.py", 1,
            "Test message", (), None
        )
        
        # Format the record
        output = formatter.format(record)
        data = json.loads(output)
        
        assert data["message"] == "Test message"
        assert data["level"] == "INFO"
        assert data["logger"] == "test"

    def test_format_error_record(self):
        """Test formatting record with exception."""
        formatter = JsonFormatter()
        logger = logging.getLogger("test")
        
        try:
            raise ValueError("Test error")
        except ValueError:
            record = logger.makeRecord(
                "test", logging.ERROR, "test.py", 1,
                "Error occurred", (), exc_info=True
            )
        
        output = formatter.format(record)
        data = json.loads(output)
        
        assert "exception" in data
        assert data["exception"]["type"] == "ValueError"
        assert data["exception"]["message"] == "Test error"

def test_structured_logging(tmp_path):
    """Test structured logging helpers."""
    # Setup test logger
    log_dir = tmp_path / "logs"
    os.makedirs(log_dir)
    app_logger = ApplicationLogger(str(log_dir))
    logger = logging.getLogger("test")
    
    # Test process event logging
    log_process_event(
        logger,
        "process_started",
        "PROC_123",
        {"step": "initialization"}
    )
    
    # Test document event logging
    log_document_event(
        logger,
        "document_processed",
        "DOC_123",
        "passport",
        {"status": "success"}
    )
    
    # Verify log files were created
    assert os.path.exists(log_dir / f"insurance_processor_{datetime.now().strftime('%Y%m%d')}.json")
    assert os.path.exists(log_dir / f"insurance_processor_{datetime.now().strftime('%Y%m%d')}.log")