from enum import Enum
from typing import Dict, Optional, Any
from datetime import datetime
import logging
import traceback
import functools

logger = logging.getLogger(__name__)

class ErrorSeverity(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

class ErrorCategory(Enum):
    EMAIL = "email"
    DOCUMENT = "document"
    DATABASE = "database"
    NETWORK = "network"
    SECURITY = "security"
    PROCESS = "process"
    VALIDATION = "validation"
    EXTERNAL_SERVICE = "external_service"

class ApplicationError(Exception):
    """Base exception for all application errors."""
    
    def __init__(self, 
                 message: str,
                 category: ErrorCategory,
                 severity: ErrorSeverity,
                 details: Optional[Dict] = None):
        super().__init__(message)
        self.category = category
        self.severity = severity
        self.details = details or {}
        self.timestamp = datetime.now()
        self.traceback = traceback.format_exc()

class ProcessingError(ApplicationError):
    """Error during document or data processing."""
    def __init__(self, message: str, details: Optional[Dict] = None):
        super().__init__(
            message,
            ErrorCategory.PROCESS,
            ErrorSeverity.MEDIUM,
            details
        )

class ValidationError(ApplicationError):
    """Error during data validation."""
    def __init__(self, message: str, details: Optional[Dict] = None):
        super().__init__(
            message,
            ErrorCategory.VALIDATION,
            ErrorSeverity.LOW,
            details
        )

class ServiceError(ApplicationError):
    """Error from external service."""
    def __init__(self, message: str, details: Optional[Dict] = None):
        super().__init__(
            message,
            ErrorCategory.EXTERNAL_SERVICE,
            ErrorSeverity.HIGH,
            details
        )

def handle_errors(error_category: ErrorCategory, error_severity: ErrorSeverity):
    """Decorator for standardized error handling."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except ApplicationError as e:
                # Already handled error, re-raise
                raise
            except Exception as e:
                # Convert to ApplicationError
                raise ApplicationError(
                    str(e),
                    error_category,
                    error_severity,
                    {
                        'function': func.__name__,
                        'args': str(args),
                        'kwargs': str(kwargs)
                    }
                )
        return wrapper
    return decorator

def log_error(error: ApplicationError, context: Optional[Dict] = None) -> None:
    """Standardized error logging."""
    error_info = {
        'timestamp': error.timestamp.isoformat(),
        'category': error.category.value,
        'severity': error.severity.value,
        'message': str(error),
        'details': error.details,
        'traceback': error.traceback
    }
    
    if context:
        error_info['context'] = context

    if error.severity in [ErrorSeverity.HIGH, ErrorSeverity.CRITICAL]:
        logger.error(f"Critical error occurred: {error_info}")
    else:
        logger.warning(f"Error occurred: {error_info}")

def retry_on_error(max_attempts: int = 3, delay_seconds: int = 1):
    """Decorator for retrying operations on failure."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            import time
            last_error = None
            
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_error = e
                    if attempt < max_attempts - 1:
                        time.sleep(delay_seconds)
                        continue
            
            raise last_error
        return wrapper
    return decorator

# Example usage:
# @handle_errors(ErrorCategory.DOCUMENT, ErrorSeverity.MEDIUM)
# def process_document(file_path: str) -> Dict:
#     # Processing logic here
#     pass