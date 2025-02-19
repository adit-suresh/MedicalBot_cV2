from enum import Enum
import logging
from typing import Dict, Optional, List, Any
import traceback
from datetime import datetime
import json
import threading
import os
from collections import deque
import time

class ErrorSeverity(Enum):
    LOW = "low"          # Non-critical errors that don't affect core functionality
    MEDIUM = "medium"    # Errors that affect current process but not system
    HIGH = "high"        # Critical errors that need immediate attention
    FATAL = "fatal"      # System-breaking errors

class ErrorCategory(Enum):
    NETWORK = "network"          # Network/connectivity issues
    AUTHENTICATION = "auth"      # Authentication/authorization errors
    VALIDATION = "validation"    # Data validation errors
    PROCESSING = "processing"    # Processing/computation errors
    SYSTEM = "system"            # System-level errors
    EXTERNAL = "external"        # External service errors
    DATABASE = "database"        # Database-related errors

class ProcessError:
    """Enhanced error class with improved context tracking."""
    
    def __init__(self, 
                 error: Exception,
                 process_id: str,
                 stage: str,
                 category: ErrorCategory,
                 severity: ErrorSeverity,
                 context: Optional[Dict] = None):
        self.error = error
        self.process_id = process_id
        self.stage = stage
        self.category = category
        self.severity = severity
        self.context = context or {}
        self.timestamp = datetime.now()
        self.traceback = traceback.format_exc()
        self.error_id = f"ERR_{int(time.time())}_{hash(str(error))}"

    def to_dict(self) -> Dict:
        """Convert error to dictionary format with improved details."""
        return {
            'error_id': self.error_id,
            'error_type': self.error.__class__.__name__,
            'error_message': str(self.error),
            'process_id': self.process_id,
            'stage': self.stage,
            'category': self.category.value,
            'severity': self.severity.value,
            'context': self.context,
            'timestamp': self.timestamp.isoformat(),
            'traceback': self.traceback
        }
        
    def get_log_message(self) -> str:
        """Get formatted log message."""
        return (f"Error [{self.error_id}] in process {self.process_id} ({self.stage}): "
                f"{self.error.__class__.__name__}: {str(self.error)}")

class ErrorHandler:
    """Improved error handler with more robust tracking and notification."""
    
    def __init__(self, slack_notifier=None, log_dir: str = "logs"):
        self.logger = logging.getLogger(__name__)
        self.slack_notifier = slack_notifier
        self.error_counts: Dict[str, int] = {}  # Track error counts by category
        self.recent_errors: deque = deque(maxlen=100)  # Keep track of recent errors
        self.lock = threading.RLock()  # Thread safety for error stats
        self.log_dir = log_dir
        self._setup_logging()

    def _setup_logging(self) -> None:
        """Set up logging with proper directory creation and log rotation."""
        if not os.path.exists(self.log_dir):
            os.makedirs(self.log_dir, exist_ok=True)
            
        error_log_path = os.path.join(self.log_dir, "errors.log")
        
        # Configure handler only if not already configured
        if not self.logger.handlers:
            # File handler for all errors
            file_handler = logging.handlers.RotatingFileHandler(
                error_log_path,
                maxBytes=10485760,  # 10MB
                backupCount=5
            )
            formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - [%(process)d] - %(message)s'
            )
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
            
            # Critical errors go to a separate file
            critical_log_path = os.path.join(self.log_dir, "critical_errors.log")
            critical_handler = logging.handlers.RotatingFileHandler(
                critical_log_path,
                maxBytes=10485760,
                backupCount=5
            )
            critical_handler.setFormatter(formatter)
            critical_handler.setLevel(logging.CRITICAL)
            self.logger.addHandler(critical_handler)
            
            self.logger.setLevel(logging.ERROR)

    def handle_error(self, error: ProcessError) -> bool:
        """
        Handle an error based on its category and severity with improved tracking.
        
        Args:
            error: ProcessError object containing error details
            
        Returns:
            bool: True if error was handled, False if it needs escalation
        """
        try:
            # Log the error with appropriate severity level
            self._log_error(error)

            # Track error statistics with thread safety
            with self.lock:
                self._update_error_stats(error)
                self.recent_errors.append(error)

            # Handle based on severity
            if error.severity in [ErrorSeverity.HIGH, ErrorSeverity.FATAL]:
                self._handle_critical_error(error)
                return False
            elif error.severity == ErrorSeverity.MEDIUM:
                return self._handle_medium_error(error)
            else:
                return self._handle_low_error(error)

        except Exception as e:
            # Handle errors in the error handler to prevent cascading failures
            self.logger.critical(f"Error in error handler: {str(e)}\n{traceback.format_exc()}")
            return False

    def _log_error(self, error: ProcessError) -> None:
        """Log error with appropriate severity level using structured format."""
        log_message = error.get_log_message()
        
        # Create a structured log entry
        extra = {
            'error_context': error.to_dict()
        }
        
        if error.severity == ErrorSeverity.FATAL:
            self.logger.critical(log_message, extra=extra)
        elif error.severity == ErrorSeverity.HIGH:
            self.logger.error(log_message, extra=extra)
        elif error.severity == ErrorSeverity.MEDIUM:
            self.logger.warning(log_message, extra=extra)
        else:
            self.logger.info(log_message, extra=extra)
            
        # Write detailed error information to specific error log
        self._write_detailed_error_log(error)

    def _write_detailed_error_log(self, error: ProcessError) -> None:
        """Write detailed error information to a process-specific log file."""
        try:
            process_log_dir = os.path.join(self.log_dir, error.process_id)
            os.makedirs(process_log_dir, exist_ok=True)
            
            error_log_path = os.path.join(
                process_log_dir, 
                f"error_{error.timestamp.strftime('%Y%m%d_%H%M%S')}.json"
            )
            
            with open(error_log_path, 'w') as f:
                json.dump(error.to_dict(), f, indent=2)
                
        except Exception as e:
            self.logger.error(f"Failed to write detailed error log: {str(e)}")

    def _update_error_stats(self, error: ProcessError) -> None:
        """Update error statistics with improved categorization."""
        category = error.category.value
        self.error_counts[category] = self.error_counts.get(category, 0) + 1
        
        # Track subcategories
        subcategory = f"{category}:{error.error.__class__.__name__}"
        self.error_counts[subcategory] = self.error_counts.get(subcategory, 0) + 1
        
        # Track by process
        process_category = f"process:{error.process_id}"
        self.error_counts[process_category] = self.error_counts.get(process_category, 0) + 1

    def _handle_critical_error(self, error: ProcessError) -> None:
        """Handle high/fatal severity errors with immediate notification."""
        if self.slack_notifier:
            self.slack_notifier.send_error_alert(
                process_id=error.process_id,
                error_message=f"CRITICAL: {error.error.__class__.__name__}: {str(error.error)}",
                error_details={
                    'error_id': error.error_id,
                    'category': error.category.value,
                    'stage': error.stage,
                    'timestamp': error.timestamp.isoformat(),
                    'context': error.context
                },
                requires_attention=True
            )
            
        # Additional actions for fatal errors
        if error.severity == ErrorSeverity.FATAL:
            self._trigger_emergency_actions(error)
            
    def _trigger_emergency_actions(self, error: ProcessError) -> None:
        """Trigger emergency actions for fatal errors."""
        # Log to critical file
        with open(os.path.join(self.log_dir, "FATAL_ERRORS.log"), "a") as f:
            f.write(f"{datetime.now().isoformat()} - {error.error_id}: {error.get_log_message()}\n")
            
        # Could implement additional actions:
        # - Call emergency webhook
        # - Send SMS/text alert
        # - Trigger system shutdown/restart procedures

    def _handle_medium_error(self, error: ProcessError) -> bool:
        """Handle medium severity errors with retry logic."""
        category = error.category.value
        error_count = self.error_counts.get(category, 0)
        process_errors = self.error_counts.get(f"process:{error.process_id}", 0)
        
        # Determine if error can be retried
        can_retry = error_count < 5 and process_errors < 3
        
        # Notify if approaching limit
        if error_count == 3 or process_errors == 2:
            if self.slack_notifier:
                self.slack_notifier.send_notification(
                    message=f"Warning: Multiple {category} errors in process {error.process_id}",
                    severity="warning",
                    process_id=error.process_id
                )
                
        if not can_retry and self.slack_notifier:
            self.slack_notifier.send_error_alert(
                process_id=error.process_id,
                error_message=f"Multiple {category} errors (retry limit reached)",
                error_details=error.to_dict()
            )
            
        return can_retry

    def _handle_low_error(self, error: ProcessError) -> bool:
        """Handle low severity errors."""
        # Always log but continue processing
        return True

    def get_error_stats(self) -> Dict[str, Any]:
        """Get comprehensive error statistics."""
        with self.lock:
            # Count errors by severity
            severity_counts = {}
            for error in self.recent_errors:
                severity = error.severity.value
                severity_counts[severity] = severity_counts.get(severity, 0) + 1
            
            # Get errors from last 24 hours
            yesterday = datetime.now().timestamp() - 86400
            recent_24h = [e for e in self.recent_errors 
                         if e.timestamp.timestamp() > yesterday]
            
            return {
                'total_errors': sum(self.error_counts.values()),
                'counts_by_category': dict(self.error_counts),
                'counts_by_severity': severity_counts,
                'recent_errors': [e.to_dict() for e in list(self.recent_errors)[-10:]],
                'errors_last_24h': len(recent_24h)
            }
            
    def get_errors_by_process(self, process_id: str) -> List[Dict]:
        """Get all errors for a specific process."""
        with self.lock:
            process_errors = [e.to_dict() for e in self.recent_errors 
                             if e.process_id == process_id]
            return process_errors

    def clear_error_stats(self) -> None:
        """Clear error statistics."""
        with self.lock:
            self.error_counts.clear()
            self.recent_errors.clear()


# Export decorators for easier error handling
def handle_errors(error_category: ErrorCategory, error_severity: ErrorSeverity):
    """Decorator for standardized error handling."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                # Get process_id from args or kwargs if available
                process_id = "unknown"
                for arg in args:
                    if isinstance(arg, str) and (arg.startswith("PROC_") or arg.startswith("process_")):
                        process_id = arg
                        break
                if "process_id" in kwargs:
                    process_id = kwargs["process_id"]
                    
                # Get module and function name for better context
                module_name = func.__module__
                function_name = func.__qualname__
                
                # Create error and raise
                error = ProcessError(
                    error=e,
                    process_id=process_id,
                    stage=f"{module_name}.{function_name}",
                    category=error_category,
                    severity=error_severity,
                    context={
                        'args': str(args),
                        'kwargs': str(kwargs),
                    }
                )
                
                # Get global error handler and handle error
                from src.utils.dependency_container import container
                try:
                    error_handler = container.resolve(ErrorHandler)
                    error_handler.handle_error(error)
                except:
                    # Fallback if container resolution fails
                    error_handler = ErrorHandler()
                    error_handler.handle_error(error)
                
                raise
                
        return wrapper
    return decorator