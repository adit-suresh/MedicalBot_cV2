from enum import Enum
import logging
from typing import Dict, Optional, List
import traceback
from datetime import datetime
import json

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
    SYSTEM = "system"           # System-level errors
    EXTERNAL = "external"       # External service errors
    DATABASE = "database"       # Database-related errors

class ProcessError:
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

    def to_dict(self) -> Dict:
        """Convert error to dictionary format."""
        return {
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

class ErrorHandler:
    def __init__(self, slack_notifier=None):
        self.logger = logging.getLogger(__name__)
        self.slack_notifier = slack_notifier
        self.error_counts: Dict[str, int] = {}  # Track error counts by category
        self.recent_errors: List[ProcessError] = []  # Keep track of recent errors
        self.max_recent_errors = 100  # Maximum number of recent errors to store

    def handle_error(self, error: ProcessError) -> bool:
        """
        Handle an error based on its category and severity.
        Returns True if error was handled, False if it needs escalation.
        """
        try:
            # Log the error
            self._log_error(error)

            # Update error statistics
            self._update_error_stats(error)

            # Store in recent errors
            self._store_recent_error(error)

            # Handle based on severity
            if error.severity in [ErrorSeverity.HIGH, ErrorSeverity.FATAL]:
                self._handle_critical_error(error)
                return False
            elif error.severity == ErrorSeverity.MEDIUM:
                return self._handle_medium_error(error)
            else:
                return self._handle_low_error(error)

        except Exception as e:
            self.logger.error(f"Error in error handler: {str(e)}")
            return False

    def _log_error(self, error: ProcessError) -> None:
        """Log error with appropriate severity level."""
        log_message = (
            f"Process {error.process_id} error in stage {error.stage}:\n"
            f"Category: {error.category.value}, Severity: {error.severity.value}\n"
            f"Error: {str(error.error)}\n"
            f"Context: {json.dumps(error.context, indent=2)}"
        )

        if error.severity == ErrorSeverity.FATAL:
            self.logger.critical(log_message)
        elif error.severity == ErrorSeverity.HIGH:
            self.logger.error(log_message)
        elif error.severity == ErrorSeverity.MEDIUM:
            self.logger.warning(log_message)
        else:
            self.logger.info(log_message)

    def _update_error_stats(self, error: ProcessError) -> None:
        """Update error statistics."""
        category = error.category.value
        self.error_counts[category] = self.error_counts.get(category, 0) + 1

    def _store_recent_error(self, error: ProcessError) -> None:
        """Store error in recent errors list."""
        self.recent_errors.append(error)
        if len(self.recent_errors) > self.max_recent_errors:
            self.recent_errors.pop(0)

    def _handle_critical_error(self, error: ProcessError) -> None:
        """Handle high/fatal severity errors."""
        # Send immediate notification
        if self.slack_notifier:
            self.slack_notifier.send_error_alert(
                process_id=error.process_id,
                error_message=str(error.error),
                error_details=error.to_dict(),
                requires_attention=True
            )

    def _handle_medium_error(self, error: ProcessError) -> bool:
        """Handle medium severity errors with retry logic."""
        category = error.category.value
        if self.error_counts.get(category, 0) < 3:
            # Can retry
            return True
        else:
            # Too many retries, escalate
            if self.slack_notifier:
                self.slack_notifier.send_error_alert(
                    process_id=error.process_id,
                    error_message=f"Multiple {category} errors",
                    error_details=error.to_dict()
                )
            return False

    def _handle_low_error(self, error: ProcessError) -> bool:
        """Handle low severity errors."""
        # Log and continue
        return True

    def get_error_stats(self) -> Dict:
        """Get error statistics."""
        return {
            'counts_by_category': self.error_counts.copy(),
            'recent_errors': [e.to_dict() for e in self.recent_errors[-10:]],
            'total_errors': sum(self.error_counts.values())
        }

    def clear_error_stats(self) -> None:
        """Clear error statistics."""
        self.error_counts.clear()
        self.recent_errors.clear()