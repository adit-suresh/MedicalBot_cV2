import logging
import logging.handlers
import os
from datetime import datetime
from typing import Optional
import json
from pathlib import Path

class ApplicationLogger:
    """Configures and manages application logging."""

    def __init__(self, 
                 log_dir: str = "logs",
                 app_name: str = "insurance_processor",
                 log_level: int = logging.INFO):
        self.log_dir = log_dir
        self.app_name = app_name
        self.log_level = log_level
        
        # Create log directory
        Path(log_dir).mkdir(parents=True, exist_ok=True)
        
        # Configure logging
        self._configure_logging()

    def _configure_logging(self) -> None:
        """Configure logging with multiple handlers."""
        # Create formatters
        standard_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        json_formatter = JsonFormatter()

        # File handlers
        file_handler = logging.handlers.RotatingFileHandler(
            filename=os.path.join(
                self.log_dir,
                f"{self.app_name}_{datetime.now().strftime('%Y%m%d')}.log"
            ),
            maxBytes=10485760,  # 10MB
            backupCount=10
        )
        file_handler.setFormatter(standard_formatter)
        
        # JSON file handler for structured logging
        json_handler = logging.handlers.RotatingFileHandler(
            filename=os.path.join(
                self.log_dir,
                f"{self.app_name}_{datetime.now().strftime('%Y%m%d')}.json"
            ),
            maxBytes=10485760,
            backupCount=10
        )
        json_handler.setFormatter(json_formatter)
        
        # Error file handler
        error_handler = logging.handlers.RotatingFileHandler(
            filename=os.path.join(
                self.log_dir,
                f"{self.app_name}_errors_{datetime.now().strftime('%Y%m%d')}.log"
            ),
            maxBytes=10485760,
            backupCount=10
        )
        error_handler.setFormatter(standard_formatter)
        error_handler.setLevel(logging.ERROR)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(standard_formatter)

        # Configure root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(self.log_level)
        
        # Remove existing handlers
        root_logger.handlers = []
        
        # Add handlers
        root_logger.addHandler(file_handler)
        root_logger.addHandler(json_handler)
        root_logger.addHandler(error_handler)
        root_logger.addHandler(console_handler)

class JsonFormatter(logging.Formatter):
    """JSON formatter for structured logging."""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_data = {
            'timestamp': self.formatTime(record),
            'logger': record.name,
            'level': record.levelname,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno
        }
        
        # Add exception info if present
        if record.exc_info:
            log_data['exception'] = {
                'type': record.exc_info[0].__name__,
                'message': str(record.exc_info[1]),
                'traceback': self.formatException(record.exc_info)
            }

        # Add extra fields if present
        if hasattr(record, 'extra_fields'):
            log_data.update(record.extra_fields)

        return json.dumps(log_data)

def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Get a configured logger instance.
    
    Args:
        name: Logger name (optional)
        
    Returns:
        logging.Logger: Configured logger instance
    """
    return logging.getLogger(name)

# Helper functions for structured logging
def log_process_event(logger: logging.Logger,
                     event_type: str,
                     process_id: str,
                     details: Optional[dict] = None,
                     level: int = logging.INFO) -> None:
    """
    Log a process-related event with structured data.
    
    Args:
        logger: Logger instance
        event_type: Type of event
        process_id: Process identifier
        details: Additional event details
        level: Logging level
    """
    extra = {
        'extra_fields': {
            'event_type': event_type,
            'process_id': process_id,
            'details': details or {}
        }
    }
    logger.log(level, f"Process event: {event_type}", extra=extra)

def log_document_event(logger: logging.Logger,
                      event_type: str,
                      document_id: str,
                      document_type: str,
                      details: Optional[dict] = None,
                      level: int = logging.INFO) -> None:
    """
    Log a document-related event with structured data.
    
    Args:
        logger: Logger instance
        event_type: Type of event
        document_id: Document identifier
        document_type: Type of document
        details: Additional event details
        level: Logging level
    """
    extra = {
        'extra_fields': {
            'event_type': event_type,
            'document_id': document_id,
            'document_type': document_type,
            'details': details or {}
        }
    }
    logger.log(level, f"Document event: {event_type}", extra=extra)