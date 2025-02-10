from src.utils.dependency_container import container
from src.utils.process_control_interface import IProcessControl
from src.utils.process_control import ProcessControl
from src.utils.slack_notifier import SlackNotifier
from src.utils.portal_checker import PortalChecker
from src.document_processor.ocr_processor import OCRProcessor
from src.document_processor.data_extractor import DataExtractor
from src.database.db_manager import DatabaseManager
from src.email_handler.outlook_client import OutlookClient
from src.email_handler.attachment_handler import AttachmentHandler

def configure_dependencies() -> None:
    """Configure all application dependencies."""
    
    # Register core services
    container.register(IProcessControl, ProcessControl)
    container.register_instance(SlackNotifier, SlackNotifier())
    container.register_instance(PortalChecker, PortalChecker())
    
    # Register document processing
    container.register_instance(OCRProcessor, OCRProcessor())
    container.register_instance(DataExtractor, DataExtractor())
    
    # Register database
    container.register_instance(DatabaseManager, DatabaseManager())
    
    # Register email handling
    container.register_instance(OutlookClient, OutlookClient())
    container.register_instance(AttachmentHandler, AttachmentHandler())

def initialize_application() -> None:
    """Initialize the application."""
    # Configure dependencies
    configure_dependencies()
    
    # Additional initialization steps can be added here
    pass