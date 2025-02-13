import os
import logging
from datetime import datetime
from dotenv import load_dotenv

from src.services.email_validator import EmailValidator
from src.services.data_combiner import DataCombiner
from src.document_processor.textract_processor import TextractProcessor
from src.document_processor.excel_processor import ExcelProcessor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_validation_workflow():
    """Test complete validation workflow."""
    
    # Initialize services
    validator = EmailValidator()
    textract_processor = TextractProcessor()
    excel_processor = ExcelProcessor()
    data_combiner = DataCombiner(textract_processor, excel_processor)
    
    # Test files
    test_files_dir = "test_files"
    document_paths = {}
    excel_path = None
    
    # Collect test files
    logger.info("Scanning for test files...")
    for filename in os.listdir(test_files_dir):
        file_path = os.path.join(test_files_dir, filename)
        if filename.lower().endswith(('.pdf', '.jpg', '.jpeg', '.png')):
            if 'passport' in filename.lower():
                document_paths['passport'] = file_path
                logger.info(f"Found passport: {filename}")
            elif 'emirates' in filename.lower():
                document_paths['emirates_id'] = file_path
                logger.info(f"Found Emirates ID: {filename}")
            elif 'visa' in filename.lower():
                document_paths['visa'] = file_path
                logger.info(f"Found visa: {filename}")
        elif filename.lower().endswith(('.xlsx', '.xls')):
            if 'template' in filename.lower():
                template_path = file_path
                logger.info(f"Found template: {filename}")
            else:
                excel_path = file_path
                logger.info(f"Found data file: {filename}")
    
    # Create output path
    output_dir = "test_output"
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(
        output_dir,
        f"populated_template_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    )
    
    try:
        # First combine the data
        logger.info("\nCombining data...")
        result = data_combiner.combine_and_populate_template(
            template_path,
            output_path,
            document_paths,
            excel_path
        )
        
        if result['status'] != 'success':
            logger.error(f"Data combination failed: {result.get('error')}")
            return
            
        # Collect metadata
        metadata = {
            'Documents Processed': len(document_paths),
            'Missing Fields': len(result.get('missing_fields', [])),
            'Process Date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Send for validation
        logger.info("\nSending for validation...")
        validation_result = validator.send_for_validation(
            output_path,
            f"TEST_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            metadata
        )
        
        if validation_result['status'] == 'sent':
            logger.info("Validation email sent successfully!")
            logger.info(f"Validation ID: {validation_result['validation_id']}")
            logger.info(f"Sent to: {validation_result['sent_to']}")
            
            # Check status
            status = validator.check_validation_status(
                validation_result['validation_id']
            )
            logger.info(f"\nValidation status: {status['status']}")
            
        else:
            logger.error(f"Failed to send validation email: {validation_result.get('error')}")
            
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")

if __name__ == "__main__":
    # Load environment variables
    load_dotenv()
    
    # Check required environment variables
    required_vars = ['CLIENT_ID', 'CLIENT_SECRET', 'TENANT_ID', 'VALIDATOR_EMAIL']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        logger.error("Please set these in your .env file")
        exit(1)
    
    # Run test
    test_validation_workflow()