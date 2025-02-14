# test_ocr_only.py

import os
import logging
from src.document_processor.textract_processor import TextractProcessor

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_single_document():
    """Test OCR extraction on a single document"""
    
    # Initialize processor
    processor = TextractProcessor()
    
    # Test file path (let's start with visa)
    test_file = "test_files/27715 VISA.pdf"  # Your visa file
    
    logger.info(f"\nTesting document: {test_file}")
    
    try:
        # Process document
        extracted_data = processor.process_document(test_file, 'visa')
        
        # Print raw output
        logger.info("\nExtracted Data:")
        for key, value in extracted_data.items():
            logger.info(f"{key}: {value}")
            
        # Validate data
        logger.info("\nValidation:")
        required_fields = {
            'entry_permit_no': str,
            'full_name': str,
            'nationality': str,
            'passport_number': str
        }
        
        for field, field_type in required_fields.items():
            value = extracted_data.get(field)
            is_valid = value is not None and value != '.' and isinstance(value, field_type)
            logger.info(f"{field}: {'✓' if is_valid else '✗'} - {value}")
            
    except Exception as e:
        logger.error(f"Error processing document: {str(e)}")

if __name__ == "__main__":
    test_single_document()