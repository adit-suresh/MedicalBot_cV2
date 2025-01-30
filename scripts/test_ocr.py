import sys
import os
import logging
from typing import Dict

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.document_processor.ocr_processor import OCRProcessor
from src.utils.logger import setup_logger
from config.settings import RAW_DATA_DIR, PROCESSED_DATA_DIR

def test_ocr_processing():
    logger = setup_logger('ocr_test')
    logger.info("Starting OCR test...")

    try:
        ocr_processor = OCRProcessor()
        
        # Process all files in RAW_DATA_DIR
        for email_dir in os.listdir(RAW_DATA_DIR):
            email_path = os.path.join(RAW_DATA_DIR, email_dir)
            
            # Skip if not a directory
            if not os.path.isdir(email_path):
                continue
                
            logger.info(f"\nProcessing directory: {email_dir}")
            
            # Process each file in the email directory
            for filename in os.listdir(email_path):
                file_path = os.path.join(email_path, filename)
                
                # Skip if not a file
                if not os.path.isfile(file_path):
                    continue
                    
                logger.info(f"\nProcessing file: {filename}")
                
                try:
                    # Process the document
                    processed_path, extracted_data = ocr_processor.process_document(file_path)
                    
                    logger.info(f"Successfully processed file")
                    logger.info(f"Processed file saved at: {processed_path}")
                    logger.info("Extracted data:")
                    
                    # Log extracted data
                    for key, value in extracted_data.items():
                        logger.info(f"  {key}: {value}")
                    
                    # Validate extracted data
                    validate_extracted_data(extracted_data, filename, logger)
                    
                except Exception as e:
                    logger.error(f"Failed to process {filename}: {str(e)}")
                    continue

        logger.info("\nOCR testing completed!")
        return True

    except Exception as e:
        logger.error(f"OCR test failed: {str(e)}")
        return False

def validate_extracted_data(data: Dict, filename: str, logger: logging.Logger) -> None:
    """Validate the extracted data based on file type and name."""
    
    # Check for expected data based on filename
    filename_lower = filename.lower()
    
    if 'passport' in filename_lower:
        if not data.get('passport_number'):
            logger.warning("No passport number found in passport document")
        else:
            logger.info("✓ Found passport number")
            
    if 'emirates' in filename_lower or 'eid' in filename_lower:
        if not data.get('emirates_id'):
            logger.warning("No Emirates ID found in Emirates ID document")
        else:
            logger.info("✓ Found Emirates ID")
            
    # Add any other specific validations based on your document types

if __name__ == "__main__":
    success = test_ocr_processing()
    sys.exit(0 if success else 1)