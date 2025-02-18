import os
import logging
from src.document_processor.textract_processor import TextractProcessor

# Configure detailed logging
logging.basicConfig(level=logging.DEBUG,
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_passport_extraction():
    processor = TextractProcessor()
    
    # Get passport files
    test_files = [
        f for f in os.listdir("test_files") 
        if "pp" in f.lower() or "passport" in f.lower()
    ]
    
    logger.info(f"Found {len(test_files)} passport files")
    
    for filename in test_files:
        file_path = os.path.join("test_files", filename)
        logger.info(f"\nProcessing: {filename}")
        
        try:
            # Process with debug info
            extracted_data = processor.process_document(file_path, 'passport')
            
            logger.info("Extracted Data:")
            for key, value in extracted_data.items():
                if value != '.':
                    logger.info(f"{key}: {value}")
                    
            # Validate extraction
            required_fields = ['passport_number', 'surname', 'given_names']
            missing_fields = [
                field for field in required_fields 
                if field not in extracted_data or extracted_data[field] == '.'
            ]
            
            if missing_fields:
                logger.warning(f"Missing required fields: {missing_fields}")
            else:
                logger.info("✓ All required fields extracted")
                
        except Exception as e:
            logger.error(f"Error processing {filename}: {str(e)}")

if __name__ == "__main__":
    test_passport_extraction()