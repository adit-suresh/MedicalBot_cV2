import os
import logging
from src.document_processor.textract_processor import TextractProcessor

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_document_detection():
    """Test document type detection."""
    processor = TextractProcessor()
    
    # Get all PDF/image files in test directory
    test_dir = "test_files"
    test_files = [f for f in os.listdir(test_dir) 
                  if f.lower().endswith(('.pdf', '.jpg', '.jpeg', '.png'))]
    
    logger.info(f"Found {len(test_files)} documents to test")
    
    for filename in test_files:
        file_path = os.path.join(test_dir, filename)
        logger.info(f"\nProcessing: {filename}")
        
        try:
            # Process document without specifying type
            extracted_data = processor.process_document(file_path)
            
            # Show extracted data
            logger.info("Extracted Data:")
            for key, value in extracted_data.items():
                if value != '.':  # Only show non-empty fields
                    logger.info(f"{key}: {value}")
            
        except Exception as e:
            logger.error(f"Error processing {filename}: {str(e)}")

if __name__ == "__main__":
    test_document_detection()