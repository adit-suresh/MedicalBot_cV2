import os
import logging
from dotenv import load_dotenv

from src.document_processor.textract_processor import TextractProcessor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_textract():
    """Test Textract processing with sample documents."""
    
    # Initialize processor
    processor = TextractProcessor()
    
    # Process each file in test_files directory
    test_files_dir = "test_files"
    for filename in os.listdir(test_files_dir):
        if filename.lower().endswith(('.pdf', '.jpg', '.jpeg', '.png')):
            file_path = os.path.join(test_files_dir, filename)
            
            logger.info(f"\nProcessing: {filename}")
            try:
                # Process document
                result = processor.process_document(file_path)
                
                # Log results
                logger.info("Extracted Data:")
                for key, value in result.items():
                    logger.info(f"{key}: {value}")
                    
            except Exception as e:
                logger.error(f"Error processing {filename}: {str(e)}")

if __name__ == "__main__":
    # Load environment variables
    load_dotenv()
    
    # Check AWS credentials
    if not os.getenv('AWS_ACCESS_KEY_ID'):
        logger.error("AWS credentials not found in .env file")
        exit(1)
    
    if not os.path.exists("test_files"):
        logger.error("test_files directory not found")
        exit(1)
    
    # Run test
    test_textract()