import os
import logging
from src.document_processor.excel_processor import ExcelProcessor
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_excel_extraction():
    """Test Excel data extraction."""
    
    # Initialize processor
    processor = ExcelProcessor()
    
    # Process each Excel file in test_files directory
    test_files_dir = "test_files"
    for filename in os.listdir(test_files_dir):
        if filename.lower().endswith(('.xlsx', '.xls')):
            file_path = os.path.join(test_files_dir, filename)
            
            logger.info(f"\nProcessing Excel file: {filename}")
            try:
                # Process Excel file
                df, errors = processor.process_excel(file_path)
                
                # Log extracted data
                logger.info("\nExtracted Data:")
                for column in df.columns:
                    values = df[column].tolist()
                    logger.info(f"{column}: {values}")
                
                # Log any validation errors
                if errors:
                    logger.warning("\nValidation Errors:")
                    for error in errors:
                        logger.warning(f"- {error}")
                
            except Exception as e:
                logger.error(f"Error processing {filename}: {str(e)}")

if __name__ == "__main__":
    # Load environment variables
    load_dotenv()
    
    if not os.path.exists("test_files"):
        logger.error("test_files directory not found")
        exit(1)
    
    # Run test
    test_excel_extraction()