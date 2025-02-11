import os
import logging
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd

from src.document_processor.textract_processor import TextractProcessor
from src.document_processor.excel_processor import ExcelProcessor
from src.services.data_combiner import DataCombiner

# Configure detailed logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_data_combination():
    """Test complete data combination workflow with detailed logging."""
    
    # Initialize processors
    textract_processor = TextractProcessor()
    excel_processor = ExcelProcessor()
    data_combiner = DataCombiner(textract_processor, excel_processor)
    
    # Test files
    test_files_dir = "test_files"
    document_paths = {}
    excel_path = None
    
    logger.info("Scanning for documents...")
    
    # Collect test files with better detection
    for filename in os.listdir(test_files_dir):
        file_path = os.path.join(test_files_dir, filename)
        
        # Detect document types
        if filename.lower().endswith(('.pdf', '.jpg', '.jpeg', '.png')):
            # Check file content or name to determine type
            if 'passport' in filename.lower():
                document_paths['passport'] = file_path
                logger.info(f"Found passport document: {filename}")
            elif 'emirates' in filename.lower() or 'eid' in filename.lower():
                document_paths['emirates_id'] = file_path
                logger.info(f"Found Emirates ID document: {filename}")
            elif 'visa' in filename.lower():
                document_paths['visa'] = file_path
                logger.info(f"Found visa document: {filename}")
                
        elif filename.lower().endswith(('.xlsx', '.xls')):
            if 'template' in filename.lower():
                template_path = file_path
                logger.info(f"Found template file: {filename}")
            elif not excel_path:  # Take first non-template Excel file
                excel_path = file_path
                logger.info(f"Found data Excel file: {filename}")
    
    # Verify files
    logger.info("\nVerifying documents...")
    if not document_paths:
        logger.warning("No documents found!")
    else:
        logger.info(f"Found documents: {list(document_paths.keys())}")
    
    if not excel_path:
        logger.warning("No Excel data file found!")
    
    template_path = os.path.join(test_files_dir, "template.xlsx")
    if not os.path.exists(template_path):
        logger.error("Template file not found!")
        return
    
    # Create output directory
    output_dir = "test_output"
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(
        output_dir,
        f"populated_template_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    )
    
    try:
        # Process each document first to verify extraction
        logger.info("\nTesting individual document extraction:")
        for doc_type, file_path in document_paths.items():
            logger.info(f"\nProcessing {doc_type}...")
            try:
                extracted_data = textract_processor.process_document(file_path, doc_type)
                logger.info("Extracted data:")
                for key, value in extracted_data.items():
                    logger.info(f"  {key}: {value}")
            except Exception as e:
                logger.error(f"Error processing {doc_type}: {str(e)}")
        
        # Process Excel if available
        if excel_path:
            logger.info("\nProcessing Excel data:")
            try:
                df, errors = excel_processor.process_excel(excel_path)
                if not df.empty:
                    logger.info("Excel data (first row):")
                    for col in df.columns:
                        logger.info(f"  {col}: {df[col].iloc[0]}")
                if errors:
                    logger.warning("Excel validation errors:")
                    for error in errors:
                        logger.warning(f"  {error}")
            except Exception as e:
                logger.error(f"Error processing Excel: {str(e)}")
        
        # Combine data and populate template
        logger.info("\nCombining data and populating template...")
        result = data_combiner.combine_and_populate_template(
            template_path,
            output_path,
            document_paths,
            excel_path
        )
        
        # Check results
        if result['status'] == 'success':
            logger.info("Template populated successfully!")
            logger.info(f"Output file: {result['output_path']}")
            
            if result['missing_fields']:
                logger.warning("\nMissing fields:")
                for field in result['missing_fields']:
                    logger.warning(f"- {field}")
            else:
                logger.info("\nAll required fields populated!")
                
        else:
            logger.error(f"Data combination failed: {result.get('error')}")
            
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")
        raise

if __name__ == "__main__":
    # Load environment variables
    load_dotenv()
    
    # Run test
    test_data_combination()