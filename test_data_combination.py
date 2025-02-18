import os
import logging
from src.document_processor.textract_processor import TextractProcessor
from src.document_processor.excel_processor import ExcelProcessor
from src.services.data_combiner import DataCombiner
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_data_combination():
    """Test data combination with multiple rows."""
    # Initialize processors
    textract = TextractProcessor()
    excel_processor = ExcelProcessor()
    data_combiner = DataCombiner(textract, excel_processor)
    
    # Create test output directory
    os.makedirs("test_output", exist_ok=True)
    
    try:
        # Step 1: Process document
        test_doc = "test_files/27715 VISA.pdf"
        logger.info(f"Processing document: {test_doc}")
        extracted_data = textract.process_document(test_doc, 'visa')
        
        logger.info("\nExtracted Data:")
        for key, value in extracted_data.items():
            logger.info(f"{key}: {value}")
        
        # Step 2: Process Excel
        test_excel = "test_files/QIC -ADDITION - AUH.xlsx"
        logger.info(f"\nProcessing Excel: {test_excel}")
        excel_df, excel_errors = excel_processor.process_excel(test_excel, dayfirst=True)
        
        if not excel_df.empty:
            logger.info(f"Excel rows found: {len(excel_df)}")
            logger.info("\nFirst row of Excel data:")
            for col in excel_df.columns:
                logger.info(f"{col}: {excel_df.iloc[0][col]}")
        
        # Step 3: Combine data
        output_path = os.path.join("test_output", "combined_data_test.xlsx")
        logger.info(f"\nCombining data to: {output_path}")
        
        result = data_combiner.combine_and_populate_template(
            "test_files/template.xlsx",
            output_path,
            extracted_data,
            excel_df
        )
        
        # Step 4: Validate result
        if result['status'] == 'success':
            logger.info(f"\nSuccessfully processed {result['rows_processed']} rows")
            
            # Read and validate output
            output_df = pd.read_excel(output_path)
            logger.info(f"\nOutput file rows: {len(output_df)}")
            
            # Check first row
            logger.info("\nFirst row of output data:")
            for col in output_df.columns:
                value = output_df.iloc[0][col]
                if value != '.':  # Only show non-default values
                    logger.info(f"{col}: {value}")
            
            return True
            
        else:
            logger.error(f"Combination failed: {result.get('error')}")
            return False
            
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")
        return False

if __name__ == "__main__":
    test_data_combination()