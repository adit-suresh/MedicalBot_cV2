import os
import logging
from typing import Dict
from datetime import datetime

from src.document_processor.textract_processor import TextractProcessor
from src.document_processor.excel_processor import ExcelProcessor
from src.services.data_combiner import DataCombiner

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WorkflowStepTester:
    def __init__(self):
        self.textract_processor = TextractProcessor()
        self.excel_processor = ExcelProcessor()
        self.data_combiner = DataCombiner(self.textract_processor, self.excel_processor)
        
        # Test files
        self.test_files = {
            'visa': 'test_files/27715 VISA.pdf',
            'excel': 'test_files/QIC -ADDITION - AUH.xlsx',
            'template': 'test_files/template.xlsx'
        }
    
    def test_step_1_document_extraction(self):
        """Test document extraction"""
        logger.info("\nStep 1: Document Extraction")
        
        try:
            extracted_data = self.textract_processor.process_document(
                self.test_files['visa'], 
                'visa'
            )
            
            logger.info("\nExtracted Data:")
            for key, value in extracted_data.items():
                logger.info(f"{key}: {value}")
                
            return extracted_data
            
        except Exception as e:
            logger.error(f"Document extraction failed: {str(e)}")
            return None
    
    def test_step_2_excel_processing(self):
        """Test Excel processing"""
        logger.info("\nStep 2: Excel Processing")
        
        try:
            df, errors = self.excel_processor.process_excel(self.test_files['excel'])
            
            if not df.empty:
                logger.info("\nFirst row of data:")
                first_row = df.iloc[0]
                for col in df.columns:
                    logger.info(f"{col}: {first_row[col]}")
                    
                if errors:
                    logger.warning("\nValidation Errors:")
                    for error in errors:
                        logger.warning(f"- {error}")
                        
                return df
                
        except Exception as e:
            logger.error(f"Excel processing failed: {str(e)}")
            return None
    
    def test_step_3_data_combination(self, doc_data, excel_data):
        """Test data combination"""
        logger.info("\nStep 3: Data Combination")
        
        try:
            output_path = os.path.join(
                "test_output",
                f"combined_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            )
            
            result = self.data_combiner.combine_and_populate_template(
                self.test_files['template'],
                output_path,
                doc_data,
                excel_data.iloc[0].to_dict() if excel_data is not None else None
            )
            
            logger.info(f"\nCombination result: {result['status']}")
            if result['status'] == 'success':
                logger.info(f"Output file: {result['output_path']}")
            else:
                logger.error(f"Combination failed: {result.get('error')}")
                
            return result
            
        except Exception as e:
            logger.error(f"Data combination failed: {str(e)}")
            return None

if __name__ == "__main__":
    # Create output directory
    os.makedirs("test_output", exist_ok=True)
    
    # Run tests
    tester = WorkflowStepTester()
    
    # Step 1
    doc_data = tester.test_step_1_document_extraction()
    if not doc_data:
        logger.error("Stopping after Step 1 due to failure")
        exit(1)
        
    # Step 2
    excel_data = tester.test_step_2_excel_processing()
    if excel_data is None:
        logger.error("Stopping after Step 2 due to failure")
        exit(1)
        
    # Step 3
    result = tester.test_step_3_data_combination(doc_data, excel_data)