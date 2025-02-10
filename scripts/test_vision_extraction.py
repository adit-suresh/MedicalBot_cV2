import os
import json
import logging
from datetime import datetime
from typing import Dict

from src.document_processor.textract_processor import VisionProcessor
from src.utils.logger import setup_logger

def test_vision_extraction(test_files_dir: str) -> None:
    """
    Test Vision API extraction with real documents.
    
    Args:
        test_files_dir: Directory containing test documents
    """
    logger = setup_logger('vision_test')
    processor = VisionProcessor()
    
    # Create results directory
    results_dir = os.path.join('test_results', datetime.now().strftime('%Y%m%d_%H%M%S'))
    os.makedirs(results_dir, exist_ok=True)
    
    results = {
        'total_files': 0,
        'successful': 0,
        'failed': 0,
        'details': []
    }
    
    # Process each file in the directory
    for filename in os.listdir(test_files_dir):
        file_path = os.path.join(test_files_dir, filename)
        if not os.path.isfile(file_path):
            continue
            
        results['total_files'] += 1
        logger.info(f"\nProcessing: {filename}")
        
        try:
            # Process document
            start_time = datetime.now()
            extracted_data = processor.process_document(file_path)
            processing_time = (datetime.now() - start_time).total_seconds()
            
            # Validate results
            validation_result = validate_extraction(extracted_data, filename)
            
            # Save results
            file_result = {
                'filename': filename,
                'success': True,
                'processing_time': processing_time,
                'extracted_data': extracted_data,
                'validation': validation_result
            }
            
            results['successful'] += 1
            logger.info("✓ Successfully processed")
            logger.info(f"Processing time: {processing_time:.2f} seconds")
            logger.info("Extracted data:")
            for key, value in extracted_data.items():
                logger.info(f"  {key}: {value}")
            
        except Exception as e:
            results['failed'] += 1
            file_result = {
                'filename': filename,
                'success': False,
                'error': str(e)
            }
            logger.error(f"✗ Failed to process: {str(e)}")
            
        results['details'].append(file_result)
    
    # Save results to file
    results_file = os.path.join(results_dir, 'test_results.json')
    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    # Print summary
    logger.info("\nTest Summary:")
    logger.info(f"Total files processed: {results['total_files']}")
    logger.info(f"Successful: {results['successful']}")
    logger.info(f"Failed: {results['failed']}")
    logger.info(f"Success rate: {(results['successful']/results['total_files']*100):.1f}%")
    logger.info(f"\nDetailed results saved to: {results_file}")

def validate_extraction(data: Dict, filename: str) -> Dict:
    """Validate extracted data based on document type."""
    validation = {
        'missing_fields': [],
        'invalid_format': []
    }
    
    # Determine expected fields based on filename
    if 'emirates' in filename.lower():
        required_fields = ['emirates_id', 'name_en', 'nationality']
    elif 'passport' in filename.lower():
        required_fields = ['passport_number', 'surname', 'given_names']
    elif 'visa' in filename.lower():
        required_fields = ['entry_permit', 'full_name', 'nationality']
    elif 'permit' in filename.lower():
        required_fields = ['full_name', 'personal_no', 'expiry_date']
    else:
        required_fields = []
    
    # Check for missing fields
    for field in required_fields:
        if not data.get(field):
            validation['missing_fields'].append(field)
    
    # Validate formats
    if 'emirates_id' in data:
        import re
        if not re.match(r'^\d{3}-\d{4}-\d{7}-\d{1}$', data['emirates_id']):
            validation['invalid_format'].append('emirates_id')
            
    if 'expiry_date' in data:
        try:
            datetime.strptime(data['expiry_date'], '%d/%m/%Y')
        except ValueError:
            validation['invalid_format'].append('expiry_date')
    
    return validation

if __name__ == "__main__":
    # Directory containing test documents
    TEST_FILES_DIR = "tests/test_files/real_documents"
    
    if not os.path.exists(TEST_FILES_DIR):
        os.makedirs(TEST_FILES_DIR)
        print(f"Created test files directory: {TEST_FILES_DIR}")
        print("Please add test documents to this directory and run the script again.")
    else:
        test_vision_extraction(TEST_FILES_DIR)