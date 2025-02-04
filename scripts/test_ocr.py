import os
import json
from datetime import datetime
from dotenv import load_dotenv
from src.document_processor.ocr_processor import OCRProcessor

def test_ocr_processing():
    """Test OCR processing with different document types."""
    load_dotenv()
    
    # Initialize OCR processor
    processor = OCRProcessor()
    
    # Create test results directory
    results_dir = os.path.join(
        'test_results',
        f'ocr_test_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
    )
    os.makedirs(results_dir, exist_ok=True)
    
    # Test documents directory
    test_docs_dir = "tests/test_files"
    
    # Document types to test
    doc_types = {
        'emirates_id': ['jpg', 'png', 'pdf'],
        'passport': ['jpg', 'png', 'pdf'],
        'visa': ['jpg', 'png', 'pdf']
    }
    
    results = {
        'timestamp': datetime.now().isoformat(),
        'total_files': 0,
        'successful': 0,
        'failed': 0,
        'results_by_type': {}
    }
    
    print("\nStarting OCR tests...")
    
    # Process each document type
    for doc_type, extensions in doc_types.items():
        type_results = {
            'total': 0,
            'successful': 0,
            'failed': 0,
            'files': []
        }
        
        print(f"\nTesting {doc_type} documents:")
        
        # Test directory for this document type
        type_dir = os.path.join(test_docs_dir, doc_type)
        if not os.path.exists(type_dir):
            print(f"Directory not found: {type_dir}")
            continue
        
        # Process each file
        for filename in os.listdir(type_dir):
            if not any(filename.lower().endswith(f".{ext}") for ext in extensions):
                continue
                
            type_results['total'] += 1
            results['total_files'] += 1
            file_path = os.path.join(type_dir, filename)
            
            print(f"\nProcessing: {filename}")
            try:
                # Process document
                start_time = datetime.now()
                extracted_data = processor.process_document(file_path, doc_type)
                processing_time = (datetime.now() - start_time).total_seconds()
                
                # Save results
                file_result = {
                    'filename': filename,
                    'success': True,
                    'processing_time': processing_time,
                    'extracted_data': extracted_data
                }
                
                type_results['successful'] += 1
                results['successful'] += 1
                
                print("✓ Success!")
                print("Extracted data:")
                for key, value in extracted_data.items():
                    print(f"  {key}: {value}")
                
            except Exception as e:
                file_result = {
                    'filename': filename,
                    'success': False,
                    'error': str(e)
                }
                
                type_results['failed'] += 1
                results['failed'] += 1
                
                print(f"✗ Failed: {str(e)}")
            
            type_results['files'].append(file_result)
        
        results['results_by_type'][doc_type] = type_results
    
    # Save detailed results
    results_file = os.path.join(results_dir, 'ocr_test_results.json')
    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    # Print summary
    print("\nTest Summary:")
    print(f"Total files processed: {results['total_files']}")
    print(f"Successful: {results['successful']}")
    print(f"Failed: {results['failed']}")
    print(f"\nDetailed results saved to: {results_file}")

if __name__ == "__main__":
    test_ocr_processing()