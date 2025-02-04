import os
import json
import logging
from datetime import datetime
from typing import Dict

from src.document_processor.claude_processor import ClaudeProcessor
from src.utils.logger import setup_logger

def test_claude_extraction(test_dir: str, doc_type: str = None) -> None:
    """
    Test Claude's extraction capabilities.
    
    Args:
        test_dir: Directory containing test documents
        doc_type: Type of document being tested
    """
    logger = setup_logger('claude_test')
    processor = ClaudeProcessor()
    
    # Create results directory
    results_dir = os.path.join('test_results', datetime.now().strftime('%Y%m%d_%H%M%S'))
    os.makedirs(results_dir, exist_ok=True)
    
    results = {
        'test_time': datetime.now().isoformat(),
        'total_files': 0,
        'successful': 0,
        'failed': 0,
        'processing_times': [],
        'details': []
    }
    
    # Process each file
    for filename in os.listdir(test_dir):
        if not filename.lower().endswith(('.jpg', '.jpeg', '.png', '.pdf')):
            continue
            
        file_path = os.path.join(test_dir, filename)
        results['total_files'] += 1
        
        logger.info(f"\nProcessing: {filename}")
        start_time = datetime.now()
        
        try:
            # Process document
            extracted_data = processor.process_document(file_path, doc_type)
            
            # Calculate processing time
            processing_time = (datetime.now() - start_time).total_seconds()
            results['processing_times'].append(processing_time)
            
            # Log results
            logger.info("✓ Successfully processed")
            logger.info(f"Processing time: {processing_time:.2f} seconds")
            logger.info("Extracted data:")
            for key, value in extracted_data.items():
                logger.info(f"  {key}: {value}")
            
            # Save to results
            results['successful'] += 1
            results['details'].append({
                'filename': filename,
                'success': True,
                'processing_time': processing_time,
                'extracted_data': extracted_data
            })
            
        except Exception as e:
            results['failed'] += 1
            logger.error(f"✗ Failed to process {filename}: {str(e)}")
            results['details'].append({
                'filename': filename,
                'success': False,
                'error': str(e)
            })
    
    # Calculate statistics
    if results['processing_times']:
        results['avg_processing_time'] = sum(results['processing_times']) / len(results['processing_times'])
        results['max_processing_time'] = max(results['processing_times'])
        results['min_processing_time'] = min(results['processing_times'])
    
    # Save results
    results_file = os.path.join(results_dir, 'claude_test_results.json')
    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    # Print summary
    logger.info("\nTest Summary:")
    logger.info(f"Total files processed: {results['total_files']}")
    logger.info(f"Successful: {results['successful']}")
    logger.info(f"Failed: {results['failed']}")
    if results['processing_times']:
        logger.info(f"Average processing time: {results['avg_processing_time']:.2f} seconds")
    logger.info(f"\nDetailed results saved to: {results_file}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Test Claude document extraction')
    parser.add_argument('--test-dir', required=True, help='Directory containing test documents')
    parser.add_argument('--doc-type', help='Type of document (emirates_id, passport, visa, work_permit)')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.test_dir):
        print(f"Error: Test directory {args.test_dir} does not exist")
        exit(1)
        
    test_claude_extraction(args.test_dir, args.doc_type)