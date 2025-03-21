# Quick test script (save as test_gpt.py)
import os
import logging
from src.document_processor.gpt_processor import GPTProcessor

# Set up logging
logging.basicConfig(level=logging.INFO)

def test_gpt_processor():
    processor = GPTProcessor()
    
    # Replace with the actual path to one of your PDF or image files
    # For example:
    test_document_path = r"C:\Users\adit.s\Documents\VISA_-_NIJANTHAN.pdf"
    
    # Check if file exists before processing
    if not os.path.exists(test_document_path):
        print(f"Error: Test file not found at {test_document_path}")
        return
        
    print(f"Processing document: {test_document_path}")
    
    # Process the document
    result = processor.process_document(test_document_path, "visa")
    print("\nExtracted data:")
    for key, value in result.items():
        print(f"  {key}: {value}")

if __name__ == "__main__":
    test_gpt_processor()