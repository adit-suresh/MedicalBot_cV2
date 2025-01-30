import sys
import os
import logging
from PIL import Image
import pytesseract
from pdf2image import convert_from_path
import shutil
from datetime import datetime

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.document_processor.ocr_processor import OCRProcessor
from src.utils.logger import setup_logger
from config.settings import RAW_DATA_DIR, PROCESSED_DATA_DIR

def verify_ocr_processing():
    logger = setup_logger('ocr_verification')
    logger.setLevel(logging.DEBUG)
    
    logger.info("Starting OCR verification...")
    
    # Create verification directory for text output
    verification_dir = os.path.join(PROCESSED_DATA_DIR, 'verification')
    os.makedirs(verification_dir, exist_ok=True)
    
    ocr_processor = OCRProcessor()
    
    # Process each file in the raw directory
    for root, _, files in os.walk(RAW_DATA_DIR):
        for filename in files:
            file_path = os.path.join(root, filename)
            file_ext = os.path.splitext(filename)[1].lower()
            
            logger.info(f"\n{'='*50}")
            logger.info(f"Processing: {filename}")
            
            # Skip Excel files
            if file_ext in ['.xlsx', '.xls']:
                logger.info("Excel file - skipping OCR processing")
                continue
                
            # Only process PDFs and images
            if file_ext not in ['.pdf', '.jpg', '.jpeg', '.png']:
                logger.info(f"Unsupported file type: {file_ext}")
                continue
                
            try:
                # Process the document
                logger.info("Starting OCR processing...")
                processed_path, extracted_data = ocr_processor.process_document(file_path)
                
                # Save extracted text to a verification file
                verification_file = os.path.join(
                    verification_dir, 
                    f"{os.path.splitext(filename)[0]}_ocr_text.txt"
                )
                
                # Get the full extracted text
                if file_ext == '.pdf':
                    text = ocr_processor._extract_text_from_pdf(file_path)
                else:
                    image = Image.open(file_path)
                    text = pytesseract.image_to_string(image)
                
                # Save the text for verification
                with open(verification_file, 'w', encoding='utf-8') as f:
                    f.write(f"OCR Results for {filename}\n")
                    f.write(f"Processed on: {datetime.now()}\n")
                    f.write("="*50 + "\n\n")
                    f.write("Extracted Text:\n")
                    f.write(text + "\n\n")
                    f.write("="*50 + "\n\n")
                    f.write("Extracted Data:\n")
                    for key, value in extracted_data.items():
                        f.write(f"{key}: {value}\n")
                
                logger.info("Processing Results:")
                logger.info(f"- Original file: {file_path}")
                logger.info(f"- Processed file: {processed_path}")
                logger.info(f"- Text output: {verification_file}")
                logger.info("\nExtracted Data:")
                for key, value in extracted_data.items():
                    logger.info(f"- {key}: {value}")
                    
                # Compare file sizes
                original_size = os.path.getsize(file_path)
                processed_size = os.path.getsize(processed_path)
                logger.info(f"\nFile Sizes:")
                logger.info(f"- Original: {original_size:,} bytes")
                logger.info(f"- Processed: {processed_size:,} bytes")
                
            except Exception as e:
                logger.error(f"Failed to process {filename}: {str(e)}")
                continue
    
    logger.info("\nVerification complete!")
    logger.info(f"Check {verification_dir} for extracted text files")

if __name__ == "__main__":
    verify_ocr_processing()