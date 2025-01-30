import os
import logging
from typing import Dict, Optional, Tuple
import re
import ocrmypdf
from PIL import Image
import pytesseract
from pdf2image import convert_from_path
import tempfile

from config.settings import PROCESSED_DATA_DIR
from config.constants import EMIRATES_ID_PATTERN, PASSPORT_NUMBER_PATTERN
from src.utils.exceptions import OCRError

logger = logging.getLogger(__name__)

class OCRProcessor:
    def __init__(self):
        self.processed_dir = PROCESSED_DATA_DIR
        os.makedirs(self.processed_dir, exist_ok=True)
        
        # Configure Tesseract
        if os.getenv('TESSDATA_PREFIX'):
            pytesseract.pytesseract.tesseract_cmd = os.getenv('TESSDATA_PREFIX')

    def process_document(self, file_path: str) -> Tuple[str, Dict[str, str]]:
        """
        Process a document through OCR and extract relevant information.
        
        Args:
            file_path: Path to the document file
            
        Returns:
            Tuple containing:
                - Path to the processed file
                - Dictionary of extracted information
        """
        try:
            file_ext = os.path.splitext(file_path)[1].lower()
            
            # Process based on file type
            if file_ext == '.pdf':
                return self._process_pdf(file_path)
            elif file_ext in ['.jpg', '.jpeg', '.png']:
                return self._process_image(file_path)
            else:
                raise OCRError(f"Unsupported file type: {file_ext}")

        except Exception as e:
            logger.error(f"Error processing document {file_path}: {str(e)}")
            raise OCRError(f"Failed to process document: {str(e)}")

    def _process_pdf(self, pdf_path: str) -> Tuple[str, Dict[str, str]]:
        """Process PDF file using OCRmyPDF."""
        try:
            output_path = os.path.join(
                self.processed_dir,
                f"processed_{os.path.basename(pdf_path)}"
            )

            # Run OCR on PDF
            ocrmypdf.ocr(
                pdf_path,
                output_path,
                deskew=True,
                clean=True,
                optimize=1,
                language='eng+ara',  # Support both English and Arabic
                force_ocr=True  # Force OCR even if text is present
            )

            # Extract text from processed PDF
            text = self._extract_text_from_pdf(output_path)
            extracted_data = self._extract_information(text)

            return output_path, extracted_data

        except Exception as e:
            logger.error(f"Error processing PDF {pdf_path}: {str(e)}")
            raise OCRError(f"Failed to process PDF: {str(e)}")

    def _process_image(self, image_path: str) -> Tuple[str, Dict[str, str]]:
        """Process image file using Tesseract."""
        try:
            # Load and preprocess image
            image = Image.open(image_path)
            
            # Convert to RGB if necessary
            if image.mode != 'RGB':
                image = image.convert('RGB')
            
            # Perform OCR
            text = pytesseract.image_to_string(
                image,
                lang='eng+ara',
                config='--psm 3 --oem 3'  # Use neural net mode
            )

            # Save processed image
            output_path = os.path.join(
                self.processed_dir,
                f"processed_{os.path.basename(image_path)}"
            )
            image.save(output_path)

            # Extract information
            extracted_data = self._extract_information(text)

            return output_path, extracted_data

        except Exception as e:
            logger.error(f"Error processing image {image_path}: {str(e)}")
            raise OCRError(f"Failed to process image: {str(e)}")

    def _extract_text_from_pdf(self, pdf_path: str) -> str:
        """Extract text from OCR-processed PDF using pdf2image and Tesseract."""
        try:
            text = ""
            # Convert PDF to images
            with tempfile.TemporaryDirectory() as temp_dir:
                images = convert_from_path(
                    pdf_path,
                    output_folder=temp_dir,
                    fmt='png',
                    dpi=300
                )
                
                # Process each page
                for image in images:
                    page_text = pytesseract.image_to_string(
                        image,
                        lang='eng+ara',
                        config='--psm 3 --oem 3'
                    )
                    text += page_text + "\n"
            
            return text

        except Exception as e:
            logger.error(f"Error extracting text from PDF {pdf_path}: {str(e)}")
            raise OCRError(f"Failed to extract text from PDF: {str(e)}")

    def _extract_information(self, text: str) -> Dict[str, str]:
        """Extract relevant information from OCR text."""
        extracted_data = {}
        
        # Log the extracted text for debugging
        logger.debug(f"Extracted text:\n{text[:500]}...")  # First 500 chars
        
        # Extract Emirates ID using pattern
        emirates_id_matches = re.finditer(EMIRATES_ID_PATTERN, text)
        for match in emirates_id_matches:
            extracted_data['emirates_id'] = match.group(0)
            logger.info(f"Found Emirates ID: {match.group(0)}")

        # Extract Passport Number
        passport_matches = re.finditer(PASSPORT_NUMBER_PATTERN, text)
        for match in passport_matches:
            extracted_data['passport_number'] = match.group(0)
            logger.info(f"Found Passport Number: {match.group(0)}")

        # Extract other potentially useful information
        # Add more extraction patterns as needed

        logger.info(f"Extracted {len(extracted_data)} pieces of information")
        return extracted_data