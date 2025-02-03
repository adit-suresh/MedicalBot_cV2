import cv2
import numpy as np
from PIL import Image
import pytesseract
from typing import Dict, Tuple, Optional
import logging

logger = logging.getLogger(__name__)

class EnhancedOCRProcessor:
    def __init__(self):
        self.templates = {
            'emirates_id_front': {
                'id_number': ((300, 50), (500, 80)),  # x1,y1, x2,y2
                'name_en': ((100, 150), (400, 180)),
                'name_ar': ((400, 150), (700, 180)),
                'nationality': ((100, 200), (400, 230))
            },
            'emirates_id_back': {
                'card_number': ((50, 20), (200, 40)),
                'occupation': ((150, 100), (600, 130)),
                'employer': ((150, 130), (600, 160)),
                'mrz': ((50, 280), (700, 350))
            },
            'uae_visa': {
                'permit_number': ((200, 50), (500, 80)),
                'full_name': ((150, 200), (500, 230)),
                'nationality': ((150, 230), (500, 260)),
                'passport_number': ((150, 300), (500, 330))
            },
            'passport': {
                'passport_number': ((400, 50), (600, 80)),
                'surname': ((150, 150), (400, 180)),
                'given_names': ((150, 180), (400, 210)),
                'dob': ((150, 210), (400, 240))
            }
        }

    def preprocess_image(self, image: np.ndarray, doc_type: str) -> np.ndarray:
        """Apply document-specific preprocessing."""
        try:
            # Convert to grayscale if not already
            if len(image.shape) == 3:
                gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
            else:
                gray = image

            # Document-specific preprocessing
            if doc_type in ['emirates_id_front', 'emirates_id_back']:
                # Enhance contrast for IDs
                gray = cv2.equalizeHist(gray)
                # Remove background patterns
                gray = cv2.GaussianBlur(gray, (3, 3), 0)
                gray = cv2.adaptiveThreshold(
                    gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, 
                    cv2.THRESH_BINARY, 11, 2
                )

            elif doc_type == 'passport':
                # Handle passport-specific preprocessing
                gray = cv2.GaussianBlur(gray, (3, 3), 0)
                gray = cv2.threshold(
                    gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU
                )[1]

            elif doc_type == 'uae_visa':
                # Handle visa-specific preprocessing
                gray = cv2.GaussianBlur(gray, (3, 3), 0)
                gray = cv2.threshold(
                    gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU
                )[1]

            return gray

        except Exception as e:
            logger.error(f"Error in image preprocessing: {str(e)}")
            raise

    def extract_text_from_region(self, image: np.ndarray, region: Tuple[Tuple[int, int], Tuple[int, int]]) -> str:
        """Extract text from a specific region of the image."""
        try:
            (x1, y1), (x2, y2) = region
            roi = image[y1:y2, x1:x2]
            
            # Additional preprocessing for text extraction
            roi = cv2.resize(roi, None, fx=2, fy=2, interpolation=cv2.INTER_CUBIC)
            
            # Extract text using Tesseract
            text = pytesseract.image_to_string(
                roi, 
                config='--psm 7 --oem 3'
            ).strip()
            
            return text

        except Exception as e:
            logger.error(f"Error extracting text from region: {str(e)}")
            return ""

    def process_document(self, image_path: str, doc_type: str) -> Dict[str, str]:
        """Process document and extract information using templates."""
        try:
            # Read image
            image = cv2.imread(image_path)
            if image is None:
                raise ValueError("Could not read image file")

            # Preprocess image
            processed = self.preprocess_image(image, doc_type)

            # Get template for document type
            template = self.templates.get(doc_type)
            if not template:
                raise ValueError(f"Unknown document type: {doc_type}")

            # Extract text from each region
            results = {}
            for field, region in template.items():
                text = self.extract_text_from_region(processed, region)
                results[field] = text

            # Post-process and validate results
            self._validate_results(results, doc_type)

            return results

        except Exception as e:
            logger.error(f"Error processing document: {str(e)}")
            raise

    def _validate_results(self, results: Dict[str, str], doc_type: str) -> None:
        """Validate extracted results based on document type."""
        if doc_type == 'emirates_id_front':
            # Validate Emirates ID number format
            if 'id_number' in results:
                if not self._is_valid_emirates_id(results['id_number']):
                    logger.warning(f"Invalid Emirates ID format: {results['id_number']}")

        elif doc_type == 'passport':
            # Validate passport number format
            if 'passport_number' in results:
                if not self._is_valid_passport_number(results['passport_number']):
                    logger.warning(f"Invalid passport number format: {results['passport_number']}")

    def _is_valid_emirates_id(self, id_number: str) -> bool:
        """Validate Emirates ID format."""
        import re
        pattern = r'^\d{3}-\d{4}-\d{7}-\d{1}$'
        return bool(re.match(pattern, id_number))

    def _is_valid_passport_number(self, passport_number: str) -> bool:
        """Validate passport number format."""
        import re
        pattern = r'^[A-Z0-9]{6,9}$'
        return bool(re.match(pattern, passport_number))