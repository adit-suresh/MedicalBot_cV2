from google.cloud import vision
import io
import logging
from typing import Dict, Optional
import os

logger = logging.getLogger(__name__)

class VisionProcessor:
    def __init__(self):
        # Initialize Google Cloud Vision client
        self.client = vision.ImageAnnotatorClient()
        
    def process_document(self, image_path: str) -> Dict[str, str]:
        """
        Process document using Google Cloud Vision API.
        
        Args:
            image_path: Path to the image file
            
        Returns:
            Dictionary containing extracted fields and their values
        """
        try:
            # Read image file
            with io.open(image_path, 'rb') as image_file:
                content = image_file.read()

            image = vision.Image(content=content)

            # Detect text in image
            response = self.client.text_detection(image=image)
            texts = response.text_annotations

            if not texts:
                logger.warning(f"No text detected in image: {image_path}")
                return {}

            # Full text from the image
            full_text = texts[0].description

            # Extract specific fields based on document type
            extracted_data = self._extract_fields(full_text, image_path)
            
            # Log successful extraction
            logger.info(f"Successfully extracted data from {image_path}")
            logger.debug(f"Extracted fields: {extracted_data}")

            return extracted_data

        except Exception as e:
            logger.error(f"Error processing document {image_path}: {str(e)}")
            raise

    def _extract_fields(self, text: str, image_path: str) -> Dict[str, str]:
        """Extract specific fields based on document type."""
        # Determine document type from filename or content
        doc_type = self._determine_document_type(image_path, text)
        
        if doc_type == "work_permit":
            return self._extract_work_permit_fields(text)
        elif doc_type == "emirates_id":
            return self._extract_emirates_id_fields(text)
        elif doc_type == "passport":
            return self._extract_passport_fields(text)
        elif doc_type == "visa":
            return self._extract_visa_fields(text)
        else:
            logger.warning(f"Unknown document type for {image_path}")
            return {}

    def _determine_document_type(self, image_path: str, text: str) -> str:
        """Determine document type from content."""
        text_lower = text.lower()
        
        if "work permit" in text_lower or "ministry of human resources" in text_lower:
            return "work_permit"
        elif "identity card" in text_lower or "emirates id" in text_lower:
            return "emirates_id"
        elif "passport" in text_lower and "republic" in text_lower:
            return "passport"
        elif "entry permit" in text_lower or "visa" in text_lower:
            return "visa"
        
        # If can't determine from content, try filename
        filename = os.path.basename(image_path).lower()
        if "permit" in filename:
            return "work_permit"
        elif "eid" in filename or "emirates" in filename:
            return "emirates_id"
        elif "passport" in filename:
            return "passport"
        elif "visa" in filename:
            return "visa"
            
        return "unknown"

    def _extract_work_permit_fields(self, text: str) -> Dict[str, str]:
        """Extract fields from work permit."""
        lines = text.split('\n')
        data = {}
        
        for i, line in enumerate(lines):
            line_lower = line.lower()
            
            if "name" in line_lower:
                data['full_name'] = line.split(':')[-1].strip()
            elif "expiry date" in line_lower:
                data['expiry_date'] = line.split(':')[-1].strip()
            elif "personal no" in line_lower:
                data['personal_no'] = line.split(':')[-1].strip()
            elif "nationality" in line_lower:
                data['nationality'] = line.split(':')[-1].strip()
            elif "profession" in line_lower:
                data['profession'] = line.split(':')[-1].strip()

        return data

    def _extract_emirates_id_fields(self, text: str) -> Dict[str, str]:
        """Extract fields from Emirates ID."""
        # Similar structure to work permit extraction
        pass

    def _extract_passport_fields(self, text: str) -> Dict[str, str]:
        """Extract fields from passport."""
        pass

    def _extract_visa_fields(self, text: str) -> Dict[str, str]:
        """Extract fields from visa."""
        pass