import re
import logging
from typing import Dict, List, Optional, Tuple
import os

logger = logging.getLogger(__name__)

class DocumentClassifier:
    """Classifies document types based on content analysis rather than just filename."""
    
    def __init__(self):
        # Key patterns to identify document types
        self.document_patterns = {
            'passport': [
                r'passport\s+no', 
                r'passport\s+number',
                r'nationality.{0,20}date of birth',
                r'surname.{0,50}given names',
                r'place of issue.{0,50}authority'
            ],
            'emirates_id': [
                r'united arab emirates.{0,50}id card', 
                r'بطاقة الهوية.{0,50}الإمارات',
                r'id number.{0,20}\d{3}-\d{4}-\d{7}-\d{1}',
                r'رقم الهوية'
            ],
            'visa': [
                r'entry permit', 
                r'residence visa',
                r'visa number',
                r'sponsor\s+name',
                r'permit no'
            ]
        }
    
    def classify_document(self, ocr_text: str, filename: str = '') -> str:
        """
        Classify document type based on OCR text content with filename as fallback.
        
        Args:
            ocr_text: Extracted text from document
            filename: Original filename as fallback
            
        Returns:
            Document type: 'passport', 'emirates_id', 'visa', or 'unknown'
        """
        if not ocr_text:
            logger.warning("No OCR text provided for classification")
            return self._classify_by_filename(filename)
        
        # Normalize text for better matching
        normalized_text = ocr_text.lower().replace('\n', ' ')
        
        # Calculate confidence scores for each document type
        scores = {}
        for doc_type, patterns in self.document_patterns.items():
            matches = 0
            for pattern in patterns:
                if re.search(pattern, normalized_text, re.IGNORECASE):
                    matches += 1
            
            # Calculate confidence score (0.0 to 1.0)
            if len(patterns) > 0:
                scores[doc_type] = matches / len(patterns)
            else:
                scores[doc_type] = 0.0
                
        logger.debug(f"Document classification scores: {scores}")
        
        # Get the type with highest confidence score
        if scores:
            max_score_type = max(scores, key=scores.get)
            max_score = scores[max_score_type]
            
            # Only classify if confidence exceeds threshold
            if max_score >= 0.3:  # At least 30% confidence
                logger.info(f"Classified as {max_score_type} with {max_score:.2f} confidence")
                return max_score_type
        
        # Fall back to filename-based classification if content classification fails
        logger.warning("Could not confidently classify document from content, trying filename")
        return self._classify_by_filename(filename)
    
    def _classify_by_filename(self, filename: str) -> str:
        """Classify based on filename patterns (fallback method)."""
        if not filename:
            return 'unknown'
            
        name = filename.lower()
        
        if name.endswith(('.xlsx', '.xls')):
            return 'excel'
        elif 'passport' in name:
            return 'passport'
        elif 'emirates' in name or 'eid' in name or 'id card' in name:
            return 'emirates_id'
        elif 'visa' in name or 'permit' in name:
            return 'visa'
            
        return 'unknown'
    
    def validate_classification(self, doc_type: str, extracted_data: Dict) -> bool:
        """
        Validate classification by checking for key fields in extracted data.
        
        Args:
            doc_type: Classified document type
            extracted_data: Dictionary of extracted data
            
        Returns:
            bool: Whether classification seems valid
        """
        validation_fields = {
            'passport': ['passport_number', 'surname', 'nationality'],
            'emirates_id': ['emirates_id', 'name_en', 'nationality'],
            'visa': ['entry_permit', 'full_name', 'nationality', 'expiry_date']
        }
        
        if doc_type not in validation_fields:
            return False
            
        required_fields = validation_fields[doc_type]
        found_fields = [field for field in required_fields if field in extracted_data and extracted_data[field]]
        
        # Classification is valid if at least half of expected fields are present
        validity_threshold = len(required_fields) / 2
        is_valid = len(found_fields) >= validity_threshold
        
        if not is_valid:
            logger.warning(
                f"Document classified as {doc_type} but missing key fields. "
                f"Found {len(found_fields)}/{len(required_fields)}: {found_fields}"
            )
            
        return is_valid