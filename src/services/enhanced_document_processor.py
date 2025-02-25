import os
import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)

class EnhancedDocumentProcessorService:
    """Service for orchestrating document processing using multiple processors."""
    
    def __init__(self, textract_processor=None, deepseek_processor=None):
        """Initialize with available processors."""
        self.textract_processor = textract_processor
        self.deepseek_processor = deepseek_processor
        
        # Flag to indicate whether DeepSeek is available
        self.deepseek_available = deepseek_processor is not None and hasattr(deepseek_processor, 'api_key') and deepseek_processor.api_key is not None
        
        # Environment variable for controlling DeepSeek usage
        self.use_deepseek_fallback = os.getenv('USE_DEEPSEEK_FALLBACK', 'True').lower() == 'true'
        
        logger.info(f"Enhanced Document Processor initialized. DeepSeek available: {self.deepseek_available}")
        if self.deepseek_available:
            logger.info(f"DeepSeek fallback enabled: {self.use_deepseek_fallback}")
        
    def process_document(self, file_path: str, doc_type: Optional[str] = None) -> Dict[str, str]:
        """Process document using available processors with fallback logic."""
        logger.info(f"Processing document: {file_path}")
        
        # First attempt with Textract
        textract_result = None
        textract_error = None
        
        try:
            textract_result = self.textract_processor.process_document(file_path, doc_type)
            logger.info(f"Textract processing complete: {len(textract_result)} fields extracted")
        except Exception as e:
            textract_error = str(e)
            logger.warning(f"Textract processing failed: {textract_error}")
            
        # If DeepSeek is not available or fallback is disabled
        if not self.deepseek_available or not self.use_deepseek_fallback:
            if textract_error:
                raise Exception(f"Document processing failed: {textract_error}")
            return textract_result
            
        # Try with DeepSeek if Textract failed or returned insufficient data
        deepseek_needed = (
            textract_error or 
            not textract_result or
            self._is_insufficient_data(textract_result, doc_type)
        )
        
        if deepseek_needed:
            try:
                logger.info("Using DeepSeek for document processing")
                deepseek_result = self.deepseek_processor.process_document(file_path, doc_type)
                
                # If Textract failed completely, return DeepSeek result
                if textract_error or not textract_result:
                    return deepseek_result
                    
                # Otherwise, merge the results
                return self._merge_results(textract_result, deepseek_result)
                
            except Exception as e:
                logger.error(f"DeepSeek processing failed: {str(e)}")
                # If Textract succeeded but with limited data, return that
                if textract_result:
                    logger.info("Falling back to Textract results due to DeepSeek error")
                    return textract_result
                # Otherwise, if both processors failed, raise an error
                if textract_error:
                    raise Exception(f"Document processing failed with both processors: {textract_error}, {str(e)}")
                # This shouldn't happen, but just in case
                raise Exception(f"Document processing failed: {str(e)}")
                
        # Return Textract result if it was sufficient
        return textract_result
        
    def _is_insufficient_data(self, result: Dict[str, str], doc_type: str) -> bool:
        """Check if extracted data is insufficient based on document type."""
        DEFAULT_VALUE = self.textract_processor.DEFAULT_VALUE if hasattr(self.textract_processor, 'DEFAULT_VALUE') else "."
        
        # Critical fields by document type
        critical_fields = {
            'passport': ['passport_number', 'surname', 'given_names'],
            'emirates_id': ['emirates_id', 'name_en'],
            'visa': ['entry_permit_no', 'full_name']
        }.get(doc_type, [])
        
        # Check if critical fields are missing
        missing_critical = [
            field for field in critical_fields 
            if field not in result or result[field] == DEFAULT_VALUE
        ]
        
        # If more than half of critical fields are missing, data is insufficient
        return len(missing_critical) > len(critical_fields) / 2
        
    def _merge_results(self, textract_result: Dict[str, str], deepseek_result: Dict[str, str]) -> Dict[str, str]:
        """Merge results from both processors, prioritizing better data."""
        DEFAULT_VALUE = self.textract_processor.DEFAULT_VALUE if hasattr(self.textract_processor, 'DEFAULT_VALUE') else "."
        merged = textract_result.copy()
        
        # For each field in DeepSeek result
        for field, deepseek_value in deepseek_result.items():
            # Skip empty values
            if deepseek_value == DEFAULT_VALUE:
                continue
                
            # If field doesn't exist in Textract or has default value, use DeepSeek
            if field not in merged or merged[field] == DEFAULT_VALUE:
                merged[field] = deepseek_value
                continue
                
            # For IDs and dates, prefer longer/more structured values
            if field in ['emirates_id', 'passport_number', 'entry_permit_no'] or 'date' in field:
                textract_value = merged[field]
                
                # Choose the value that seems more complete/structured
                if len(deepseek_value) > len(textract_value) or (
                    '-' in deepseek_value and '-' not in textract_value
                ):
                    merged[field] = deepseek_value
                    
        return merged