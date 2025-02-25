import os
import logging
from typing import Dict, List, Tuple, Optional
from datetime import datetime

from src.document_processor.ocr_processor import OCRProcessor
from src.document_processor.data_extractor import DataExtractor
from src.database.db_manager import DatabaseManager
from src.utils.exceptions import OCRError
from src.utils.dependency_container import inject

logger = logging.getLogger(__name__)

@inject(OCRProcessor, DataExtractor, DatabaseManager)
class DocumentProcessorService:
    """Service for processing and managing documents."""

    def process_new_documents(self, email_id: str, documents: List[Dict[str, str]]) -> Tuple[bool, str]:
        """
        Process a set of new documents from an email.
        
        Args:
            email_id: Reference email ID
            documents: List of document dictionaries with paths and types
            
        Returns:
            Tuple of (success: bool, message: str)
        """
        try:
            logger.info(f"Processing documents from email {email_id}")
            
            # First, process passport if present
            passport_doc = next(
                (doc for doc in documents if 'passport' in doc['file_path'].lower()),
                None
            )
            
            if not passport_doc:
                return False, "No passport found in documents"

            # Extract passport data first
            client_data = self._process_passport(passport_doc['file_path'])
            if not client_data:
                return False, "Failed to extract passport data"

            # Check for existing client
            if self._db_manager.client_exists(client_data['passport_number']):
                return False, f"Client with passport {client_data['passport_number']} already exists"

            # Add client to database
            client_id = self._db_manager.add_client(client_data)
            logger.info(f"Added new client with ID: {client_id}")

            # Process remaining documents
            processed_docs = []
            for doc in documents:
                if doc != passport_doc:  # Skip passport as it's already processed
                    success = self._process_additional_document(client_id, doc)
                    if success:
                        processed_docs.append(doc['file_path'])

            # Record submission
            submission_data = {
                "status": "pending",
                "insurance_company": "Default Insurance Co",  # This should come from configuration
                "reference_email_id": email_id
            }
            submission_id = self._db_manager.add_submission(client_id, submission_data)

            # Check for missing documents
            missing_docs = self._db_manager.get_missing_documents(client_id)
            
            if missing_docs:
                return True, f"Processed successfully but missing documents: {', '.join(missing_docs)}"
            return True, "All documents processed successfully"

        except Exception as e:
            logger.error(f"Error processing documents: {str(e)}")
            return False, f"Error processing documents: {str(e)}"

    def _process_passport(self, file_path: str) -> Optional[Dict[str, str]]:
        """Process passport and extract client data."""
        try:
            # Process document through OCR
            processed_path, extracted_text = self._ocr_processor.process_document(file_path)
            
            # Extract data from OCR text
            passport_data = self._data_extractor.extract_passport_data(extracted_text)
            
            if not passport_data.get('passport_number'):
                logger.error("No passport number found in passport")
                return None

            return passport_data

        except OCRError as e:
            logger.error(f"OCR Error processing passport: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Error processing passport: {str(e)}")
            return None

    def _process_additional_document(self, client_id: int, document: Dict[str, str]) -> bool:
        """Process additional documents (Emirates ID, visa, etc.)."""
        try:
            file_path = document['file_path']
            doc_type = self._determine_document_type(file_path)
            
            # Process through OCR if needed
            if doc_type in ['emirates_id', 'visa']:
                processed_path, extracted_text = self._ocr_processor.process_document(file_path)
                
                # Extract specific data based on document type
                if doc_type == 'emirates_id':
                    data = self._data_extractor.extract_emirates_id_data(extracted_text)
                elif doc_type == 'visa':
                    data = self._data_extractor.extract_visa_data(extracted_text)
                
                # Record document in database
                doc_data = {
                    "document_type": doc_type,
                    "file_path": file_path,
                    "processed_path": processed_path,
                    "status": "valid" if data else "invalid"
                }
                
                self._db_manager.add_document(client_id, doc_data)
                return True

            # Handle Excel sheet
            elif doc_type == 'excel_sheet':
                doc_data = {
                    "document_type": doc_type,
                    "file_path": file_path,
                    "processed_path": file_path,  # No OCR needed for Excel
                    "status": "valid"
                }
                self._db_manager.add_document(client_id, doc_data)
                return True

            return False

        except Exception as e:
            logger.error(f"Error processing additional document: {str(e)}")
            return False

    def _determine_document_type(self, file_path: str) -> str:
        """Determine document type from file path or content."""
        file_name = os.path.basename(file_path).lower()
        
        if 'passport' in file_name:
            return 'passport'
        elif 'emirates' in file_name or 'eid' in file_name:
            return 'emirates_id'
        elif 'visa' in file_name:
            return 'visa'
        elif file_name.endswith(('.xlsx', '.xls')):
            return 'excel_sheet'
        
        return 'unknown'
    

class EnhancedDocumentProcessorService:
    """Service for orchestrating document processing using multiple processors."""
    
    def __init__(self, textract_processor=None, deepseek_processor=None):
        """Initialize with available processors."""
        self.textract_processor = textract_processor
        self.deepseek_processor = deepseek_processor
        
        # Flag to indicate whether DeepSeek is available
        self.deepseek_available = deepseek_processor is not None and deepseek_processor.api_key is not None
        
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
        if not (self.deepseek_available and self.use_deepseek_fallback):
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
                    return textract_result
                # Otherwise, propagate the error
                raise Exception(f"Document processing failed with both processors: {textract_error}, {str(e)}")
                
        # Return Textract result if it was sufficient
        return textract_result
        
    def _is_insufficient_data(self, result: Dict[str, str], doc_type: str) -> bool:
        """Check if extracted data is insufficient based on document type."""
        DEFAULT_VALUE = "."
        
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
        DEFAULT_VALUE = "."
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