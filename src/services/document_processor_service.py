import os
import logging
from typing import Dict, List, Tuple, Optional
from datetime import datetime

from src.document_processor.ocr_processor import OCRProcessor
from src.document_processor.data_extractor import DataExtractor
from src.database.db_manager import DatabaseManager
from src.utils.exceptions import OCRError

logger = logging.getLogger(__name__)

class DocumentProcessorService:
    def __init__(self):
        self.ocr_processor = OCRProcessor()
        self.data_extractor = DataExtractor()
        self.db_manager = DatabaseManager()

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
            if self.db_manager.client_exists(client_data['passport_number']):
                return False, f"Client with passport {client_data['passport_number']} already exists"

            # Add client to database
            client_id = self.db_manager.add_client(client_data)
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
            submission_id = self.db_manager.add_submission(client_id, submission_data)

            # Check for missing documents
            missing_docs = self.db_manager.get_missing_documents(client_id)
            
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
            processed_path, extracted_text = self.ocr_processor.process_document(file_path)
            
            # Extract data from OCR text
            passport_data = self.data_extractor.extract_passport_data(extracted_text)
            
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
                processed_path, extracted_text = self.ocr_processor.process_document(file_path)
                
                # Extract specific data based on document type
                if doc_type == 'emirates_id':
                    data = self.data_extractor.extract_emirates_id_data(extracted_text)
                elif doc_type == 'visa':
                    data = self.data_extractor.extract_visa_data(extracted_text)
                
                # Record document in database
                doc_data = {
                    "document_type": doc_type,
                    "file_path": file_path,
                    "processed_path": processed_path,
                    "status": "valid" if data else "invalid"
                }
                
                self.db_manager.add_document(client_id, doc_data)
                return True

            # Handle Excel sheet
            elif doc_type == 'excel_sheet':
                doc_data = {
                    "document_type": doc_type,
                    "file_path": file_path,
                    "processed_path": file_path,  # No OCR needed for Excel
                    "status": "valid"
                }
                self.db_manager.add_document(client_id, doc_data)
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