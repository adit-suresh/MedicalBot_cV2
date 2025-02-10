import boto3
import io
import logging
from typing import Dict, Optional, List
from botocore.exceptions import BotoCoreError, ClientError
import os
import json

from src.utils.error_handling import (
    ServiceError, handle_errors, ErrorCategory, ErrorSeverity
)

logger = logging.getLogger(__name__)

class TextractProcessor:
    """Processes documents using AWS Textract with form and table analysis."""

    def __init__(self):
        """Initialize AWS Textract client."""
        self.textract = boto3.client(
            'textract',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION', 'us-east-1')
        )
        
        # Define field mappings for different document types
        self.field_mappings = {
            'passport': {
                'passport number': 'passport_number',
                'passport no': 'passport_number',
                'surname': 'surname',
                'given names': 'given_names',
                'nationality': 'nationality',
                'date of birth': 'date_of_birth',
                'sex': 'gender'
            },
            'emirates_id': {
                'id number': 'emirates_id',
                'name': 'name_en',
                'nationality': 'nationality',
                'sex': 'gender'
            },
            'visa': {
                'permit no': 'entry_permit',
                'full name': 'full_name',
                'nationality': 'nationality',
                'issue date': 'issue_date',
                'expiry date': 'expiry_date'
            }
        }

    @handle_errors(ErrorCategory.EXTERNAL_SERVICE, ErrorSeverity.HIGH)
    def process_document(self, file_path: str, doc_type: str = None) -> Dict[str, str]:
        """
        Process document using AWS Textract.
        
        Args:
            file_path: Path to document file
            doc_type: Type of document (passport, emirates_id, etc.)
            
        Returns:
            Dictionary containing extracted fields
        """
        try:
            # Read the file
            with open(file_path, 'rb') as document:
                file_bytes = document.read()

            # Get both text detection and form analysis
            response = self.textract.analyze_document(
                Document={'Bytes': file_bytes},
                FeatureTypes=['FORMS', 'TABLES']
            )

            # Extract and normalize data
            extracted_data = self._extract_form_data(response, doc_type)
            
            # Validate extracted data
            self._validate_extraction(extracted_data, doc_type)
            
            logger.info(f"Successfully processed document {file_path}")
            logger.debug(f"Extracted data: {extracted_data}")

            return extracted_data

        except (BotoCoreError, ClientError) as e:
            logger.error(f"AWS Textract error processing {file_path}: {str(e)}")
            raise ServiceError(f"Textract processing failed: {str(e)}")
        except Exception as e:
            logger.error(f"Error processing document {file_path}: {str(e)}")
            raise

    def _extract_form_data(self, response: Dict, doc_type: str) -> Dict[str, str]:
        """Extract form fields from Textract response."""
        extracted_data = {}
        blocks = response['Blocks']
        
        # Process key-value sets
        for block in blocks:
            if block['BlockType'] == 'KEY_VALUE_SET':
                if 'KEY' in block.get('EntityTypes', []):
                    key_block = block
                    value_block = self._get_value_block(blocks, key_block)
                    
                    if value_block:
                        key_text = self._get_text(blocks, key_block).lower()
                        value_text = self._get_text(blocks, value_block)
                        
                        # Map field name if document type is known
                        if doc_type and doc_type in self.field_mappings:
                            key_text = self.field_mappings[doc_type].get(
                                key_text,
                                key_text
                            )
                        
                        extracted_data[key_text] = value_text

        # Add any table data if relevant
        table_data = self._extract_table_data(blocks)
        if table_data:
            extracted_data.update(table_data)

        return extracted_data

    def _get_value_block(self, blocks: List[Dict], key_block: Dict) -> Optional[Dict]:
        """Get the value block associated with a key block."""
        for relationship in key_block.get('Relationships', []):
            if relationship['Type'] == 'VALUE':
                for value_id in relationship['Ids']:
                    value_block = next(
                        (block for block in blocks if block['Id'] == value_id),
                        None
                    )
                    if value_block:
                        return value_block
        return None

    def _get_text(self, blocks: List[Dict], block: Dict) -> str:
        """Get text from a block, including any child blocks."""
        text = []
        if 'Relationships' in block:
            for relationship in block['Relationships']:
                if relationship['Type'] == 'CHILD':
                    for child_id in relationship['Ids']:
                        child_block = next(
                            (b for b in blocks if b['Id'] == child_id),
                            None
                        )
                        if child_block and 'Text' in child_block:
                            text.append(child_block['Text'])
        return ' '.join(text)

    def _extract_table_data(self, blocks: List[Dict]) -> Dict[str, str]:
        """Extract data from tables in the document."""
        table_data = {}
        current_table = None
        
        for block in blocks:
            if block['BlockType'] == 'TABLE':
                current_table = []
            elif current_table is not None and block['BlockType'] == 'CELL':
                if 'Text' in block:
                    current_table.append(block['Text'])
                    
                # Check if this is the last cell in the table
                if not any(rel['Type'] == 'CHILD' for rel in block.get('Relationships', [])):
                    # Process the completed table
                    if len(current_table) >= 2:
                        # Assuming first item is header, second is value
                        table_data[current_table[0].lower()] = current_table[1]
                    current_table = None

        return table_data

    def _validate_extraction(self, data: Dict[str, str], doc_type: str) -> None:
        """Validate extracted data based on document type."""
        if not doc_type:
            return

        required_fields = {
            'passport': ['passport_number', 'surname', 'given_names'],
            'emirates_id': ['emirates_id', 'name_en'],
            'visa': ['entry_permit', 'full_name']
        }.get(doc_type, [])

        missing_fields = [
            field for field in required_fields 
            if field not in data or not data[field]
        ]

        if missing_fields:
            logger.warning(
                f"Missing required fields for {doc_type}: {missing_fields}"
            )