import boto3
import logging
from typing import Dict, List, Optional, Tuple
from botocore.exceptions import ClientError
import json
import os
import re
from datetime import datetime

from src.utils.error_handling import (
    ServiceError, handle_errors, ErrorCategory, ErrorSeverity, retry_on_error
)

logger = logging.getLogger(__name__)

class TextractProcessor:
    """AWS Textract processor for OCR and data extraction."""

    def __init__(self):
        """Initialize AWS Textract client."""
        self.textract = boto3.client(
            'textract',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION', 'us-east-1')
        )
        
        # Default value for missing fields
        self.DEFAULT_VALUE = "."

    def _clean_text(self, text: str) -> str:
        """Clean extracted text."""
        if not text:
            return self.DEFAULT_VALUE
        # Remove extra whitespace and newlines
        text = ' '.join(text.split())
        # Remove any additional text after main value
        text = text.split('/')[0].strip()
        return text if text else self.DEFAULT_VALUE

    def _extract_emirates_id_data(self, text_content: str) -> Dict[str, str]:
        """Extract Emirates ID specific data."""
        data = {
            'emirates_id': self.DEFAULT_VALUE,
            'name_en': self.DEFAULT_VALUE,
            'nationality': self.DEFAULT_VALUE,
            'unified_no': self.DEFAULT_VALUE  # This might be the same as emirates_id
        }
        
        # ID Number (try multiple patterns)
        id_patterns = [
            r'ID Number[/:\s]*(\d{3}-\d{4}-\d{7}-\d{1})',
            r'(784-\d{4}-\d{7}-\d{1})'
        ]
        for pattern in id_patterns:
            match = re.search(pattern, text_content)
            if match:
                data['emirates_id'] = match.group(1)
                data['unified_no'] = match.group(1)  # Using same number for unified_no
                break
        
        # Name (English)
        name_match = re.search(r'Name:\s*([A-Za-z\s]+?)(?:\s*\n|$)', text_content)
        if name_match:
            full_name = name_match.group(1).strip()
            name_parts = full_name.split()
            if len(name_parts) >= 3:
                data['first_name'] = name_parts[0]
                data['middle_name'] = name_parts[1]
                data['last_name'] = ' '.join(name_parts[2:])
            elif len(name_parts) == 2:
                data['first_name'] = name_parts[0]
                data['middle_name'] = self.DEFAULT_VALUE
                data['last_name'] = name_parts[1]
            else:
                data['first_name'] = full_name
                data['middle_name'] = self.DEFAULT_VALUE
                data['last_name'] = self.DEFAULT_VALUE
        
        # Nationality
        nationality_match = re.search(r'Nationality:\s*([A-Za-z\s]+?)(?:\s*\n|$)', text_content)
        if nationality_match:
            data['nationality'] = nationality_match.group(1).strip()

        return data

    def _extract_passport_data(self, text_content: str) -> Dict[str, str]:
        """Extract passport specific data."""
        data = {
            'passport_number': self.DEFAULT_VALUE,
            'first_name': self.DEFAULT_VALUE,
            'middle_name': self.DEFAULT_VALUE,
            'last_name': self.DEFAULT_VALUE,
            'nationality': self.DEFAULT_VALUE,
            'date_of_birth': self.DEFAULT_VALUE,
            'sex': self.DEFAULT_VALUE,
            'passport_expiry': self.DEFAULT_VALUE
        }
        
        # Passport Number (try multiple patterns)
        passport_patterns = [
            r'[Pp]ass(?:port|\.)\s*[Nn]o\.?:?\s*([A-Z0-9]+)(?:\s*\n|$)',
            r'[Pp]asaporte[:/\s]*([A-Z0-9]+)(?:\s*\n|$)'
        ]
        for pattern in passport_patterns:
            match = re.search(pattern, text_content)
            if match:
                data['passport_number'] = match.group(1).strip()
                break
        
        # Names
        surname_match = re.search(r'[Ss]urname[:/\s]*([A-Z\s]+?)(?:/|\n|$)', text_content)
        if surname_match:
            data['last_name'] = surname_match.group(1).strip()
        
        given_names_match = re.search(r'[Gg]iven\s*[Nn]ames?[:/\s]*([A-Z\s]+?)(?:/|\n|$)', text_content)
        if given_names_match:
            given_names = given_names_match.group(1).strip().split()
            if given_names:
                data['first_name'] = given_names[0]
                if len(given_names) > 1:
                    data['middle_name'] = ' '.join(given_names[1:])
        
        # Additional fields
        patterns = {
            'nationality': r'[Nn]ationality[:/\s]*([A-Z]+)(?:/|\n|$)',
            'date_of_birth': r'[Dd]ate\s+of\s+[Bb]irth[:/\s]*(\d{2}\s*[A-Z]{3}\s*\d{4})(?:/|\n|$)',
            'sex': r'\b[Ss]ex[:/\s]*([MF])(?:/|\n|$)',
            'passport_expiry': r'[Vv]alid\s+[Uu]ntil[:/\s]*(\d{2}\s*[A-Z]{3}\s*\d{4})(?:/|\n|$)'
        }
        
        for field, pattern in patterns.items():
            match = re.search(pattern, text_content)
            if match:
                data[field] = match.group(1).strip()

        return data

    def _extract_visa_data(self, text_content: str) -> Dict[str, str]:
        """Extract visa specific data."""
        data = {
            'entry_permit_no': self.DEFAULT_VALUE,
            'full_name': self.DEFAULT_VALUE,
            'nationality': self.DEFAULT_VALUE,
            'passport_number': self.DEFAULT_VALUE,
            'date_of_birth': self.DEFAULT_VALUE,
            'profession': self.DEFAULT_VALUE,
            'visa_issue_date': self.DEFAULT_VALUE,
            'visa_file_number': self.DEFAULT_VALUE,
            'visa_issuance_emirate': self.DEFAULT_VALUE
        }
        
        patterns = {
            'entry_permit_no': r'ENTRY PERMIT NO[:/\s]*(\d+\s*/\s*\d+\s*/\s*[\d/]+)',
            'full_name': r'Full Name[:/\s]*([A-Z\s]+?)(?:\n|$)',
            'nationality': r'Nationality[:/\s]*([A-Z\s]+?)(?:\n|$)',
            'passport_number': r'Passport No[:/\s]*([A-Z0-9/]+?)(?:/|\n|$)',
            'date_of_birth': r'Date of Birth[:/\s]*([\d/]+)',
            'profession': r'Profession[:/\s]*([A-Z\s]+?)(?:\n|$)',
            'visa_issue_date': r'Date & Place of Issue[:/\s]*([\d/]+)',
            'visa_file_number': r'File No[.:/\s]*(\d+)'
        }
        
        for field, pattern in patterns.items():
            match = re.search(pattern, text_content)
            if match:
                data[field] = match.group(1).strip()
        
        # Try to extract emirate from issue place
        emirate_match = re.search(r'Issue[:/\s]*([A-Z\s]+?)(?:\n|$)', text_content)
        if emirate_match:
            data['visa_issuance_emirate'] = emirate_match.group(1).strip()

        return data

    def _detect_document_type(self, text_content: str) -> str:
        """Detect document type from content."""
        # Check for Emirates ID
        if re.search(r'Identity Card|بطاقة هوية', text_content, re.IGNORECASE) or \
           re.search(r'784-\d{4}-\d{7}-\d{1}', text_content):
            return 'emirates_id'
        
        # Check for visa/entry permit
        if re.search(r'ENTRY PERMIT|eVisa', text_content):
            return 'visa'
        
        # Check for passport
        if re.search(r'PASSPORT|PASAPORTE', text_content, re.IGNORECASE):
            return 'passport'
        
        return 'unknown'

    @handle_errors(ErrorCategory.EXTERNAL_SERVICE, ErrorSeverity.HIGH)
    @retry_on_error(max_attempts=3)
    def process_document(self, file_path: str, doc_type: Optional[str] = None) -> Dict[str, str]:
        """Process document using AWS Textract."""
        try:
            with open(file_path, 'rb') as document:
                file_bytes = document.read()

            response = self.textract.analyze_document(
                Document={'Bytes': file_bytes},
                FeatureTypes=['FORMS', 'TABLES']
            )

            text_content = '\n'.join(
                block['Text'] for block in response['Blocks'] 
                if block['BlockType'] == 'LINE'
            )

            if not doc_type:
                doc_type = self._detect_document_type(text_content)

            logger.info(f"Processing {doc_type} document: {file_path}")

            # Extract data based on document type
            if doc_type == 'emirates_id':
                return self._extract_emirates_id_data(text_content)
            elif doc_type == 'passport':
                return self._extract_passport_data(text_content)
            elif doc_type == 'visa':
                return self._extract_visa_data(text_content)
            else:
                return {}

        except ClientError as e:
            logger.error(f"AWS Textract error: {str(e)}")
            raise ServiceError(f"Textract processing failed: {str(e)}")