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
        """Extract data from passport."""
        data = {
            'passport_number': self.DEFAULT_VALUE,
            'surname': self.DEFAULT_VALUE,
            'given_names': self.DEFAULT_VALUE,
            'nationality': self.DEFAULT_VALUE,
            'date_of_birth': self.DEFAULT_VALUE,
            'place_of_birth': self.DEFAULT_VALUE,
            'gender': self.DEFAULT_VALUE,
            'date_of_issue': self.DEFAULT_VALUE,
            'date_of_expiry': self.DEFAULT_VALUE
        }
        
        # Debug log
        logger.debug("Raw passport text:")
        logger.debug(text_content)
        
        # Clean text content
        text = text_content.upper()
        text = re.sub(r'\s+', ' ', text)  # Normalize whitespace
        
        # Pattern maps with multiple variations for each field
        patterns = {
            'passport_number': [
                r'PASSPORT\s*NO[.:\s]*([A-Z0-9]{6,12})',
                r'DOCUMENT\s*NO[.:\s]*([A-Z0-9]{6,12})',
                r'NO[.:\s]*([A-Z0-9]{6,12})',
                r'[A-Z]\d{8}',  # Common passport number format
                r'P<[A-Z]{3}[A-Z0-9]{6,10}[0-9]'  # MRZ format
            ],
            'surname': [
                r'SURNAME[.:\s]*([A-Z\s]+?)(?=\n|Given|\s{2,}|$)',
                r'LAST\s*NAME[.:\s]*([A-Z\s]+?)(?=\n|Given|\s{2,}|$)',
                r'NOM[.:\s]*([A-Z\s]+?)(?=\n|Given|\s{2,}|$)'  # French
            ],
            'given_names': [
                r'GIVEN\s*NAMES?[.:\s]*([A-Z\s]+?)(?=\n|Date|\s{2,}|$)',
                r'FIRST\s*NAMES?[.:\s]*([A-Z\s]+?)(?=\n|Date|\s{2,}|$)',
                r'PRENOM[.:\s]*([A-Z\s]+?)(?=\n|Date|\s{2,}|$)'  # French
            ],
            'nationality': [
                r'NATIONALITY[.:\s]*([A-Z\s]+?)(?=\n|\s{2,}|$)',
                r'NATIONALITE[.:\s]*([A-Z\s]+?)(?=\n|\s{2,}|$)'  # French
            ],
            'date_of_birth': [
                r'DATE\s*OF\s*BIRTH[.:\s]*(\d{1,2}[-/.]\d{1,2}[-/.]\d{2,4})',
                r'DOB[.:\s]*(\d{1,2}[-/.]\d{1,2}[-/.]\d{2,4})',
                r'BIRTH\s*DATE[.:\s]*(\d{1,2}[-/.]\d{1,2}[-/.]\d{2,4})'
            ],
            'gender': [
                r'SEX[.:\s]*([MF])',
                r'GENDER[.:\s]*([MF])',
                r'SEXE[.:\s]*([MF])'  # French
            ],
            'date_of_expiry': [
                r'EXPIRY\s*DATE[.:\s]*(\d{1,2}[-/.]\d{1,2}[-/.]\d{2,4})',
                r'EXPIRATION\s*DATE[.:\s]*(\d{1,2}[-/.]\d{1,2}[-/.]\d{2,4})',
                r'VALID\s*UNTIL[.:\s]*(\d{1,2}[-/.]\d{1,2}[-/.]\d{2,4})'
            ]
        }
        
        # Try each pattern for each field
        for field, field_patterns in patterns.items():
            for pattern in field_patterns:
                match = re.search(pattern, text)
                if match:
                    extracted_value = match.group(1).strip() if match.groups() else match.group(0).strip()
                    data[field] = extracted_value
                    logger.debug(f"Found {field}: {extracted_value} using pattern: {pattern}")
                    break
        
        # Try to extract from MRZ if other methods fail
        if data['passport_number'] == self.DEFAULT_VALUE:
            mrz_data = self._extract_from_mrz(text)
            data.update(mrz_data)
        
        return data

    def _extract_from_mrz(self, text: str) -> Dict[str, str]:
        """Extract data from passport MRZ (Machine Readable Zone)."""
        data = {}
        
        # Look for MRZ pattern (two or three lines of 44 characters)
        mrz_lines = []
        lines = text.split('\n')
        for line in lines:
            # Clean the line
            clean_line = re.sub(r'[^A-Z0-9<]', '', line.upper())
            if len(clean_line) == 44 and '<' in clean_line:
                mrz_lines.append(clean_line)
        
        if len(mrz_lines) >= 2:
            try:
                # First line format: P<ISSNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNN
                # Second line format: YYMMDDNM<YYMMDDNNNNNNNNNNNNNNNNNNNNN
                
                # Get passport number from first line
                if len(mrz_lines[0]) >= 44:
                    possible_number = re.search(r'[A-Z0-9]{9}', mrz_lines[0][5:44])
                    if possible_number:
                        data['passport_number'] = possible_number.group(0)
                
                # Get date of birth from second line
                if len(mrz_lines[1]) >= 6:
                    dob = mrz_lines[1][0:6]  # YYMMDD format
                    try:
                        year = int(dob[0:2])
                        month = int(dob[2:4])
                        day = int(dob[4:6])
                        # Assume years 00-24 are 2000s, 25-99 are 1900s
                        year = year + (2000 if year < 25 else 1900)
                        data['date_of_birth'] = f"{day:02d}/{month:02d}/{year}"
                    except ValueError:
                        pass
                
            except Exception as e:
                logger.debug(f"Error extracting from MRZ: {str(e)}")
        
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
            'entry_permit_no': [
                r'Entry permit no[.:\s]*(\d+\s*\/\s*\d+\s*\/\s*[\d\/]+)',
                r'Permit number[.:\s]*(\d+\s*\/\s*\d+\s*\/\s*[\d\/]+)',
                r'Permit No[.:\s]*(\d+\s*\/\s*\d+\s*\/\s*[\d\/]+)'
            ],
            'full_name': [
                r'Full Name[.:\s]*([A-Z\s]+?)(?=\n|$)',
                r'Name[.:\s]*([A-Z\s]+?)(?=\n|$)',
                r'NAME[.:\s]*([A-Z\s]+?)(?=\n|$)'
            ],
            'nationality': [
                r'Nationality[.:\s]*([A-Z\s]+?)(?=\n|$)',
                r'NATIONALITY[.:\s]*([A-Z\s]+?)(?=\n|$)'
            ],
            'passport_number': [
                r'Passport No[.:\s]*([A-Z0-9]+)(?=\n|$)',
                r'Passport[.:\s]*([A-Z0-9]+)(?=\n|$)',
                r'PASSPORT NO[.:\s]*([A-Z0-9]+)(?=\n|$)'
            ],
            'date_of_birth': [
                r'Date of Birth[.:\s]*(\d{2}/\d{2}/\d{4})',
                r'DOB[.:\s]*(\d{2}/\d{2}/\d{4})',
                r'Birth Date[.:\s]*(\d{2}/\d{2}/\d{4})'
            ],
            'profession': [
                r'Profession[.:\s]*([A-Z\s]+?)(?=\n|$)',
                r'PROFESSION[.:\s]*([A-Z\s]+?)(?=\n|$)',
                r'Occupation[.:\s]*([A-Z\s]+?)(?=\n|$)'
            ],
            'visa_issue_date': [
                r'Date of Issue[.:\s]*(\d{2}/\d{2}/\d{4})',
                r'Issue Date[.:\s]*(\d{2}/\d{2}/\d{4})',
                r'Issued on[.:\s]*(\d{2}/\d{2}/\d{4})'
            ]
        }

        # Try each pattern for each field
        for field, pattern_list in patterns.items():
            for pattern in pattern_list:
                match = re.search(pattern, text_content, re.IGNORECASE)
                if match:
                    data[field] = match.group(1).strip()
                    break
    
        # Post-process name
        if data['full_name'] != self.DEFAULT_VALUE:
            # Remove duplicates in name while preserving order
            name_parts = data['full_name'].split()
            seen = set()
            unique_parts = []
            for part in name_parts:
                if part not in seen:
                    seen.add(part)
                    unique_parts.append(part)
            data['full_name'] = ' '.join(unique_parts)
        
        return data

    def detect_document_type(self, text_content: str) -> str:
        """
        Detect document type from content patterns.
    
        Args:
            text_content: Extracted text from document
        
        Returns:
            str: Document type ('visa', 'emirates_id', 'passport', or 'unknown')
        """
        # Convert to uppercase for consistent matching
        text = text_content.upper()
    
        # Visa/Entry Permit patterns
        visa_patterns = [
            r'ENTRY\s+PERMIT',
            r'PERMIT\s+NO',
            r'VISA\s+FILE',
            r'RESIDENCE\s+VISA',
            r'\d{3}\s*/\s*\d{4}\s*/\s*\d+',  # Visa file number pattern
        ]
        
        # Emirates ID patterns
        eid_patterns = [
            r'IDENTITY\s+CARD',
            r'EMIRATES\s+ID',
            r'ID\s+NUMBER',
            r'\d{3}-\d{4}-\d{7}-\d{1}',  # Emirates ID number pattern
            r'الهوية الإماراتية'  # Arabic text for Emirates ID
        ]
        
        # Passport patterns
        passport_patterns = [
            r'PASSPORT',
            r'NATIONALITY',
            r'DATE\s+OF\s+ISSUE',
            r'PLACE\s+OF\s+BIRTH',
            r'P<',  # Common pattern in machine readable passport lines
            r'PASSEPORT',  # French
            r'REISEPASS',  # German
            r'جواز سفر'  # Arabic
        ]
        
        # Check each pattern set
        for pattern in visa_patterns:
            if re.search(pattern, text):
                logger.info(f"Detected visa document (pattern: {pattern})")
                return 'visa'
                
        for pattern in eid_patterns:
            if re.search(pattern, text):
                logger.info(f"Detected Emirates ID (pattern: {pattern})")
                return 'emirates_id'
                
        for pattern in passport_patterns:
            if re.search(pattern, text):
                logger.info(f"Detected passport (pattern: {pattern})")
                return 'passport'
        
        logger.warning("Could not determine document type from content")
        return 'unknown'

    def process_document(self, file_path: str, doc_type: Optional[str] = None) -> Dict[str, str]:
        """
        Process document and extract data.
        
        Args:
            file_path: Path to document file
            doc_type: Optional document type (will detect if not provided)
            
        Returns:
            Dict containing extracted fields
        """
        try:
            # Read file content
            with open(file_path, 'rb') as f:
                file_bytes = f.read()

            # Get Textract response
            response = self.textract.analyze_document(
                Document={'Bytes': file_bytes},
                FeatureTypes=['FORMS', 'TABLES']
            )

            # Extract text content
            text_content = '\n'.join(
                block['Text'] for block in response['Blocks'] 
                if block['BlockType'] == 'LINE'
            )

            # Detect document type if not provided
            detected_type = doc_type or self.detect_document_type(text_content)
            
            # Extract data based on detected type
            if detected_type == 'visa':
                return self._extract_visa_data(text_content)
            elif detected_type == 'emirates_id':
                return self._extract_emirates_id_data(text_content)
            elif detected_type == 'passport':
                return self._extract_passport_data(text_content)
            else:
                logger.warning(f"Unknown document type for file: {file_path}")
                return {}

        except Exception as e:
            logger.error(f"Error processing document {file_path}: {str(e)}")
            raise