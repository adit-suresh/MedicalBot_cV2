import re
import logging
from typing import Dict, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class DataExtractor:
    """Extracts structured data from OCR text."""

    def extract_passport_data(self, text: str) -> Dict[str, str]:
        """Extract data from passport OCR text."""
        data = {}
        
        # Define regex patterns
        patterns = {
            'passport_number': r'Passport No[.:]\s*([A-Z0-9]{6,9})',
            'surname': r'Surname[.:]\s*([A-Za-z\s]+)',
            'given_names': r'Given Names[.:]\s*([A-Za-z\s]+)',
            'nationality': r'Nationality[.:]\s*([A-Za-z\s]+)',
            'date_of_birth': r'Date of Birth[.:]\s*(\d{1,2}\s*(?:JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\s*\d{4})',
            'gender': r'Sex[.:]\s*([MF])',
        }

        # Extract each field
        for field, pattern in patterns.items():
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                data[field] = match.group(1).strip()
                logger.debug(f"Extracted {field}: {data[field]}")
            else:
                logger.debug(f"Could not find {field}")

        # Additional validation and cleanup
        if 'date_of_birth' in data:
            try:
                # Convert to standard format
                parsed_date = datetime.strptime(data['date_of_birth'], '%d %b %Y')
                data['date_of_birth'] = parsed_date.strftime('%Y-%m-%d')
            except ValueError:
                logger.warning("Could not parse date of birth")

        return data

    def extract_emirates_id_data(self, text: str) -> Dict[str, str]:
        """Extract data from Emirates ID OCR text."""
        data = {}
        
        patterns = {
            'emirates_id': r'ID Number[.:]\s*(\d{3}-\d{4}-\d{7}-\d{1})',
            'name_en': r'Name[.:]\s*([A-Za-z\s]+)',
            'name_ar': r'الاسم[.:]\s*([؀-ۿ\s]+)',
            'nationality': r'Nationality[.:]\s*([A-Za-z\s]+)',
            'gender': r'Sex[.:]\s*([MF])'
        }

        for field, pattern in patterns.items():
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                data[field] = match.group(1).strip()
                logger.debug(f"Extracted {field}: {data[field]}")
            else:
                logger.debug(f"Could not find {field}")

        return data

    def extract_visa_data(self, text: str) -> Dict[str, str]:
        """Extract data from visa document OCR text."""
        data = {}
        
        patterns = {
            'entry_permit': r'Permit No[.:]\s*(\d+)',
            'full_name': r'Name[.:]\s*([A-Za-z\s]+)',
            'nationality': r'Nationality[.:]\s*([A-Za-z\s]+)',
            'sponsor': r'Sponsor[.:]\s*([A-Za-z\s]+)',
            'issue_date': r'Issue Date[.:]\s*(\d{2}/\d{2}/\d{4})',
            'expiry_date': r'Expiry Date[.:]\s*(\d{2}/\d{2}/\d{4})'
        }

        for field, pattern in patterns.items():
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                data[field] = match.group(1).strip()
                logger.debug(f"Extracted {field}: {data[field]}")
            else:
                logger.debug(f"Could not find {field}")

        return data

    def validate_extracted_data(self, data: Dict[str, str], doc_type: str) -> Dict[str, bool]:
        """Validate extracted data based on document type."""
        validation = {
            'is_valid': True,
            'missing_fields': [],
            'invalid_format': []
        }

        required_fields = {
            'passport': ['passport_number', 'surname', 'given_names'],
            'emirates_id': ['emirates_id', 'name_en', 'nationality'],
            'visa': ['entry_permit', 'full_name', 'nationality']
        }.get(doc_type, [])

        # Check required fields
        for field in required_fields:
            if field not in data or not data[field]:
                validation['missing_fields'].append(field)
                validation['is_valid'] = False

        # Validate formats
        if 'emirates_id' in data:
            if not re.match(r'^\d{3}-\d{4}-\d{7}-\d{1}$', data['emirates_id']):
                validation['invalid_format'].append('emirates_id')
                validation['is_valid'] = False

        if 'passport_number' in data:
            if not re.match(r'^[A-Z0-9]{6,9}$', data['passport_number']):
                validation['invalid_format'].append('passport_number')
                validation['is_valid'] = False

        return validation