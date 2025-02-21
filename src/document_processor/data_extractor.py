import re
import logging
from typing import Dict, Optional, List
from datetime import datetime
import dateutil.parser

logger = logging.getLogger(__name__)

class EnhancedDataExtractor:
    """Enhanced extractor for structured data from OCR text with improved pattern matching."""

    def __init__(self):
        # Initialize date formatter
        self.output_date_format = '%d-%m-%Y'  # Changed to dd-mm-yyyy

    def extract_passport_data(self, text: str) -> Dict[str, str]:
        """Extract data from passport OCR text with improved pattern matching."""
        data = {}
        
        # Define regex patterns with more variations
        patterns = {
            'passport_number': [
                r'Passport No[.:]?\s*([A-Z0-9]{6,9})',
                r'Passport Number[.:]?\s*([A-Z0-9]{6,9})',
                r'No.?\s*([A-Z0-9]{6,9})',
                r'Document No[.:]?\s*([A-Z0-9]{6,9})'
            ],
            'surname': [
                r'Surname[.:]?\s*([A-Za-z\s]+)',
                r'Last Name[.:]?\s*([A-Za-z\s]+)',
                r'Family Name[.:]?\s*([A-Za-z\s]+)'
            ],
            'given_names': [
                r'Given Names[.:]?\s*([A-Za-z\s]+)',
                r'First Name[.:]?\s*([A-Za-z\s]+)',
                r'Given Name[.:]?\s*([A-Za-z\s]+)',
                r'First Names[.:]?\s*([A-Za-z\s]+)'
            ],
            'nationality': [
                r'Nationality[.:]?\s*([A-Za-z\s]+)',
                r'Citizen of[.:]?\s*([A-Za-z\s]+)',
                r'Citizenship[.:]?\s*([A-Za-z\s]+)'
            ],
            'date_of_birth': [
                r'Date of Birth[.:]?\s*(\d{1,2}\s*(?:JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\s*\d{4})',
                r'Date of Birth[.:]?\s*(\d{1,2}[-/.]\d{1,2}[-/.]\d{2,4})',
                r'DOB[.:]?\s*(\d{1,2}[-/.]\d{1,2}[-/.]\d{2,4})',
                r'Born[.:]?\s*(\d{1,2}[-/.]\d{1,2}[-/.]\d{2,4})'
            ],
            'gender': [
                r'Sex[.:]?\s*([MF])',
                r'Gender[.:]?\s*([MF])',
                r'Sex[.:]?\s*(MALE|FEMALE)',
                r'Gender[.:]?\s*(MALE|FEMALE)'
            ],
            'passport_expiry_date': [
                r'Date of Expiry[.:]?\s*(\d{1,2}\s*(?:JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\s*\d{4})',
                r'Expiry Date[.:]?\s*(\d{1,2}[-/.]\d{1,2}[-/.]\d{2,4})',
                r'Expiry[.:]?\s*(\d{1,2}[-/.]\d{1,2}[-/.]\d{2,4})',
                r'Expires[.:]?\s*(\d{1,2}[-/.]\d{1,2}[-/.]\d{2,4})'
            ],
            'place_of_birth': [
                r'Place of Birth[.:]?\s*([A-Za-z\s]+)',
                r'Birth Place[.:]?\s*([A-Za-z\s]+)'
            ]
        }

        # Extract each field with multiple pattern attempts
        for field, pattern_list in patterns.items():
            for pattern in pattern_list:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    data[field] = match.group(1).strip()
                    logger.debug(f"Extracted {field}: {data[field]}")
                    break  # Stop trying patterns for this field once found
                
        # Handle name fields special cases
        if 'given_names' in data and 'surname' not in data:
            # Try to extract surname from full name format
            full_name = data['given_names']
            parts = full_name.split()
            if len(parts) >= 2:
                data['surname'] = parts[-1]
                data['given_names'] = ' '.join(parts[:-1])
                logger.debug(f"Split full name into given_names: {data['given_names']} and surname: {data['surname']}")
                
        # Create first_name and last_name fields for standardization
        if 'given_names' in data and 'surname' in data:
            data['first_name'] = data['given_names'].split()[0] if data['given_names'] else ''
            data['last_name'] = data['surname']
        elif 'given_names' in data:
            parts = data['given_names'].split()
            if len(parts) >= 2:
                data['first_name'] = parts[0]
                data['last_name'] = parts[-1]
            else:
                data['first_name'] = data['given_names']
                data['last_name'] = ''

        # Process dates to standard format
        date_fields = ['date_of_birth', 'passport_expiry_date']
        for date_field in date_fields:
            if date_field in data:
                try:
                    # Try to parse various date formats
                    parsed_date = self._parse_date(data[date_field])
                    data[date_field] = parsed_date.strftime(self.output_date_format)
                    logger.debug(f"Standardized {date_field}: {data[date_field]}")
                except (ValueError, TypeError) as e:
                    logger.warning(f"Could not parse {date_field}: {data[date_field]} - {str(e)}")

        return data

    def extract_emirates_id_data(self, text: str) -> Dict[str, str]:
        """Extract data from Emirates ID OCR text with more robust patterns."""
        data = {}
        
        patterns = {
            'emirates_id': [
                r'ID Number[.:]?\s*(\d{3}-\d{4}-\d{7}-\d{1})',
                r'ID No[.:]?\s*(\d{3}-\d{4}-\d{7}-\d{1})',
                r'Identity Number[.:]?\s*(\d{3}-\d{4}-\d{7}-\d{1})',
                r'ID[.:]?\s*(\d{3}-\d{4}-\d{7}-\d{1})',
                r'(\d{3}-\d{4}-\d{7}-\d{1})'  # Try just the format itself
            ],
            'name_en': [
                r'Name[.:]?\s*([A-Za-z\s]+)',
                r'Full Name[.:]?\s*([A-Za-z\s]+)',
            ],
            'name_ar': [
                r'الاسم[.:]?\s*([؀-ۿ\s]+)',
                r'اسم[.:]?\s*([؀-ۿ\s]+)',
            ],
            'nationality': [
                r'Nationality[.:]?\s*([A-Za-z\s]+)',
                r'الجنسية[.:]?\s*([A-Za-z\s]+)'
            ],
            'gender': [
                r'Sex[.:]?\s*([MF])',
                r'Gender[.:]?\s*([MF])',
                r'Sex[.:]?\s*(MALE|FEMALE)',
                r'Gender[.:]?\s*(MALE|FEMALE)',
                r'الجنس[.:]?\s*([ذأ])'
            ],
            'date_of_birth': [
                r'Date of Birth[.:]?\s*(\d{1,2}/\d{1,2}/\d{4})',
                r'DOB[.:]?\s*(\d{1,2}/\d{1,2}/\d{4})',
                r'Birth Date[.:]?\s*(\d{1,2}/\d{1,2}/\d{4})',
                r'تاريخ الميلاد[.:]?\s*(\d{1,2}/\d{1,2}/\d{4})'
            ],
            'emirates_id_expiry': [
                r'Expiry Date[.:]?\s*(\d{1,2}/\d{1,2}/\d{4})',
                r'Card Expiry[.:]?\s*(\d{1,2}/\d{1,2}/\d{4})',
                r'Valid Until[.:]?\s*(\d{1,2}/\d{1,2}/\d{4})',
                r'تاريخ الانتهاء[.:]?\s*(\d{1,2}/\d{1,2}/\d{4})'
            ]
        }

        # Extract each field
        for field, pattern_list in patterns.items():
            for pattern in pattern_list:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    data[field] = match.group(1).strip()
                    logger.debug(f"Extracted {field}: {data[field]}")
                    break  # Stop trying patterns once found

        # Handle name extraction for first/last name
        if 'name_en' in data:
            name_parts = data['name_en'].split()
            if len(name_parts) >= 2:
                data['first_name'] = name_parts[0]
                data['last_name'] = name_parts[-1]
            else:
                data['first_name'] = data['name_en']
                data['last_name'] = ''

        # Process dates to standard format
        date_fields = ['date_of_birth', 'emirates_id_expiry']
        for date_field in date_fields:
            if date_field in data:
                try:
                    parsed_date = self._parse_date(data[date_field])
                    data[date_field] = parsed_date.strftime(self.output_date_format)
                    logger.debug(f"Standardized {date_field}: {data[date_field]}")
                except (ValueError, TypeError) as e:
                    logger.warning(f"Could not parse {date_field}: {data[date_field]} - {str(e)}")

        return data

    def extract_visa_data(self, text: str) -> Dict[str, str]:
        """Extract data from visa document OCR text with improved pattern matching."""
        data = {}
        
        patterns = {
            'entry_permit': [
                r'Permit No[.:]?\s*(\d+)',
                r'Permit Number[.:]?\s*(\d+)',
                r'Visa No[.:]?\s*(\d+)',
                r'Visa Number[.:]?\s*(\d+)'
            ],
            'full_name': [
                r'Name[.:]?\s*([A-Za-z\s]+)',
                r'Full Name[.:]?\s*([A-Za-z\s]+)',
                r'Passenger Name[.:]?\s*([A-Za-z\s]+)'
            ],
            'nationality': [
                r'Nationality[.:]?\s*([A-Za-z\s]+)',
                r'Country[.:]?\s*([A-Za-z\s]+)'
            ],
            'sponsor': [
                r'Sponsor[.:]?\s*([A-Za-z0-9\s]+)',
                r'Sponsored By[.:]?\s*([A-Za-z0-9\s]+)'
            ],
            'issue_date': [
                r'Issue Date[.:]?\s*(\d{1,2}/\d{1,2}/\d{4})',
                r'Date of Issue[.:]?\s*(\d{1,2}/\d{1,2}/\d{4})',
                r'Issued on[.:]?\s*(\d{1,2}/\d{1,2}/\d{4})'
            ],
            'expiry_date': [
                r'Expiry Date[.:]?\s*(\d{1,2}/\d{1,2}/\d{4})',
                r'Date of Expiry[.:]?\s*(\d{1,2}/\d{1,2}/\d{4})',
                r'Valid Until[.:]?\s*(\d{1,2}/\d{1,2}/\d{4})'
            ],
            'visa_type': [
                r'Visa Type[.:]?\s*([A-Za-z\s]+)',
                r'Type[.:]?\s*([A-Za-z\s]+)'
            ]
        }

        # Extract each field
        for field, pattern_list in patterns.items():
            for pattern in pattern_list:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    data[field] = match.group(1).strip()
                    logger.debug(f"Extracted {field}: {data[field]}")
                    break  # Stop trying patterns once found

        # Handle name extraction for first/last name
        if 'full_name' in data:
            name_parts = data['full_name'].split()
            if len(name_parts) >= 2:
                data['first_name'] = name_parts[0]
                data['last_name'] = name_parts[-1]
            else:
                data['first_name'] = data['full_name']
                data['last_name'] = ''

        # Process dates to standard format
        date_fields = ['issue_date', 'expiry_date']
        for date_field in date_fields:
            if date_field in data:
                try:
                    parsed_date = self._parse_date(data[date_field])
                    data[date_field] = parsed_date.strftime(self.output_date_format)
                    # Map visa expiry_date to visa_expiry_date for consistency
                    if date_field == 'expiry_date':
                        data['visa_expiry_date'] = data[date_field]
                except (ValueError, TypeError) as e:
                    logger.warning(f"Could not parse {date_field}: {data[date_field]} - {str(e)}")

        return data

    def _parse_date(self, date_str: str) -> datetime:
        """
        Parse date from various formats to datetime object.
        
        Handles various formats like:
        - 12 JAN 1990
        - 12/01/1990
        - 12-01-1990
        - 01/12/90
        """
        # Handle special case for "12 JAN 1990" format
        month_pattern = r'(\d{1,2})\s*(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\s*(\d{4})'
        month_match = re.match(month_pattern, date_str, re.IGNORECASE)
        
        if month_match:
            day = month_match.group(1)
            month = {
                'JAN': '01', 'FEB': '02', 'MAR': '03', 'APR': '04', 'MAY': '05', 'JUN': '06',
                'JUL': '07', 'AUG': '08', 'SEP': '09', 'OCT': '10', 'NOV': '11', 'DEC': '12'
            }[month_match.group(2).upper()]
            year = month_match.group(3)
            
            normalized_date = f"{day}/{month}/{year}"
            return datetime.strptime(normalized_date, '%d/%m/%Y')
        
        # Try various date formats with dateutil parser
        try:
            return dateutil.parser.parse(date_str, dayfirst=True)  # Assume day comes first
        except:
            # Last resort: try to normalize the date string
            # Convert separators to consistent format
            normalized = re.sub(r'[/\-\.]', '/', date_str)
            
            # Try common formats
            formats = ['%d/%m/%Y', '%d/%m/%y', '%Y/%m/%d', '%m/%d/%Y']
            for fmt in formats:
                try:
                    return datetime.strptime(normalized, fmt)
                except ValueError:
                    continue
            
            # If all else fails, raise error
            raise ValueError(f"Could not parse date: {date_str}")
            
    def consolidate_data(self, document_data: List[Dict[str, str]]) -> Dict[str, str]:
        """
        Consolidate data from multiple documents with conflict resolution.
        
        Args:
            document_data: List of extracted data dictionaries from documents
            
        Returns:
            Consolidated data dictionary
        """
        if not document_data:
            return {}
            
        # Priorities for data sources (highest to lowest)
        doc_type_priority = {
            'passport': 3,  # Highest priority
            'emirates_id': 2,
            'visa': 1
        }
        
        # Field priorities by document type
        field_priorities = {
            'first_name': ['passport', 'emirates_id', 'visa'],
            'last_name': ['passport', 'emirates_id', 'visa'],
            'nationality': ['passport', 'emirates_id', 'visa'],
            'date_of_birth': ['passport', 'emirates_id', 'visa'],
            'gender': ['passport', 'emirates_id', 'visa'],
            'passport_number': ['passport'],
            'passport_expiry_date': ['passport'],
            'emirates_id': ['emirates_id'],
            'emirates_id_expiry': ['emirates_id'],
            'visa_expiry_date': ['visa']
        }
        
        # Organize data by document type
        typed_data = {}
        for data in document_data:
            doc_type = data.get('_doc_type', 'unknown')
            typed_data[doc_type] = data
            
        # Consolidate data based on priorities
        consolidated = {}
        
        for field in field_priorities.keys():
            # Get prioritized list of document types for this field
            doc_types = field_priorities.get(field, [])
            
            # Try each document type in priority order
            for doc_type in doc_types:
                if doc_type in typed_data and field in typed_data[doc_type] and typed_data[doc_type][field]:
                    consolidated[field] = typed_data[doc_type][field]
                    break
                    
        # Add any remaining fields not in our priority list
        for data in document_data:
            for field, value in data.items():
                if field != '_doc_type' and field not in consolidated and value:
                    consolidated[field] = value
                    
        return consolidated
            
    def validate_extracted_data(self, data: Dict[str, str], doc_type: str) -> Dict[str, bool]:
        """Validate extracted data based on document type."""
        validation = {
            'is_valid': True,
            'missing_fields': [],
            'invalid_format': []
        }

        required_fields = {
            'passport': ['passport_number', 'first_name', 'last_name', 'nationality'],
            'emirates_id': ['emirates_id', 'first_name', 'last_name', 'nationality'],
            'visa': ['entry_permit', 'first_name', 'last_name', 'nationality']
        }.get(doc_type, [])

        # Check required fields
        for field in required_fields:
            if field not in data or not data[field]:
                validation['missing_fields'].append(field)
                validation['is_valid'] = False

        # Validate specific formats
        format_validations = {
            'emirates_id': r'^\d{3}-\d{4}-\d{7}-\d{1}$',
            'passport_number': r'^[A-Z0-9]{6,9}$',
            'date_of_birth': r'^\d{2}-\d{2}-\d{4}$',
            'passport_expiry_date': r'^\d{2}-\d{2}-\d{4}$',
            'emirates_id_expiry': r'^\d{2}-\d{2}-\d{4}$',
            'visa_expiry_date': r'^\d{2}-\d{2}-\d{4}$'
        }
        
        for field, pattern in format_validations.items():
            if field in data and data[field]:
                if not re.match(pattern, data[field]):
                    validation['invalid_format'].append(field)
                    validation['is_valid'] = False

        return validation