import re
from typing import Dict, Optional
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class DataExtractor:
    """Extracts and validates data from OCR text for insurance template."""
    
    def __init__(self):
        # Regex patterns for different data types
        self.patterns = {
            'passport_number': r'P[A-Z]\d{7}|[A-Z]\d{7}',  # Common passport formats
            'emirates_id': r'\d{3}-\d{4}-\d{7}-\d{1}',
            'date': r'\d{2}[/-]\d{2}[/-]\d{4}',  # DD/MM/YYYY or DD-MM-YYYY
            'name': r'(?:Given|First) Names?[:\s]+([^\n]+)|Names?[:\s]+([^\n]+)',
            'surname': r'Surname[:\s]+([^\n]+)',
            'nationality': r'Nationality[:\s]+([^\n]+)',
            'birth_date': r'Date of Birth[:\s]+([^\n]+)',
            'gender': r'Sex[:\s]+([MF])',
            'expiry_date': r'Date of Expiry[:\s]+([^\n]+)',
            'visa_number': r'Visa No[.\s]+(\d+)',
        }

    def extract_passport_data(self, text: str) -> Dict[str, str]:
        """Extract all relevant data from passport OCR text."""
        data = {}
        
        # Log the text being processed (first 500 chars)
        logger.debug(f"Processing text:\n{text[:500]}...")

        try:
            # Extract basic passport information
            data.update(self._extract_passport_basic_info(text))
            
            # Extract name components
            data.update(self._extract_name_components(text))
            
            # Extract dates
            data.update(self._extract_dates(text))
            
            # Validate and clean extracted data
            data = self._validate_and_clean_data(data)
            
            logger.info("Extracted passport data:")
            for key, value in data.items():
                logger.info(f"{key}: {value}")
            
            return data

        except Exception as e:
            logger.error(f"Error extracting passport data: {str(e)}")
            return data

    def _extract_passport_basic_info(self, text: str) -> Dict[str, str]:
        """Extract basic passport information."""
        data = {}
        
        for field, pattern in self.patterns.items():
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                if field in ['name', 'surname', 'nationality']:
                    # Get the first non-empty group
                    value = next((g for g in match.groups() if g), '')
                    data[field] = value.strip()
                else:
                    data[field] = match.group(0).strip()
                logger.debug(f"Found {field}: {data[field]}")

        return data

    def _extract_name_components(self, text: str) -> Dict[str, str]:
        """Extract and separate name components."""
        data = {}
        
        # Try to find full name first
        full_name_match = re.search(self.patterns['name'], text, re.IGNORECASE)
        if full_name_match:
            full_name = next((g for g in full_name_match.groups() if g), '').strip()
            
            # Split name into components
            name_parts = full_name.split()
            if len(name_parts) >= 1:
                data['first_name'] = name_parts[0]
            if len(name_parts) >= 2:
                data['middle_name'] = ' '.join(name_parts[1:-1]) if len(name_parts) > 2 else ''
            if len(name_parts) >= 2:
                data['last_name'] = name_parts[-1]
                
            logger.debug(f"Extracted name components: {data}")

        return data

    def _extract_dates(self, text: str) -> Dict[str, str]:
        """Extract and validate dates."""
        data = {}
        
        # Extract birth date
        birth_match = re.search(self.patterns['birth_date'], text)
        if birth_match:
            date_str = birth_match.group(1).strip()
            try:
                # Try to parse and standardize the date
                date_obj = datetime.strptime(date_str, '%d/%m/%Y')
                data['birth_date'] = date_obj.strftime('%Y-%m-%d')
            except ValueError:
                logger.warning(f"Could not parse birth date: {date_str}")

        # Extract passport expiry date
        expiry_match = re.search(self.patterns['expiry_date'], text)
        if expiry_match:
            date_str = expiry_match.group(1).strip()
            try:
                date_obj = datetime.strptime(date_str, '%d/%m/%Y')
                data['passport_expiry'] = date_obj.strftime('%Y-%m-%d')
            except ValueError:
                logger.warning(f"Could not parse expiry date: {date_str}")

        return data

    def _validate_and_clean_data(self, data: Dict[str, str]) -> Dict[str, str]:
        """Validate and clean extracted data."""
        cleaned_data = {}
        
        for key, value in data.items():
            if value:
                # Remove extra whitespace and special characters
                cleaned_value = ' '.join(value.split())
                # Convert to uppercase for consistency where appropriate
                if key in ['passport_number', 'emirates_id']:
                    cleaned_value = cleaned_value.upper()
                cleaned_data[key] = cleaned_value

        return cleaned_data

    def extract_emirates_id_data(self, text: str) -> Dict[str, str]:
        """Extract data from Emirates ID OCR text."""
        # Similar structure to passport extraction but for Emirates ID
        # To be implemented based on actual Emirates ID format
        pass

    def extract_visa_data(self, text: str) -> Dict[str, str]:
        """Extract data from visa OCR text."""
        # Similar structure to passport extraction but for visa
        # To be implemented based on actual visa format
        pass