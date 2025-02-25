import json
import logging
import os
import base64
from typing import Dict, Optional

from openai import OpenAI

from src.utils.error_handling import handle_errors, ErrorCategory, ErrorSeverity

logger = logging.getLogger(__name__)

class DeepseekProcessor:
    """Document processor using DeepSeek API for improved OCR and document understanding."""
    
    def __init__(self, api_key: str = None, base_url: str = None):
        """Initialize DeepSeek processor.
        
        Args:
            api_key: DeepSeek API key (defaults to environment variable)
            base_url: DeepSeek API base URL (defaults to environment variable)
        """
        self.api_key = api_key or os.getenv('DEEPSEEK_API_KEY')
        self.base_url = base_url or os.getenv('DEEPSEEK_BASE_URL', 'https://api.deepseek.com')
        self.DEFAULT_VALUE = "."
        
        if not self.api_key:
            logger.warning("DEEPSEEK_API_KEY not set. DeepSeek processing will not be available.")
            self.client = None
        else:
            try:
                self.client = OpenAI(api_key=self.api_key, base_url=self.base_url)
                logger.info(f"DeepSeek client initialized with base URL: {self.base_url}")
            except Exception as e:
                logger.error(f"Failed to initialize DeepSeek client: {str(e)}")
                self.client = None
    
    @handle_errors(ErrorCategory.EXTERNAL_SERVICE, ErrorSeverity.HIGH)
    def process_document(self, file_path: str, doc_type: Optional[str] = None) -> Dict[str, str]:
        """Process document with DeepSeek API."""
        if not self.client:
            raise ValueError("DeepSeek API key not configured or client initialization failed")
            
        # 1. Determine document type if not provided
        detected_type = doc_type or self._detect_from_filename(file_path)
        logger.info(f"Processing document as: {detected_type}")
        
        # 2. Prepare the extraction prompt based on document type
        field_extraction_prompt = self._get_extraction_prompt(detected_type)
        
        # 3. Create a base64 string for the image
        try:
            base64_image = ""
            with open(file_path, "rb") as image_file:
                base64_image = base64.b64encode(image_file.read()).decode('utf-8')
            
            # 4. Call DeepSeek API with text only first as a fallback
            logger.info(f"Calling DeepSeek API for text extraction")
            response = self.client.chat.completions.create(
                model="deepseek-chat",  # Use text model as fallback
                messages=[
                    {"role": "user", "content": f"Based on a document I'm looking at, which is a {detected_type}, {field_extraction_prompt}"}
                ],
                max_tokens=1024
            )
            
            # 5. Parse and format the extracted data
            content = response.choices[0].message.content
            logger.debug(f"DeepSeek API response content: {content}")
            extracted_data = self._parse_response_content(content)
            formatted_data = self._format_extracted_data(extracted_data, detected_type)
            
            return formatted_data
            
        except Exception as e:
            logger.error(f"Error calling DeepSeek API: {str(e)}")
            # Return empty data with default values
            default_fields = {
                'passport': ['passport_number', 'surname', 'given_names', 'nationality', 
                        'date_of_birth', 'place_of_birth', 'gender', 
                        'date_of_issue', 'date_of_expiry'],
                'emirates_id': ['emirates_id', 'name_en', 'nationality', 'gender', 
                            'date_of_birth', 'expiry_date'],
                'visa': ['entry_permit_no', 'full_name', 'nationality', 'passport_number',
                    'date_of_birth', 'gender', 'profession', 'issue_date', 
                    'expiry_date', 'sponsor', 'visa_type']
            }.get(detected_type, [])
            
            return {field: self.DEFAULT_VALUE for field in default_fields}

    def detect_document_type(self, file_path: str) -> str:
        """Detect document type using DeepSeek's document classification."""
        if not self.client:
            logger.warning("DeepSeek client not available, using filename-based detection")
            return self._detect_from_filename(file_path)
            
        try:
            # Read and encode the file
            with open(file_path, 'rb') as f:
                file_content = f.read()
                encoded_image = base64.b64encode(file_content).decode('utf-8')
                
            # Create classification prompt
            detection_prompt = """
            Identify the type of document in this image. Choose exactly one of:
            - passport: If it's a passport from any country
            - emirates_id: If it's an Emirates ID card
            - visa: If it's a visa or entry permit document
            - unknown: If you cannot determine the document type
            
            Respond with ONLY the document type, nothing else.
            """
            
            # Call the API
            logger.info("Calling DeepSeek API for document classification")
            messages = [
                {
                    "role": "user", 
                    "content": [
                        {"type": "text", "text": detection_prompt},
                        {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{encoded_image}"}}
                    ]
                }
            ]
            
            response = self.client.chat.completions.create(
                model="deepseek-vision",
                messages=messages,
                max_tokens=50
            )
            
            # Extract content
            content = response.choices[0].message.content.strip().lower()
            logger.info(f"Document type detected by DeepSeek: {content}")
            
            if content in ["passport", "emirates_id", "visa", "unknown"]:
                return content
                
            # Fallback to filename-based detection
            logger.warning(f"Unexpected document type from DeepSeek: {content}")
            return self._detect_from_filename(file_path)
            
        except Exception as e:
            logger.error(f"Error detecting document type: {str(e)}")
            return self._detect_from_filename(file_path)
    
    def _detect_from_filename(self, file_path: str) -> str:
        """Detect document type from filename."""
        name = os.path.basename(file_path).lower()
        
        if 'passport' in name:
            return 'passport'
        elif 'emirates' in name or 'eid' in name or 'id card' in name:
            return 'emirates_id'
        elif 'visa' in name or 'permit' in name or 'residence' in name:
            return 'visa'
            
        return 'unknown'
    
    def _get_extraction_prompt(self, doc_type: str) -> str:
        """Get the appropriate extraction prompt based on document type."""
        prompts = {
            "passport": """
            Extract the following information from this passport image:
            - passport_number: The passport number
            - surname: Last/family name
            - given_names: First and middle names
            - nationality: Country of citizenship
            - date_of_birth: Date of birth (DD/MM/YYYY)
            - place_of_birth: Place of birth
            - gender: Gender (M or F)
            - date_of_issue: Date of issue (DD/MM/YYYY)
            - date_of_expiry: Expiry date (DD/MM/YYYY)
            
            Return as a JSON object with these exact field names. Use "." for missing fields.
            """,
            
            "emirates_id": """
            Extract the following information from this Emirates ID:
            - emirates_id: Full ID number (format: XXX-XXXX-XXXXXXX-X)
            - name_en: Full name in English
            - nationality: Nationality/citizenship
            - gender: Gender (M or F)
            - date_of_birth: Date of birth (DD/MM/YYYY)
            - expiry_date: Expiry date (DD/MM/YYYY)
            
            Return as a JSON object with these exact field names. Use "." for missing fields.
            """,
            
            "visa": """
            Extract the following information from this visa document:
            - entry_permit_no: Visa/entry permit number
            - full_name: Full name as shown
            - nationality: Nationality/citizenship
            - passport_number: Associated passport number
            - date_of_birth: Date of birth (DD/MM/YYYY)
            - gender: Gender (M or F)
            - profession: Job title/profession
            - issue_date: Issue date (DD/MM/YYYY)
            - expiry_date: Expiry date (DD/MM/YYYY)
            - sponsor: Sponsor name (if shown)
            - visa_type: Type of visa/permit
            
            Return as a JSON object with these exact field names. Use "." for missing fields.
            """
        }
        
        return prompts.get(doc_type, """
        Extract as much information as possible from this document, including:
        - Any ID numbers (passport, Emirates ID, visa numbers)
        - Full name
        - Nationality
        - Date of birth
        - Gender
        - Issue date
        - Expiry date
        
        Return as a JSON object with appropriate field names. Use "." for missing fields.
        """)
    
    def _parse_response_content(self, content: str) -> Dict:
        """Parse the content from the DeepSeek API response."""
        try:
            # First try to parse as JSON
            return json.loads(content)
        except json.JSONDecodeError:
            # If not valid JSON, try to extract key-value pairs
            return self._extract_key_values_from_text(content)
    
    def _extract_key_values_from_text(self, text: str) -> Dict:
        """Extract key-value pairs from text when JSON parsing fails."""
        import re
        
        # Try to find patterns like "key: value" or "key - value"
        pairs = {}
        
        # Different patterns to try
        patterns = [
            r'(\w+)[\s:]+([^"\n]+)',  # key: value
            r'"(\w+)"[\s:]+([^"\n]+)',  # "key": value
            r'(\w+)[\s:]+\"([^"]+)\"',  # key: "value"
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text)
            for key, value in matches:
                key = key.strip().lower()
                value = value.strip().strip(',"\'')
                if key and value:
                    pairs[key] = value
        
        return pairs
    
    def _format_extracted_data(self, data: Dict, doc_type: str) -> Dict:
        """Format and clean the extracted data."""
        # Default fields based on document type
        default_fields = {
            'passport': ['passport_number', 'surname', 'given_names', 'nationality', 
                        'date_of_birth', 'place_of_birth', 'gender', 
                        'date_of_issue', 'date_of_expiry'],
            'emirates_id': ['emirates_id', 'name_en', 'nationality', 'gender', 
                          'date_of_birth', 'expiry_date'],
            'visa': ['entry_permit_no', 'full_name', 'nationality', 'passport_number',
                    'date_of_birth', 'gender', 'profession', 'issue_date', 
                    'expiry_date', 'sponsor', 'visa_type']
        }.get(doc_type, [])
        
        # Initialize with default values
        formatted = {field: self.DEFAULT_VALUE for field in default_fields}
        
        # Update with extracted values
        for key, value in data.items():
            norm_key = self._normalize_field_name(key)
            if norm_key in formatted:
                # Clean and format the value
                formatted[norm_key] = self._clean_value(value, norm_key)
            elif norm_key:  # Add additional fields if they have values
                formatted[norm_key] = self._clean_value(value, norm_key)
        
        # Special handling for Emirates ID format
        if 'emirates_id' in formatted and formatted['emirates_id'] != self.DEFAULT_VALUE:
            formatted['emirates_id'] = self._format_emirates_id(formatted['emirates_id'])
        
        return formatted
    
    def _normalize_field_name(self, field: str) -> str:
        """Normalize field names for consistency."""
        if not field:
            return ""
            
        field = field.lower().strip()
        
        # Map common variations to standard field names
        field_mapping = {
            'passport no': 'passport_number',
            'passport number': 'passport_number',
            'document number': 'passport_number',
            'id number': 'emirates_id',
            'emirates id': 'emirates_id',
            'eid': 'emirates_id',
            'name': 'full_name',
            'surname': 'last_name',
            'given names': 'first_name',
            'date of birth': 'date_of_birth',
            'birth date': 'date_of_birth',
            'dob': 'date_of_birth',
            'sex': 'gender',
            'issue date': 'date_of_issue',
            'date of issue': 'date_of_issue',
            'expiry date': 'date_of_expiry',
            'date of expiry': 'date_of_expiry',
            'valid until': 'date_of_expiry',
            'entry permit': 'entry_permit_no',
            'permit number': 'entry_permit_no',
            'visa number': 'entry_permit_no',
            'occupation': 'profession'
        }
        
        return field_mapping.get(field, field.replace(' ', '_'))
    
    def _clean_value(self, value: str, field_type: str) -> str:
        """Clean and format values based on field type."""
        if not value or value == self.DEFAULT_VALUE:
            return self.DEFAULT_VALUE
            
        value = str(value).strip()
        
        # Date field formatting
        if field_type in ['date_of_birth', 'date_of_issue', 'date_of_expiry', 'expiry_date']:
            return self._format_date(value)
            
        # Gender normalization
        if field_type == 'gender':
            value = value.upper()
            if value in ['MALE', 'M']:
                return 'M'
            elif value in ['FEMALE', 'F']:
                return 'F'
            return value
            
        return value
    
    def _format_date(self, date_str: str) -> str:
        """Format date to DD/MM/YYYY."""
        import re
        from datetime import datetime
        
        # Remove any extraneous text
        date_str = re.sub(r'[^0-9/\-.]', ' ', date_str).strip()
        
        # Try different date formats
        formats = [
            ('%d/%m/%Y', r'\d{1,2}/\d{1,2}/\d{4}'),
            ('%d-%m-%Y', r'\d{1,2}-\d{1,2}-\d{4}'),
            ('%Y/%m/%d', r'\d{4}/\d{1,2}/\d{1,2}'),
            ('%Y-%m-%d', r'\d{4}-\d{1,2}-\d{1,2}'),
            ('%d.%m.%Y', r'\d{1,2}\.\d{1,2}\.\d{4}')
        ]
        
        for fmt, pattern in formats:
            if re.match(pattern, date_str):
                try:
                    parsed_date = datetime.strptime(date_str, fmt)
                    return parsed_date.strftime('%d/%m/%Y')
                except ValueError:
                    continue
        
        # If standard formats fail, try more aggressive parsing
        try:
            # Extract numbers
            numbers = re.findall(r'\d+', date_str)
            if len(numbers) >= 3:
                day, month, year = int(numbers[0]), int(numbers[1]), int(numbers[2])
                
                # Adjust year if it's a 2-digit year
                if year < 100:
                    year = 2000 + year if year < 30 else 1900 + year
                    
                # Validate and create date
                if 1 <= day <= 31 and 1 <= month <= 12 and 1900 <= year <= 2100:
                    return f"{day:02d}/{month:02d}/{year}"
        except:
            pass
            
        # Return original if parsing fails
        return date_str
    
    def _format_emirates_id(self, eid: str) -> str:
        """Format Emirates ID to include hyphens in correct positions."""
        import re
        
        # Remove any non-digit characters
        digits = re.sub(r'[^0-9]', '', str(eid))
        
        # Format correctly if we have 15 digits
        if len(digits) == 15:
            return f"{digits[:3]}-{digits[3:7]}-{digits[7:14]}-{digits[14]}"
            
        return eid