import json
import logging
import os
import requests
from typing import Dict, Optional

from src.utils.error_handling import handle_errors, ErrorCategory, ErrorSeverity

logger = logging.getLogger(__name__)

class DeepseekProcessor:
    """Document processor using DeepSeek API for improved OCR and document understanding."""
    
    def __init__(self, api_key: str = None, api_url: str = None):
        """Initialize DeepSeek processor.
        
        Args:
            api_key: DeepSeek API key (defaults to environment variable)
            api_url: DeepSeek API URL (defaults to environment variable)
        """
        self.api_key = api_key or os.getenv('DEEPSEEK_API_KEY')
        self.api_url = api_url or os.getenv('DEEPSEEK_API_URL')
        self.DEFAULT_VALUE = "."
        
        if not self.api_key:
            logger.warning("DEEPSEEK_API_KEY not set. DeepSeek processing will not be available.")
            
        if not self.api_url:
            # Use default API URL or warn if completely missing
            self.api_url = "https://api.deepseek.com"
            logger.info(f"DEEPSEEK_API_URL not set. Using default: {self.api_url}")
    
    @handle_errors(ErrorCategory.EXTERNAL_SERVICE, ErrorSeverity.HIGH)
    def process_document(self, file_path: str, doc_type: Optional[str] = None) -> Dict[str, str]:
        """Process document with DeepSeek API.
        
        Args:
            file_path: Path to document file
            doc_type: Optional document type (will detect if not provided)
            
        Returns:
            Dict containing extracted fields
        """
        if not self.api_key:
            raise ValueError("DeepSeek API key not configured")
            
        # 1. Read the file
        with open(file_path, 'rb') as f:
            file_content = f.read()
            
        # 2. Detect document type if not provided
        detected_type = doc_type or self.detect_document_type(file_path)
        logger.info(f"Processing document as: {detected_type}")
        
        # 3. Prepare the API request based on document type
        field_extraction_prompt = self._get_extraction_prompt(detected_type)
        
        # 4. Call DeepSeek API
        extracted_data = self._call_deepseek_api(file_content, field_extraction_prompt, detected_type)
        
        # 5. Format and clean the response
        formatted_data = self._format_extracted_data(extracted_data, detected_type)
        
        return formatted_data

    def detect_document_type(self, file_path: str) -> str:
        """Detect document type using DeepSeek's document classification.
        
        Args:
            file_path: Path to document file
            
        Returns:
            Document type: 'passport', 'emirates_id', 'visa', or 'unknown'
        """
        if not self.api_key:
            raise ValueError("DeepSeek API key not configured")
            
        # Read the file
        with open(file_path, 'rb') as f:
            file_content = f.read()
            
        # Create classification prompt
        detection_prompt = """
        Identify the type of document in this image. Choose exactly one of:
        - passport: If it's a passport from any country
        - emirates_id: If it's an Emirates ID card
        - visa: If it's a visa or entry permit document
        - unknown: If you cannot determine the document type
        
        Respond with ONLY the document type, nothing else.
        """
        
        # Call API for classification
        response = self._call_deepseek_api(file_content, detection_prompt, "classification")
        
        # Extract the document type from response
        if response and isinstance(response, str):
            doc_type = response.strip().lower()
            if doc_type in ["passport", "emirates_id", "visa", "unknown"]:
                return doc_type
                
        # Default to unknown if we can't determine
        logger.warning(f"Could not determine document type for {file_path}, defaulting to 'unknown'")
        return "unknown"
    
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
    
    def _call_deepseek_api(self, file_content: bytes, prompt: str, context: str) -> Dict:
        """Call DeepSeek API with the file and prompt."""
        try:
            # Encode image in base64
            import base64
            encoded_image = base64.b64encode(file_content).decode('utf-8')
            
            # Prepare payload
            payload = {
                "model": "deepseek-reasoner",
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": prompt},
                            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{encoded_image}"}}
                        ]
                    }
                ],
                "max_tokens": 1024
            }
            
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}"
            }
            
            # Log API call (without the image data for security)
            safe_payload = payload.copy()
            safe_payload["messages"][0]["content"][1]["image_url"]["url"] = "[IMAGE_DATA_REDACTED]"
            logger.debug(f"Headers being sent: {headers}")
            logger.debug(f"Calling DeepSeek API for {context}: {json.dumps(safe_payload)}")
            
            # Make the API call
            response = requests.post(self.api_url, headers=headers, json=payload)
        
            # Log response status
            logger.info(f"DeepSeek API response status: {response.status_code}")
            
            # If not successful, log the response content
            if response.status_code != 200:
                logger.error(f"DeepSeek API error response: {response.text}")
                
            response.raise_for_status()
            
            # Parse the response
            result = response.json()
            logger.debug(f"DeepSeek API response: {json.dumps(result)}")
            
            # Extract the content from the response
            if "choices" in result and result["choices"]:
                content = result["choices"][0]["message"]["content"]
                
                # For classification, just return the raw content
                if context == "classification":
                    return content
                
                # For data extraction, try to parse as JSON
                try:
                    return json.loads(content)
                except json.JSONDecodeError:
                    # If it's not valid JSON, extract key-value pairs using regex
                    return self._extract_key_values_from_text(content)
            
            logger.error(f"Unexpected API response format: {result}")
            return {}
            
        except requests.exceptions.RequestException as e:
            logger.error(f"DeepSeek API request failed: {str(e)}")
            if hasattr(e, 'response') and e.response:
                logger.error(f"Response status: {e.response.status_code}")
                logger.error(f"Response body: {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"Error calling DeepSeek API: {str(e)}")
            raise
    
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