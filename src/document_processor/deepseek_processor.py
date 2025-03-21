# src/document_processor/deepseek_processor.py
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
        """Initialize DeepSeek processor."""
        self.api_key = api_key or os.getenv('DEEPSEEK_API_KEY')
        self.base_url = base_url or os.getenv('DEEPSEEK_BASE_URL', 'https://api.deepseek.com')
        self.DEFAULT_VALUE = "."
        self.vision_model = "deepseek-reasoner"  # Use DeepSeek's vision model
        
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
    
    def _encode_image(self, image_path: str) -> str:
        """
        Read and encode image file to base64.
        
        Args:
            image_path: Path to the image file
            
        Returns:
            Base64 encoded string of the image
        """
        try:
            with open(image_path, "rb") as image_file:
                return base64.b64encode(image_file.read()).decode('utf-8')
        except Exception as e:
            logger.error(f"Error encoding image {image_path}: {str(e)}")
            raise ValueError(f"Failed to encode image: {str(e)}")
            
    def _get_mime_type(self, file_path: str) -> str:
        """
        Determine MIME type from file extension.
        
        Args:
            file_path: Path to the file
            
        Returns:
            MIME type as string
        """
        import mimetypes
        mime_type, _ = mimetypes.guess_type(file_path)
        if mime_type:
            return mime_type
        
        # Default to common image types based on extension
        ext = os.path.splitext(file_path)[1].lower()
        if ext == '.pdf':
            return 'application/pdf'
        elif ext in ['.jpg', '.jpeg']:
            return 'image/jpeg'
        elif ext == '.png':
            return 'image/png'
        else:
            return 'application/octet-stream'
    
    @handle_errors(ErrorCategory.EXTERNAL_SERVICE, ErrorSeverity.MEDIUM)
    def extract_names_from_passport(self, file_path: str) -> Dict[str, str]:
        """Extract just the first and last name from a passport document."""
        if not self.client:
            logger.warning("DeepSeek client not available, cannot extract names")
            return {"first_name": self.DEFAULT_VALUE, "last_name": self.DEFAULT_VALUE}
            
        try:
            logger.info(f"Attempting to extract names from passport: {file_path}")
            
            # Check if file exists
            if not os.path.exists(file_path):
                logger.error(f"File not found: {file_path}")
                return {"first_name": self.DEFAULT_VALUE, "last_name": self.DEFAULT_VALUE}
            
            # Encode the image
            try:
                base64_image = self._encode_image(file_path)
                mime_type = self._get_mime_type(file_path)
                image_url = f"data:{mime_type};base64,{base64_image}"
                logger.info(f"Successfully encoded image with MIME type: {mime_type}")
            except Exception as e:
                logger.error(f"Failed to encode image: {str(e)}")
                return {"first_name": self.DEFAULT_VALUE, "last_name": self.DEFAULT_VALUE}
            
            # Create prompt with the image
            name_extraction_prompt = """
            Extract the first name and last name from this passport document.
            The typical format of names on passports is:
            - Surname/Last name field: Contains family name
            - Given name/First name field: Contains first and middle names
            
            Return ONLY a JSON object with two fields:
            {"first_name": "...", "last_name": "..."}
            
            Use "." for any missing fields.
            """
            
            # Create message with image content
            messages = [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": name_extraction_prompt},
                        {"type": "image_url", "image_url": {"url": image_url}}
                    ]
                }
            ]
            
            # Call the vision model API
            logger.info(f"Calling DeepSeek Vision API for document analysis")
            try:
                response = self.client.chat.completions.create(
                    model=self.vision_model,
                    messages=messages,
                    max_tokens=500
                )
                
                # Extract content
                content = response.choices[0].message.content.strip()
                logger.info(f"DeepSeek API response: {content}")
                
                # Try to parse as JSON
                try:
                    # Extract JSON from response if it contains other text
                    json_start = content.find('{')
                    json_end = content.rfind('}') + 1
                    if json_start >= 0 and json_end > json_start:
                        json_str = content[json_start:json_end]
                        name_data = json.loads(json_str)
                    else:
                        name_data = json.loads(content)
                        
                    first_name = name_data.get("first_name", self.DEFAULT_VALUE)
                    last_name = name_data.get("last_name", self.DEFAULT_VALUE)
                    
                    logger.info(f"Successfully extracted names: First={first_name}, Last={last_name}")
                    return {"first_name": first_name, "last_name": last_name}
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse DeepSeek response as JSON: {str(e)}")
                    logger.warning(f"Raw response: {content}")
                    
                    # Try to extract names with regex as fallback
                    import re
                    first_match = re.search(r'"first_name":\s*"([^"]+)"', content)
                    last_match = re.search(r'"last_name":\s*"([^"]+)"', content)
                    
                    first_name = first_match.group(1) if first_match else self.DEFAULT_VALUE
                    last_name = last_match.group(1) if last_match else self.DEFAULT_VALUE
                    
                    if first_name != self.DEFAULT_VALUE or last_name != self.DEFAULT_VALUE:
                        logger.info(f"Extracted names via regex: First={first_name}, Last={last_name}")
                        return {"first_name": first_name, "last_name": last_name}
                    
            except Exception as e:
                logger.error(f"DeepSeek API call failed: {str(e)}")
            
            # Fallback to default values
            logger.info("Using default values for name extraction as fallback")
            return {"first_name": self.DEFAULT_VALUE, "last_name": self.DEFAULT_VALUE}
                
        except Exception as e:
            logger.error(f"Error extracting names from passport: {str(e)}")
            return {"first_name": self.DEFAULT_VALUE, "last_name": self.DEFAULT_VALUE}
    
    @handle_errors(ErrorCategory.EXTERNAL_SERVICE, ErrorSeverity.MEDIUM)
    def process_document(self, file_path: str, doc_type: str) -> Dict[str, str]:
        """
        Process a document with DeepSeek to extract structured data.
        
        Args:
            file_path: Path to the document file
            doc_type: Type of document ('passport', 'emirates_id', 'visa', etc.)
            
        Returns:
            Dictionary of extracted fields
        """
        if not self.client:
            logger.warning("DeepSeek client not available, cannot process document")
            return {"error": "DeepSeek client not available"}
            
        try:
            logger.info(f"Processing {doc_type} document with DeepSeek: {file_path}")
            
            # Check if file exists
            if not os.path.exists(file_path):
                logger.error(f"File not found: {file_path}")
                return {"error": "File not found"}
            
            # Encode the image
            try:
                base64_image = self._encode_image(file_path)
                mime_type = self._get_mime_type(file_path)
                image_url = f"data:{mime_type};base64,{base64_image}"
                logger.info(f"Successfully encoded image with MIME type: {mime_type}")
            except Exception as e:
                logger.error(f"Failed to encode image: {str(e)}")
                return {"error": f"Image encoding failed: {str(e)}"}
            
            # Create type-specific extraction prompt
            if doc_type == 'passport':
                extraction_prompt = """
                Extract the following information from this passport document:
                - passport_number
                - surname (last name)
                - given_names (first and middle names)
                - nationality
                - date_of_birth (in DD/MM/YYYY format)
                - place_of_birth
                - gender (M or F)
                - date_of_issue (in DD/MM/YYYY format)
                - date_of_expiry (in DD/MM/YYYY format)
                
                Return ONLY a JSON object with these fields. Use "." for any missing fields.
                """
            elif doc_type == 'emirates_id':
                extraction_prompt = """
                Extract the following information from this Emirates ID card:
                - emirates_id (in format 784-XXXX-XXXXXXX-X)
                - name_en (full name in English)
                - name_ar (full name in Arabic if present)
                - nationality
                - gender (M or F)
                - date_of_birth (in DD/MM/YYYY format)
                - expiry_date (in DD/MM/YYYY format)
                
                Return ONLY a JSON object with these fields. Use "." for any missing fields.
                """
            elif doc_type == 'visa':
                extraction_prompt = """
                Extract the following information from this visa/residence permit:
                - entry_permit_no or visa_file_number
                - unified_no
                - full_name
                - nationality
                - passport_number
                - date_of_birth (in DD/MM/YYYY format)
                - gender (M or F)
                - profession
                - issue_date (in DD/MM/YYYY format)
                - expiry_date (in DD/MM/YYYY format)
                - sponsor_name
                
                Return ONLY a JSON object with these fields. Use "." for any missing fields.
                """
            else:
                extraction_prompt = f"""
                Extract all important information from this {doc_type} document.
                Pay special attention to:
                - Personal identification numbers
                - Full name
                - Dates (birth, issue, expiry)
                - Nationality
                
                Return ONLY a JSON object with the extracted fields. Use "." for any missing fields.
                """
            
            # Create message with image content
            messages = [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": extraction_prompt},
                        {"type": "image_url", "image_url": {"url": image_url}}
                    ]
                }
            ]
            
            # Call the vision model API
            logger.info(f"Calling DeepSeek Vision API for {doc_type} document analysis")
            try:
                response = self.client.chat.completions.create(
                    model=self.vision_model,
                    messages=messages,
                    max_tokens=1000
                )
                
                # Extract content
                content = response.choices[0].message.content.strip()
                logger.info(f"DeepSeek API response: {content}")
                
                # Try to parse as JSON
                try:
                    # Extract JSON from response if it contains other text
                    json_start = content.find('{')
                    json_end = content.rfind('}') + 1
                    if json_start >= 0 and json_end > json_start:
                        json_str = content[json_start:json_end]
                        extracted_data = json.loads(json_str)
                    else:
                        extracted_data = json.loads(content)
                    
                    # Ensure all values are strings
                    for key, value in extracted_data.items():
                        if value is None:
                            extracted_data[key] = self.DEFAULT_VALUE
                        else:
                            extracted_data[key] = str(value)
                    
                    logger.info(f"Successfully extracted {len(extracted_data)} fields from {doc_type}")
                    return extracted_data
                    
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse DeepSeek response as JSON: {str(e)}")
                    logger.warning(f"Raw response: {content}")
                    return {"error": "Failed to parse response", "raw_response": content}
                    
            except Exception as e:
                logger.error(f"DeepSeek API call failed: {str(e)}")
                return {"error": f"API call failed: {str(e)}"}
            
        except Exception as e:
            logger.error(f"Error processing document with DeepSeek: {str(e)}")
            return {"error": f"Processing error: {str(e)}"}