# src/document_processor/gpt_processor.py

import json
import logging
import os
import base64
from typing import Dict, Optional, Any
import re
from openai import OpenAI
import tempfile
from PIL import Image

from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()


from src.utils.error_handling import handle_errors, ErrorCategory, ErrorSeverity

logger = logging.getLogger(__name__)

class GPTProcessor:
    """Document processor using OpenAI GPT-4o mini for improved OCR and document understanding."""
    
    def __init__(self, api_key: str = None):
        """Initialize GPT processor."""
        self.api_key = api_key or os.getenv('OPENAI_API_KEY')
        self.DEFAULT_VALUE = "."
        self.vision_model = "gpt-4o-mini"  # GPT-4o mini model
        
        if not self.api_key:
            logger.warning("OPENAI_API_KEY not set. GPT processing will not be available.")
            self.client = None
        else:
            try:
                self.client = OpenAI(api_key=self.api_key)
                logger.info(f"OpenAI client initialized with GPT-4o mini model")
            except Exception as e:
                logger.error(f"Failed to initialize OpenAI client: {str(e)}")
                self.client = None
    
    def _process_document_file(self, file_path: str) -> tuple:
        """
        Process the document file, converting PDFs to images if needed.
        
        Args:
            file_path: Path to the document file
            
        Returns:
            Tuple of (processed_file_path, is_temp_file)
        """
        # Check file extension
        file_ext = os.path.splitext(file_path)[1].lower()
        
        # If it's a PDF, convert to image
        if file_ext == '.pdf':
            logger.info(f"Converting PDF to image for API processing: {file_path}")
            try:
                # Import pdf2image here to avoid making it a hard dependency
                from pdf2image import convert_from_path
                
                # Create a temporary directory for the converted image
                converted_dir = os.path.join(os.path.dirname(file_path), "converted")
                os.makedirs(converted_dir, exist_ok=True)
                
                # Generate output path
                output_path = os.path.join(converted_dir, f"{os.path.basename(file_path)}.jpg")
                
                # Convert first page of PDF to image
                images = convert_from_path(file_path, first_page=1, last_page=1, dpi=300)
                
                if images and len(images) > 0:
                    # Save the first page as image
                    images[0].save(output_path, 'JPEG')
                    logger.info(f"PDF successfully converted to image: {output_path}")
                    return output_path, True  # Return path and flag as temporary
                
                logger.error(f"Failed to convert PDF to image, no pages extracted")
                # Instead of proceeding with original file (which will fail), raise error
                raise ValueError("PDF conversion failed - no pages extracted")
                
            except Exception as e:
                logger.error(f"Error converting PDF to image: {str(e)}")
                # Instead of proceeding with original, create explicit error
                raise ValueError(f"PDF conversion failed: {str(e)}")
        
        # For image files, check if they're valid
        elif file_ext in ['.jpg', '.jpeg', '.png']:
            try:
                # Try to open with PIL to verify it's a valid image
                img = Image.open(file_path)
                img.verify()  # Verify it's a valid image
                return file_path, False
            except Exception as e:
                logger.error(f"Invalid image file {file_path}: {str(e)}")
                raise ValueError(f"Invalid image file: {str(e)}")
        
        # For other file types, raise error to prevent processing failure
        else:
            logger.error(f"Unsupported file type: {file_ext}. Only PDF, JPG, JPEG, PNG supported.")
            raise ValueError(f"Unsupported file type: {file_ext}. Only PDF, JPG, JPEG, PNG supported.")
    
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
    
    def _clean_date_format(self, date_str: str) -> str:
        """
        Standardize date format to DD/MM/YYYY.
        
        Args:
            date_str: Date string in various formats
            
        Returns:
            Standardized date string
        """
        if not date_str or date_str == self.DEFAULT_VALUE:
            return self.DEFAULT_VALUE
            
        # Define month mapping
        month_map = {
            'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
            'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
        }
        
        # Try to match common date formats
        patterns = [
            # DD/MM/YYYY
            (r'(\d{1,2})[/\-\.](\d{1,2})[/\-\.](\d{4})', lambda m: f"{int(m.group(1)):02d}/{int(m.group(2)):02d}/{m.group(3)}"),
            # MM/DD/YYYY
            (r'(\d{1,2})[/\-\.](\d{1,2})[/\-\.](\d{4})', lambda m: f"{int(m.group(2)):02d}/{int(m.group(1)):02d}/{m.group(3)}"),
            # YYYY/MM/DD
            (r'(\d{4})[/\-\.](\d{1,2})[/\-\.](\d{1,2})', lambda m: f"{int(m.group(3)):02d}/{int(m.group(2)):02d}/{m.group(1)}"),
            # DD-MMM-YYYY (e.g., 01-JAN-2020)
            (r'(\d{1,2})[/\-\.\s]([A-Za-z]{3})[/\-\.\s](\d{4})', 
            lambda m: f"{int(m.group(1)):02d}/{month_map.get(m.group(2).upper(), 1)}/{m.group(3)}")
        ]
        
        for pattern, formatter in patterns:
            match = re.search(pattern, date_str)
            if match:
                try:
                    return formatter(match)
                except:
                    pass
                    
        # If no pattern matched, return original
        return date_str
    
    def _format_emirates_id(self, eid: str) -> str:
        """
        Format Emirates ID to standard format.
        
        Args:
            eid: Emirates ID string
            
        Returns:
            Formatted Emirates ID
        """
        if not eid or eid == self.DEFAULT_VALUE:
            return self.DEFAULT_VALUE
            
        # Remove non-digits
        digits = re.sub(r'\D', '', eid)
        
        # Check if we have the right number of digits
        if len(digits) == 15:
            return f"{digits[:3]}-{digits[3:7]}-{digits[7:14]}-{digits[14]}"
        
        return eid
    
    @handle_errors(ErrorCategory.EXTERNAL_SERVICE, ErrorSeverity.MEDIUM)
    def process_document(self, file_path: str, doc_type: str) -> Dict[str, str]:
        """
        Process a document with GPT-4o mini to extract structured data.
        
        Args:
            file_path: Path to the document file
            doc_type: Type of document ('passport', 'emirates_id', 'visa', etc.)
            
        Returns:
            Dictionary of extracted fields
        """
        if not self.client:
            logger.warning("OpenAI client not available, cannot process document")
            return {"error": "OpenAI client not available"}
        
        temp_file_created = False    
        try:
            logger.info(f"Processing {doc_type} document with GPT-4o mini: {file_path}")
            
            # Check if file exists
            if not os.path.exists(file_path):
                logger.error(f"File not found: {file_path}")
                return {"error": "File not found"}
            
            # Process the file (convert if needed)
            processed_file, temp_file_created = self._process_document_file(file_path)
            
            # Encode the image
            try:
                base64_image = self._encode_image(processed_file)
                # Always use image MIME type for converted files
                if temp_file_created:
                    mime_type = "image/jpeg"  # Use the correct MIME type for the converted image
                else:
                    mime_type = self._get_mime_type(processed_file)
                image_url = f"data:{mime_type};base64,{base64_image}"
                logger.info(f"Successfully encoded image with MIME type: {mime_type}")
            except Exception as e:
                logger.error(f"Failed to encode image: {str(e)}")
                return {"error": f"Image encoding failed: {str(e)}"}
            
            # Create type-specific extraction prompt
            system_prompt = "You are a document data extraction assistant. Extract the requested information accurately from the document image."
            
            if doc_type == 'passport':
                extraction_prompt = """
                Extract the following information from this passport document:
                - passport_number: The passport number (very important)
                - surname: The last name/surname (may be labeled as "Surname")
                - given_names: The first and middle names (may be labeled as "Given Name(s)")
                - nationality: The person's nationality
                - date_of_birth: Birth date in DD/MM/YYYY format
                - place_of_birth: Place of birth
                - gender: Either "Male" or "Female" (may be labeled as "Sex")
                - date_of_issue: Issue date in DD/MM/YYYY format
                - date_of_expiry: Expiry date in DD/MM/YYYY format
                
                Pay special attention to accurately extracting:
                1. The passport number
                2. The surname and given names
                3. The nationality
                4. The date of birth
                5. The gender/sex (report as "Male" or "Female", not as "M" or "F")
                
                Return ONLY a clean JSON object with these exact field names. Use "." for any missing fields.
                """
            elif doc_type == 'emirates_id':
                extraction_prompt = """
                Extract the following information from this Emirates ID card:
                - emirates_id: The ID number in format 784-XXXX-XXXXXXX-X
                - name_en: The full name in English
                - name_ar: The full name in Arabic if present
                - nationality: The person's nationality
                - gender: M or F
                - date_of_birth: Birth date in DD/MM/YYYY format
                - expiry_date: Expiry date in DD/MM/YYYY format
                
                Return ONLY a clean JSON object with these exact field names. Use "." for any missing fields.
                """
            elif doc_type == 'visa':
                extraction_prompt = """
                Extract the following information from this visa/residence permit:
                - entry_permit_no: The entry permit number
                - unified_no: The unified number (may be labeled as "U.I.D No") (typically a 10-digit number without slashes)
                - file: The file number (may be labeled as "File" or "File No.")
                - visa_file_number: The visa file number (should start with 10 or 20, often in format XXX/YYYY/ZZZZZ with slashes, always starts with XXX/YYYY/...)
                - full_name: The person's full name
                - nationality: The person's nationality
                - passport_number: The passport number
                - date_of_birth: Birth date in DD/MM/YYYY format
                - gender: Either "Male" or "Female" (not M or F)
                - profession: The profession/occupation listed
                - issue_date: Issue date in DD/MM/YYYY format
                - expiry_date: Expiry date in DD/MM/YYYY format
                - sponsor_name: The sponsor's name (employer)
                
                Pay special attention to accurately extracting:
                1. The file number (labeled "File" or "File No.")
                2. The unified number (typically a 10-digit number WITHOUT slashes)
                3. The full name
                4. The passport number
                5. Gender (report as "Male" or "Female", not as "M" or "F")
                
                Visa file number should typically start with '10' or '20'. If you see a "File" field with a value starting with these digits, extract it as visa_file_number.
                
                Return ONLY a clean JSON object with these exact field names. Use "." for any missing fields.
                """

            else:
                extraction_prompt = f"""
                Extract all important information from this {doc_type} document.
                Pay special attention to:
                - Personal identification numbers (passport number, ID number, etc.)
                - Full name
                - Dates (birth, issue, expiry)
                - Nationality
                - Gender
                
                Return ONLY a clean JSON object with extracted fields. Use "." for any missing fields.
                """
            
            # Create message with image content
            messages = [
                {"role": "system", "content": system_prompt},
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": extraction_prompt},
                        {"type": "image_url", "image_url": {"url": image_url}}
                    ]
                }
            ]
            
            # Call the vision model API
            logger.info(f"Calling GPT-4o mini API for {doc_type} document analysis")
            try:
                response = self.client.chat.completions.create(
                    model=self.vision_model,
                    messages=messages,
                    max_tokens=1500,
                    temperature=0.1  # Lower temperature for more deterministic outputs
                )
                
                # Extract content
                content = response.choices[0].message.content.strip()
                logger.debug(f"OpenAI API response: {content}")
                
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
                    
                    # Process and standardize extracted data
                    processed_data = self._post_process_extracted_data(extracted_data, doc_type)
                    
                    logger.info(f"Successfully extracted {len(processed_data)} fields from {doc_type}")
                    return processed_data
                    
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse GPT response as JSON: {str(e)}")
                    logger.warning(f"Raw response: {content}")
                    
                    # Try to extract fields with regex as fallback
                    extracted_data = self._extract_with_regex(content, doc_type)
                    if extracted_data:
                        logger.info(f"Extracted {len(extracted_data)} fields using regex fallback")
                        return extracted_data
                    
                    return {"error": "Failed to parse response", "raw_response": content}
                    
            except Exception as e:
                logger.error(f"OpenAI API call failed: {str(e)}")
                return {"error": f"API call failed: {str(e)}"}
            
        except Exception as e:
            logger.error(f"Error processing document with GPT-4o mini: {str(e)}")
            return {"error": f"Processing error: {str(e)}"}
        finally:
            # Clean up temporary file if one was created
            if temp_file_created and os.path.exists(processed_file):
                try:
                    os.remove(processed_file)
                    os.rmdir(os.path.dirname(processed_file))
                    logger.debug("Cleaned up temporary files")
                except:
                    pass
    
    def _post_process_extracted_data(self, data: Dict[str, Any], doc_type: str) -> Dict[str, str]:
        """
        Clean and standardize extracted data.
        
        Args:
            data: Raw extracted data
            doc_type: Document type
            
        Returns:
            Processed data dictionary
        """
        processed = {}
        
        # Ensure all values are strings and handle None values
        for key, value in data.items():
            if value is None or value == "":
                processed[key] = self.DEFAULT_VALUE
            else:
                processed[key] = str(value).strip()
        
        # Process date fields
        date_fields = [
            'date_of_birth', 'dob', 'date_of_issue', 'issue_date', 
            'date_of_expiry', 'expiry_date', 'passport_expiry_date', 
            'visa_expiry_date'
        ]
        
        for field in date_fields:
            if field in processed:
                processed[field] = self._clean_date_format(processed[field])
        
        # Format Emirates ID
        if 'emirates_id' in processed:
            processed['emirates_id'] = self._format_emirates_id(processed['emirates_id'])
            
        # Validate visa file number format
        if 'visa_file_number' in processed:
            visa_file = processed['visa_file_number']
            
            # If it doesn't have the expected format or doesn't start with 10/20
            if '/' not in visa_file or not any(visa_file.startswith(prefix) for prefix in ['10', '20']):
                # Check if passport number was mistakenly used as visa file number
                if 'passport_number' in processed and visa_file == processed['passport_number']:
                    processed['visa_file_number'] = self.DEFAULT_VALUE
                    logger.info("Cleared visa_file_number as it matched passport_number")
                
                # Look for a 'File' field in the raw data
                if 'file' in data and data['file'] != self.DEFAULT_VALUE:
                    # Check if it starts with 10 or 20
                    file_value = str(data['file']).strip()
                    digits_only = ''.join(filter(str.isdigit, file_value))
                    if digits_only.startswith(('10', '20')):
                        processed['visa_file_number'] = file_value
                        logger.info(f"Using 'file' field as visa_file_number: {file_value}")
        
        # Format gender to full words
        if 'gender' in processed:
            gender = processed['gender'].upper()
            if gender in ['M', 'MALE']:
                processed['gender'] = 'Male'
            elif gender in ['F', 'FEMALE']:
                processed['gender'] = 'Female'
                
        # Validate visa file number format
        if 'visa_file_number' in processed:
            visa_file = processed['visa_file_number']
            
            # If it doesn't have the expected format or doesn't start with 10/20
            if '/' not in visa_file or not any(visa_file.startswith(prefix) for prefix in ['10', '20']):
                # Check if passport number was mistakenly used as visa file number
                if 'passport_number' in processed and visa_file == processed['passport_number']:
                    processed['visa_file_number'] = self.DEFAULT_VALUE
                    logger.info("Cleared visa_file_number as it matched passport_number")
                
                # Look for a 'File' field in the raw data
                if 'file' in data and data['file'] != self.DEFAULT_VALUE:
                    # Check if it starts with 10 or 20
                    file_value = str(data['file']).strip()
                    digits_only = ''.join(filter(str.isdigit, file_value))
                    if digits_only.startswith(('10', '20')):
                        processed['visa_file_number'] = file_value
                        logger.info(f"Using 'file' field as visa_file_number: {file_value}")
        
        # Validate and fix unified_no vs visa_file_number
        if 'unified_no' in processed and 'visa_file_number' in processed:
            # Check if they might be swapped
            unified = processed['unified_no']
            visa_file = processed['visa_file_number']
            
            # Unified numbers are typically all digits and around 10 digits long
            # Visa file numbers typically have format XXX/YYYY/ZZZZZ with slashes
            
            # If unified has slashes and visa_file doesn't, they might be swapped
            if ('/' in unified and '/' not in visa_file and 
                len(visa_file.replace('-', '').replace(' ', '')) >= 8):
                # Swap them
                processed['unified_no'] = visa_file
                processed['visa_file_number'] = unified
                logger.info("Swapped unified_no and visa_file_number as they appeared to be mixed up")
        
        return processed

    
    def _extract_with_regex(self, content: str, doc_type: str) -> Dict[str, str]:
        """
        Extract fields using regex as fallback when JSON parsing fails.
        
        Args:
            content: Raw text response
            doc_type: Document type
            
        Returns:
            Dictionary of extracted fields
        """
        extracted = {}
        
        # Common patterns
        patterns = {
            "passport_number": r'"passport_number"\s*:\s*"([^"]+)"',
            "surname": r'"surname"\s*:\s*"([^"]+)"',
            "given_names": r'"given_names"\s*:\s*"([^"]+)"',
            "full_name": r'"full_name"\s*:\s*"([^"]+)"',
            "nationality": r'"nationality"\s*:\s*"([^"]+)"',
            "date_of_birth": r'"date_of_birth"\s*:\s*"([^"]+)"',
            "gender": r'"gender"\s*:\s*"([^"]+)"',
            "emirates_id": r'"emirates_id"\s*:\s*"([^"]+)"',
            "unified_no": r'"unified_no"\s*:\s*"([^"]+)"',
            "entry_permit_no": r'"entry_permit_no"\s*:\s*"([^"]+)"',
            "visa_file_number": r'"visa_file_number"\s*:\s*"([^"]+)"',
            "expiry_date": r'"expiry_date"\s*:\s*"([^"]+)"',
            "date_of_expiry": r'"date_of_expiry"\s*:\s*"([^"]+)"',
            "issue_date": r'"issue_date"\s*:\s*"([^"]+)"',
            "date_of_issue": r'"date_of_issue"\s*:\s*"([^"]+)"',
            "profession": r'"profession"\s*:\s*"([^"]+)"',
            "sponsor_name": r'"sponsor_name"\s*:\s*"([^"]+)"'
        }
        
        # Extract values using regex patterns
        for field, pattern in patterns.items():
            match = re.search(pattern, content)
            if match:
                extracted[field] = match.group(1).strip()
        
        # Apply default values for missing fields
        if doc_type == 'passport':
            required_fields = ['passport_number', 'surname', 'given_names', 'nationality', 'date_of_birth', 'gender']
        elif doc_type == 'emirates_id':
            required_fields = ['emirates_id', 'name_en', 'nationality', 'date_of_birth']
        elif doc_type == 'visa':
            required_fields = ['entry_permit_no', 'unified_no', 'visa_file_number', 'full_name', 'nationality']
        else:
            required_fields = []
        
        for field in required_fields:
            if field not in extracted:
                extracted[field] = self.DEFAULT_VALUE
        
        return extracted if extracted else None