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
import time
import random
import threading

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
        # Remove non-digits and non-hyphens
        digits_only = re.sub(r'[^0-9]', '', str(eid))
        
        # Check if we have the right number of digits
        if len(digits_only) == 15:
            formatted = f"{digits_only[:3]}-{digits_only[3:7]}-{digits_only[7:14]}-{digits_only[14]}"
            logger.info(f"Formatted Emirates ID with hyphens: {formatted}")
            return formatted
        # If it has fewer digits, it might be missing the check digit
        elif len(digits_only) == 14:
            # Assume last digit is missing, add a placeholder
            formatted = f"{digits_only[:3]}-{digits_only[3:7]}-{digits_only[7:14]}-1"
            logger.warning(f"Emirates ID missing check digit, added placeholder: {formatted}")
            return formatted
        
        # If the format is completely off, return original
        return eid
    
    @handle_errors(ErrorCategory.EXTERNAL_SERVICE, ErrorSeverity.MEDIUM)
    def process_document(self, file_path: str, doc_type: str) -> Dict[str, str]:
        """
        Process a document with GPT-4o mini to extract structured data.
        Uses aggressive rate limiting to prevent hitting API limits.
        
        Args:
            file_path: Path to the document file
            doc_type: Type of document ('passport', 'emirates_id', 'visa', etc.)
            
        Returns:
            Dictionary of extracted fields
        """
        if not self.client:
            logger.warning("OpenAI client not available, cannot process document")
            return {"error": "OpenAI client not available"}
        
        # ADVANCED RATE LIMITER
        # Add class variables if they don't exist
        if not hasattr(type(self), '_request_times'):
            type(self)._request_times = []
        if not hasattr(type(self), '_request_count'):
            type(self)._request_count = 0
        if not hasattr(type(self), '_rate_limit_lock'):
            import threading
            type(self)._rate_limit_lock = threading.RLock()
            
        # Use thread-safe locking to handle rate limiting
        with type(self)._rate_limit_lock:
            current_time = time.time()
            
            # Clean up old timestamps (older than 60 seconds)
            type(self)._request_times = [t for t in type(self)._request_times if current_time - t < 60]
            
            # Calculate current rate
            current_rate = len(type(self)._request_times)
            
            # If we're above 40 requests per minute, throttle
            if current_rate >= 15:
                # Calculate how long to wait
                if type(self)._request_times:
                    oldest_timestamp = min(type(self)._request_times)
                    wait_time = 60 - (current_time - oldest_timestamp)
                    
                    # Make sure wait time is reasonable
                    wait_time = max(1.0, min(wait_time, 5.0))
                    
                    logger.info(f"Rate limiting: Waiting {wait_time:.2f}s to avoid rate limits (current rate: {current_rate} requests/min)")
                    time.sleep(wait_time)
                    
                    # Refresh times after waiting
                    current_time = time.time()
                    type(self)._request_times = [t for t in type(self)._request_times if current_time - t < 60]
            
            # Minimum delay between requests (300ms)
            if type(self)._request_times and (current_time - max(type(self)._request_times)) < 0.3:
                delay = 0.3 - (current_time - max(type(self)._request_times))
                time.sleep(delay)
                current_time = time.time()
            
            # Record this request time
            type(self)._request_times.append(current_time)
            type(self)._request_count += 1
            
            # Log request stats periodically
            if type(self)._request_count % 5 == 0:
                logger.info(f"GPT API request stats: {current_rate} requests in last minute, total: {type(self)._request_count}")
        
        # PROCESS OPTIMIZATION: Check document type for faster handling
        # For passport or emirates_id, which have well-defined structures, we can skip processing if we have good existing data
        if doc_type in ['passport', 'emirates_id']:
            # See if we have key fields already from other documents
            existing_fields = 0
            if hasattr(self, '_extracted_cache'):
                # Check if key fields for this document type already exist in our cache
                if doc_type == 'passport' and all(k in self._extracted_cache for k in ['passport_number', 'surname', 'given_names', 'nationality']):
                    existing_fields += 1
                elif doc_type == 'emirates_id' and all(k in self._extracted_cache for k in ['emirates_id', 'name_en', 'nationality']):
                    existing_fields += 1
                    
            # If we have strong existing data, we might skip processing 50% of the time
            if existing_fields > 0 and random.random() < 0.5:
                logger.info(f"Optimization: Skipping {doc_type} processing since we already have good data")
                return {"skipped": "Already have good data for key fields"}
        
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
                - emirates_id: The ID number in format 784-XXXX-XXXXXXX-X (MUST CONTAIN THE HYPHENS)
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
                YOUR MOST CRITICAL TASK IS TO EXTRACT THESE TWO DISTINCT NUMBERS:

                1. unified_no (HIGHEST PRIORITY):
                - CONTAINS ONLY DIGITS, NO SLASHES OR HYPHENS
                - Usually 8-15 digits long (e.g., "12345678" or "784123456789321" etc.)
                - Appears near text like "U.I.D. No.", "ID Number", "Unified No.", "Unified Number", or "UID"
                - May be displayed as "UID: 12345678" or "Unified No: 12345678" or "241104237 : U.I.D No" or "784197228451752 : U.I.D No"
                - IS COMPLETELY DIFFERENT FROM VISA FILE NUMBER
                - NEVER includes slashes - if you see slashes, it's NOT the unified number
                - OFTEN appears at the top part of the document

                2. visa_file_number (SECOND HIGHEST PRIORITY):
                - ALWAYS CONTAINS SLASHES in format XXX/YYYY/ZZ.... or XXX/YYYY/Z/......
                - Examples: "201/2023/1234567" or "101/2024/987654"
                - Usually labeled as "ENTRY PERMIT NO", "File", "File No", "Visa File Number"
                - First section (before first slash) is often "201" (Dubai) or "101" (Abu Dhabi)
                - ALWAYS has slashes separating the parts

                CRITICAL: These are two different numbers. DO NOT extract one from the other.
                NEVER create a unified_no by removing slashes from visa_file_number.
                If you can't find the unified_no, use "." instead of guessing.

  
                Extract the following CRITICAL information from this visa/residence permit:
                - entry_permit_no: The entry permit number (can be same as visa_file_number)
                - unified_no: The unified number (digits only, NO SLASHES)
                - visa_file_number: The visa file number (has SLASHES in it)
                - full_name: The person's full name (HIGHEST PRIORITY)
                - nationality: The person's nationality (CRITICAL)
                - passport_number: The passport number (CRITICAL)
                - date_of_birth: Birth date in DD/MM/YYYY format (CRITICAL)
                - gender: "Male" or "Female" (CRITICAL)
                - profession: The profession/occupation listed
                - issue_date: Issue date in DD/MM/YYYY format
                - expiry_date: Expiry date in DD/MM/YYYY format
                - sponsor_name: The sponsor's name (employer)

                
                Pay special attention to accurately extracting:
                
                1. entry_permit_no - this is critical (may appear as "Entry Permit No", "File", or "File No.")
                2. unified_no - this is critical (typically a 10-digit number WITHOUT slashes)
                3. visa_file_number should contain '/' (slashes) and often starts with '20/' or '10/'
                4. The full name
                5. The passport number
                
                Note that the entry permit number and visa file number might be the same in some documents, and different in others.
                The unified number is typically a 10-digit number WITHOUT slashes and often appears near "U.I.D No".
                
                Return ONLY a clean JSON object with these exact field names. Use "." for any missing fields.
                """

            else:
                extraction_prompt = f"""
                Extract all important information from this {doc_type} document.
                Pay special attention to:
                - Personal identification numbers (passport number, emirated ID number, Visa File Number, Unified Number)
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
            
            # Use lower temperature for more deterministic outputs
            temperature = 0.1
            
            # Call the vision model API
            logger.info(f"Calling GPT-4o mini API for {doc_type} document analysis")
            
            # Add optimized retry handling with exponential backoff
            max_retries = 5
            base_delay = 1.0  # 1 second base delay
            
            for attempt in range(max_retries):
                try:
                    # Add jitter to prevent thundering herd
                    jitter = random.uniform(0.1, 0.5)
                    
                    response = self.client.chat.completions.create(
                        model=self.vision_model,
                        messages=messages,
                        max_tokens=1500,
                        temperature=temperature
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
                        
                        # OPTIMIZATION: Cache good results for future reference
                        # This lets us potentially skip some document processing
                        if not hasattr(self, '_extracted_cache'):
                            self._extracted_cache = {}
                            
                        # Update cache with non-default values
                        for key, value in processed_data.items():
                            if value != self.DEFAULT_VALUE:
                                self._extracted_cache[key] = value
                        
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
                    error_message = str(e)
                    
                    # Check if it's a rate limit error
                    if "rate_limit_exceeded" in error_message or "429" in error_message:
                        # Calculate backoff time with exponential increase
                        delay = base_delay * (2 ** attempt) + jitter
                        logger.info(f"Rate limit reached. Retrying in {delay:.2f}s (attempt {attempt+1}/{max_retries})")
                        time.sleep(delay)
                        
                        # Apply more aggressive rate limiting after hitting a limit
                        with type(self)._rate_limit_lock:
                            # Force a longer delay for all subsequent requests
                            type(self)._request_times = [t for t in type(self)._request_times if current_time - t < 30]
                            type(self)._request_times.append(current_time)
                        
                        # Continue to next attempt if we haven't exhausted retries
                        if attempt < max_retries - 1:
                            continue
                    
                    # Either not a rate limit error or we've exhausted retries
                    logger.error(f"OpenAI API call failed: {str(e)}")
                    return {"error": f"API call failed: {str(e)}"}
            
            # If we've exhausted all retries and still getting rate limit errors
            logger.error(f"Failed to process document after {max_retries} retries due to rate limits")
            return {"error": f"Rate limit exceeded after {max_retries} retries"}
            
        except Exception as e:
            logger.error(f"Error processing document with GPT-4o mini: {str(e)}")
            return {"error": f"Processing error: {str(e)}"}
        finally:
            # Clean up temporary file if one was created
            if temp_file_created and os.path.exists(processed_file):
                try:
                    os.remove(processed_file)
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
        
        # Validate unified_no vs visa_file_number 
        if 'unified_no' in processed and '/' in processed['unified_no']:
            logger.warning(f"Fixing incorrect unified_no with slashes: {processed['unified_no']}")
            # Save the incorrect value in case we need it
            incorrect_unified = processed['unified_no']
            
            # Extract just digits
            digits = ''.join(filter(str.isdigit, processed['unified_no']))
            if len(digits) >= 8:
                processed['unified_no'] = digits
                logger.info(f"Extracted digits for unified_no: {digits}")
            else:
                processed['unified_no'] = self.DEFAULT_VALUE
                
            # If visa_file_number is not set, use the incorrect unified_no value
            if 'visa_file_number' not in processed or processed['visa_file_number'] == self.DEFAULT_VALUE:
                processed['visa_file_number'] = incorrect_unified
                logger.info(f"Set visa_file_number from incorrect unified_no: {incorrect_unified}")
                
        # Improve visa-related field extraction
        if doc_type == 'visa':
            # If we have entry_permit_no but no visa_file_number, copy it
            if 'entry_permit_no' in processed and processed['entry_permit_no'] != self.DEFAULT_VALUE:
                if 'visa_file_number' not in processed or processed['visa_file_number'] == self.DEFAULT_VALUE:
                    processed['visa_file_number'] = processed['entry_permit_no']
                    logger.info(f"Set visa_file_number from entry_permit_no: {processed['entry_permit_no']}")
            
            # If we have file_no or file field, use it for visa_file_number
            for field in ['file_no', 'file', 'file_number']:
                if field in processed and processed[field] != self.DEFAULT_VALUE:
                    if 'visa_file_number' not in processed or processed['visa_file_number'] == self.DEFAULT_VALUE:
                        processed['visa_file_number'] = processed[field]
                        logger.info(f"Set visa_file_number from {field}: {processed[field]}")
            
            # Look for unified_no in various field names
            for field in ['unified_no', 'uid', 'u.i.d._no.', 'unified_number', 'unified']:
                if field in processed and processed[field] != self.DEFAULT_VALUE:
                    processed['unified_no'] = processed[field]
                    logger.info(f"Set unified_no from {field}: {processed[field]}")
                    
        # Prevent unified_no from being derived from visa_file_number
        if ('unified_no' in processed and 'visa_file_number' in processed and 
            processed['unified_no'] != self.DEFAULT_VALUE and processed['visa_file_number'] != self.DEFAULT_VALUE):
            # Check if unified_no looks like visa_file_number with slashes removed
            unified = processed['unified_no']
            visa_file = processed['visa_file_number']
            visa_file_no_slashes = visa_file.replace('/', '')
            
            # If they're the same or very similar, the unified_no might be incorrect
            if unified == visa_file_no_slashes or (len(unified) >= 8 and unified in visa_file_no_slashes):
                logger.warning(f"CRITICAL ERROR: unified_no appears to be derived from visa_file_number")
                logger.warning(f"  unified_no: {unified}")
                logger.warning(f"  visa_file_number: {visa_file}")
                logger.warning(f"  visa_file without slashes: {visa_file_no_slashes}")
                
                # Check if there's a more likely unified_no candidate elsewhere in the data
                # Look for a 9-10 digit number that doesn't match the visa file pattern
                for field, value in processed.items():
                    if field not in ['unified_no', 'visa_file_number'] and value != self.DEFAULT_VALUE:
                        # Look for patterns of numbers that could be unified numbers
                        number_candidates = re.findall(r'\b\d{8,11}\b', str(value))
                        for candidate in number_candidates:
                            # If the candidate doesn't match the visa file number pattern
                            if candidate != visa_file_no_slashes and len(candidate) >= 8:
                                logger.info(f"Found potential unified_no candidate in {field}: {candidate}")
                                processed['unified_no'] = candidate
                                return processed
                                
                # If no better candidate found, set to default value to avoid using incorrect data
                processed['unified_no'] = self.DEFAULT_VALUE
                logger.warning("Set unified_no to default value to avoid using incorrect data")
        
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