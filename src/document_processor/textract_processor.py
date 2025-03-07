import boto3
import logging
import os
from typing import Dict, List, Optional, Tuple, Any
import re
from datetime import datetime
import hashlib
import concurrent.futures
from botocore.exceptions import ClientError
from functools import lru_cache
import time

from src.utils.error_handling import (
    ServiceError, ApplicationError, handle_errors, 
    ErrorCategory, ErrorSeverity, retry_on_error
)

logger = logging.getLogger(__name__)

class LRUCache:
    """Simple LRU cache implementation with size limit."""
    
    def __init__(self, max_size: int = 100):
        self.cache = {}
        self.max_size = max_size
        self.access_order = []
        
    def __contains__(self, key: str) -> bool:
        return key in self.cache
        
    def __getitem__(self, key: str) -> Any:
        if key in self.cache:
            # Update access order
            self.access_order.remove(key)
            self.access_order.append(key)
            return self.cache[key]
        raise KeyError(key)
        
    def __setitem__(self, key: str, value: Any) -> None:
        if key in self.cache:
            # Update existing key
            self.access_order.remove(key)
        elif len(self.cache) >= self.max_size:
            # Remove least recently used item
            oldest_key = self.access_order.pop(0)
            del self.cache[oldest_key]
            
        # Add new item
        self.cache[key] = value
        self.access_order.append(key)

class TextractProcessor:
    """Optimized AWS Textract processor with caching and improved extraction."""
    
    def __init__(self):
        # Add result caching to prevent redundant processing
        self._cache = LRUCache(max_size=100)
        self.textract = boto3.client(
            'textract',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION', 'us-east-1')
        )
        self.DEFAULT_VALUE = "."
        
    @handle_errors(ErrorCategory.EXTERNAL_SERVICE, ErrorSeverity.HIGH)
    def process_document(self, file_path: str, doc_type: Optional[str] = None) -> Dict[str, str]:
        """Process document with improved extraction reliability."""
        try:
            # Start timing
            start_time = time.time()
            
            # Add debug logging
            logger.info(f"Processing document: {file_path}")
            
            # Read file efficiently
            file_bytes = self._read_file_bytes(file_path)

            # Try multiple Textract features for better extraction
            # First try with FORMS and TABLES
            response = self._get_textract_response(file_bytes, ["FORMS", "TABLES"])
            
            # Extract text content more efficiently
            text_content = self._extract_text_content(response)
            
            # Log a sample of the extracted text for debugging
            text_sample = text_content[:200] + "..." if len(text_content) > 200 else text_content
            logger.info(f"Extracted text sample: {text_sample}")
            
            # Auto-detect document type if not provided
            detected_type = doc_type or self.detect_document_type(text_content)
            logger.info(f"Document type: {detected_type}")
            
            # Extract data based on detected type
            extraction_start = time.time()
            
            # First attempt with detected type
            if detected_type == 'visa':
                extracted_data = self._extract_visa_data(text_content)
            elif detected_type == 'emirates_id':
                extracted_data = self._extract_emirates_id_data(text_content)
            elif detected_type == 'passport':
                extracted_data = self._extract_passport_data(text_content)
            else:
                # Generic extraction for unknown document types
                extracted_data = self._extract_generic_data(text_content, response)
                
            # If critical fields are missing, try generic extraction as backup
            if self._is_extraction_incomplete(extracted_data, detected_type):
                logger.warning(f"Incomplete extraction for {detected_type}, trying generic extraction")
                generic_data = self._extract_generic_data(text_content, response)
                
                # Add missing fields from generic extraction
                for key, value in generic_data.items():
                    if key not in extracted_data or extracted_data[key] == self.DEFAULT_VALUE:
                        extracted_data[key] = value
            
            # Try raw text searching for critical fields if still missing
            if self._is_extraction_incomplete(extracted_data, detected_type):
                logger.warning(f"Still missing critical fields, trying direct text search")
                self._extract_missing_fields_from_text(extracted_data, text_content, detected_type)
            
            # Validate extracted data
            self._validate_extracted_data(extracted_data, detected_type)
            
            # Log extraction results
            logger.info(f"Extraction results for {detected_type}:")
            for key, value in extracted_data.items():
                if value != self.DEFAULT_VALUE:
                    logger.info(f"  {key}: {value}")
            
            # Log overall processing time
            total_time = time.time() - start_time
            logger.info(f"Document processed in {total_time:.2f}s: {len(extracted_data)} fields extracted")
            
            return extracted_data

        except Exception as e:
            logger.error(f"Error processing document {file_path}: {str(e)}", exc_info=True)
            raise

    def _is_extraction_incomplete(self, data: Dict[str, str], doc_type: str) -> bool:
        """Check if critical fields are missing from extraction."""
        critical_fields = {
            'passport': ['passport_number'],
            'emirates_id': ['emirates_id'],
            'visa': ['entry_permit_no', 'unified_no', 'visa_file_number']
        }.get(doc_type, [])
        
        return any(field not in data or data[field] == self.DEFAULT_VALUE for field in critical_fields)

    def _extract_missing_fields_from_text(self, data: Dict[str, str], text: str, doc_type: str) -> None:
        """Extract critical missing fields directly from text using aggressive patterns."""
        if doc_type == 'passport' and (data.get('passport_number') == self.DEFAULT_VALUE):
            # Try various passport number patterns
            patterns = [
                r'(?<!\w)([A-Z]\d{7,8})(?!\w)',  # Common passport format A1234567
                r'(?<!\w)(\d{7,9}[A-Z])(?!\w)',  # Reversed format 1234567A
                r'PASSPORT\s*(?:NO|NUMBER)[.:\s]*([A-Z0-9]{6,12})',  # With label
                r'(?<!\w)([A-Z][0-9]{6,10})(?!\w)'  # Common format with 6-10 digits
            ]
            for pattern in patterns:
                matches = re.findall(pattern, text, re.IGNORECASE)
                if matches:
                    data['passport_number'] = matches[0]
                    logger.info(f"Found passport number with direct search: {matches[0]}")
                    break
                    
        elif doc_type == 'visa':
            # Try to find visa file number
            if data.get('visa_file_number') == self.DEFAULT_VALUE:
                patterns = [
                    r'(?:\d{3}/\d{4}/\d{4,10})',  # Common visa file format
                    r'(?:FILE|VISA)[.:\s]*(?:NO|NUMBER)[.:\s]*(\d+[\d/]*\d+)',  # With label
                    r'\b(\d{3,4}[/-]\d{4,}[/-]\d{4,})\b'  # Generic format
                ]
                for pattern in patterns:
                    matches = re.findall(pattern, text)
                    if matches:
                        data['visa_file_number'] = matches[0]
                        logger.info(f"Found visa file number with direct search: {matches[0]}")
                        # Also use this for entry permit if missing
                        if data.get('entry_permit_no') == self.DEFAULT_VALUE:
                            data['entry_permit_no'] = matches[0]
                        break
                        
            # Try to find unified number
            if data.get('unified_no') == self.DEFAULT_VALUE:
                patterns = [
                    r'(?:U\.?I\.?D|UNIFIED)[.:\s]*(?:NO|NUMBER)[.:\s]*(\d[\d\s]*)',
                    r'\b(2\d{9})\b'  # Unified numbers often start with 2 and have 10 digits
                ]
                for pattern in patterns:
                    matches = re.findall(pattern, text, re.IGNORECASE)
                    if matches:
                        data['unified_no'] = re.sub(r'\s', '', matches[0])
                        logger.info(f"Found unified number with direct search: {matches[0]}")
                        break
            
    def _get_cache_key(self, file_path: str, doc_type: Optional[str]) -> str:
        """Generate a unique cache key for the document."""
        file_hash = hashlib.md5(file_path.encode()).hexdigest()
        return f"{file_hash}:{doc_type or 'auto'}"
        
    def _read_file_bytes(self, file_path: str) -> bytes:
        """Read file efficiently with proper error handling."""
        try:
            with open(file_path, 'rb') as f:
                return f.read()
        except IOError as e:
            logger.error(f"Failed to read file {file_path}: {str(e)}")
            raise ServiceError(f"File read error: {str(e)}")
    
    @retry_on_error(max_attempts=3)
    def _get_textract_response(self, file_bytes: bytes) -> Dict:
        """Get Textract response with retry logic."""
        try:
            response = self.textract.analyze_document(
                Document={'Bytes': file_bytes},
                FeatureTypes=['FORMS', 'TABLES']
            )
            return response
        except ClientError as e:
            logger.warning(f"Textract API error: {str(e)}")
            raise  # Will be retried by decorator
    
    def _extract_text_content(self, response: Dict) -> str:
        """Extract text content from Textract response concurrently."""
        text_blocks = []
        
        # Get all LINE blocks
        line_blocks = [
            block for block in response['Blocks'] 
            if block['BlockType'] == 'LINE'
        ]
        
        # Process concurrently for large documents
        if len(line_blocks) > 20:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                results = list(executor.map(
                    lambda block: block.get('Text', ''),
                    line_blocks
                ))
                text_blocks = results
        else:
            # Process sequentially for small documents
            text_blocks = [block.get('Text', '') for block in line_blocks]
                
        return '\n'.join(text_blocks)
    
    def detect_document_type(self, text_content: str) -> str:
        """
        Detect document type from content patterns.
    
        Args:
            text_content: Extracted text from document
        
        Returns:
            str: Document type ('visa', 'emirates_id', 'passport', or 'unknown')
        """
        # Convert to uppercase and normalize whitespace for consistent matching
        text = re.sub(r'\s+', ' ', text_content.upper())
    
        # Visa/Entry Permit patterns with confidence scores
        visa_patterns = [
            (r'E-?VISA', 0.8),
            (r'ENTRY\s+PERMIT', 0.8),
            (r'PERMIT\s+NO', 0.7),
            (r'VISA\s+FILE', 0.8),
            (r'RESIDENCE\s+VISA', 0.9),
            (r'\d{3}\s*/\s*\d{4}\s*/\s*\d+', 0.6),  # Visa file number pattern
            (r'UNIFIED\s+NUMBER', 0.7),
        ]
        
        # Emirates ID patterns
        eid_patterns = [
            (r'IDENTITY\s+CARD', 0.8),
            (r'EMIRATES\s+ID', 0.9),
            (r'ID\s+NUMBER', 0.7),
            (r'\d{3}-\d{4}-\d{7}-\d{1}', 0.95),  # Emirates ID number pattern
            (r'الهوية الإماراتية', 0.9),  # Arabic text for Emirates ID
            (r'UNITED\s+ARAB\s+EMIRATES', 0.6),
        ]
        
        # Passport patterns
        passport_patterns = [
            (r'PASSPORT', 0.9),
            (r'NATIONALITY', 0.7),
            (r'DATE\s+OF\s+ISSUE', 0.6),
            (r'PLACE\s+OF\s+BIRTH', 0.8),
            (r'SURNAME', 0.8),
            (r'GIVEN\s+NAMES?', 0.8),
            (r'P<', 0.9),  # Common pattern in machine readable passport lines
            (r'PASSEPORT', 0.9),  # French
            (r'REISEPASS', 0.9),  # German
            (r'جواز سفر', 0.9)  # Arabic
        ]
        
        # Calculate confidence scores for each document type
        visa_confidence = sum(weight for pattern, weight in visa_patterns if re.search(pattern, text))
        eid_confidence = sum(weight for pattern, weight in eid_patterns if re.search(pattern, text))
        passport_confidence = sum(weight for pattern, weight in passport_patterns if re.search(pattern, text))
        
        # Log confidences for debugging
        logger.debug(f"Document type confidence scores: "
                    f"visa={visa_confidence:.2f}, "
                    f"emirates_id={eid_confidence:.2f}, "
                    f"passport={passport_confidence:.2f}")
        
        # Determine document type based on highest confidence
        max_confidence = max(visa_confidence, eid_confidence, passport_confidence)
        
        if max_confidence < 0.5:
            logger.warning("Low confidence in document type detection")
            if "PASSPORT" in text:
                return 'passport'
            elif "EMIRATES" in text or "ID" in text:
                return 'emirates_id'
            return 'unknown'
            
        if visa_confidence == max_confidence:
            return 'visa'
        elif eid_confidence == max_confidence:
            return 'emirates_id'
        elif passport_confidence == max_confidence:
            return 'passport'
        
        return 'unknown'

    def _extract_emirates_id_data(self, text_content: str) -> Dict[str, str]:
        """Extract Emirates ID specific data with improved pattern matching."""
        data = {
            'emirates_id': self.DEFAULT_VALUE,
            'name_en': self.DEFAULT_VALUE,
            'name_ar': self.DEFAULT_VALUE,
            'nationality': self.DEFAULT_VALUE,
            'gender': self.DEFAULT_VALUE,
            'date_of_birth': self.DEFAULT_VALUE,
            'expiry_date': self.DEFAULT_VALUE,
        }
        
        # Normalize text - remove excess whitespace and make case insensitive matches easier
        text = re.sub(r'\s+', ' ', text_content)
        text_upper = text.upper()
        
        # ID Number (try multiple patterns)
        eid_patterns = [
            r'ID\s+(?:Number|No)[.:\s]*(\d{3}[-\s]?\d{4}[-\s]?\d{7}[-\s]?\d{1})',
            r'(?:Number|No)[.:\s]*(\d{3}[-\s]?\d{4}[-\s]?\d{7}[-\s]?\d{1})',
            r'(?<!\d)(\d{3}[-\s]?\d{4}[-\s]?\d{7}[-\s]?\d{1})(?!\d)'  # Standalone EID pattern
        ]
        
        for pattern in eid_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                # Clean up the ID number to ensure correct format
                eid = re.sub(r'\s', '', match.group(1))
                if '-' not in eid:
                    eid = f"{eid[:3]}-{eid[3:7]}-{eid[7:14]}-{eid[14:]}"
                data['emirates_id'] = eid
                break
                
        # Name (English) - try multiple patterns
        name_patterns = [
            r'Name[.:\s]*([A-Za-z\s]+?)(?=\s*\n|\s*$|\s*\d)',
            r'(?:^|\n)(?:Mr\.?|Mrs\.?|Ms\.?)?\s*([A-Za-z\s]+?)(?=\s*\n|\s*$|\s*ID|\s*Date)',
        ]
        
        for pattern in name_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                data['name_en'] = self._clean_text(match.group(1))
                break
        
        # Nationality
        nationality_match = re.search(r'Nationality[.:\s]*([A-Za-z\s]+?)(?=\s*\n|\s*$)', text, re.IGNORECASE)
        if nationality_match:
            data['nationality'] = self._clean_text(nationality_match.group(1))
            
        # Gender
        gender_match = re.search(r'(?:Sex|Gender)[.:\s]*([MF]|MALE|FEMALE)', text_upper)
        if gender_match:
            gender_value = gender_match.group(1)
            if gender_value == 'M' or gender_value == 'MALE':
                data['gender'] = 'M'
            elif gender_value == 'F' or gender_value == 'FEMALE':
                data['gender'] = 'F'
                
        # Date of birth - try multiple formats
        dob_patterns = [
            r'(?:Date of Birth|DOB)[.:\s]*(\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4})',
            r'(?:Date of Birth|DOB)[.:\s]*(\d{1,2}\s*(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s*\d{2,4})'
        ]
        
        for pattern in dob_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                data['date_of_birth'] = self._normalize_date(match.group(1))
                break
                
        # Expiry date
        expiry_match = re.search(r'(?:Expiry|Valid Until)[.:\s]*(\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4})', text, re.IGNORECASE)
        if expiry_match:
            data['expiry_date'] = self._normalize_date(expiry_match.group(1))

        return data

    def _extract_passport_data(self, text_content: str) -> Dict[str, str]:
        """Extract data from passport with enhanced accuracy."""
        data = {
            'passport_number': self.DEFAULT_VALUE,
            'surname': self.DEFAULT_VALUE,
            'given_names': self.DEFAULT_VALUE,
            'nationality': self.DEFAULT_VALUE,
            'date_of_birth': self.DEFAULT_VALUE,
            'place_of_birth': self.DEFAULT_VALUE,
            'gender': self.DEFAULT_VALUE,
            'date_of_issue': self.DEFAULT_VALUE,
            'date_of_expiry': self.DEFAULT_VALUE,
            'mrz_line1': self.DEFAULT_VALUE,
            'mrz_line2': self.DEFAULT_VALUE,
        }
        
        # Normalize text for better matching
        text = re.sub(r'\s+', ' ', text_content)
        text_upper = text.upper()
        
        # More aggressive passport number search - try multiple patterns
        passport_number_patterns = [
            r'Passport\s*No[.:\s]*([A-Z0-9]{6,12})',
            r'Document\s*No[.:\s]*([A-Z0-9]{6,12})',
            r'Passport\s*Number[.:\s]*([A-Z0-9]{6,12})',
            r'No[.:\s]*([A-Z0-9]{6,12})(?=\s+|$)',
            r'(?<!\w)([A-Z][0-9]{6,10})(?!\w)',  # Common passport format like A1234567
            r'(?<!\w)([0-9]{6,9}[A-Z])(?!\w)',   # Format with numbers then letter like 1234567A
            r'(?:^|\s)([A-Z][0-9]{6,9})(?:$|\s)' # Another common format
        ]
        
        for pattern in passport_number_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                data['passport_number'] = match.group(1).strip()
                break
        
        # Try direct search in upper-case text as fallback
        if data['passport_number'] == self.DEFAULT_VALUE:
            # Look for patterns that might be passport numbers
            potential_numbers = re.findall(r'(?<!\w)([A-Z][0-9]{6,9}|[0-9]{6,9}[A-Z])(?!\w)', text_upper)
            if potential_numbers:
                data['passport_number'] = potential_numbers[0]
                logger.info(f"Found potential passport number with direct search: {potential_numbers[0]}")
                
        # Name fields - only fill if MRZ didn't provide
        if data['surname'] == self.DEFAULT_VALUE:
            surname_match = re.search(r'Surname[.:\s]*([A-Za-z\s]+?)(?=\s*\n|\s*Given|\s*$)', text, re.IGNORECASE)
            if surname_match:
                data['surname'] = self._clean_text(surname_match.group(1))
                
        if data['given_names'] == self.DEFAULT_VALUE:
            given_names_match = re.search(r'Given\s*Names?[.:\s]*([A-Za-z\s]+?)(?=\s*\n|\s*$)', text, re.IGNORECASE)
            if given_names_match:
                data['given_names'] = self._clean_text(given_names_match.group(1))
                
        # Nationality
        if data['nationality'] == self.DEFAULT_VALUE:
            nationality_match = re.search(r'Nationality[.:\s]*([A-Za-z\s]+?)(?=\s*\n|\s*$)', text, re.IGNORECASE)
            if nationality_match:
                data['nationality'] = self._clean_text(nationality_match.group(1))
                
        # Gender
        if data['gender'] == self.DEFAULT_VALUE:
            gender_match = re.search(r'(?:Sex|Gender)[.:\s]*([MF])', text_upper)
            if gender_match:
                data['gender'] = gender_match.group(1)
                
        # Date fields
        date_patterns = {
            'date_of_birth': [
                r'(?:Date of Birth|DOB)[.:\s]*(\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4})',
                r'(?:Date of Birth|DOB)[.:\s]*(\d{1,2}\s*(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s*\d{2,4})'
            ],
            'date_of_issue': [
                r'(?:Date of Issue)[.:\s]*(\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4})', 
                r'(?:Issued|Issue Date)[.:\s]*(\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4})'
            ],
            'date_of_expiry': [
                r'(?:Date of Expiry|Expiry Date)[.:\s]*(\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4})',
                r'(?:Valid Until)[.:\s]*(\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4})'
            ]
        }
        
        for field, patterns in date_patterns.items():
            if data[field] == self.DEFAULT_VALUE:
                for pattern in patterns:
                    match = re.search(pattern, text, re.IGNORECASE)
                    if match:
                        data[field] = self._normalize_date(match.group(1))
                        break
                        
        # Place of birth
        place_of_birth_match = re.search(r'Place of Birth[.:\s]*([A-Za-z\s]+?)(?=\s*\n|\s*$)', text, re.IGNORECASE)
        if place_of_birth_match:
            data['place_of_birth'] = self._clean_text(place_of_birth_match.group(1))

        return data

    def _extract_from_mrz(self, text: str) -> Dict[str, str]:
        """Extract data from passport MRZ (Machine Readable Zone)."""
        data = {}
        
        # Find potential MRZ lines (fixed width character sequences with common MRZ patterns)
        mrz_pattern = r'(?:[A-Z0-9<]{30,44})(?:\n|\r\n|\r)(?:[A-Z0-9<]{30,44})'
        mrz_match = re.search(mrz_pattern, text)
        
        if not mrz_match:
            # Try with relaxed pattern
            mrz_pattern = r'P<[A-Z]{3}[A-Z0-9<]{30,40}(?:\n|\r\n|\r)[A-Z0-9<]{30,40}'
            mrz_match = re.search(mrz_pattern, text)
            
        if mrz_match:
            mrz_text = mrz_match.group(0)
            mrz_lines = mrz_text.strip().split()
            
            if len(mrz_lines) >= 2:
                # Store raw MRZ lines
                data['mrz_line1'] = mrz_lines[0]
                data['mrz_line2'] = mrz_lines[1]
                
                try:
                    # Parse MRZ data
                    # First line format: P<ISSUING_COUNTRY<SURNAME<<GIVEN_NAMES
                    if len(mrz_lines[0]) >= 5 and mrz_lines[0][0] == 'P':
                        # Get country code
                        country_code = mrz_lines[0][2:5].replace('<', '')
                        if country_code:
                            data['nationality'] = country_code
                            
                        # Get name parts
                        name_part = mrz_lines[0][5:]
                        name_parts = name_part.split('<<')
                        if len(name_parts) >= 2:
                            data['surname'] = name_parts[0].replace('<', ' ').strip()
                            data['given_names'] = name_parts[1].replace('<', ' ').strip()
                    
                    # Second line format: PASSPORT_NUMBER<COUNTRY_CODE<DOB<GENDER<EXPIRY_DATE<PERSONAL_NUMBER
                    if len(mrz_lines[1]) >= 20:
                        # Passport number (position 0-9)
                        passport_number = mrz_lines[1][0:9].replace('<', '')
                        if passport_number:
                            data['passport_number'] = passport_number
                            
                        # Date of birth (position 13-19) in YYMMDD format
                        dob = mrz_lines[1][13:19]
                        try:
                            year = int(dob[0:2])
                            month = int(dob[2:4])
                            day = int(dob[4:6])
                            # Assume years 00-24 are 2000s, 25-99 are 1900s (adjust as needed)
                            century = 2000 if year < 25 else 1900
                            data['date_of_birth'] = f"{day:02d}/{month:02d}/{year+century}"
                        except (ValueError, IndexError):
                            pass
                            
                        # Gender (position 20)
                        if len(mrz_lines[1]) > 20:
                            gender = mrz_lines[1][20]
                            if gender in 'MF':
                                data['gender'] = gender
                                
                        # Expiry date (position 21-27) in YYMMDD format
                        if len(mrz_lines[1]) >= 27:
                            expiry = mrz_lines[1][21:27]
                            try:
                                year = int(expiry[0:2])
                                month = int(expiry[2:4])
                                day = int(expiry[4:6])
                                # Assume years 00-24 are 2000s, 25-99 are 1900s
                                century = 2000 if year < 25 else 1900
                                data['date_of_expiry'] = f"{day:02d}/{month:02d}/{year+century}"
                            except (ValueError, IndexError):
                                pass
                                
                except Exception as e:
                    logger.debug(f"Error parsing MRZ data: {str(e)}")
                    
        return data

    def _extract_visa_data(self, text_content: str) -> Dict[str, str]:
        """Extract visa specific data with enhanced pattern matching."""
        data = {
            'entry_permit_no': self.DEFAULT_VALUE,
            'unified_no': self.DEFAULT_VALUE,
            'visa_file_number': self.DEFAULT_VALUE,
            'full_name': self.DEFAULT_VALUE,
            'nationality': self.DEFAULT_VALUE,
            'passport_number': self.DEFAULT_VALUE,
            'date_of_birth': self.DEFAULT_VALUE,
            'gender': self.DEFAULT_VALUE,
            'profession': self.DEFAULT_VALUE,
            'issue_date': self.DEFAULT_VALUE,
            'expiry_date': self.DEFAULT_VALUE,
            'sponsor': self.DEFAULT_VALUE,
            'visa_type': self.DEFAULT_VALUE,
        }
        
        # Normalize text for better matching
        text = re.sub(r'\s+', ' ', text_content)
        text_upper = text.upper()
        
        # Add specific patterns for E-Visa format
        # Example:
        unified_patterns = [
            r'(?:U\.?I\.?D\.?\s*No\.?|UNIFIED\s*(?:NO|NUMBER))[.:\s]*(\d[\d\s/]*)',
            r'(?<!\w)U\.?I\.?D\.?\s*[:#]?\s*(\d[\d\s/]*)',
            r'UNIFIED\s*(?:NO|NUMBER)[.:\s]*([0-9\s]{5,15})',
            r'(?<!\w)(2\d{9})(?!\w)',  # Unified numbers often start with 2 and have 10 digits
            r'(?:\s|^)(3\d{9})(?:\s|$)'  # Try similar pattern for 3-prefix
        ]
        
        for pattern in unified_patterns:
            match = re.search(pattern, text_content, re.IGNORECASE)
            if match:
                # Clean up the value (remove spaces, etc.)
                unified_no = re.sub(r'\s', '', match.group(1))
                data['unified_no'] = unified_no
                break
            
        for pattern in unified_patterns:
            match = re.search(pattern, text_content, re.IGNORECASE)
            if match:
                # Clean up the value (remove spaces, etc.)
                unified_no = re.sub(r'\s', '', match.group(1))
                data['unified_no'] = unified_no
                break
        
        # More aggressive visa file number extraction
        visa_file_patterns = [
            r'Visa\s+File\s+No[.:\s]*([0-9/\-\s]+)',
            r'File\s+No[.:\s]*([0-9/\-\s]+)',
            r'(?<!\w)(\d{3}/\d{4}/\d{4,10})(?!\w)', # Common format like 101/2023/1234567
            r'(?<!\w)(\d{3}-\d{4}-\d{4,10})(?!\w)', # With dashes instead of slashes
            r'(?<!\w)([0-9]{3}[\s/\-][0-9]{4}[\s/\-][0-9]{4,10})(?!\w)'
        ]
        
        for pattern in visa_file_patterns:
            match = re.search(pattern, text_content, re.IGNORECASE)
            if match:
                # Clean up the value
                visa_file = re.sub(r'\s', '', match.group(1))
                data['visa_file_number'] = visa_file
                # Also use as entry permit if that's missing
                if data['entry_permit_no'] == self.DEFAULT_VALUE:
                    data['entry_permit_no'] = visa_file
                break
        
        # Entry permit/visa number
        permit_patterns = [
            r'Entry\s+permit\s+(?:no|number)[.:\s]*([A-Z0-9\s/\-]+)(?=\s*\n|\s*$)',
            r'Permit\s+(?:no|number)[.:\s]*([A-Z0-9\s/\-]+)(?=\s*\n|\s*$)',
            r'Visa\s+(?:no|number)[.:\s]*([A-Z0-9\s/\-]+)(?=\s*\n|\s*$)',
            r'(?:no|number)[.:\s]*(\d+\s*\/\s*\d+\s*\/\s*[\d\/]+)(?=\s*\n|\s*$)'
        ]
        
        for pattern in permit_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                data['entry_permit_no'] = self._clean_text(match.group(1))
                break
                
        # Full name - try various patterns
        name_patterns = [
            r'Full Name[.:\s]*([A-Za-z\s]+?)(?=\s*\n|\s*$)',
            r'Name[.:\s]*([A-Za-z\s]+?)(?=\s*\n|\s*$)',
            r'(?:^|\n)(?:Mr\.?|Mrs\.?|Ms\.?)?\s*([A-Za-z\s]+?)(?=\s*\n|\s*Nationality|\s*$)'
        ]
        
        for pattern in name_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                data['full_name'] = self._clean_text(match.group(1))
                # Remove duplicates in name (common OCR issue)
                parts = data['full_name'].split()
                seen = set()
                unique_parts = []
                for part in parts:
                    if part.lower() not in seen:
                        seen.add(part.lower())
                        unique_parts.append(part)
                data['full_name'] = ' '.join(unique_parts)
                break
                
        # Nationality
        nationality_match = re.search(r'Nationality[.:\s]*([A-Za-z\s]+?)(?=\s*\n|\s*$)', text, re.IGNORECASE)
        if nationality_match:
            data['nationality'] = self._clean_text(nationality_match.group(1))
            
        # Passport number
        passport_match = re.search(r'Passport(?:\s+No)?[.:\s]*([A-Z0-9]+)(?=\s*\n|\s*$)', text, re.IGNORECASE)
        if passport_match:
            data['passport_number'] = passport_match.group(1).strip()
            
        # Date of birth - try multiple formats
        dob_patterns = [
            r'(?:Date of Birth|DOB)[.:\s]*(\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4})',
            r'(?:Date of Birth|DOB)[.:\s]*(\d{1,2}\s*(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s*\d{2,4})'
        ]
        
        for pattern in dob_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                data['date_of_birth'] = self._normalize_date(match.group(1))
                break
                
        # Gender
        gender_match = re.search(r'(?:Sex|Gender)[.:\s]*([MF]|MALE|FEMALE)', text_upper)
        if gender_match:
            gender_value = gender_match.group(1)
            if gender_value == 'M' or gender_value == 'MALE':
                data['gender'] = 'M'
            elif gender_value == 'F' or gender_value == 'FEMALE':
                data['gender'] = 'F'
                
        # Profession/Occupation
        profession_match = re.search(r'(?:Profession|Occupation)[.:\s]*([A-Za-z\s]+?)(?=\s*\n|\s*$)', text, re.IGNORECASE)
        if profession_match:
            data['profession'] = self._clean_text(profession_match.group(1))
            
        # Date fields - issue date and expiry date
        date_patterns = {
            'issue_date': [
                r'(?:Date of Issue|Issue Date|Issued on)[.:\s]*(\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4})',
            ],
            'expiry_date': [
                r'(?:Date of Expiry|Expiry Date|Valid Until)[.:\s]*(\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4})',
            ]
        }
        
        for field, patterns in date_patterns.items():
            for pattern in patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    data[field] = self._normalize_date(match.group(1))
                    break
                    
        # Sponsor
        sponsor_match = re.search(r'Sponsor[.:\s]*([A-Za-z\s]+?)(?=\s*\n|\s*$)', text, re.IGNORECASE)
        if sponsor_match:
            data['sponsor'] = self._clean_text(sponsor_match.group(1))
            
        # Visa type
        visa_type_patterns = [
            r'(?:Visa Type|Type)[.:\s]*([A-Za-z\s]+?)(?=\s*\n|\s*$)',
            r'RESIDENCE\s*(VISA|PERMIT)',
            r'VISIT\s*(VISA|PERMIT)',
            r'TOURIST\s*(VISA)'
        ]
        
        for pattern in visa_type_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                data['visa_type'] = self._clean_text(match.group(1))
                break

        return data

    def _extract_generic_data(self, text_content: str, response: Dict) -> Dict[str, str]:
        """Extract generic data for unknown document types using key-value pairs."""
        data = {}
        
        # Extract key-value pairs from forms
        key_values = self._extract_key_value_pairs(response)
        for key, value in key_values.items():
            # Normalize key
            norm_key = self._normalize_field_name(key)
            if norm_key and value:  # Only add non-empty values
                data[norm_key] = value
                
        # Try to extract common fields using general patterns
        id_fields = {
            'passport_number': [
                r'Passport\s*(?:No|Number)[.:\s]*([A-Z0-9]{6,12})',
                r'(?<!\w)([A-Z]\d{7,9})(?!\w)'  # Common passport format
            ],
            'emirates_id': [
                r'(?:Emirates ID|ID Number)[.:\s]*(\d{3}-\d{4}-\d{7}-\d{1})',
                r'(?<!\d)(\d{3}-\d{4}-\d{7}-\d{1})(?!\d)'  # Standalone EID
            ],
            'visa_number': [
                r'(?:Visa|Permit)\s*(?:No|Number)[.:\s]*([A-Z0-9\s/\-]+)',
                r'(?<!\w)(\d{3}/\d{4}/\d{4,10})(?!\w)'  # Common visa format
            ]
        }
        
        for field, patterns in id_fields.items():
            if field not in data:
                for pattern in patterns:
                    match = re.search(pattern, text_content, re.IGNORECASE)
                    if match:
                        data[field] = match.group(1).strip()
                        break
                        
        # Try to detect name
        if 'name' not in data and 'full_name' not in data:
            name_match = re.search(r'Name[.:\s]*([A-Za-z\s]+?)(?=\s*\n|\s*$)', text_content, re.IGNORECASE)
            if name_match:
                data['name'] = self._clean_text(name_match.group(1))
                
        return data

    def _extract_key_value_pairs(self, response: Dict) -> Dict[str, str]:
        """Extract key-value pairs from Textract FORMS analysis."""
        key_values = {}
        
        for block in response['Blocks']:
            if block['BlockType'] == 'KEY_VALUE_SET':
                if 'KEY' in block.get('EntityTypes', []):
                    key_block = block
                    value_block = self._get_value_block(response['Blocks'], key_block)
                    
                    if value_block:
                        key = self._get_text_from_block(response['Blocks'], key_block)
                        value = self._get_text_from_block(response['Blocks'], value_block)
                        
                        if key and value:
                            # Clean and normalize
                            key = key.strip().rstrip(':')
                            value = value.strip()
                            key_values[key] = value
                            
        return key_values

    def _get_value_block(self, blocks: List[Dict], key_block: Dict) -> Optional[Dict]:
        """Get the value block associated with a key block."""
        for relationship in key_block.get('Relationships', []):
            if relationship['Type'] == 'VALUE':
                for value_id in relationship['Ids']:
                    for block in blocks:
                        if block['Id'] == value_id:
                            return block
        return None

    def _get_text_from_block(self, blocks: List[Dict], block: Dict) -> str:
        """Get text from a block including its child blocks."""
        text = []
        
        if 'Relationships' in block:
            for relationship in block['Relationships']:
                if relationship['Type'] == 'CHILD':
                    for child_id in relationship['Ids']:
                        for child_block in blocks:
                            if child_block['Id'] == child_id and 'Text' in child_block:
                                text.append(child_block['Text'])
                                
        return ' '.join(text)

    def _normalize_field_name(self, field: str) -> str:
        """Normalize field names for consistency."""
        field = field.lower().strip()
        
        # Mapping of common field variations
        field_mapping = {
            'passport no': 'passport_number',
            'passport number': 'passport_number',
            'document number': 'passport_number',
            'document no': 'passport_number',
            'id number': 'emirates_id',
            'emirates id': 'emirates_id',
            'eid': 'emirates_id',
            'id no': 'emirates_id',
            'full name': 'name',
            'surname': 'last_name',
            'given names': 'first_name',
            'first name': 'first_name',
            'last name': 'last_name',
            'date of birth': 'date_of_birth',
            'birth date': 'date_of_birth',
            'dob': 'date_of_birth',
            'gender': 'gender',
            'sex': 'gender',
            'nationality': 'nationality',
            'birth place': 'place_of_birth',
            'place of birth': 'place_of_birth',
            'issue date': 'date_of_issue',
            'date of issue': 'date_of_issue',
            'expiry date': 'date_of_expiry',
            'date of expiry': 'date_of_expiry',
            'valid until': 'date_of_expiry',
            'entry permit': 'entry_permit_no',
            'permit number': 'entry_permit_no',
            'visa number': 'entry_permit_no',
            'profession': 'profession',
            'occupation': 'profession',
            'sponsor': 'sponsor'
        }
        
        return field_mapping.get(field, field.replace(' ', '_'))

    def _normalize_date(self, date_str: str) -> str:
        """Normalize date to standard format DD/MM/YYYY."""
        date_str = date_str.strip()
        
        # Try different date formats
        date_formats = [
            ('%d/%m/%Y', r'\d{1,2}/\d{1,2}/\d{4}'),
            ('%d-%m-%Y', r'\d{1,2}-\d{1,2}-\d{4}'),
            ('%d.%m.%Y', r'\d{1,2}\.\d{1,2}\.\d{4}'),
            ('%d %b %Y', r'\d{1,2} [A-Za-z]{3} \d{4}'),
            ('%d %B %Y', r'\d{1,2} [A-Za-z]+ \d{4}'),
            ('%Y/%m/%d', r'\d{4}/\d{1,2}/\d{1,2}'),
            ('%Y-%m-%d', r'\d{4}-\d{1,2}-\d{1,2}'),
            ('%m/%d/%Y', r'\d{1,2}/\d{1,2}/\d{4}')
        ]
        
        for date_format, pattern in date_formats:
            if re.match(pattern, date_str, re.IGNORECASE):
                try:
                    parsed_date = datetime.strptime(date_str, date_format)
                    return parsed_date.strftime('%d/%m/%Y')
                except ValueError:
                    continue
                    
        # Handle 2-digit years
        short_year_formats = [
            ('%d/%m/%y', r'\d{1,2}/\d{1,2}/\d{2}'),
            ('%d-%m-%y', r'\d{1,2}-\d{1,2}-\d{2}'),
            ('%d.%m.%y', r'\d{1,2}\.\d{1,2}\.\d{2}'),
            ('%d %b %y', r'\d{1,2} [A-Za-z]{3} \d{2}')
        ]
        
        for date_format, pattern in short_year_formats:
            if re.match(pattern, date_str, re.IGNORECASE):
                try:
                    parsed_date = datetime.strptime(date_str, date_format)
                    # Adjust century for 2-digit years
                    year = parsed_date.year
                    if year < 50:  # Assuming years 00-49 are 2000s
                        year += 2000
                    elif year < 100:  # Years 50-99 are 1900s
                        year += 1900
                        
                    return f"{parsed_date.day:02d}/{parsed_date.month:02d}/{year}"
                except ValueError:
                    continue
                    
        # If no format matches, return original
        return date_str

    def _clean_text(self, text: str) -> str:
        """Clean extracted text."""
        if not text:
            return self.DEFAULT_VALUE
            
        # Remove extra whitespace and normalize
        text = re.sub(r'\s+', ' ', text).strip()
        
        # Remove any additional text after main value (common in forms)
        text = text.split('/')[0].strip()
        
        return text if text else self.DEFAULT_VALUE

    def _validate_extracted_data(self, data: Dict[str, str], doc_type: str) -> None:
        """Validate extracted data and log warnings for missing fields."""
        required_fields = {
            'passport': ['passport_number', 'surname', 'given_names'],
            'emirates_id': ['emirates_id', 'name_en'],
            'visa': ['entry_permit_no', 'full_name']
        }.get(doc_type, [])
        
        missing_fields = []
        for field in required_fields:
            if field not in data or data[field] == self.DEFAULT_VALUE:
                missing_fields.append(field)
                
        if missing_fields:
            logger.warning(
                f"Missing required fields in {doc_type}: {', '.join(missing_fields)}"
            )
            
    def _determine_file_type(self, file_path: str, content_text: str) -> str:
            """Determine file type from content and filename."""
            name = file_path.lower()
            doc_type = 'unknown'

            # Check content patterns first
            content_lower = content_text.lower()
            if any(term in content_lower for term in ['passport no', 'surname', 'given names', 'nationality']):
                doc_type = 'passport'
            elif any(term in content_lower for term in ['emirates id', 'id number', 'هوية الإمارات']) or re.search(r'\d{3}-\d{4}-\d{7}-\d{1}', content_text):
                doc_type = 'emirates_id'
            elif any(term in content_lower for term in ['entry permit', 'visa', 'permit no', 'sponsor']):
                doc_type = 'visa'

            # If no content match, check filename
            if doc_type == 'unknown':
                if 'passport' in name:
                    doc_type = 'passport'
                elif 'emirates' in name or 'eid' in name:
                    doc_type = 'emirates_id'
                elif 'visa' in name:
                    doc_type = 'visa'

            return doc_type
        
    def verify_extracted_data(self, data: Dict, doc_type: str) -> Dict:
        """Verify and clean extracted data."""
        verified = {}
        
        # Define required fields per document type
        required_fields = {
            'passport': ['passport_number', 'first_name', 'last_name', 'nationality', 'date_of_birth'],
            'emirates_id': ['emirates_id', 'name_en', 'nationality'],
            'visa': ['visa_file_number', 'full_name', 'nationality', 'expiry_date']
        }
        
        # Define format validators
        validators = {
            'emirates_id': lambda x: bool(re.match(r'^\d{3}-\d{4}-\d{7}-\d{1}$', str(x))),
            'passport_number': lambda x: bool(re.match(r'^[A-Z0-9]{6,12}$', str(x))),
            'date_of_birth': lambda x: bool(re.match(r'^\d{2}/\d{2}/\d{4}$', str(x))),
            'expiry_date': lambda x: bool(re.match(r'^\d{2}/\d{2}/\d{4}$', str(x)))
        }
        
        # Verify and clean each field
        for field, value in data.items():
            # Clean the value
            clean_value = str(value).strip()
            
            # Validate format if applicable
            if field in validators and not validators[field](clean_value):
                logger.warning(f"Invalid format for {field}: {clean_value}")
                continue
                
            verified[field] = clean_value
        
        # Check required fields
        if doc_type in required_fields:
            missing = [field for field in required_fields[doc_type] 
                    if field not in verified or not verified[field]]
            if missing:
                logger.warning(f"Missing required fields for {doc_type}: {missing}")
        
        return verified