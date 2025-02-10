import boto3
import requests
import os
import logging
from typing import Dict, Tuple, List, Optional
import json
from PIL import Image
import io
from botocore.exceptions import BotoCoreError, ClientError

from src.utils.error_handling import ServiceError, handle_errors, ErrorCategory, ErrorSeverity

logger = logging.getLogger(__name__)

class EnhancedOCRProcessor:
    """OCR processor using AWS Textract and DeepSeek R1."""

    def __init__(self):
        # Initialize AWS Textract client
        self.textract = boto3.client(
            'textract',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION', 'us-east-1')
        )
        
        # DeepSeek configuration
        self.deepseek_api_key = os.getenv('DEEPSEEK_API_KEY')
        self.deepseek_api_url = os.getenv('DEEPSEEK_API_URL')
        
        # Configure confidence thresholds
        self.MIN_CONFIDENCE = 0.6
        self.HIGH_CONFIDENCE = 0.9

    @handle_errors(ErrorCategory.EXTERNAL_SERVICE, ErrorSeverity.HIGH)
    def process_document(self, file_path: str, doc_type: str) -> Tuple[str, Dict[str, str]]:
        """
        Process document using both AWS Textract and DeepSeek.
        
        Args:
            file_path: Path to document file
            doc_type: Type of document (passport, emirates_id, etc.)
            
        Returns:
            Tuple of (processed_file_path, extracted_data)
        """
        try:
            # Read the file
            with open(file_path, 'rb') as document:
                file_bytes = document.read()

            # Process with both services
            textract_result = self._process_with_textract(file_bytes)
            deepseek_result = self._process_with_deepseek(file_bytes, doc_type)

            # Combine and validate results
            combined_results = self._combine_results(
                textract_result,
                deepseek_result,
                doc_type
            )

            # Save processed file if necessary
            processed_path = self._save_processed_file(file_path, combined_results)

            return processed_path, combined_results

        except Exception as e:
            logger.error(f"Error processing document {file_path}: {str(e)}")
            raise

    def _process_with_textract(self, file_bytes: bytes) -> Dict:
        """Process document with AWS Textract."""
        try:
            # Get both text detection and form analysis
            response = self.textract.analyze_document(
                Document={'Bytes': file_bytes},
                FeatureTypes=['FORMS', 'TABLES']
            )

            # Extract key-value pairs
            key_values = {}
            for block in response['Blocks']:
                if block['BlockType'] == 'KEY_VALUE_SET':
                    if 'KEY' in block['EntityTypes']:
                        key_block = block
                        value_block = self._get_value_block(response['Blocks'], key_block)
                        if value_block:
                            key = self._get_text(response['Blocks'], key_block)
                            value = self._get_text(response['Blocks'], value_block)
                            confidence = min(
                                key_block.get('Confidence', 0),
                                value_block.get('Confidence', 0)
                            )
                            key_values[key] = {
                                'value': value,
                                'confidence': confidence
                            }

            return {
                'key_values': key_values,
                'raw_response': response
            }

        except (BotoCoreError, ClientError) as e:
            logger.error(f"AWS Textract error: {str(e)}")
            raise ServiceError(f"Textract processing failed: {str(e)}")

    def _process_with_deepseek(self, file_bytes: bytes, doc_type: str) -> Dict:
        """Process document with DeepSeek R1."""
        try:
            # Prepare prompt based on document type
            prompt = self._get_deepseek_prompt(doc_type)
            
            headers = {
                'Authorization': f'Bearer {self.deepseek_api_key}',
                'Content-Type': 'application/json'
            }

            # Prepare the request payload
            payload = {
                'image': file_bytes.decode('latin1'),  # Base64 encoded image
                'prompt': prompt,
                'model': 'deepseek-vision-r1'
            }

            response = requests.post(
                self.deepseek_api_url,
                headers=headers,
                json=payload
            )
            response.raise_for_status()

            return response.json()

        except requests.RequestException as e:
            logger.error(f"DeepSeek API error: {str(e)}")
            raise ServiceError(f"DeepSeek processing failed: {str(e)}")

    def _get_deepseek_prompt(self, doc_type: str) -> str:
        """Get appropriate prompt for DeepSeek based on document type."""
        prompts = {
            'passport': (
                "Extract the following fields from this passport: "
                "passport number, surname, given names, nationality, "
                "date of birth, and gender. Return as JSON."
            ),
            'emirates_id': (
                "Extract the following fields from this Emirates ID: "
                "ID number, name (in English and Arabic), nationality. "
                "Return as JSON."
            ),
            'visa': (
                "Extract the following fields from this visa: "
                "visa number, full name, nationality, issue date, "
                "expiry date. Return as JSON."
            )
        }
        return prompts.get(doc_type, "Extract all text and return as JSON.")

    def _combine_results(self, textract_result: Dict, 
                        deepseek_result: Dict, doc_type: str) -> Dict:
        """
        Combine and validate results from both services.
        Uses confidence scores to select best results.
        """
        combined_data = {}
        
        # Process Textract key-value pairs
        for key, data in textract_result['key_values'].items():
            if data['confidence'] >= self.MIN_CONFIDENCE:
                key_normalized = self._normalize_key(key)
                combined_data[key_normalized] = {
                    'value': data['value'],
                    'confidence': data['confidence'],
                    'source': 'textract'
                }

        # Process DeepSeek results
        deepseek_data = self._parse_deepseek_response(deepseek_result)
        for key, value in deepseek_data.items():
            key_normalized = self._normalize_key(key)
            
            # If we already have this field from Textract with high confidence,
            # keep the Textract value
            if (key_normalized in combined_data and 
                combined_data[key_normalized]['confidence'] >= self.HIGH_CONFIDENCE):
                continue
                
            combined_data[key_normalized] = {
                'value': value,
                'confidence': 0.7,  # Default confidence for DeepSeek
                'source': 'deepseek'
            }

        # Return just the values for simplicity
        return {
            key: data['value'] 
            for key, data in combined_data.items()
        }

    def _normalize_key(self, key: str) -> str:
        """Normalize key names from different sources."""
        key = key.lower().strip()
        
        # Mapping of various key formats to standard names
        mappings = {
            'passport no': 'passport_number',
            'passport number': 'passport_number',
            'document number': 'passport_number',
            'surname': 'surname',
            'last name': 'surname',
            'given names': 'given_names',
            'first name': 'given_names',
            'date of birth': 'date_of_birth',
            'birth date': 'date_of_birth',
            'nationality': 'nationality',
            'id number': 'emirates_id',
            'emirates id': 'emirates_id'
        }
        
        return mappings.get(key, key)

    def _save_processed_file(self, original_path: str, results: Dict) -> str:
        """Save processed file with extracted data."""
        # For now, just return the original path
        # In the future, we could add annotations or markings
        return original_path

    def _get_value_block(self, blocks: List[Dict], key_block: Dict) -> Optional[Dict]:
        """Get the value block associated with a key block in Textract response."""
        for relationship in key_block.get('Relationships', []):
            if relationship['Type'] == 'VALUE':
                for value_id in relationship['Ids']:
                    value_block = next(
                        (block for block in blocks if block['Id'] == value_id),
                        None
                    )
                    if value_block:
                        return value_block
        return None

    def _get_text(self, blocks: List[Dict], block: Dict) -> str:
        """Get text from a block in Textract response."""
        text = []
        if 'Relationships' in block:
            for relationship in block['Relationships']:
                if relationship['Type'] == 'CHILD':
                    for child_id in relationship['Ids']:
                        child_block = next(
                            (b for b in blocks if b['Id'] == child_id),
                            None
                        )
                        if child_block and 'Text' in child_block:
                            text.append(child_block['Text'])
        return ' '.join(text)

    def _parse_deepseek_response(self, response: Dict) -> Dict:
        """Parse DeepSeek response into structured data."""
        try:
            # DeepSeek returns a JSON string in the response
            if isinstance(response.get('text'), str):
                return json.loads(response['text'])
            return response.get('text', {})
        except json.JSONDecodeError:
            logger.warning("Failed to parse DeepSeek JSON response")
            return {}