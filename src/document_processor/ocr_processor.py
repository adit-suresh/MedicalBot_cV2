import boto3
import requests
import os
import logging
from typing import Dict, Optional
from PIL import Image
import io
from botocore.exceptions import BotoCoreError, ClientError
import json

logger = logging.getLogger(__name__)

class OCRProcessor:
    def __init__(self):
        # Initialize AWS Textract client
        self.textract = boto3.client(
            'textract',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION', 'us-east-1')
        )
        
        # DeepSeek API configuration
        self.deepseek_api_key = os.getenv('DEEPSEEK_API_KEY')
        self.deepseek_api_url = os.getenv('DEEPSEEK_API_URL')

    def process_document(self, file_path: str, doc_type: str) -> Dict[str, str]:
        """
        Process document using both AWS Textract and DeepSeek for optimal results.
        
        Args:
            file_path: Path to document file
            doc_type: Type of document (emirates_id, passport, etc.)
            
        Returns:
            Dictionary containing extracted fields
        """
        try:
            # Read the file
            with open(file_path, 'rb') as document:
                file_bytes = document.read()

            # Get results from both services
            textract_result = self._process_with_textract(file_bytes)
            deepseek_result = self._process_with_deepseek(file_bytes)

            # Combine and validate results
            combined_results = self._combine_results(
                textract_result,
                deepseek_result,
                doc_type
            )

            return combined_results

        except Exception as e:
            logger.error(f"Error processing document {file_path}: {str(e)}")
            raise

    def _process_with_textract(self, file_bytes: bytes) -> Dict:
        """Process document with AWS Textract."""
        try:
            response = self.textract.detect_document_text(
                Document={'Bytes': file_bytes}
            )

            # Extract text blocks
            text_blocks = []
            for item in response['Blocks']:
                if item['BlockType'] == 'LINE':
                    text_blocks.append({
                        'text': item['Text'],
                        'confidence': item['Confidence'],
                        'boundingBox': item['Geometry']['BoundingBox']
                    })

            return {
                'blocks': text_blocks,
                'raw_response': response
            }

        except (BotoCoreError, ClientError) as e:
            logger.error(f"AWS Textract error: {str(e)}")
            raise

    def _process_with_deepseek(self, file_bytes: bytes) -> Dict:
        """Process document with DeepSeek R1."""
        try:
            headers = {
                'Authorization': f'Bearer {self.deepseek_api_key}',
                'Content-Type': 'application/octet-stream'
            }

            response = requests.post(
                self.deepseek_api_url,
                headers=headers,
                data=file_bytes
            )
            response.raise_for_status()

            return response.json()

        except requests.RequestException as e:
            logger.error(f"DeepSeek API error: {str(e)}")
            raise

    def _combine_results(self, textract_result: Dict, 
                        deepseek_result: Dict, doc_type: str) -> Dict:
        """
        Combine and validate results from both services.
        Uses confidence scores to select best results.
        """
        combined_data = {}
        
        if doc_type == 'emirates_id':
            combined_data.update(
                self._extract_emirates_id_data(textract_result, deepseek_result)
            )
        elif doc_type == 'passport':
            combined_data.update(
                self._extract_passport_data(textract_result, deepseek_result)
            )
        elif doc_type == 'visa':
            combined_data.update(
                self._extract_visa_data(textract_result, deepseek_result)
            )

        return combined_data

    def _extract_emirates_id_data(self, textract_result: Dict, 
                                deepseek_result: Dict) -> Dict:
        """Extract Emirates ID specific data."""
        data = {}
        
        # Process Textract blocks
        for block in textract_result['blocks']:
            text = block['text']
            
            # Extract Emirates ID number
            if 'ID Number' in text or 'رقم الهوية' in text:
                next_block = self._get_next_block(textract_result['blocks'], block)
                if next_block:
                    data['emirates_id'] = next_block['text']

            # Extract name
            if 'Name' in text or 'الاسم' in text:
                next_block = self._get_next_block(textract_result['blocks'], block)
                if next_block:
                    data['name'] = next_block['text']

        # Compare with DeepSeek results and use highest confidence
        # Implement DeepSeek specific extraction here

        return data

    def _extract_passport_data(self, textract_result: Dict, 
                             deepseek_result: Dict) -> Dict:
        """Extract passport specific data."""
        # Similar to Emirates ID extraction but for passport fields
        pass

    def _extract_visa_data(self, textract_result: Dict, 
                          deepseek_result: Dict) -> Dict:
        """Extract visa specific data."""
        # Similar to Emirates ID extraction but for visa fields
        pass

    def _get_next_block(self, blocks: list, current_block: Dict) -> Optional[Dict]:
        """Get next text block based on position."""
        current_box = current_block['boundingBox']
        
        # Find blocks that are below current block
        next_blocks = [
            b for b in blocks
            if b['boundingBox']['Top'] > current_box['Top'] + current_box['Height']
            and abs(b['boundingBox']['Left'] - current_box['Left']) < 0.1
        ]
        
        if next_blocks:
            # Return the closest block
            return min(next_blocks, 
                      key=lambda b: b['boundingBox']['Top'])
        return None