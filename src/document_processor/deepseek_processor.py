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
    
    def extract_names_from_passport(self, file_path: str) -> Dict[str, str]:
        """Extract just the first and last name from a passport document."""
        if not self.client:
            logger.warning("DeepSeek client not available, cannot extract names")
            return {"first_name": self.DEFAULT_VALUE, "last_name": self.DEFAULT_VALUE}
            
        try:
            logger.info(f"Attempting to extract names from passport: {file_path}")
            
            # First try text-only approach
            name_extraction_prompt = f"""
            I need to extract the first name and last name from a passport document.
            The typical format of names on passports is:
            - Surname/Last name field: Contains family name
            - Given name/First name field: Contains first and middle names
            
            Please return only a simple JSON object with two fields:
            {{"first_name": "...", "last_name": "..."}}
            
            Use "." for any missing fields.
            """
            
            # Call the API with text-only prompt
            logger.info("Calling DeepSeek API with text-only prompt")
            messages = [
                {
                    "role": "user", 
                    "content": name_extraction_prompt
                }
            ]
            
            try:
                response = self.client.chat.completions.create(
                    model="deepseek-chat",  # Use text-only model
                    messages=messages,
                    max_tokens=500
                )
                
                # Extract content
                content = response.choices[0].message.content.strip()
                logger.info(f"DeepSeek API response: {content}")
                
                # Try to parse as JSON
                try:
                    name_data = json.loads(content)
                    first_name = name_data.get("first_name", self.DEFAULT_VALUE)
                    last_name = name_data.get("last_name", self.DEFAULT_VALUE)
                    
                    if first_name != self.DEFAULT_VALUE or last_name != self.DEFAULT_VALUE:
                        logger.info(f"Successfully extracted names: First={first_name}, Last={last_name}")
                        return {"first_name": first_name, "last_name": last_name}
                except:
                    logger.warning("Failed to parse DeepSeek response as JSON")
                    
            except Exception as e:
                logger.warning(f"Text-only API call failed: {str(e)}")
            
            # Fallback to the Textract data
            logger.info("Using Textract data for name extraction as fallback")
            return {"first_name": self.DEFAULT_VALUE, "last_name": self.DEFAULT_VALUE}
                
        except Exception as e:
            logger.error(f"Error extracting names from passport: {str(e)}")
            return {"first_name": self.DEFAULT_VALUE, "last_name": self.DEFAULT_VALUE}