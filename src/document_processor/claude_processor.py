import os
import time
import logging
import base64
from typing import Dict, Optional, List
import anthropic
import requests
from PIL import Image
import io
import hashlib

logger = logging.getLogger(__name__)

class ClaudeProcessor:
    def __init__(self, cache_dir: str = "data/cache"):
        """Initialize Claude processor with caching."""
        self.client = anthropic.Client(api_key=os.getenv('ANTHROPIC_API_KEY'))
        self.cache_dir = cache_dir
        self.rate_limit_delay = 1  # Delay between API calls in seconds
        self.max_retries = 3
        os.makedirs(cache_dir, exist_ok=True)

    def _preprocess_image(self, image_path: str) -> bytes:
        """Preprocess image for better OCR quality."""
        try:
            with Image.open(image_path) as img:
                # Convert to RGB if necessary
                if img.mode != 'RGB':
                    img = img.convert('RGB')
                
                # Resize if too large (Claude has size limits)
                max_size = 5000  # Maximum dimension
                if max(img.size) > max_size:
                    ratio = max_size / max(img.size)
                    new_size = tuple(int(dim * ratio) for dim in img.size)
                    img = img.resize(new_size, Image.LANCZOS)

                # Adjust contrast and brightness if needed
                from PIL import ImageEnhance
                enhancer = ImageEnhance.Contrast(img)
                img = enhancer.enhance(1.2)  # Slight contrast boost
                
                # Save to bytes
                img_byte_arr = io.BytesIO()
                img.save(img_byte_arr, format='JPEG', quality=95)
                return img_byte_arr.getvalue()

        except Exception as e:
            logger.error(f"Error preprocessing image {image_path}: {str(e)}")
            raise

    def _get_cache_key(self, image_data: bytes) -> str:
        """Generate cache key for image data."""
        return hashlib.md5(image_data).hexdigest()

    def _check_cache(self, cache_key: str) -> Optional[Dict]:
        """Check if results exist in cache."""
        cache_file = os.path.join(self.cache_dir, f"{cache_key}.json")
        if os.path.exists(cache_file):
            import json
            with open(cache_file, 'r') as f:
                return json.load(f)
        return None

    def _save_to_cache(self, cache_key: str, data: Dict) -> None:
        """Save results to cache."""
        cache_file = os.path.join(self.cache_dir, f"{cache_key}.json")
        import json
        with open(cache_file, 'w') as f:
            json.dump(data, f)

    def process_document(self, image_path: str, doc_type: str = None) -> Dict[str, str]:
        """
        Process document using Claude's API with caching and rate limiting.
        
        Args:
            image_path: Path to the image file
            doc_type: Type of document (optional, for specialized extraction)
            
        Returns:
            Dictionary containing extracted fields
        """
        try:
            # Preprocess image
            image_data = self._preprocess_image(image_path)
            
            # Check cache
            cache_key = self._get_cache_key(image_data)
            cached_result = self._check_cache(cache_key)
            if cached_result:
                logger.info(f"Using cached results for {image_path}")
                return cached_result

            # Convert image to base64
            image_base64 = base64.b64encode(image_data).decode('utf-8')

            # Prepare the prompt based on document type
            prompt = self._get_prompt_for_doc_type(doc_type)

            # Make API call with retries
            for attempt in range(self.max_retries):
                try:
                    response = self.client.messages.create(
                        model="claude-3-haiku-20240307",
                        max_tokens=1000,
                        messages=[{
                            "role": "user",
                            "content": [
                                {
                                    "type": "image",
                                    "source": {
                                        "type": "base64",
                                        "media_type": "image/jpeg",
                                        "data": image_base64
                                    }
                                },
                                {
                                    "type": "text",
                                    "text": prompt
                                }
                            ]
                        }]
                    )

                    # Process Claude's response
                    extracted_data = self._parse_claude_response(response.content)
                    
                    # Cache the results
                    self._save_to_cache(cache_key, extracted_data)
                    
                    # Respect rate limits
                    time.sleep(self.rate_limit_delay)
                    
                    return extracted_data

                except anthropic.RateLimitError:
                    if attempt < self.max_retries - 1:
                        wait_time = (attempt + 1) * 2  # Exponential backoff
                        logger.warning(f"Rate limit hit, waiting {wait_time} seconds...")
                        time.sleep(wait_time)
                    else:
                        raise
                except Exception as e:
                    if attempt == self.max_retries - 1:
                        raise
                    logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
                    time.sleep(1)

        except Exception as e:
            logger.error(f"Error processing document {image_path}: {str(e)}")
            raise

    def _get_prompt_for_doc_type(self, doc_type: str) -> str:
        """Get appropriate prompt based on document type."""
        base_prompt = "Please extract all relevant information from this document. "
        
        prompts = {
            'emirates_id': base_prompt + """
                Focus on:
                - Emirates ID number
                - Full name (in English and Arabic)
                - Nationality
                Return the information in a clear, structured format.
                """,
            'passport': base_prompt + """
                Focus on:
                - Passport number
                - Full name
                - Date of birth
                - Nationality
                - Expiry date
                Return the information in a clear, structured format.
                """,
            'visa': base_prompt + """
                Focus on:
                - Entry permit number
                - Full name
                - Nationality
                - Issue date
                Return the information in a clear, structured format.
                """,
            'work_permit': base_prompt + """
                Focus on:
                - Full name
                - Personal number
                - Expiry date
                - Profession
                - Employer details
                Return the information in a clear, structured format.
                """
        }
        
        return prompts.get(doc_type, base_prompt)

    def _parse_claude_response(self, response: str) -> Dict[str, str]:
        """Parse Claude's response into structured data."""
        try:
            # Add logic to parse Claude's natural language response
            # This will depend on how Claude formats its responses
            # You might want to use regex or other parsing methods
            
            # For now, returning a simple dictionary
            # You'll need to enhance this based on actual responses
            return {
                'raw_text': response,
                # Add more structured fields based on the response
            }
        except Exception as e:
            logger.error(f"Error parsing Claude response: {str(e)}")
            raise