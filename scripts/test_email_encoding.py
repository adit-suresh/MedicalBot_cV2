import sys
import os
from urllib.parse import quote
import logging

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import USER_EMAIL
from src.utils.logger import setup_logger

logger = setup_logger('email_encoding_test')

def test_email_encoding():
    logger.info("Testing email encoding...")
    
    # Original email
    logger.info(f"Original email: {USER_EMAIL}")
    
    # Basic encoding
    encoded_email = quote(USER_EMAIL)
    logger.info(f"URL encoded email: {encoded_email}")
    
    # Double check the encoding worked correctly
    if '@' in encoded_email:
        logger.warning("Warning: @ symbol not encoded properly!")
    else:
        logger.info("Email encoded successfully - @ symbol properly converted to %40")
        
    # Show example API URL
    example_url = f"https://graph.microsoft.com/v1.0/users/{encoded_email}/messages"
    logger.info(f"Example API URL: {example_url}")

if __name__ == "__main__":
    test_email_encoding()