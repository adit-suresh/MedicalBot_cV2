import sys
import os
import logging
import requests
from urllib.parse import quote

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.email_handler.outlook_client import OutlookClient
from src.utils.logger import setup_logger
from config.settings import GRAPH_API_ENDPOINT, USER_EMAIL

def test_basic_access():
    logger = setup_logger('basic_access_test')
    logger.setLevel(logging.DEBUG)
    
    try:
        # Initialize client
        client = OutlookClient()
        headers = {
            "Authorization": f"Bearer {client.access_token}",
            "Content-Type": "application/json"
        }
        
        # Test endpoints
        endpoints = [
            f"/users/{quote(USER_EMAIL)}",  # Basic user info
            f"/users/{quote(USER_EMAIL)}/messages?$top=1",  # Single message
            f"/users/{quote(USER_EMAIL)}/mailFolders",  # List mail folders
        ]
        
        for endpoint in endpoints:
            url = f"{GRAPH_API_ENDPOINT}{endpoint}"
            logger.info(f"\nTesting endpoint: {url}")
            
            response = requests.get(url, headers=headers)
            logger.info(f"Status code: {response.status_code}")
            logger.info(f"Response: {response.text[:200]}...")  # First 200 chars
            
            if response.status_code == 200:
                logger.info("✓ Success!")
            else:
                logger.error("✗ Failed!")

    except Exception as e:
        logger.error(f"Test failed: {str(e)}")

if __name__ == "__main__":
    test_basic_access()