import sys
import os
import logging
import requests
from urllib.parse import quote

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.email_handler.outlook_client import OutlookClient
from src.utils.logger import setup_logger
from config.settings import GRAPH_API_ENDPOINT, USER_EMAIL, TARGET_MAILBOX

def test_graph_permissions():
    logger = setup_logger('permissions_test')
    logger.info("Starting permissions test...")

    try:
        # Initialize client
        client = OutlookClient()
        headers = {
            "Authorization": f"Bearer {client.access_token}",
            "Content-Type": "application/json"
        }

        # Test different endpoints
        test_endpoints = [
            {
                "name": "Service Account Info",
                "url": f"/users/{quote(USER_EMAIL)}",
                "expected_code": 200
            },
            {
                "name": "Target Mailbox Info",
                "url": f"/users/{quote(TARGET_MAILBOX)}",
                "expected_code": 200
            },
            {
                "name": "Target Mailbox Messages",
                "url": f"/users/{quote(TARGET_MAILBOX)}/messages?$top=1",
                "expected_code": 200
            },
            {
                "name": "Target Mailbox Folders",
                "url": f"/users/{quote(TARGET_MAILBOX)}/mailFolders",
                "expected_code": 200
            }
        ]

        all_tests_passed = True
        
        for test in test_endpoints:
            logger.info(f"\nTesting: {test['name']}")
            url = f"{GRAPH_API_ENDPOINT}{test['url']}"
            logger.info(f"Endpoint: {url}")
            
            response = requests.get(url, headers=headers)
            status = response.status_code
            
            logger.info(f"Status code: {status}")
            
            if status == test['expected_code']:
                logger.info(f"✓ Success! {test['name']} test passed")
            else:
                all_tests_passed = False
                logger.error(f"✗ Failed! {test['name']} test failed")
                logger.error(f"Response: {response.text}")

        if all_tests_passed:
            logger.info("\n🎉 All permission tests passed!")
        else:
            logger.error("\n⚠️ Some permission tests failed!")

        return all_tests_passed

    except Exception as e:
        logger.error(f"Test failed with error: {str(e)}")
        return False

if __name__ == "__main__":
    success = test_graph_permissions()
    sys.exit(0 if success else 1)