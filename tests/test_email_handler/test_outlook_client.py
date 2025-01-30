import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta
import requests
from requests.exceptions import RequestException

from src.email_handler.outlook_client import OutlookClient
from src.utils.exceptions import AuthenticationError, EmailFetchError

class TestOutlookClient(unittest.TestCase):
    def setUp(self):
        self.test_email_id = "test_email_123"
        self.test_access_token = "test_token_123"
        # Create patches for all external dependencies
        self.msal_patcher = patch('src.email_handler.outlook_client.ConfidentialClientApplication')
        self.requests_patcher = patch('src.email_handler.outlook_client.requests')
        
        # Start the patches
        self.mock_msal = self.msal_patcher.start()
        self.mock_requests = self.requests_patcher.start()
        
        # Set up the MSAL mock
        self.mock_app = MagicMock()
        self.mock_msal.return_value = self.mock_app

    def tearDown(self):
        # Stop all patches
        self.msal_patcher.stop()
        self.requests_patcher.stop()

    def test_successful_authentication(self):
        # Configure mock
        self.mock_app.acquire_token_silent.return_value = None
        self.mock_app.acquire_token_for_client.return_value = {
            "access_token": self.test_access_token
        }

        # Create client
        client = OutlookClient()
        
        # Verify the token was set correctly
        self.assertEqual(client.access_token, self.test_access_token)
        
        # Verify the mock was called correctly
        self.mock_app.acquire_token_for_client.assert_called_once()

    def test_failed_authentication(self):
        # Configure mock to simulate authentication failure
        self.mock_app.acquire_token_silent.return_value = None
        self.mock_app.acquire_token_for_client.return_value = {}  # Empty dict = no token

        # Verify that creating the client raises an error
        with self.assertRaises(AuthenticationError):
            client = OutlookClient()

        # Verify the mock was called
        self.mock_app.acquire_token_for_client.assert_called_once()

    def test_fetch_emails(self):
        # Configure authentication mock
        self.mock_app.acquire_token_silent.return_value = None
        self.mock_app.acquire_token_for_client.return_value = {
            "access_token": self.test_access_token
        }

        # Configure requests mock
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "value": [
                {
                    "id": "1",
                    "subject": "Insurance Application",
                    "hasAttachments": True,
                    "receivedDateTime": "2024-01-30T10:00:00Z"
                }
            ]
        }
        self.mock_requests.get.return_value = mock_response

        # Create client and fetch emails
        client = OutlookClient()
        emails = client.fetch_emails()

        # Verify the results
        self.assertEqual(len(emails), 1)
        self.assertEqual(emails[0]["id"], "1")
        
        # Verify the mock was called with correct parameters
        self.mock_requests.get.assert_called_once()

    def test_fetch_emails_with_error(self):
        # Configure authentication mock
        self.mock_app.acquire_token_silent.return_value = None
        self.mock_app.acquire_token_for_client.return_value = {
            "access_token": self.test_access_token
        }

        # Configure requests mock to raise a RequestException
        self.mock_requests.get.side_effect = requests.exceptions.RequestException("Network error")

        # Create client
        client = OutlookClient()

        # Verify that fetching emails raises our custom EmailFetchError
        with self.assertRaises(EmailFetchError) as context:
            client.fetch_emails()

        # Verify the error message contains our network error
        self.assertIn("Network error", str(context.exception))

        # Verify the mock was called
        self.mock_requests.get.assert_called_once()

    def test_get_attachments(self):
        # Configure authentication mock
        self.mock_app.acquire_token_silent.return_value = None
        self.mock_app.acquire_token_for_client.return_value = {
            "access_token": self.test_access_token
        }

        # Configure requests mock
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "value": [
                {
                    "id": "att1",
                    "name": "passport.pdf",
                    "contentBytes": "base64content"
                }
            ]
        }
        self.mock_requests.get.return_value = mock_response

        # Create client and get attachments
        client = OutlookClient()
        attachments = client.get_attachments(self.test_email_id)

        # Verify the results
        self.assertEqual(len(attachments), 1)
        self.assertEqual(attachments[0]["id"], "att1")
        
        # Verify the mock was called with correct parameters
        self.mock_requests.get.assert_called()


if __name__ == '__main__':
    unittest.main()