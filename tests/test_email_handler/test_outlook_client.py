import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta

from src.email_handler.outlook_client import OutlookClient
from src.utils.exceptions import AuthenticationError, EmailFetchError

class TestOutlookClient(unittest.TestCase):
    def setUp(self):
        self.test_email_id = "test_email_123"
        self.test_access_token = "test_token_123"

    @patch('msal.ConfidentialClientApplication')
    def test_successful_authentication(self, mock_msal):
        # Mock the MSAL authentication
        mock_app = MagicMock()
        mock_app.acquire_token_silent.return_value = None
        mock_app.acquire_token_for_client.return_value = {
            "access_token": self.test_access_token
        }
        mock_msal.return_value = mock_app

        client = OutlookClient()
        self.assertEqual(client.access_token, self.test_access_token)

    @patch('msal.ConfidentialClientApplication')
    def test_failed_authentication(self, mock_msal):
        # Mock failed authentication
        mock_app = MagicMock()
        mock_app.acquire_token_silent.return_value = None
        mock_app.acquire_token_for_client.return_value = {}
        mock_msal.return_value = mock_app

        with self.assertRaises(AuthenticationError):
            OutlookClient()

    @patch('requests.get')
    def test_fetch_emails(self, mock_get):
        # Mock successful email fetch
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
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        client = OutlookClient()
        client.access_token = self.test_access_token
        emails = client.fetch_emails()

        self.assertEqual(len(emails), 1)
        self.assertEqual(emails[0]["id"], "1")

    @patch('requests.get')
    def test_fetch_emails_with_error(self, mock_get):
        # Mock failed email fetch
        mock_get.side_effect = Exception("Network error")

        client = OutlookClient()
        client.access_token = self.test_access_token

        with self.assertRaises(EmailFetchError):
            client.fetch_emails()

    @patch('requests.get')
    def test_get_attachments(self, mock_get):
        # Mock successful attachment fetch
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
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        client = OutlookClient()
        client.access_token = self.test_access_token
        attachments = client.get_attachments(self.test_email_id)

        self.assertEqual(len(attachments), 1)
        self.assertEqual(attachments[0]["id"], "att1")