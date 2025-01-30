import os
import logging
from datetime import datetime
from typing import List, Dict, Optional
import json
from urllib.parse import quote

from msal import ConfidentialClientApplication
import requests
from requests.exceptions import RequestException

from config.settings import (
    GRAPH_API_ENDPOINT, CLIENT_ID, CLIENT_SECRET, 
    TENANT_ID, USER_EMAIL, TARGET_MAILBOX, MAX_EMAIL_FETCH
)
from config.constants import SUBJECT_KEYWORDS
from src.utils.exceptions import AuthenticationError, EmailFetchError

logger = logging.getLogger(__name__)

class OutlookClient:
    def __init__(self):
        self.client_id = CLIENT_ID
        self.client_secret = CLIENT_SECRET
        self.tenant_id = TENANT_ID
        self.service_email = quote(USER_EMAIL)
        self.target_mailbox = quote(TARGET_MAILBOX)
        self.scope = ['https://graph.microsoft.com/.default']
        self.access_token = None

        # Validate configuration
        if not all([self.client_id, self.client_secret, self.tenant_id, 
                   USER_EMAIL, TARGET_MAILBOX]):
            raise ValueError("Missing required configuration values")

        logger.info("Initializing OutlookClient with:")
        logger.info(f"Service Account: {USER_EMAIL}")
        logger.info(f"Target Mailbox: {TARGET_MAILBOX}")
        
        self._authenticate()

    def _authenticate(self) -> None:
        """Authenticate with Microsoft Graph API using application permissions."""
        try:
            logger.info("Starting authentication process...")
            app = ConfidentialClientApplication(
                client_id=self.client_id,
                client_credential=self.client_secret,
                authority=f"https://login.microsoftonline.com/{self.tenant_id}"
            )
            
            result = app.acquire_token_for_client(scopes=self.scope)

            if "error" in result:
                logger.error(f"Error in token result: {result.get('error')}")
                logger.error(f"Error description: {result.get('error_description')}")
                raise AuthenticationError(f"Failed to acquire token: {result.get('error_description')}")

            if "access_token" in result:
                self.access_token = result["access_token"]
                logger.info("Successfully acquired access token")
            else:
                raise AuthenticationError("No access token in response")

        except Exception as e:
            logger.error(f"Authentication failed: {str(e)}")
            raise AuthenticationError(f"Failed to authenticate: {str(e)}")

    def fetch_emails(self, last_check_time: Optional[datetime] = None) -> List[Dict]:
        """Fetch emails from target mailbox."""
        try:
            endpoint = f"{GRAPH_API_ENDPOINT}/users/{self.target_mailbox}/messages"
            logger.info(f"Fetching emails from: {endpoint}")

            params = {
                "$top": MAX_EMAIL_FETCH,
                "$select": "id,subject,receivedDateTime,hasAttachments",
                "$orderby": "receivedDateTime desc"
            }

            if last_check_time:
                formatted_date = last_check_time.strftime("%Y-%m-%dT%H:%M:%SZ")
                params["$filter"] = f"receivedDateTime ge {formatted_date}"
                logger.info(f"Using date filter: {formatted_date}")

            response = requests.get(
                endpoint,
                headers=self._get_headers(),
                params=params
            )

            if response.status_code == 401:
                logger.info("Token expired, refreshing...")
                self._authenticate()
                response = requests.get(
                    endpoint,
                    headers=self._get_headers(),
                    params=params
                )

            if response.status_code != 200:
                logger.error(f"Error response: {response.text}")
                response.raise_for_status()

            emails = response.json().get("value", [])
            logger.info(f"Retrieved {len(emails)} emails")

            filtered_emails = [
                email for email in emails 
                if email.get("hasAttachments") and 
                any(keyword.lower() in email.get("subject", "").lower() 
                    for keyword in SUBJECT_KEYWORDS)
            ]
            
            logger.info(f"Filtered to {len(filtered_emails)} relevant emails")
            return filtered_emails

        except Exception as e:
            logger.error(f"Failed to fetch emails: {str(e)}")
            raise EmailFetchError(f"Failed to fetch emails: {str(e)}")

    def get_attachments(self, email_id: str) -> List[Dict]:
        """Get attachments from target mailbox."""
        try:
            endpoint = f"{GRAPH_API_ENDPOINT}/users/{self.target_mailbox}/messages/{email_id}/attachments"
            logger.info(f"Fetching attachments from: {endpoint}")

            response = requests.get(endpoint, headers=self._get_headers())

            if response.status_code == 401:
                logger.info("Token expired, refreshing...")
                self._authenticate()
                response = requests.get(endpoint, headers=self._get_headers())

            if response.status_code != 200:
                logger.error(f"Error response: {response.text}")
                response.raise_for_status()

            attachments = response.json().get("value", [])
            logger.info(f"Found {len(attachments)} attachments")
            return attachments

        except Exception as e:
            logger.error(f"Failed to get attachments: {str(e)}")
            raise EmailFetchError(f"Failed to get attachments: {str(e)}")

    def _get_headers(self) -> Dict[str, str]:
        """Get headers for API requests."""
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }