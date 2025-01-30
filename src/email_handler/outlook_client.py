import os
import logging
from datetime import datetime
from typing import List, Dict, Optional

from msal import ConfidentialClientApplication
import requests
from requests.exceptions import RequestException

from config.settings import (
    GRAPH_API_ENDPOINT, CLIENT_ID, CLIENT_SECRET, 
    TENANT_ID, SCOPE, EMAIL_FOLDER, MAX_EMAIL_FETCH
)
from config.constants import SUBJECT_KEYWORDS
from src.utils.exceptions import AuthenticationError, EmailFetchError

logger = logging.getLogger(__name__)

class OutlookClient:
    def __init__(self):
        self.client_id = CLIENT_ID
        self.client_secret = CLIENT_SECRET
        self.tenant_id = TENANT_ID
        self.scope = SCOPE
        self.access_token = None
        self._authenticate()

    def _authenticate(self) -> None:
        """Authenticate with Microsoft Graph API using MSAL."""
        try:
            app = ConfidentialClientApplication(
                client_id=self.client_id,
                client_credential=self.client_secret,
                authority=f"https://login.microsoftonline.com/{self.tenant_id}"
            )
            
            result = app.acquire_token_silent(self.scope, account=None)
            if not result:
                result = app.acquire_token_for_client(scopes=self.scope)

            if "access_token" in result:
                self.access_token = result["access_token"]
            else:
                raise AuthenticationError("Failed to acquire access token")

        except Exception as e:
            logger.error(f"Authentication failed: {str(e)}")
            raise AuthenticationError(f"Failed to authenticate: {str(e)}")

    def _get_headers(self) -> Dict[str, str]:
        """Get headers for API requests."""
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }

    def fetch_emails(self, last_check_time: Optional[datetime] = None) -> List[Dict]:
        """
        Fetch emails from Outlook with attachments.
        
        Args:
            last_check_time: Optional datetime to fetch emails after this time
        
        Returns:
            List of dictionaries containing email data
        """
        try:
            filter_query = ""
            if last_check_time:
                filter_query = f"receivedDateTime ge {last_check_time.isoformat()}Z"

            endpoint = f"{GRAPH_API_ENDPOINT}/me/mailFolders/{EMAIL_FOLDER}/messages"
            params = {
                "$top": MAX_EMAIL_FETCH,
                "$filter": filter_query,
                "$select": "id,subject,receivedDateTime,hasAttachments",
                "$orderby": "receivedDateTime desc"
            }

            response = requests.get(
                endpoint,
                headers=self._get_headers(),
                params=params
            )
            response.raise_for_status()
            
            emails = response.json().get("value", [])
            return [
                email for email in emails 
                if email.get("hasAttachments") and 
                any(keyword.lower() in email.get("subject", "").lower() 
                    for keyword in SUBJECT_KEYWORDS)
            ]

        except RequestException as e:
            logger.error(f"Failed to fetch emails: {str(e)}")
            raise EmailFetchError(f"Failed to fetch emails: {str(e)}")

    def get_attachments(self, email_id: str) -> List[Dict]:
        """
        Get attachments for a specific email.
        
        Args:
            email_id: ID of the email
            
        Returns:
            List of dictionaries containing attachment data
        """
        try:
            endpoint = f"{GRAPH_API_ENDPOINT}/me/messages/{email_id}/attachments"
            response = requests.get(endpoint, headers=self._get_headers())
            response.raise_for_status()
            
            return response.json().get("value", [])

        except RequestException as e:
            logger.error(f"Failed to get attachments for email {email_id}: {str(e)}")
            raise EmailFetchError(f"Failed to get attachments: {str(e)}")