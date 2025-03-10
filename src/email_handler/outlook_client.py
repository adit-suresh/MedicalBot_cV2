import os
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import json
from urllib.parse import quote
import time
import threading

from msal import ConfidentialClientApplication
import requests
from requests.exceptions import RequestException, Timeout, ConnectionError

from config.settings import (
    GRAPH_API_ENDPOINT, CLIENT_ID, CLIENT_SECRET, 
    TENANT_ID, USER_EMAIL, TARGET_MAILBOX, MAX_EMAIL_FETCH
)
from config.constants import SUBJECT_KEYWORDS
from src.utils.error_handling import handle_errors, ErrorCategory, ErrorSeverity
from src.utils.exceptions import AuthenticationError, EmailFetchError
from src.email_tracker.email_tracker import EmailTracker

logger = logging.getLogger(__name__)

class TokenManager:
    """Manages authentication tokens with automatic refresh."""
    
    def __init__(self, client_id: str, client_secret: str, tenant_id: str):
        """Initialize token manager.
        
        Args:
            client_id: Application client ID
            client_secret: Application client secret
            tenant_id: Microsoft tenant ID
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self.scope = ['https://graph.microsoft.com/.default']
        self.access_token = None
        self.token_expiry = datetime.min
        self.lock = threading.RLock()
        self.refresh_margin = timedelta(minutes=5)  # Refresh token 5 minutes before expiry
        self.email_tracker = EmailTracker()
        
    def get_token(self) -> str:
        """Get a valid access token, refreshing if necessary.
        
        Returns:
            str: Valid access token
            
        Raises:
            AuthenticationError: If token acquisition fails
        """
        with self.lock:
            now = datetime.now()
            if not self.access_token or now >= (self.token_expiry - self.refresh_margin):
                self._acquire_token()
            return self.access_token
            
    def _acquire_token(self) -> None:
        """Acquire a new access token.
        
        Raises:
            AuthenticationError: If token acquisition fails
        """
        try:
            logger.info("Acquiring new access token...")
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
                # Calculate token expiry time
                if "expires_in" in result:
                    expires_in_seconds = int(result["expires_in"])
                    self.token_expiry = datetime.now() + timedelta(seconds=expires_in_seconds)
                else:
                    # Default expiry if not provided
                    self.token_expiry = datetime.now() + timedelta(hours=1)
                logger.info(f"Successfully acquired access token (expires: {self.token_expiry.isoformat()})")
            else:
                raise AuthenticationError("No access token in response")

        except Exception as e:
            logger.error(f"Authentication failed: {str(e)}")
            raise AuthenticationError(f"Failed to authenticate: {str(e)}")

    def invalidate_token(self) -> None:
        """Invalidate the current token, forcing a refresh on next get_token call."""
        with self.lock:
            self.token_expiry = datetime.min

class OutlookClient:
    """Enhanced Outlook client with connection pooling and retry logic."""
    
    def __init__(self):
        """Initialize Outlook client with Microsoft Graph API."""
        # Validate configuration
        self._validate_config()
        
        # Initialize token manager
        self.token_manager = TokenManager(CLIENT_ID, CLIENT_SECRET, TENANT_ID)
        
        # Set up connection pooling
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10,
            pool_maxsize=20,
            max_retries=3
        )
        self.session.mount('https://', adapter)
        
        # Email address format for API requests
        self.service_email = quote(USER_EMAIL)
        self.target_mailbox = quote(TARGET_MAILBOX)
        
        # Cache to store frequently used data
        self._mailbox_cache = {}
        self._cache_expiry = {}
        self._cache_lock = threading.RLock()
        
        logger.info("Initializing OutlookClient with:")
        logger.info(f"Service Account: {USER_EMAIL}")
        logger.info(f"Target Mailbox: {TARGET_MAILBOX}")

    def _validate_config(self) -> None:
        """Validate required configuration.
        
        Raises:
            ValueError: If required configuration is missing
        """
        # Check required settings with descriptive error messages
        missing = []
        for name, value in [
            ('Client ID', CLIENT_ID),
            ('Client Secret', CLIENT_SECRET),
            ('Tenant ID', TENANT_ID),
            ('User Email', USER_EMAIL),
            ('Target Mailbox', TARGET_MAILBOX)
        ]:
            if not value:
                missing.append(name)
                
        if missing:
            missing_str = ', '.join(missing)
            error_msg = f"Missing required configuration: {missing_str}"
            logger.error(error_msg)
            raise ValueError(error_msg)

    @handle_errors(ErrorCategory.NETWORK, ErrorSeverity.HIGH)
    def fetch_emails(self, last_check_time: Optional[datetime] = None) -> List[Dict]:
        """
        Fetch emails from target mailbox with incremental filtering.
        
        If last_check_time is provided, it is used directly; otherwise, the method
        will attempt to fetch emails from these incremental time windows:
        - Last 1 hour
        - Last 3 hours
        - Last 5 day
        
        Returns:
            List of email dictionaries that pass client-side filtering.
            
        Raises:
            EmailFetchError: If fetching emails fails.
        """
        try:
            # Initialize EmailTracker directly inside the method to avoid reference issues
            from src.email_tracker.email_tracker import EmailTracker
            email_tracker = EmailTracker()
            
            if last_check_time is not None:
                emails = self._fetch_emails_with_last_check(last_check_time)
                if emails:
                    logger.info(f"Found {len(emails)} emails before filtering processed ones")
                    # Log subjects for debugging
                    subjects = [email.get('subject', 'No Subject') for email in emails]
                    logger.info(f"Email subjects before filtering: {subjects}")
                    
                    # Filter out already processed emails
                    unprocessed_emails = [email for email in emails if not email_tracker.is_processed(email['id'])]
                    logger.info(f"After filtering processed emails: {len(unprocessed_emails)} emails remaining")
                    
                    return unprocessed_emails
                return []
            else:
                now = datetime.now()
                # Define incremental time windows: last hour, last 3 hours, last 5 days.
                time_windows = [now - timedelta(hours=1), now - timedelta(hours=3), now - timedelta(days=5)]
                for window in time_windows:
                    logger.info(f"Trying to fetch emails since {window.isoformat()}...")
                    emails = self._fetch_emails_with_last_check(window)
                    if emails:
                        logger.info(f"Found {len(emails)} emails since {window.isoformat()} before filtering processed ones")
                        # Log subjects for debugging
                        subjects = [email.get('subject', 'No Subject') for email in emails]
                        logger.info(f"Email subjects before filtering: {subjects}")
                        
                        # Filter out already processed emails
                        unprocessed_emails = [email for email in emails if not email_tracker.is_processed(email['id'])]
                        logger.info(f"After filtering processed emails: {len(unprocessed_emails)} emails remaining")
                        
                        if unprocessed_emails:
                            return unprocessed_emails
                
                logger.info("No unprocessed emails found in any time window")
                return []
        except Exception as e:
            logger.error(f"Error in fetch_emails: {str(e)}")
            raise EmailFetchError(f"Failed to fetch emails: {str(e)}")

    def _fetch_emails_with_last_check(self, last_check_time: datetime) -> List[Dict]:
        """Internal helper that fetches emails with deduplication."""
        try:
            emails = []
            seen_emails = set()  # Track by message ID
            seen_subjects = {}   # Track by subject
            next_link = None
            
            # Load previously processed emails
            from src.email_tracker.email_tracker import EmailTracker
            email_tracker = EmailTracker()
            
            logger.info(f"Fetching emails since {last_check_time.isoformat()}")
            
            params = self._build_email_params(last_check_time)
            endpoint = f"{GRAPH_API_ENDPOINT}/users/{self.target_mailbox}/messages"
            
            while True:
                response = self._execute_request(
                    "GET", 
                    next_link if next_link else endpoint,
                    params=None if next_link else params
                )
                
                data = response.json()
                batch = data.get("value", [])
                filtered_batch = self._filter_emails(batch)
                
                for email in filtered_batch:
                    email_id = email.get('id')
                    subject = email.get('subject', '').strip()
                    received_time = email.get('receivedDateTime')
                    
                    # Skip if we've seen this email ID in this run or it's been processed before
                    if email_id in seen_emails or email_tracker.is_processed(email_id):
                        continue
                        
                    # For same subject, keep only the latest
                    if subject in seen_subjects:
                        old_time = seen_subjects[subject]['time']
                        if received_time <= old_time:
                            continue
                        # Remove older version
                        emails = [e for e in emails if e['subject'] != subject]
                    
                    seen_emails.add(email_id)
                    seen_subjects[subject] = {
                        'time': received_time,
                        'id': email_id
                    }
                    emails.append(email)
                
                if "@odata.nextLink" in data:
                    next_link = data["@odata.nextLink"]
                else:
                    break
                    
                if len(emails) >= self.MAX_EMAIL_FETCH:
                    break
            
            # Sort by received time (newest first)
            emails.sort(
                key=lambda x: x.get('receivedDateTime', ''),
                reverse=True
            )
            
            logger.info(f"Found {len(emails)} unique emails")
            return emails
            
        except Exception as e:
            logger.error(f"Failed to fetch emails: {str(e)}")
            raise EmailFetchError(f"Failed to fetch emails: {str(e)}")

    @handle_errors(ErrorCategory.NETWORK, ErrorSeverity.MEDIUM)
    def get_attachments(self, email_id: str) -> List[Dict]:
        """Get attachments from an email with improved error handling.
        
        Args:
            email_id: Email identifier
            
        Returns:
            List of attachment dictionaries
            
        Raises:
            EmailFetchError: If fetching attachments fails
        """
        try:
            endpoint = f"{GRAPH_API_ENDPOINT}/users/{self.target_mailbox}/messages/{email_id}/attachments"
            logger.info(f"Fetching attachments for email {email_id}")

            # Execute request with proper error handling
            response = self._execute_request("GET", endpoint)
            attachments = response.json().get("value", [])
            
            # Log attachment details
            if attachments:
                logger.info(f"Found {len(attachments)} attachments")
                for idx, attachment in enumerate(attachments, 1):
                    size_kb = attachment.get('size', 0) / 1024
                    logger.debug(f"Attachment {idx}: {attachment.get('name', 'Unknown')} ({size_kb:.1f} KB)")
            else:
                logger.info("No attachments found")
                
            return attachments

        except Exception as e:
            logger.error(f"Failed to get attachments for email {email_id}: {str(e)}")
            raise EmailFetchError(f"Failed to get attachments: {str(e)}")

    def _execute_request(self, method: str, url: str, headers: Optional[Dict] = None, 
                        params: Optional[Dict] = None, json_data: Optional[Dict] = None,
                        retry_count: int = 3, retry_delay: float = 1.0) -> requests.Response:
        """Execute HTTP request with retry logic and token refresh.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            url: Request URL
            headers: Optional additional headers
            params: Optional query parameters
            json_data: Optional JSON body
            retry_count: Maximum number of retries
            retry_delay: Initial delay between retries
            
        Returns:
            Response object
            
        Raises:
            RequestException: If request fails after all retries
        """
        request_headers = self._get_headers()
        if headers:
            request_headers.update(headers)
            
        for attempt in range(retry_count):
            try:
                # Ensure token is valid before request
                self.token_manager.get_token()
                request_headers['Authorization'] = f"Bearer {self.token_manager.access_token}"
                
                response = self.session.request(
                    method=method,
                    url=url,
                    headers=request_headers,
                    params=params,
                    json=json_data,
                    timeout=(5, 30)  # Connection timeout, read timeout
                )
                
                # Handle auth errors
                if response.status_code == 401:
                    logger.warning("Received 401 unauthorized, refreshing token...")
                    self.token_manager.invalidate_token()
                    continue
                
                # Handle rate limiting
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', retry_delay * (2 ** attempt)))
                    logger.warning(f"Rate limited, waiting {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue
                    
                # Handle server errors
                if response.status_code >= 500:
                    backoff = retry_delay * (2 ** attempt)
                    logger.warning(f"Server error {response.status_code}, retrying in {backoff:.1f}s...")
                    time.sleep(backoff)
                    continue
                
                # Raise for other error codes
                response.raise_for_status()
                return response
                
            except (ConnectionError, Timeout) as e:
                # Network errors are retryable
                if attempt < retry_count - 1:
                    backoff = retry_delay * (2 ** attempt)
                    logger.warning(f"Network error on attempt {attempt+1}/{retry_count}: {str(e)}")
                    logger.warning(f"Retrying in {backoff:.1f}s...")
                    time.sleep(backoff)
                else:
                    logger.error(f"Network error, all retries failed: {str(e)}")
                    raise
            except RequestException as e:
                # Handle other request exceptions
                if response.status_code == 404:
                    logger.error(f"Resource not found: {url}")
                    raise EmailFetchError(f"Resource not found: {response.text}")
                elif 400 <= response.status_code < 500:
                    # Client errors are generally not retryable
                    logger.error(f"Client error {response.status_code}: {response.text}")
                    raise EmailFetchError(f"API error {response.status_code}: {response.text}")
                else:
                    # Unexpected error
                    logger.error(f"Request failed: {str(e)}")
                    raise
                    
        # If we get here, all retries failed
        raise EmailFetchError(f"Request failed after {retry_count} attempts")

    def _get_headers(self) -> Dict[str, str]:
        """Get headers for API requests."""
        return {
            "Authorization": f"Bearer {self.token_manager.access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "client-request-id": f"{hash(time.time())}",  # Unique request ID
            "return-client-request-id": "true"
        }
        
    def _build_email_params(self, last_check_time: Optional[datetime]) -> Dict[str, str]:
        """Build query parameters with efficient filtering.
        
        Args:
            last_check_time: Optional datetime to filter emails by received time
            
        Returns:
            Dictionary of query parameters
        """
        # Base parameters
        params = {
            "$top": str(MAX_EMAIL_FETCH),
            "$select": "id,subject,receivedDateTime,hasAttachments,importance,from"
        }
        
        # Build filter conditions
        filters = []
        
        # Date filter
        if last_check_time:
            formatted_date = last_check_time.strftime("%Y-%m-%dT%H:%M:%SZ")
            filters.append(f"receivedDateTime ge {formatted_date}")
        
        # Only emails with attachments
        filters.append("hasAttachments eq true")
        
        # Combine all filters
        if filters:
            params["$filter"] = " and ".join(filters)
            
        return params
        
    def _filter_emails(self, emails: List[Dict]) -> List[Dict]:
        """Apply additional client-side filtering to emails."""
        filtered_emails = []
        skipped_count = 0
        
        for email in emails:
            # Skip emails without attachments (though the server filter should catch this)
            if not email.get('hasAttachments', False):
                skipped_count += 1
                continue
            
            # Check for keywords in subject
            subject = email.get('subject', '').lower()
            if not any(keyword.lower() in subject for keyword in SUBJECT_KEYWORDS):
                skipped_count += 1
                continue
                
            # Only keep high and normal importance emails
            importance = email.get('importance', 'normal')
            if importance == 'low':
                skipped_count += 1
                continue
                
            filtered_emails.append(email)
        
        # Log filtering results
        if len(emails) > 0:
            logger.info(f"Email filtering: kept {len(filtered_emails)}/{len(emails)} emails ({skipped_count} skipped)")
            
        return filtered_emails

    def get_folder_id(self, folder_name: str) -> str:
        """Get folder ID by name with caching.
        
        Args:
            folder_name: Name of the folder to find
            
        Returns:
            Folder ID
            
        Raises:
            EmailFetchError: If folder cannot be found
        """
        # Check cache first
        cache_key = f"folder:{folder_name}:{self.target_mailbox}"
        with self._cache_lock:
            # Use cached value if available and not expired
            if cache_key in self._mailbox_cache and datetime.now() < self._cache_expiry.get(cache_key, datetime.min):
                return self._mailbox_cache[cache_key]
                
        # Fetch folders
        endpoint = f"{GRAPH_API_ENDPOINT}/users/{self.target_mailbox}/mailFolders"
        response = self._execute_request("GET", endpoint)
        folders = response.json().get('value', [])
        
        # Find folder by name (case-insensitive)
        folder_name_lower = folder_name.lower()
        for folder in folders:
            if folder.get('displayName', '').lower() == folder_name_lower:
                folder_id = folder['id']
                
                # Cache result for 1 hour
                with self._cache_lock:
                    self._mailbox_cache[cache_key] = folder_id
                    self._cache_expiry[cache_key] = datetime.now() + timedelta(hours=1)
                    
                return folder_id
                
        # If folder not found, try to create it
        return self._create_folder(folder_name)
        
    def _create_folder(self, folder_name: str) -> str:
        """Create a new mail folder.
        
        Args:
            folder_name: Name for the new folder
            
        Returns:
            ID of the created folder
            
        Raises:
            EmailFetchError: If folder creation fails
        """
        endpoint = f"{GRAPH_API_ENDPOINT}/users/{self.target_mailbox}/mailFolders"
        data = {
            "displayName": folder_name
        }
        
        try:
            response = self._execute_request("POST", endpoint, json_data=data)
            folder_id = response.json().get('id')
            
            if not folder_id:
                raise EmailFetchError(f"Failed to create folder '{folder_name}'")
                
            # Cache the new folder ID
            cache_key = f"folder:{folder_name}:{self.target_mailbox}"
            with self._cache_lock:
                self._mailbox_cache[cache_key] = folder_id
                self._cache_expiry[cache_key] = datetime.now() + timedelta(hours=1)
                
            logger.info(f"Created mail folder '{folder_name}' with ID: {folder_id}")
            return folder_id
            
        except Exception as e:
            logger.error(f"Failed to create folder '{folder_name}': {str(e)}")
            raise EmailFetchError(f"Failed to create folder: {str(e)}")
            
    def move_email_to_folder(self, email_id: str, folder_name: str) -> bool:
        """Move an email to a specified folder.
        
        Args:
            email_id: ID of the email to move
            folder_name: Destination folder name
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Get folder ID
            folder_id = self.get_folder_id(folder_name)
            
            # Move the email
            endpoint = f"{GRAPH_API_ENDPOINT}/users/{self.target_mailbox}/messages/{email_id}/move"
            data = {
                "destinationId": folder_id
            }
            
            response = self._execute_request("POST", endpoint, json_data=data)
            
            # Log success
            new_id = response.json().get('id')
            if new_id:
                logger.info(f"Moved email {email_id} to folder '{folder_name}'")
                return True
                
            return False
            
        except Exception as e:
            logger.error(f"Failed to move email {email_id} to folder '{folder_name}': {str(e)}")
            return False

    def mark_as_read(self, email_id: str) -> bool:
        """Mark an email as read.
        
        Args:
            email_id: ID of the email to mark as read
            
        Returns:
            True if successful, False otherwise
        """
        try:
            endpoint = f"{GRAPH_API_ENDPOINT}/users/{self.target_mailbox}/messages/{email_id}"
            data = {
                "isRead": True
            }
            
            self._execute_request("PATCH", endpoint, json_data=data)
            logger.info(f"Marked email {email_id} as read")
            return True
            
        except Exception as e:
            logger.error(f"Failed to mark email {email_id} as read: {str(e)}")
            return False

    def get_email_details(self, email_id: str) -> Dict:
        """Get detailed information about an email.
        
        Args:
            email_id: Email identifier
            
        Returns:
            Dictionary with email details
            
        Raises:
            EmailFetchError: If fetching email details fails
        """
        try:
            endpoint = f"{GRAPH_API_ENDPOINT}/users/{self.target_mailbox}/messages/{email_id}"
            params = {
                "$select": "id,subject,receivedDateTime,body,from,toRecipients,ccRecipients"
            }
            
            response = self._execute_request("GET", endpoint, params=params)
            return response.json()
            
        except Exception as e:
            logger.error(f"Failed to get email details for {email_id}: {str(e)}")
            raise EmailFetchError(f"Failed to get email details: {str(e)}")