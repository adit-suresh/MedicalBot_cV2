class AuthenticationError(Exception):
    """Raised when authentication with Microsoft Graph API fails."""
    pass

class EmailFetchError(Exception):
    """Raised when there's an error fetching emails."""
    pass

class AttachmentError(Exception):
    """Raised when there's an error processing attachments."""
    pass