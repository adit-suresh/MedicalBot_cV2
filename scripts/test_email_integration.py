import sys
import os
import logging
from datetime import datetime, timedelta

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.email_handler.outlook_client import OutlookClient
from src.email_handler.attachment_handler import AttachmentHandler
from src.utils.logger import setup_logger
from src.utils.exceptions import AuthenticationError, EmailFetchError, AttachmentError
from src.utils.cleanup import cleanup_files
from config.settings import USER_EMAIL

def test_email_integration():
    # Set up logging with more detail
    logger = setup_logger('email_integration_test')
    logger.setLevel(logging.DEBUG)  # Set to DEBUG for more detailed output
    
    logger.info("Starting email integration test...")
    logger.info(f"Using service account: {USER_EMAIL}")

    try:
        # Clean up of old files
        logger.info("Cleaning up old files...")
        cleanup_files(retention_days=7)
        
        # Initialize the clients
        logger.info("Initializing Outlook client...")
        outlook_client = OutlookClient()
        attachment_handler = AttachmentHandler()

        # Test authentication
        logger.info("Testing authentication...")
        if not outlook_client.access_token:
            raise AuthenticationError("Failed to obtain access token")
        logger.info("Authentication successful!")
        
        # Print the first part of the token for verification (safely)
        token_preview = outlook_client.access_token[:20] + "..." if outlook_client.access_token else "None"
        logger.info(f"Access token obtained (preview): {token_preview}")

        # Try to fetch emails from the last hour first
        logger.info("Attempting to fetch emails from the last hour...")
        last_hour = datetime.now() - timedelta(hours=1)
        
        try:
            recent_emails = outlook_client.fetch_emails(last_check_time=last_hour)
            logger.info(f"Found {len(recent_emails)} emails in the last hour")
        except EmailFetchError as e:
            logger.warning(f"Error fetching recent emails: {str(e)}")
            logger.info("Trying without time filter...")
            recent_emails = outlook_client.fetch_emails()
            logger.info(f"Found {len(recent_emails)} emails total")

        if not recent_emails:
            logger.warning("No emails found in the initial fetch.")
            # Try with a longer time range
            logger.info("Trying with 24-hour time range...")
            last_day = datetime.now() - timedelta(days=1)
            recent_emails = outlook_client.fetch_emails(last_check_time=last_day)
            logger.info(f"Found {len(recent_emails)} emails in the last 24 hours")

        # Process each email's attachments
        for email in recent_emails:
            try:
                logger.info(f"\nProcessing email:")
                logger.info(f"  ID: {email['id']}")
                logger.info(f"  Subject: {email.get('subject', 'No subject')}")
                logger.info(f"  Received: {email.get('receivedDateTime', 'Unknown date')}")
                
                # Get attachments
                attachments = outlook_client.get_attachments(email['id'])
                logger.info(f"Found {len(attachments)} attachments")

                if attachments:
                    # Log attachment details
                    for i, att in enumerate(attachments, 1):
                        logger.info(f"  Attachment {i}:")
                        logger.info(f"    Name: {att.get('name', 'Unknown')}")
                        logger.info(f"    Size: {att.get('size', 'Unknown')} bytes")
                        logger.info(f"    Type: {att.get('contentType', 'Unknown')}")

                    # Process attachments
                    saved_paths = attachment_handler.process_attachments(attachments, email['id'])
                    logger.info(f"Successfully saved {len(saved_paths)} attachments")
                    
                    # Verify saved files
                    for path in saved_paths:
                        logger.info(f"Saved file: {path}")
                        if os.path.exists(path):
                            size = os.path.getsize(path)
                            logger.info(f"  File size: {size} bytes")
                            if size == 0:
                                logger.warning(f"  Warning: File is empty")
                        else:
                            logger.error(f"  Error: File does not exist")

            except EmailFetchError as e:
                logger.error(f"Error fetching attachments: {str(e)}")
                continue
            except AttachmentError as e:
                logger.error(f"Error processing attachments: {str(e)}")
                continue
            except Exception as e:
                logger.error(f"Unexpected error: {str(e)}")
                continue

        logger.info("\nEmail integration test completed!")
        return True

    except AuthenticationError as e:
        logger.error(f"Authentication failed: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during integration test: {str(e)}")
        return False

if __name__ == "__main__":
    success = test_email_integration()
    sys.exit(0 if success else 1)