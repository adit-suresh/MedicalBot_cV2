import sys
import os
import logging
from datetime import datetime, timedelta

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.email_handler.outlook_client import OutlookClient
from src.email_handler.attachment_handler import AttachmentHandler
from src.utils.logger import setup_logger
from config.settings import ATTACHMENT_TYPES

def diagnose_attachments():
    logger = setup_logger('attachment_diagnosis')
    logger.setLevel(logging.DEBUG)

    try:
        # Initialize clients
        outlook_client = OutlookClient()
        attachment_handler = AttachmentHandler()

        # Fetch last 24 hours of emails
        last_24h = datetime.now() - timedelta(days=1)
        emails = outlook_client.fetch_emails(last_check_time=last_24h)

        logger.info(f"\nFound {len(emails)} emails in the last 24 hours")

        for email in emails:
            logger.info(f"\n{'='*50}")
            logger.info(f"Email ID: {email['id']}")
            logger.info(f"Subject: {email.get('subject', 'No subject')}")
            logger.info(f"Received: {email.get('receivedDateTime', 'Unknown date')}")

            # Get all attachments for this email
            attachments = outlook_client.get_attachments(email['id'])
            logger.info(f"\nTotal attachments found: {len(attachments)}")

            # Log details of each attachment
            for idx, attachment in enumerate(attachments, 1):
                logger.info(f"\nAttachment {idx}:")
                logger.info(f"Name: {attachment.get('name', 'Unknown')}")
                logger.info(f"Type: {attachment.get('contentType', 'Unknown')}")
                logger.info(f"Size: {attachment.get('size', 'Unknown')} bytes")

                # Check if it passes our filter
                is_valid = attachment_handler.is_valid_attachment(attachment)
                logger.info(f"Passes our filters: {is_valid}")

                # Show why it might be filtered out
                file_ext = os.path.splitext(attachment.get('name', ''))[1].lower()
                if not is_valid:
                    if file_ext not in ATTACHMENT_TYPES:
                        logger.info(f"Filtered out: Extension {file_ext} not in {ATTACHMENT_TYPES}")
                    else:
                        logger.info("Filtered out: Failed filename pattern match")

            # Try to process attachments
            try:
                saved_paths = attachment_handler.process_attachments(attachments, email['id'])
                logger.info(f"\nSuccessfully saved {len(saved_paths)} attachments:")
                for path in saved_paths:
                    logger.info(f"Saved: {path}")
            except Exception as e:
                logger.error(f"Error processing attachments: {str(e)}")

        logger.info("\nDiagnosis complete!")

    except Exception as e:
        logger.error(f"Diagnosis failed: {str(e)}")

if __name__ == "__main__":
    diagnose_attachments()