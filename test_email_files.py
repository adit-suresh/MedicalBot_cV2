import os
import logging
from src.email_handler.outlook_client import OutlookClient
from src.email_handler.attachment_handler import AttachmentHandler

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_email_attachments():
    """Check what files we're getting from each email."""
    # Initialize clients
    outlook = OutlookClient()
    attachment_handler = AttachmentHandler()
    
    # Get recent emails
    emails = outlook.fetch_emails()
    logger.info(f"\nFound {len(emails)} emails")
    
    for email in emails:
        try:
            logger.info(f"\n{'='*50}")
            logger.info(f"Email ID: {email['id']}")
            logger.info(f"Subject: {email.get('subject', 'No subject')}")
            
            # Get attachments
            attachments = outlook.get_attachments(email['id'])
            logger.info(f"\nAttachments found: {len(attachments)}")
            
            # Log each attachment
            for idx, attachment in enumerate(attachments, 1):
                name = attachment.get('name', 'Unknown')
                logger.info(f"\nAttachment {idx}:")
                logger.info(f"Name: {name}")
                
                # Determine file type
                file_type = 'unknown'
                if name.lower().endswith(('.xlsx', '.xls')):
                    file_type = 'excel'
                elif 'passport' in name.lower():
                    file_type = 'passport'
                elif 'emirates' in name.lower() or 'eid' in name.lower():
                    file_type = 'emirates_id'
                elif 'visa' in name.lower():
                    file_type = 'visa'
                    
                logger.info(f"Type: {file_type}")
            
            # Process and save attachments
            saved_files = attachment_handler.process_attachments(attachments, email['id'])
            logger.info(f"\nSaved files: {len(saved_files)}")
            for path in saved_files:
                logger.info(f"Saved: {path}")
                
            logger.info("\nRequired files check:")
            has_excel = any(path.lower().endswith(('.xlsx', '.xls')) for path in saved_files)
            has_passport = any('passport' in path.lower() for path in saved_files)
            has_eid = any(('emirates' in path.lower() or 'eid' in path.lower()) for path in saved_files)
            has_visa = any('visa' in path.lower() for path in saved_files)
            
            logger.info(f"Has Excel: {'✓' if has_excel else '✗'}")
            logger.info(f"Has Passport: {'✓' if has_passport else '✗'}")
            logger.info(f"Has Emirates ID: {'✓' if has_eid else '✗'}")
            logger.info(f"Has Visa: {'✓' if has_visa else '✗'}")
            
        except Exception as e:
            logger.error(f"Error processing email {email['id']}: {str(e)}")
            continue

if __name__ == "__main__":
    check_email_attachments()