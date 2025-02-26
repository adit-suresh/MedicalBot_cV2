# test_email.py
import os
from dotenv import load_dotenv
from src.utils.email_sender import EmailSender

# Load environment variables
load_dotenv()

# Test email sending
sender = EmailSender()
result = sender.send_email(
    subject="Test Email from Medical Bot",
    body="This is a test email to verify the email functionality is working.",
    attachment_path=None  # Optional - specify a file path to test attachment
)

print(f"Email sent: {result}")