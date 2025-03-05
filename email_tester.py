from src.utils.email_sender import EmailSender

sender = EmailSender()
result = sender.send_email(
    subject="Test Email",
    body="This is a test email to verify the functionality.",
    attachment_path=None
)

print(f"Email sent successfully: {result}")