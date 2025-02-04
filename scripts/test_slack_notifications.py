import os
import sys
from datetime import datetime
from dotenv import load_dotenv

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from src.utils.slack_notifier import SlackNotifier

def test_slack_notifications():
    """Test various types of Slack notifications."""
    load_dotenv()
    
    # Initialize Slack notifier
    slack = SlackNotifier()
    
    # Generate a test process ID
    test_process_id = f"TEST_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    print("\nTesting Slack notifications...")
    
    # Test 1: Basic notification
    print("1. Sending basic info notification...")
    slack.send_notification(
        message="🧪 Test notification from Medical Bot",
        severity="info"
    )
    
    # Test 2: Process update
    print("2. Sending process update notification...")
    slack.send_process_update(
        process_id=test_process_id,
        stage="Document Processing",
        status="In Progress",
        details={
            "documents_processed": 3,
            "current_document": "passport.pdf"
        }
    )
    
    # Test 3: Error alert
    print("3. Sending error alert...")
    slack.send_error_alert(
        process_id=test_process_id,
        error_message="Failed to process Emirates ID",
        error_details={
            "error_type": "OCR_FAILED",
            "file": "emirates_id.jpg"
        },
        requires_attention=True
    )
    
    # Test 4: Completion notification
    print("4. Sending completion notification...")
    slack.send_completion_notification(
        process_id=test_process_id,
        success=True,
        summary={
            "documents_processed": 4,
            "processing_time": "2m 30s",
            "extracted_fields": ["name", "passport_number", "emirates_id"]
        }
    )
    
    print("\nAll test notifications sent! Please check your Slack channel.")

if __name__ == "__main__":
    test_slack_notifications()