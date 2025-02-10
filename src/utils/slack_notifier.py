import os
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import logging
from datetime import datetime
from typing import Optional, Dict

logger = logging.getLogger(__name__)

class SlackNotifier:
    def __init__(self):
        self.token = os.getenv('SLACK_BOT_TOKEN')
        self.default_channel = os.getenv('SLACK_DEFAULT_CHANNEL', '#process-notifications')
        self.enabled = bool(self.token)
        
        if not self.enabled:
            logger.warning("Slack notifications disabled: No SLACK_BOT_TOKEN found in environment variables")
        else:
            self.client = WebClient(token=self.token)

    def send_notification(self, 
                         message: str, 
                         channel: Optional[str] = None,
                         severity: str = "info",
                         process_id: Optional[str] = None,
                         additional_info: Optional[Dict] = None) -> bool:
        """Send notification to Slack."""
        if not self.enabled:
            logger.info(f"Would send Slack notification: {message}")
            return False

        try:
            response = self.client.chat_postMessage(
                channel=channel or self.default_channel,
                text=message
            )
            logger.info(f"Slack notification sent: {response['ts']}")
        except SlackApiError as e:
            logger.error(f"Failed to send Slack notification: {str(e)}")
            return False

    def send_process_update(self, 
                          process_id: str,
                          stage: str,
                          status: str,
                          details: Optional[Dict] = None) -> None:
        """Send process stage update notification."""
        if not self.enabled:
            logger.info(
                f"Would send process update: Process {process_id} - "
                f"Stage: {stage}, Status: {status}"
            )
            return

        message = f"*Process Stage Update*\nStage: {stage}\nStatus: {status}"
        severity = "error" if "fail" in status.lower() else "info"
        
        self.send_notification(
            message=message,
            severity=severity,
            process_id=process_id,
            additional_info=details
        )

    def send_error_alert(self,
                        process_id: str,
                        error_message: str,
                        error_details: Optional[Dict] = None,
                        requires_attention: bool = False) -> None:
        """Send error alert notification."""
        if not self.enabled:
            logger.info(
                f"Would send error alert: Process {process_id} - "
                f"Error: {error_message}"
            )
            return

        message = f"*Error Alert*\n{error_message}"
        if requires_attention:
            message += "\n<!here> *Manual intervention required*"
            
        self.send_notification(
            message=message,
            severity="error",
            process_id=process_id,
            additional_info=error_details
        )

    def send_completion_notification(self,
                                  process_id: str,
                                  success: bool,
                                  summary: Dict) -> None:
        """Send process completion notification."""
        if not self.enabled:
            logger.info(
                f"Would send completion notification: Process {process_id} - "
                f"Success: {success}"
            )
            return

        status = "Success" if success else "Failed"
        message = f"*Process Completed*\nFinal Status: {status}"
        
        self.send_notification(
            message=message,
            severity="success" if success else "error",
            process_id=process_id,
            additional_info=summary
        )