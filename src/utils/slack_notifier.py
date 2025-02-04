import os
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import logging
from datetime import datetime
from typing import Optional, Dict

logger = logging.getLogger(__name__)

class SlackNotifier:
    def __init__(self):
        self.client = WebClient(token=os.getenv('SLACK_BOT_TOKEN'))
        self.default_channel = os.getenv('SLACK_DEFAULT_CHANNEL', '#process-notifications')

    def send_notification(self, 
                         message: str, 
                         channel: Optional[str] = None,
                         severity: str = "info",
                         process_id: Optional[str] = None,
                         additional_info: Optional[Dict] = None) -> bool:
        """
        Send notification to Slack.
        
        Args:
            message: Main message text
            channel: Override default channel
            severity: info/warning/error
            process_id: Related process ID
            additional_info: Any additional data to include
        """
        try:
            # Color coding based on severity
            colors = {
                "info": "#36a64f",      # Green
                "warning": "#ffa500",   # Orange
                "error": "#ff0000",     # Red
                "success": "#2eb886"    # Emerald
            }

            # Build the message blocks
            blocks = []
            
            # Header with timestamp and process ID
            header_text = f"*{severity.upper()}* | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            if process_id:
                header_text += f" | Process ID: {process_id}"
            
            blocks.append({
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": header_text
                }
            })

            # Main message
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": message
                }
            })

            # Additional info if provided
            if additional_info:
                info_text = "\n".join([f"*{k}:* {v}" for k, v in additional_info.items()])
                blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"Additional Information:\n{info_text}"
                    }
                })

            # Send the message
            self.client.chat_postMessage(
                channel=channel or self.default_channel,
                blocks=blocks,
                text=message,  # Fallback text
                attachments=[{
                    "color": colors.get(severity, colors["info"])
                }]
            )
            return True

        except SlackApiError as e:
            logger.error(f"Failed to send Slack notification: {str(e)}")
            return False

    def send_process_update(self, 
                          process_id: str,
                          stage: str,
                          status: str,
                          details: Optional[Dict] = None) -> None:
        """Send process stage update notification."""
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
        status = "Success" if success else "Failed"
        message = f"*Process Completed*\nFinal Status: {status}"
        
        self.send_notification(
            message=message,
            severity="success" if success else "error",
            process_id=process_id,
            additional_info=summary
        )