import logging
from typing import Dict, Optional
from datetime import datetime

from src.utils.slack_notifier import SlackNotifier
from src.utils.process_control import ProcessControl, ProcessStatus, ProcessStage
from src.utils.portal_checker import PortalChecker, PortalStatus

logger = logging.getLogger(__name__)

class ProcessManager:
    def __init__(self):
        self.slack = SlackNotifier()
        self.process_control = ProcessControl()
        self.portal_checker = PortalChecker()

    def handle_process(self, process_id: str) -> None:
        """
        Main process handler with error recovery and notifications.
        """
        try:
            self.process_control.start_process(process_id)
            self.slack.send_process_update(
                process_id=process_id,
                stage="Process Started",
                status="Running"
            )

            # Check portal status before starting
            portal_status = self.portal_checker.check_status()
            if portal_status != PortalStatus.UP:
                self._handle_portal_down(process_id)
                return

            # Process stages
            stages = [
                (ProcessStage.EMAIL_PROCESSING, self._process_email),
                (ProcessStage.DOCUMENT_EXTRACTION, self._extract_documents),
                (ProcessStage.DATA_VALIDATION, self._validate_data),
                (ProcessStage.PORTAL_SUBMISSION, self._submit_to_portal)
            ]

            for stage, handler in stages:
                success = self._execute_stage(process_id, stage, handler)
                if not success:
                    return

            # Process completed successfully
            self._complete_process(process_id, success=True)

        except Exception as e:
            logger.error(f"Process failed: {str(e)}")
            self._handle_failure(process_id, str(e))

    def _execute_stage(self, process_id: str, stage: ProcessStage, 
                      handler: callable) -> bool:
        """Execute a single stage with error handling."""
        try:
            self.process_control.update_stage(
                process_id, stage, ProcessStatus.RUNNING
            )
            self.slack.send_process_update(
                process_id=process_id,
                stage=stage.value,
                status="Started"
            )

            # Execute the stage handler
            result = handler(process_id)

            if result.get('success'):
                self.slack.send_process_update(
                    process_id=process_id,
                    stage=stage.value,
                    status="Completed",
                    details=result.get('details')
                )
                return True
            else:
                self._handle_stage_failure(
                    process_id, stage, 
                    result.get('error'), 
                    result.get('requires_input', False)
                )
                return False

        except Exception as e:
            self._handle_stage_failure(process_id, stage, str(e))
            return False

    def _handle_stage_failure(self, process_id: str, stage: ProcessStage, 
                            error: str, requires_input: bool = False) -> None:
        """Handle stage failure."""
        if requires_input:
            self.process_control.pause_process(
                process_id,
                reason=error,
                manual_input_type=stage.value
            )
            self.slack.send_error_alert(
                process_id=process_id,
                error_message=f"Stage {stage.value} requires manual intervention",
                error_details={"error": error},
                requires_attention=True
            )
        else:
            self.process_control.update_stage(
                process_id, stage, ProcessStatus.FAILED,
                {"error": error}
            )
            self.slack.send_error_alert(
                process_id=process_id,
                error_message=f"Stage {stage.value} failed: {error}"
            )

    def _handle_portal_down(self, process_id: str) -> None:
        """Handle portal unavailability."""
        self.process_control.pause_process(
            process_id,
            reason="Insurance portal is unavailable",
            manual_input_type="portal_status"
        )
        self.slack.send_error_alert(
            process_id=process_id,
            error_message="Insurance portal is down",
            error_details=self.portal_checker.get_detailed_status(),
            requires_attention=True
        )

    def _handle_failure(self, process_id: str, error: str) -> None:
        """Handle process failure."""
        self.process_control.update_stage(
            process_id,
            ProcessStage.COMPLETION,
            ProcessStatus.FAILED,
            {"error": error}
        )
        self.slack.send_completion_notification(
            process_id=process_id,
            success=False,
            summary={"error": error}
        )

    def _complete_process(self, process_id: str, success: bool) -> None:
        """Complete the process."""
        self.process_control.update_stage(
            process_id,
            ProcessStage.COMPLETION,
            ProcessStatus.COMPLETED
        )
        self.slack.send_completion_notification(
            process_id=process_id,
            success=True,
            summary=self.process_control.get_process_status(process_id)
        )

    # Stage handler methods would be implemented here
    def _process_email(self, process_id: str) -> Dict:
        """Handle email processing stage."""
        # Implement email processing
        pass

    def _extract_documents(self, process_id: str) -> Dict:
        """Handle document extraction stage."""
        # Implement document extraction
        pass

    def _validate_data(self, process_id: str) -> Dict:
        """Handle data validation stage."""
        # Implement data validation
        pass

    def _submit_to_portal(self, process_id: str) -> Dict:
        """Handle portal submission stage."""
        # Implement portal submission
        pass