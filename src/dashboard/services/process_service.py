import logging
from datetime import datetime
from typing import Dict, Any, Optional
import json

from src.utils.dependency_container import container
from src.utils.process_control import ProcessControl
from src.utils.process_control_interface import ProcessStatus, ProcessStage
from src.utils.error_handler import ErrorHandler

logger = logging.getLogger(__name__)

class ProcessService:
    """Service for managing processes through the dashboard."""
    
    def __init__(self):
        """Initialize process service with dependencies."""
        try:
            self.process_control = container.resolve(ProcessControl)
            self.error_handler = container.resolve(ErrorHandler)
        except Exception as e:
            logger.error(f"Error initializing ProcessService: {str(e)}")
            # Fallback initialization
            self.process_control = ProcessControl()
            self.error_handler = ErrorHandler()
    
    def resume_process(self, process_id: str, user_id: str, notes: str = '') -> Dict[str, Any]:
        """Resume a paused or error process."""
        try:
            # Get process details
            process = self.process_control.get_process(process_id)
            if not process:
                return {
                    'success': False,
                    'error': 'Process not found'
                }
            
            # Check if process can be resumed
            resumable_statuses = [
                ProcessStatus.PAUSED,
                ProcessStatus.ERROR,
                ProcessStatus.MANUAL_REVIEW
            ]
            
            if process.get('status') not in resumable_statuses:
                return {
                    'success': False,
                    'error': f"Process cannot be resumed from status: {process.get('status')}"
                }
            
            # Log the action
            event = {
                'process_id': process_id,
                'event_type': 'process_resumed',
                'user_id': user_id,
                'timestamp': datetime.now().isoformat(),
                'details': {
                    'previous_status': process.get('status'),
                    'notes': notes
                }
            }
            self.process_control.add_process_event(process_id, event)
            
            # Resume process
            result = self.process_control.resume_process(
                process_id=process_id,
                user_id=user_id
            )
            
            if result:
                return {
                    'success': True
                }
            else:
                return {
                    'success': False,
                    'error': 'Failed to update priority'
                }
            
        except Exception as e:
            logger.error(f"Error updating priority for process {process_id}: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def resolve_manual_review(self, process_id: str, user_id: str,
                             decision: str, notes: str = '') -> Dict[str, Any]:
        """Resolve a process in manual review status."""
        try:
            # Validate decision
            valid_decisions = ['approve', 'reject', 'modify']
            if decision not in valid_decisions:
                return {
                    'success': False,
                    'error': f"Invalid decision. Must be one of: {', '.join(valid_decisions)}"
                }
            
            # Get process details
            process = self.process_control.get_process(process_id)
            if not process:
                return {
                    'success': False,
                    'error': 'Process not found'
                }
            
            # Check if process is in manual review
            if process.get('status') != ProcessStatus.MANUAL_REVIEW:
                return {
                    'success': False,
                    'error': f"Process is not in manual review status. Current status: {process.get('status')}"
                }
            
            # Log the action
            event = {
                'process_id': process_id,
                'event_type': 'manual_review_resolved',
                'user_id': user_id,
                'timestamp': datetime.now().isoformat(),
                'details': {
                    'decision': decision,
                    'notes': notes
                }
            }
            self.process_control.add_process_event(process_id, event)
            
            # Handle different decisions
            if decision == 'approve':
                # Approve the process and continue
                result = self.process_control.resume_process(
                    process_id=process_id,
                    user_id=user_id
                )
            elif decision == 'reject':
                # Reject and mark as failed
                result = self.process_control.update_process(
                    process_id=process_id,
                    updates={
                        'status': ProcessStatus.FAILED,
                        'updated_by': user_id,
                        'last_updated': datetime.now().isoformat(),
                        'failure_reason': f"Rejected during manual review: {notes}"
                    }
                )
            elif decision == 'modify':
                # Mark for modification and put in pending status
                result = self.process_control.update_process(
                    process_id=process_id,
                    updates={
                        'status': ProcessStatus.PENDING,
                        'updated_by': user_id,
                        'last_updated': datetime.now().isoformat(),
                        'modification_notes': notes
                    }
                )
            
            if result:
                return {
                    'success': True,
                    'decision': decision
                }
            else:
                return {
                    'success': False,
                    'error': f"Failed to apply {decision} decision"
                }
            
        except Exception as e:
            logger.error(f"Error resolving manual review for process {process_id}: {str(e)}")
            return {
                'success': False,
                'error': str(e)
                }
            
        except Exception as e:
            logger.error(f"Error resuming process {process_id}: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def cancel_process(self, process_id: str, user_id: str, reason: str = '') -> Dict[str, Any]:
        """Cancel a process."""
        try:
            # Get process details
            process = self.process_control.get_process(process_id)
            if not process:
                return {
                    'success': False,
                    'error': 'Process not found'
                }
            
            # Check if process can be cancelled
            terminal_statuses = [
                ProcessStatus.COMPLETED,
                ProcessStatus.CANCELLED,
                ProcessStatus.FAILED
            ]
            
            if process.get('status') in terminal_statuses:
                return {
                    'success': False,
                    'error': f"Process cannot be cancelled from status: {process.get('status')}"
                }
            
            # Log the action
            event = {
                'process_id': process_id,
                'event_type': 'process_cancelled',
                'user_id': user_id,
                'timestamp': datetime.now().isoformat(),
                'details': {
                    'previous_status': process.get('status'),
                    'reason': reason
                }
            }
            self.process_control.add_process_event(process_id, event)
            
            # Cancel process
            result = self.process_control.cancel_process(
                process_id=process_id,
                user_id=user_id,
                reason=reason
            )
            
            if result:
                return {
                    'success': True
                }
            else:
                return {
                    'success': False,
                    'error': 'Failed to cancel process'
                }
            
        except Exception as e:
            logger.error(f"Error cancelling process {process_id}: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def retry_process(self, process_id: str, user_id: str, 
                      from_stage: Optional[str] = None) -> Dict[str, Any]:
        """Retry a failed process, optionally from a specific stage."""
        try:
            # Get process details
            process = self.process_control.get_process(process_id)
            if not process:
                return {
                    'success': False,
                    'error': 'Process not found'
                }
            
            # Check if process can be retried
            retryable_statuses = [
                ProcessStatus.FAILED,
                ProcessStatus.ERROR
            ]
            
            if process.get('status') not in retryable_statuses:
                return {
                    'success': False,
                    'error': f"Process cannot be retried from status: {process.get('status')}"
                }
            
            # Log the action
            event = {
                'process_id': process_id,
                'event_type': 'process_retry',
                'user_id': user_id,
                'timestamp': datetime.now().isoformat(),
                'details': {
                    'previous_status': process.get('status'),
                    'from_stage': from_stage
                }
            }
            self.process_control.add_process_event(process_id, event)
            
            # Retry process
            result = self.process_control.retry_process(
                process_id=process_id,
                user_id=user_id,
                from_stage=from_stage
            )
            
            if result:
                return {
                    'success': True
                }
            else:
                return {
                    'success': False,
                    'error': 'Failed to retry process'
                }
            
        except Exception as e:
            logger.error(f"Error retrying process {process_id}: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def add_process_note(self, process_id: str, user_id: str, note: str) -> Dict[str, Any]:
        """Add a note to a process."""
        try:
            # Validate inputs
            if not process_id or not user_id or not note:
                return {
                    'success': False,
                    'error': 'Process ID, user ID, and note are required'
                }
            
            # Get process details
            process = self.process_control.get_process(process_id)
            if not process:
                return {
                    'success': False,
                    'error': 'Process not found'
                }
            
            # Create note event
            event = {
                'process_id': process_id,
                'event_type': 'note_added',
                'user_id': user_id,
                'timestamp': datetime.now().isoformat(),
                'details': {
                    'note': note
                }
            }
            
            # Add note event
            result = self.process_control.add_process_event(process_id, event)
            
            if result:
                return {
                    'success': True,
                    'note_id': result.get('event_id')
                }
            else:
                return {
                    'success': False,
                    'error': 'Failed to add note'
                }
            
        except Exception as e:
            logger.error(f"Error adding note to process {process_id}: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def update_process_priority(self, process_id: str, user_id: str, 
                               priority: int) -> Dict[str, Any]:
        """Update the priority of a process."""
        try:
            # Validate priority
            if priority not in [1, 2, 3, 4, 5]:
                return {
                    'success': False,
                    'error': 'Priority must be between 1 and 5'
                }
            
            # Get process details
            process = self.process_control.get_process(process_id)
            if not process:
                return {
                    'success': False,
                    'error': 'Process not found'
                }
            
            # Log the action
            event = {
                'process_id': process_id,
                'event_type': 'priority_updated',
                'user_id': user_id,
                'timestamp': datetime.now().isoformat(),
                'details': {
                    'previous_priority': process.get('priority', 3),
                    'new_priority': priority
                }
            }
            self.process_control.add_process_event(process_id, event)
            
            # Update priority
            result = self.process_control.update_process(
                process_id=process_id,
                updates={
                    'priority': priority,
                    'updated_by': user_id,
                    'last_updated': datetime.now().isoformat()
                }
            )
            
            if result:
                return {
                    'success': True
                }
            else:
                return {
                    'success': False,
                    'error': 'Failed to update priority'
                }
        finally:
            pass
                