from abc import ABC, abstractmethod
from typing import Dict, List, Optional
from enum import Enum

class ProcessStatus(Enum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    AWAITING_INPUT = "awaiting_input"

class ProcessStage(Enum):
    EMAIL_PROCESSING = "email_processing"
    DOCUMENT_EXTRACTION = "document_extraction"
    DATA_VALIDATION = "data_validation"
    PORTAL_SUBMISSION = "portal_submission"
    COMPLETION = "completion"

class IProcessControl(ABC):
    """Interface for process control operations."""

    @abstractmethod
    def start_process(self, process_id: str) -> None:
        """Initialize a new process."""
        pass

    @abstractmethod
    def update_stage(self, process_id: str, stage: ProcessStage, 
                    status: ProcessStatus, stage_data: Optional[Dict] = None) -> None:
        """Update process stage."""
        pass

    @abstractmethod
    def pause_process(self, process_id: str, reason: str,
                     manual_input_type: Optional[str] = None,
                     required_data: Optional[Dict] = None) -> None:
        """Pause process for manual intervention."""
        pass

    @abstractmethod
    def resume_process(self, process_id: str, 
                      manual_input: Optional[Dict] = None) -> None:
        """Resume process after manual intervention."""
        pass

    @abstractmethod
    def get_process_status(self, process_id: str) -> Dict:
        """Get current process status and details."""
        pass

    @abstractmethod
    def get_processes_needing_attention(self) -> List[Dict]:
        """Get all processes that need manual intervention."""
        pass

    @abstractmethod
    def get_all_processes(self) -> List[Dict]:
        """Get all processes with their current status."""
        pass

    @abstractmethod
    def get_stats(self) -> Dict:
        """Get process statistics."""
        pass

    @abstractmethod
    def get_process_timeline(self, process_id: str) -> List[Dict]:
        """Get timeline of process stages."""
        pass