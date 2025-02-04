import sqlite3
from typing import Dict, Optional, List
import json
import logging
from datetime import datetime
from enum import Enum

logger = logging.getLogger(__name__)

class ProcessStatus(Enum):
    RUNNING = "running"
    PAUSED = "paused"
    FAILED = "failed"
    COMPLETED = "completed"
    AWAITING_INPUT = "awaiting_input"

class ProcessStage(Enum):
    EMAIL_PROCESSING = "email_processing"
    DOCUMENT_EXTRACTION = "document_extraction"
    DATA_VALIDATION = "data_validation"
    PORTAL_SUBMISSION = "portal_submission"
    COMPLETION = "completion"

class ProcessControl:
    def __init__(self, db_path: str = "data/process_control.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Initialize the database."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Process control table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS process_control (
                    process_id TEXT PRIMARY KEY,
                    status TEXT,
                    current_stage TEXT,
                    stage_data TEXT,
                    error_message TEXT,
                    manual_input_required BOOLEAN,
                    manual_input_type TEXT,
                    manual_input_data TEXT,
                    last_updated TIMESTAMP
                )
            """)
            
            # Process history table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS process_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    process_id TEXT,
                    timestamp TIMESTAMP,
                    stage TEXT,
                    status TEXT,
                    details TEXT
                )
            """)

    def start_process(self, process_id: str) -> None:
        """Initialize a new process."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO process_control (
                    process_id, status, current_stage, last_updated
                ) VALUES (?, ?, ?, ?)
            """, (process_id, ProcessStatus.RUNNING.value,
                 ProcessStage.EMAIL_PROCESSING.value, datetime.now()))
            
            self._add_history(process_id, "Process started", ProcessStatus.RUNNING)

    def update_stage(self, 
                    process_id: str,
                    stage: ProcessStage,
                    status: ProcessStatus,
                    stage_data: Optional[Dict] = None) -> None:
        """Update process stage."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE process_control
                SET current_stage = ?, status = ?, stage_data = ?, last_updated = ?
                WHERE process_id = ?
            """, (stage.value, status.value,
                 json.dumps(stage_data) if stage_data else None,
                 datetime.now(), process_id))
            
            self._add_history(process_id, f"Stage updated: {stage.value}", status)

    def pause_process(self, 
                     process_id: str, 
                     reason: str,
                     manual_input_type: Optional[str] = None,
                     required_data: Optional[Dict] = None) -> None:
        """Pause process for manual intervention."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE process_control
                SET status = ?, manual_input_required = TRUE,
                    manual_input_type = ?, manual_input_data = ?,
                    error_message = ?, last_updated = ?
                WHERE process_id = ?
            """, (ProcessStatus.AWAITING_INPUT.value, manual_input_type,
                 json.dumps(required_data) if required_data else None,
                 reason, datetime.now(), process_id))
            
            self._add_history(
                process_id,
                f"Process paused: {reason}",
                ProcessStatus.AWAITING_INPUT
            )

    def resume_process(self, 
                      process_id: str,
                      manual_input: Optional[Dict] = None) -> None:
        """Resume process after manual intervention."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Store manual input in history
            if manual_input:
                self._add_history(
                    process_id,
                    "Manual input provided",
                    ProcessStatus.RUNNING,
                    manual_input
                )
            
            # Resume process
            cursor.execute("""
                UPDATE process_control
                SET status = ?, manual_input_required = FALSE,
                    manual_input_type = NULL, manual_input_data = NULL,
                    error_message = NULL, last_updated = ?
                WHERE process_id = ?
            """, (ProcessStatus.RUNNING.value, datetime.now(), process_id))

    def get_process_status(self, process_id: str) -> Dict:
        """Get current process status and details."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT status, current_stage, stage_data,
                       error_message, manual_input_required,
                       manual_input_type, manual_input_data,
                       last_updated
                FROM process_control
                WHERE process_id = ?
            """, (process_id,))
            
            result = cursor.fetchone()
            if not result:
                return {}
                
            return {
                'status': result[0],
                'current_stage': result[1],
                'stage_data': json.loads(result[2]) if result[2] else None,
                'error_message': result[3],
                'manual_input_required': bool(result[4]),
                'manual_input_type': result[5],
                'manual_input_data': json.loads(result[6]) if result[6] else None,
                'last_updated': result[7]
            }

    def _add_history(self,
                    process_id: str,
                    message: str,
                    status: ProcessStatus,
                    details: Optional[Dict] = None) -> None:
        """Add entry to process history."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO process_history (
                    process_id, timestamp, stage, status, details
                ) VALUES (?, ?, ?, ?, ?)
            """, (process_id, datetime.now(),
                 message, status.value,
                 json.dumps(details) if details else None))

    def get_processes_needing_attention(self) -> List[Dict]:
        """Get all processes that need manual intervention."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT process_id, current_stage, error_message,
                       manual_input_type, manual_input_data, last_updated
                FROM process_control
                WHERE manual_input_required = TRUE
                ORDER BY last_updated DESC
            """)
            
            results = cursor.fetchall()
            return [{
                'process_id': row[0],
                'stage': row[1],
                'error': row[2],
                'input_type': row[3],
                'required_data': json.loads(row[4]) if row[4] else None,
                'paused_at': row[5]
            } for row in results]