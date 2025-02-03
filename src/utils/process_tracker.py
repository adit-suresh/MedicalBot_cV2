import logging
from datetime import datetime
from typing import Dict, List, Optional
import json
import sqlite3
from enum import Enum

class ProcessStatus(Enum):
    STARTED = "started"
    EMAIL_RECEIVED = "email_received"
    DOCUMENTS_DOWNLOADED = "documents_downloaded"
    OCR_COMPLETED = "ocr_completed"
    DATA_VALIDATED = "data_validated"
    SUBMISSION_STARTED = "submission_started"
    SUBMISSION_COMPLETED = "submission_completed"
    FAILED = "failed"
    ERROR = "error"

class ProcessTracker:
    def __init__(self, db_path: str = "data/process_tracking.db"):
        self.db_path = db_path
        self.setup_database()
        self.logger = logging.getLogger(__name__)

    def setup_database(self):
        """Initialize tracking database."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Process tracking table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS process_tracking (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client_reference TEXT UNIQUE,
                    current_status TEXT,
                    start_time TIMESTAMP,
                    last_update TIMESTAMP,
                    completion_time TIMESTAMP,
                    missing_documents TEXT,
                    errors TEXT,
                    details TEXT
                )
            """)
            
            # Status history table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS status_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    process_id INTEGER,
                    status TEXT,
                    timestamp TIMESTAMP,
                    details TEXT,
                    FOREIGN KEY (process_id) REFERENCES process_tracking(id)
                )
            """)
            
            conn.commit()

    def start_process(self, client_reference: str) -> int:
        """Start tracking a new process."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                now = datetime.now()
                cursor.execute("""
                    INSERT INTO process_tracking 
                    (client_reference, current_status, start_time, last_update)
                    VALUES (?, ?, ?, ?)
                """, (client_reference, ProcessStatus.STARTED.value, now, now))
                
                process_id = cursor.lastrowid
                
                # Add to history
                cursor.execute("""
                    INSERT INTO status_history (process_id, status, timestamp)
                    VALUES (?, ?, ?)
                """, (process_id, ProcessStatus.STARTED.value, now))
                
                conn.commit()
                
                self.logger.info(f"Started tracking process for {client_reference}")
                return process_id

        except Exception as e:
            self.logger.error(f"Error starting process tracking: {str(e)}")
            raise

    def update_status(self, process_id: int, status: ProcessStatus, 
                     details: Optional[Dict] = None) -> None:
        """Update process status and add to history."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                now = datetime.now()
                
                # Update main tracking
                cursor.execute("""
                    UPDATE process_tracking 
                    SET current_status = ?, last_update = ?, details = ?
                    WHERE id = ?
                """, (status.value, now, 
                     json.dumps(details) if details else None,
                     process_id))
                
                # Add to history
                cursor.execute("""
                    INSERT INTO status_history (process_id, status, timestamp, details)
                    VALUES (?, ?, ?, ?)
                """, (process_id, status.value, now,
                     json.dumps(details) if details else None))
                
                conn.commit()
                
                self.logger.info(
                    f"Updated process {process_id} status to {status.value}"
                )

        except Exception as e:
            self.logger.error(f"Error updating process status: {str(e)}")
            raise

    def log_error(self, process_id: int, error: str, 
                 error_details: Optional[Dict] = None) -> None:
        """Log an error for the process."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                error_data = {
                    "error": error,
                    "details": error_details,
                    "timestamp": datetime.now().isoformat()
                }
                
                # Get current errors
                cursor.execute(
                    "SELECT errors FROM process_tracking WHERE id = ?", 
                    (process_id,)
                )
                result = cursor.fetchone()
                current_errors = json.loads(result[0]) if result[0] else []
                
                # Append new error
                current_errors.append(error_data)
                
                # Update database
                cursor.execute("""
                    UPDATE process_tracking 
                    SET errors = ?, current_status = ?
                    WHERE id = ?
                """, (json.dumps(current_errors), ProcessStatus.ERROR.value, process_id))
                
                conn.commit()
                
                self.logger.error(f"Logged error for process {process_id}: {error}")

        except Exception as e:
            self.logger.error(f"Error logging process error: {str(e)}")
            raise

    def get_process_status(self, process_id: int) -> Dict:
        """Get current status and details of a process."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Get current status
                cursor.execute("""
                    SELECT current_status, start_time, last_update, 
                           completion_time, missing_documents, errors, details
                    FROM process_tracking 
                    WHERE id = ?
                """, (process_id,))
                
                result = cursor.fetchone()
                if not result:
                    return {}
                    
                # Get status history
                cursor.execute("""
                    SELECT status, timestamp, details
                    FROM status_history
                    WHERE process_id = ?
                    ORDER BY timestamp DESC
                """, (process_id,))
                
                history = cursor.fetchall()
                
                return {
                    "current_status": result[0],
                    "start_time": result[1],
                    "last_update": result[2],
                    "completion_time": result[3],
                    "missing_documents": json.loads(result[4]) if result[4] else [],
                    "errors": json.loads(result[5]) if result[5] else [],
                    "details": json.loads(result[6]) if result[6] else {},
                    "history": [{
                        "status": h[0],
                        "timestamp": h[1],
                        "details": json.loads(h[2]) if h[2] else {}
                    } for h in history]
                }

        except Exception as e:
            self.logger.error(f"Error getting process status: {str(e)}")
            raise