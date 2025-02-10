from typing import Dict, List, Optional
import json
from datetime import datetime

from src.utils.base_db_handler import BaseDBHandler
from src.utils.process_control_interface import (
    IProcessControl, ProcessStatus, ProcessStage
)

class ProcessControl(BaseDBHandler, IProcessControl):
    """Manages process control and tracking."""

    def _create_tables(self, cursor) -> None:
        """Create necessary database tables."""
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
                last_updated TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
                details TEXT,
                FOREIGN KEY (process_id) REFERENCES process_control(process_id)
            )
        """)

    def start_process(self, process_id: str) -> None:
        """Initialize a new process."""
        data = {
            'process_id': process_id,
            'status': ProcessStatus.RUNNING.value,
            'current_stage': ProcessStage.EMAIL_PROCESSING.value,
            'last_updated': datetime.now().isoformat()
        }
        
        self.insert('process_control', data)
        self._add_history(process_id, "Process started", ProcessStatus.RUNNING)

    def update_stage(self, process_id: str, stage: ProcessStage,
                    status: ProcessStatus, stage_data: Optional[Dict] = None) -> None:
        """Update process stage."""
        update_data = {
            'current_stage': stage.value,
            'status': status.value,
            'stage_data': json.dumps(stage_data) if stage_data else None,
            'last_updated': datetime.now().isoformat()
        }
        
        self.update('process_control', process_id, update_data)
        self._add_history(process_id, f"Stage updated: {stage.value}", status)

    def pause_process(self, process_id: str, reason: str,
                     manual_input_type: Optional[str] = None,
                     required_data: Optional[Dict] = None) -> None:
        """Pause process for manual intervention."""
        update_data = {
            'status': ProcessStatus.AWAITING_INPUT.value,
            'manual_input_required': True,
            'manual_input_type': manual_input_type,
            'manual_input_data': json.dumps(required_data) if required_data else None,
            'error_message': reason,
            'last_updated': datetime.now().isoformat()
        }
        
        self.update('process_control', process_id, update_data)
        self._add_history(
            process_id,
            f"Process paused: {reason}",
            ProcessStatus.AWAITING_INPUT
        )

    def resume_process(self, process_id: str,
                      manual_input: Optional[Dict] = None) -> None:
        """Resume process after manual intervention."""
        if manual_input:
            self._add_history(
                process_id,
                "Manual input provided",
                ProcessStatus.RUNNING,
                manual_input
            )
        
        update_data = {
            'status': ProcessStatus.RUNNING.value,
            'manual_input_required': False,
            'manual_input_type': None,
            'manual_input_data': None,
            'error_message': None,
            'last_updated': datetime.now().isoformat()
        }
        
        self.update('process_control', process_id, update_data)

    def get_process_status(self, process_id: str) -> Dict:
        """Get current process status and details."""
        query = """
            SELECT status, current_stage, stage_data,
                   error_message, manual_input_required,
                   manual_input_type, manual_input_data,
                   last_updated
            FROM process_control
            WHERE process_id = ?
        """
        
        result = self.execute_query(query, (process_id,))
        if not result:
            return {}
            
        row = result[0]
        return {
            'status': row[0],
            'current_stage': row[1],
            'stage_data': json.loads(row[2]) if row[2] else None,
            'error_message': row[3],
            'manual_input_required': bool(row[4]),
            'manual_input_type': row[5],
            'manual_input_data': json.loads(row[6]) if row[6] else None,
            'last_updated': row[7]
        }

    def get_processes_needing_attention(self) -> List[Dict]:
        """Get all processes that need manual intervention."""
        query = """
            SELECT process_id, current_stage, error_message,
                   manual_input_type, manual_input_data, last_updated
            FROM process_control
            WHERE manual_input_required = TRUE
            ORDER BY last_updated DESC
        """
        
        results = self.execute_query(query)
        return [{
            'process_id': row[0],
            'stage': row[1],
            'error': row[2],
            'input_type': row[3],
            'required_data': json.loads(row[4]) if row[4] else None,
            'paused_at': row[5]
        } for row in results]

    def get_all_processes(self) -> List[Dict]:
        """Get all processes with their current status."""
        query = """
            SELECT p.process_id, p.current_stage, p.status,
                   p.created_at, p.last_updated,
                   COUNT(DISTINCT h.id) as history_count
            FROM process_control p
            LEFT JOIN process_history h ON p.process_id = h.process_id
            GROUP BY p.process_id
            ORDER BY p.last_updated DESC
        """
        
        results = self.execute_query(query)
        return [{
            'process_id': row[0],
            'stage': row[1],
            'status': row[2],
            'created_at': row[3],
            'last_updated': row[4],
            'history_count': row[5]
        } for row in results]

    def get_stats(self) -> Dict:
        """Get process statistics."""
        stats = {
            'active_processes': 0,
            'success_rate': 0,
            'avg_process_time': 0
        }
        
        # Get active processes count
        result = self.execute_query(
            "SELECT COUNT(*) FROM process_control WHERE status = ?",
            (ProcessStatus.RUNNING.value,)
        )
        stats['active_processes'] = result[0][0] if result else 0
        
        # Get success rate
        result = self.execute_query("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN status = ? THEN 1 ELSE 0 END) as completed
            FROM process_control
            WHERE status IN (?, ?)
        """, (ProcessStatus.COMPLETED.value,
              ProcessStatus.COMPLETED.value,
              ProcessStatus.FAILED.value))
        
        if result and result[0][0] > 0:
            total, completed = result[0]
            stats['success_rate'] = (completed / total * 100)
        
        return stats

    def get_process_timeline(self, process_id: str) -> List[Dict]:
        """Get timeline of process stages."""
        query = """
            SELECT stage, status, timestamp, details
            FROM process_history
            WHERE process_id = ?
            ORDER BY timestamp ASC
        """
        
        results = self.execute_query(query, (process_id,))
        return [{
            'stage': row[0],
            'status': row[1],
            'timestamp': row[2],
            'details': json.loads(row[3]) if row[3] else None
        } for row in results]

    def _add_history(self, process_id: str, message: str,
                    status: ProcessStatus, details: Optional[Dict] = None) -> None:
        """Add entry to process history."""
        data = {
            'process_id': process_id,
            'stage': message,
            'status': status.value,
            'timestamp': datetime.now().isoformat(),
            'details': json.dumps(details) if details else None
        }
        
        self.insert('process_history', data)