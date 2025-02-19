from typing import Dict, List, Optional, Any, Tuple
import json
import sqlite3
from datetime import datetime, timedelta
import threading
import time
import logging
import os
from enum import Enum
import uuid

from src.utils.base_db_handler import BaseDBHandler
from src.utils.process_control_interface import (
    IProcessControl, ProcessStatus, ProcessStage
)
from src.utils.error_handling import handle_errors, ErrorCategory, ErrorSeverity

logger = logging.getLogger(__name__)

class ProcessControl(BaseDBHandler, IProcessControl):
    """Enhanced process control with improved transaction handling and performance."""

    def __init__(self, db_path: str = "data/process_control.db"):
        """Initialize process control with DB connection.
        
        Args:
            db_path: Path to SQLite database
        """
        super().__init__(db_path)
        self._stats_cache = {}
        self._stats_last_updated = datetime.min
        self._stats_cache_ttl = timedelta(minutes=5)
        self._lock = threading.RLock()
        self._ensure_db_dir_exists(db_path)
        
    def _ensure_db_dir_exists(self, db_path: str) -> None:
        """Ensure database directory exists."""
        db_dir = os.path.dirname(db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)

    @handle_errors(ErrorCategory.DATABASE, ErrorSeverity.HIGH)
    def start_process(self, process_id: str) -> None:
        """Initialize a new process with proper transaction handling.
        
        Args:
            process_id: Unique process identifier
        """
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                # Check if process already exists
                cursor.execute(
                    "SELECT process_id FROM process_control WHERE process_id = ?",
                    (process_id,)
                )
                if cursor.fetchone():
                    logger.warning(f"Process {process_id} already exists, resetting state")
                    # Delete existing process to start fresh
                    cursor.execute("DELETE FROM process_control WHERE process_id = ?", (process_id,))
                    cursor.execute("DELETE FROM process_history WHERE process_id = ?", (process_id,))
                
                # Start a transaction
                cursor.execute("BEGIN TRANSACTION")
                
                # Insert process record
                current_time = datetime.now().isoformat()
                data = {
                    'process_id': process_id,
                    'status': ProcessStatus.RUNNING.value,
                    'current_stage': ProcessStage.EMAIL_PROCESSING.value,
                    'stage_data': None,
                    'error_message': None,
                    'manual_input_required': False,
                    'manual_input_type': None,
                    'manual_input_data': None,
                    'last_updated': current_time,
                    'created_at': current_time
                }
                self._insert(cursor, 'process_control', data)
                
                # Add history entry
                history_data = {
                    'process_id': process_id,
                    'timestamp': current_time,
                    'stage': "process_started",
                    'status': ProcessStatus.RUNNING.value,
                    'details': json.dumps({
                        'action': 'start',
                        'timestamp': current_time
                    })
                }
                self._insert(cursor, 'process_history', history_data)
                
                # Commit transaction
                cursor.execute("COMMIT")
                
                # Invalidate stats cache
                self._invalidate_stats_cache()
                
                logger.info(f"Process {process_id} started")
                
        except sqlite3.Error as e:
            logger.error(f"Database error pausing process {process_id}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Failed to pause process {process_id}: {str(e)}")
            raise

    @handle_errors(ErrorCategory.DATABASE, ErrorSeverity.MEDIUM)
    def resume_process(self, process_id: str, manual_input: Optional[Dict] = None) -> None:
        """Resume process after manual intervention.
        
        Args:
            process_id: Process identifier
            manual_input: Optional data provided by manual intervention
        """
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                # Get current process state for history
                cursor.execute(
                    "SELECT current_stage, manual_input_type FROM process_control WHERE process_id = ?",
                    (process_id,)
                )
                result = cursor.fetchone()
                if not result:
                    logger.warning(f"Process {process_id} not found during resume operation")
                    return
                    
                current_stage, input_type = result
                
                # Start a transaction
                cursor.execute("BEGIN TRANSACTION")
                
                # Log manual input if provided
                if manual_input:
                    history_data = {
                        'process_id': process_id,
                        'timestamp': datetime.now().isoformat(),
                        'stage': current_stage,
                        'status': ProcessStatus.RUNNING.value,
                        'details': json.dumps({
                            'action': 'manual_input',
                            'input_type': input_type,
                            'data': manual_input
                        })
                    }
                    self._insert(cursor, 'process_history', history_data)
                
                # Update process record to resume
                update_data = {
                    'status': ProcessStatus.RUNNING.value,
                    'manual_input_required': False,
                    'manual_input_type': None,
                    'manual_input_data': None,
                    'error_message': None,
                    'last_updated': datetime.now().isoformat()
                }
                
                # Use parameterized query for safety
                set_clause = ', '.join(f"{k} = ?" for k in update_data.keys())
                update_query = f"UPDATE process_control SET {set_clause} WHERE process_id = ?"
                
                cursor.execute(
                    update_query,
                    list(update_data.values()) + [process_id]
                )
                
                # Add resume history entry
                history_data = {
                    'process_id': process_id,
                    'timestamp': datetime.now().isoformat(),
                    'stage': current_stage,
                    'status': ProcessStatus.RUNNING.value,
                    'details': json.dumps({
                        'action': 'resume',
                        'with_input': manual_input is not None
                    })
                }
                self._insert(cursor, 'process_history', history_data)
                
                # Commit transaction
                cursor.execute("COMMIT")
                
                # Invalidate stats cache
                self._invalidate_stats_cache()
                
                logger.info(f"Process {process_id} resumed")
                
        except sqlite3.Error as e:
            logger.error(f"Database error resuming process {process_id}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Failed to resume process {process_id}: {str(e)}")
            raise

    def get_process_status(self, process_id: str) -> Dict:
        """Get current process status and details with optimized query.
        
        Args:
            process_id: Process identifier
            
        Returns:
            Dictionary with process status and details
        """
        try:
            query = """
                SELECT p.status, p.current_stage, p.stage_data,
                       p.error_message, p.manual_input_required,
                       p.manual_input_type, p.manual_input_data,
                       p.last_updated, p.created_at,
                       COUNT(h.id) as history_count,
                       MAX(CASE WHEN h.status = 'failed' THEN 1 ELSE 0 END) as had_failures
                FROM process_control p
                LEFT JOIN process_history h ON p.process_id = h.process_id
                WHERE p.process_id = ?
                GROUP BY p.process_id
            """
            
            result = self.execute_query(query, (process_id,))
            if not result:
                logger.warning(f"Process {process_id} not found")
                return {}
                
            row = result[0]
            
            # Calculate process duration
            try:
                start_time = datetime.fromisoformat(row[8])
                last_update = datetime.fromisoformat(row[7])
                duration_seconds = (last_update - start_time).total_seconds()
            except (ValueError, TypeError):
                duration_seconds = 0
                
            return {
                'process_id': process_id,
                'status': row[0],
                'current_stage': row[1],
                'stage_data': json.loads(row[2]) if row[2] else None,
                'error_message': row[3],
                'manual_input_required': bool(row[4]),
                'manual_input_type': row[5],
                'manual_input_data': json.loads(row[6]) if row[6] else None,
                'last_updated': row[7],
                'created_at': row[8],
                'duration_seconds': duration_seconds,
                'history_count': row[9],
                'had_failures': bool(row[10])
            }
            
        except Exception as e:
            logger.error(f"Error getting status for process {process_id}: {str(e)}")
            return {'error': str(e)}

    def get_processes_needing_attention(self) -> List[Dict]:
        """Get all processes that need manual intervention with optimized query.
        
        Returns:
            List of processes needing attention
        """
        try:
            query = """
                SELECT p.process_id, p.current_stage, p.error_message,
                       p.manual_input_type, p.manual_input_data, p.last_updated,
                       p.created_at, 
                       JULIANDAY('now') - JULIANDAY(p.last_updated) as hours_waiting
                FROM process_control p
                WHERE p.manual_input_required = 1
                ORDER BY p.last_updated ASC
            """
            
            results = self.execute_query(query)
            return [{
                'process_id': row[0],
                'stage': row[1],
                'error': row[2],
                'input_type': row[3],
                'required_data': json.loads(row[4]) if row[4] else None,
                'paused_at': row[5],
                'created_at': row[6],
                'hours_waiting': float(row[7]) * 24 if row[7] is not None else 0
            } for row in results]
            
        except Exception as e:
            logger.error(f"Error getting processes needing attention: {str(e)}")
            return []

    def get_all_processes(self) -> List[Dict]:
        """Get all processes with enhanced details.
        
        Returns:
            List of all processes with their current status
        """
        try:
            query = """
                SELECT p.process_id, p.current_stage, p.status,
                       p.created_at, p.last_updated,
                       COUNT(DISTINCT h.id) as history_count,
                       JULIANDAY(p.last_updated) - JULIANDAY(p.created_at) as duration_days,
                       p.error_message,
                       p.manual_input_required
                FROM process_control p
                LEFT JOIN process_history h ON p.process_id = h.process_id
                GROUP BY p.process_id
                ORDER BY p.last_updated DESC
                LIMIT 1000
            """
            
            results = self.execute_query(query)
            
            # Format and enhance results
            processes = []
            for row in results:
                # Calculate duration in hours or minutes for better readability
                duration_days = float(row[6]) if row[6] is not None else 0
                
                if duration_days < 1/24:  # Less than an hour
                    duration_str = f"{int(duration_days*24*60)} minutes"
                elif duration_days < 1:  # Less than a day
                    duration_str = f"{int(duration_days*24)} hours"
                else:
                    duration_str = f"{duration_days:.1f} days"
                
                processes.append({
                    'process_id': row[0],
                    'stage': row[1],
                    'status': row[2],
                    'created_at': row[3],
                    'last_updated': row[4],
                    'history_count': row[5],
                    'duration': duration_str,
                    'error_message': row[7],
                    'needs_attention': bool(row[8])
                })
                
            return processes
            
        except Exception as e:
            logger.error(f"Error getting all processes: {str(e)}")
            return []

    def get_stats(self) -> Dict:
        """Get process statistics with caching for better performance.
        
        Returns:
            Dictionary of process statistics
        """
        with self._lock:
            # Return cached stats if still valid
            now = datetime.now()
            if (now - self._stats_last_updated < self._stats_cache_ttl and 
                self._stats_cache):
                return self._stats_cache.copy()
            
            # Calculate fresh stats
            stats = self._calculate_fresh_stats()
            
            # Update cache
            self._stats_cache = stats
            self._stats_last_updated = now
            
            return stats.copy()

    def _calculate_fresh_stats(self) -> Dict:
        """Calculate fresh statistics with efficient queries.
        
        Returns:
            Dictionary of process statistics
        """
        try:
            stats = {
                'active_processes': 0,
                'processes_needing_attention': 0, 
                'success_rate': 0,
                'avg_process_time': 0,
                'completed_today': 0,
                'failed_today': 0,
                'started_today': 0,
                'stages': {},
                'historical': {
                    'last_7_days': {
                        'total': 0,
                        'successful': 0,
                        'failed': 0
                    },
                    'last_30_days': {
                        'total': 0,
                        'successful': 0,
                        'failed': 0
                    }
                }
            }
            
            # Count active processes and processes needing attention
            status_query = """
                SELECT 
                    SUM(CASE WHEN status = ? THEN 1 ELSE 0 END) as active,
                    SUM(CASE WHEN manual_input_required = 1 THEN 1 ELSE 0 END) as needs_attention
                FROM process_control
            """
            result = self.execute_query(status_query, (ProcessStatus.RUNNING.value,))
            if result and len(result[0]) >= 2:
                stats['active_processes'] = result[0][0] or 0
                stats['processes_needing_attention'] = result[0][1] or 0
            
            # Calculate success rate
            success_query = """
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN status = ? THEN 1 ELSE 0 END) as completed
                FROM process_control
                WHERE status IN (?, ?)
            """
            result = self.execute_query(
                success_query, 
                (ProcessStatus.COMPLETED.value, ProcessStatus.COMPLETED.value, ProcessStatus.FAILED.value)
            )
            if result and result[0][0] > 0:
                total, completed = result[0]
                stats['success_rate'] = round((completed / total * 100), 1)
            
            # Calculate average process time
            time_query = """
                SELECT AVG(
                    (JULIANDAY(last_updated) - JULIANDAY(created_at)) * 24 * 60 * 60
                ) 
                FROM process_control 
                WHERE status = ?
            """
            result = self.execute_query(time_query, (ProcessStatus.COMPLETED.value,))
            if result and result[0][0]:
                stats['avg_process_time'] = int(result[0][0] or 0)
            
            # Count processes by stage
            stage_query = """
                SELECT current_stage, COUNT(*) 
                FROM process_control
                WHERE status = ?
                GROUP BY current_stage
            """
            result = self.execute_query(stage_query, (ProcessStatus.RUNNING.value,))
            stats['stages'] = {row[0]: row[1] for row in result}
            
            # Count today's processes
            today_query = """
                SELECT 
                    SUM(CASE WHEN status = ? AND DATE(last_updated) = DATE('now') THEN 1 ELSE 0 END) as completed,
                    SUM(CASE WHEN status = ? AND DATE(last_updated) = DATE('now') THEN 1 ELSE 0 END) as failed,
                    SUM(CASE WHEN DATE(created_at) = DATE('now') THEN 1 ELSE 0 END) as started
                FROM process_control
            """
            result = self.execute_query(
                today_query, 
                (ProcessStatus.COMPLETED.value, ProcessStatus.FAILED.value)
            )
            if result:
                stats['completed_today'] = result[0][0] or 0
                stats['failed_today'] = result[0][1] or 0
                stats['started_today'] = result[0][2] or 0
            
            # Historical stats - last 7 days
            self._calculate_historical_stats(stats, 7)
            
            # Historical stats - last 30 days
            self._calculate_historical_stats(stats, 30)
            
            return stats
            
        except Exception as e:
            logger.error(f"Error calculating process statistics: {str(e)}")
            return {
                'error': str(e),
                'active_processes': 0,
                'success_rate': 0
            }

    def _calculate_historical_stats(self, stats: Dict, days: int) -> None:
        """Calculate historical statistics for the specified number of days.
        
        Args:
            stats: Statistics dictionary to update
            days: Number of days to analyze
        """
        period_key = f"last_{days}_days"
        historical_query = """
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN status = ? THEN 1 ELSE 0 END) as successful,
                SUM(CASE WHEN status = ? THEN 1 ELSE 0 END) as failed
            FROM process_control
            WHERE DATE(created_at) >= DATE('now', ?)
        """
        result = self.execute_query(
            historical_query, 
            (ProcessStatus.COMPLETED.value, ProcessStatus.FAILED.value, f"-{days} days")
        )
        
        if result:
            stats['historical'][period_key]['total'] = result[0][0] or 0
            stats['historical'][period_key]['successful'] = result[0][1] or 0
            stats['historical'][period_key]['failed'] = result[0][2] or 0

    def get_process_timeline(self, process_id: str) -> List[Dict]:
        """Get timeline of process stages with enhanced information.
        
        Args:
            process_id: Process identifier
            
        Returns:
            List of timeline entries
        """
        try:
            query = """
                SELECT stage, status, timestamp, details,
                       (JULIANDAY(timestamp) - JULIANDAY(
                           (SELECT MIN(timestamp) FROM process_history 
                            WHERE process_id = ? AND stage = 'process_started')
                       )) * 24 * 60 * 60 as elapsed_seconds
                FROM process_history
                WHERE process_id = ?
                ORDER BY timestamp ASC
            """
            
            results = self.execute_query(query, (process_id, process_id))
            timeline = []
            
            prev_timestamp = None
            for row in results:
                timestamp = datetime.fromisoformat(row[2]) if row[2] else None
                
                # Calculate duration from previous step
                duration_seconds = None
                if prev_timestamp and timestamp:
                    duration_seconds = (timestamp - prev_timestamp).total_seconds()
                
                entry = {
                    'stage': row[0],
                    'status': row[1],
                    'timestamp': row[2],
                    'details': json.loads(row[3]) if row[3] else None,
                    'elapsed_seconds': int(row[4]) if row[4] is not None else None,
                    'duration_seconds': duration_seconds
                }
                
                # Add human-readable durations
                if entry['elapsed_seconds'] is not None:
                    entry['elapsed_human'] = self._format_duration(entry['elapsed_seconds'])
                if duration_seconds is not None:
                    entry['duration_human'] = self._format_duration(duration_seconds)
                
                timeline.append(entry)
                prev_timestamp = timestamp
                
            return timeline
            
        except Exception as e:
            logger.error(f"Error getting timeline for process {process_id}: {str(e)}")
            return []

    def _format_duration(self, seconds: float) -> str:
        """Format duration in seconds to human-readable format.
        
        Args:
            seconds: Duration in seconds
            
        Returns:
            Human-readable duration string
        """
        if seconds < 60:
            return f"{int(seconds)}s"
        elif seconds < 3600:
            return f"{int(seconds/60)}m {int(seconds%60)}s"
        else:
            hours = int(seconds / 3600)
            minutes = int((seconds % 3600) / 60)
            return f"{hours}h {minutes}m"

    def _get_connection(self):
        """Get database connection with retry logic.
        
        Returns:
            SQLite connection
            
        Raises:
            sqlite3.OperationalError: If connection fails after retries
        """
        retries = 3
        backoff = 0.5
        
        for attempt in range(retries):
            try:
                return sqlite3.connect(self.db_path)
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < retries - 1:
                    time.sleep(backoff * (2 ** attempt))
                else:
                    raise

    def _insert(self, cursor: sqlite3.Cursor, table: str, data: Dict) -> int:
        """Insert data into table with improved parameter handling.
        
        Args:
            cursor: SQLite cursor
            table: Table name
            data: Data to insert
            
        Returns:
            ID of inserted row
        """
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['?'] * len(data))
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        cursor.execute(query, list(data.values()))
        return cursor.lastrowid

    def _invalidate_stats_cache(self) -> None:
        """Invalidate statistics cache."""
        with self._lock:
            self._stats_last_updated = datetime.min
            self._stats_cache = {}

    def cancel_process(self, process_id: str, reason: str) -> bool:
        """Cancel a running process.
        
        Args:
            process_id: Process identifier
            reason: Reason for cancellation
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                # Check if process exists and is cancellable
                cursor.execute(
                    "SELECT status FROM process_control WHERE process_id = ?",
                    (process_id,)
                )
                result = cursor.fetchone()
                if not result:
                    logger.warning(f"Process {process_id} not found for cancellation")
                    return False
                    
                current_status = result[0]
                if current_status in [ProcessStatus.COMPLETED.value, ProcessStatus.FAILED.value]:
                    logger.warning(f"Process {process_id} is already {current_status}, cannot cancel")
                    return False
                
                # Start transaction
                cursor.execute("BEGIN TRANSACTION")
                
                # Update process status
                update_data = {
                    'status': ProcessStatus.FAILED.value,
                    'error_message': f"Cancelled: {reason}",
                    'last_updated': datetime.now().isoformat()
                }
                
                set_clause = ', '.join(f"{k} = ?" for k in update_data.keys())
                cursor.execute(
                    f"UPDATE process_control SET {set_clause} WHERE process_id = ?",
                    list(update_data.values()) + [process_id]
                )
                
                # Add history entry
                history_data = {
                    'process_id': process_id,
                    'timestamp': datetime.now().isoformat(),
                    'stage': 'cancelled',
                    'status': ProcessStatus.FAILED.value,
                    'details': json.dumps({
                        'action': 'cancel',
                        'reason': reason
                    })
                }
                self._insert(cursor, 'process_history', history_data)
                
                # Commit transaction
                cursor.execute("COMMIT")
                
                # Invalidate stats cache
                self._invalidate_stats_cache()
                
                logger.info(f"Process {process_id} cancelled: {reason}")
                return True
                
        except Exception as e:
            logger.error(f"Failed to cancel process {process_id}: {str(e)}")
            return False

    def cleanup_old_processes(self, days: int = 30) -> int:
        """Remove old completed processes.
        
        Args:
            days: Remove processes older than this many days
            
        Returns:
            Number of processes removed
        """
        try:
            cutoff_date = (datetime.now() - timedelta(days=days)).isoformat()
            
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                # Get IDs to remove for history cleanup
                cursor.execute(
                    """
                    SELECT process_id FROM process_control 
                    WHERE status IN (?, ?) AND last_updated < ?
                    """,
                    (ProcessStatus.COMPLETED.value, ProcessStatus.FAILED.value, cutoff_date)
                )
                process_ids = [row[0] for row in cursor.fetchall()]
                
                if not process_ids:
                    return 0
                
                # Delete from history
                placeholders = ','.join(['?'] * len(process_ids))
                cursor.execute(
                    f"DELETE FROM process_history WHERE process_id IN ({placeholders})",
                    process_ids
                )
                
                # Delete from process control
                cursor.execute(
                    f"DELETE FROM process_control WHERE process_id IN ({placeholders})",
                    process_ids
                )
                
                # Invalidate stats cache
                self._invalidate_stats_cache()
                
                removed_count = cursor.rowcount
                logger.info(f"Removed {removed_count} old processes older than {days} days")
                return removed_count
                
        except Exception as e:
            logger.error(f"Failed to cleanup old processes: {str(e)}")
            return 0

        except sqlite3.Error as e:
            logger.error(f"Database error while starting process: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Failed to start process: {str(e)}")
            raise

    @handle_errors(ErrorCategory.DATABASE, ErrorSeverity.MEDIUM)
    def update_stage(self, process_id: str, stage: ProcessStage, 
                    status: ProcessStatus, stage_data: Optional[Dict] = None) -> None:
        """Update process stage with atomic transaction.
        
        Args:
            process_id: Process identifier
            stage: New process stage
            status: Current process status
            stage_data: Optional data associated with the stage
        """
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                # Start a transaction
                cursor.execute("BEGIN TRANSACTION")
                
                # Update process record
                update_data = {
                    'current_stage': stage.value,
                    'status': status.value,
                    'stage_data': json.dumps(stage_data) if stage_data else None,
                    'last_updated': datetime.now().isoformat()
                }
                
                if status == ProcessStatus.FAILED:
                    update_data['error_message'] = stage_data.get('error') if stage_data else None
                
                # Use parameterized query for safety
                set_clause = ', '.join(f"{k} = ?" for k in update_data.keys())
                update_query = f"UPDATE process_control SET {set_clause} WHERE process_id = ?"
                
                cursor.execute(
                    update_query,
                    list(update_data.values()) + [process_id]
                )
                
                if cursor.rowcount == 0:
                    logger.warning(f"Process {process_id} not found during stage update")
                    cursor.execute("ROLLBACK")
                    return
                
                # Add history entry
                history_data = {
                    'process_id': process_id,
                    'timestamp': datetime.now().isoformat(),
                    'stage': stage.value,
                    'status': status.value,
                    'details': json.dumps({
                        'action': 'update_stage',
                        'data': stage_data
                    }) if stage_data else None
                }
                self._insert(cursor, 'process_history', history_data)
                
                # Commit transaction
                cursor.execute("COMMIT")
                
                # Invalidate stats cache
                self._invalidate_stats_cache()
                
                logger.info(f"Updated process {process_id} to stage {stage.value}, status {status.value}")
                
        except sqlite3.Error as e:
            logger.error(f"Database error updating stage for process {process_id}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Failed to update stage for process {process_id}: {str(e)}")
            raise

    @handle_errors(ErrorCategory.DATABASE, ErrorSeverity.MEDIUM)
    def pause_process(self, process_id: str, reason: str,
                     manual_input_type: Optional[str] = None,
                     required_data: Optional[Dict] = None) -> None:
        """Pause process for manual intervention with detailed tracking.
        
        Args:
            process_id: Process identifier
            reason: Reason for pausing the process
            manual_input_type: Type of manual input required
            required_data: Optional data needed for manual input
        """
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                # Get current process state for history
                cursor.execute(
                    "SELECT current_stage FROM process_control WHERE process_id = ?",
                    (process_id,)
                )
                result = cursor.fetchone()
                if not result:
                    logger.warning(f"Process {process_id} not found during pause operation")
                    return
                    
                current_stage = result[0]
                
                # Start a transaction
                cursor.execute("BEGIN TRANSACTION")
                
                # Update process record
                current_time = datetime.now().isoformat()
                update_data = {
                    'status': ProcessStatus.AWAITING_INPUT.value,
                    'manual_input_required': True,
                    'manual_input_type': manual_input_type,
                    'manual_input_data': json.dumps(required_data) if required_data else None,
                    'error_message': reason,
                    'last_updated': current_time
                }
                
                # Use parameterized query for safety
                set_clause = ', '.join(f"{k} = ?" for k in update_data.keys())
                update_query = f"UPDATE process_control SET {set_clause} WHERE process_id = ?"
                
                cursor.execute(
                    update_query,
                    list(update_data.values()) + [process_id]
                )
                
                # Add history entry
                history_data = {
                    'process_id': process_id,
                    'timestamp': current_time,
                    'stage': current_stage,
                    'status': ProcessStatus.AWAITING_INPUT.value,
                    'details': json.dumps({
                        'action': 'pause',
                        'reason': reason,
                        'input_type': manual_input_type,
                        'required_data': required_data
                    })
                }
                self._insert(cursor, 'process_history', history_data)
                
                # Commit transaction
                cursor.execute("COMMIT")
                
                # Invalidate stats cache
                self._invalidate_stats_cache()
                
        except Exception as e:
            logger.error(f"Failed to pause process {process_id}: {str(e)}")
            raise