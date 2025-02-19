import os
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import json
from src.utils.dependency_container import container
from src.utils.process_control import ProcessControl
from src.utils.process_control_interface import ProcessStatus, ProcessStage
from src.utils.error_handler import ErrorHandler
from src.database.db_manager import DatabaseManager

logger = logging.getLogger(__name__)

class DashboardService:
    """Service for providing dashboard data and functionality."""
    
    def __init__(self):
        """Initialize dashboard service with dependencies."""
        try:
            self.process_control = container.resolve(ProcessControl)
            self.error_handler = container.resolve(ErrorHandler)
            self.db_manager = container.resolve(DatabaseManager)
        except Exception as e:
            logger.error(f"Error initializing DashboardService: {str(e)}")
            # Fallback initialization
            self.process_control = ProcessControl()
            self.error_handler = ErrorHandler()
            self.db_manager = None
    
    def get_dashboard_stats(self, days: int = 7) -> Dict[str, Any]:
        """Get dashboard statistics."""
        try:
            # Get process stats
            process_stats = self.process_control.get_stats()
            
            # Get error stats
            error_stats = self.error_handler.get_error_stats()
            
            # Calculate success rate
            total_completed = process_stats.get('historical', {}).get(f'last_{days}_days', {}).get('total', 0)
            successful = process_stats.get('historical', {}).get(f'last_{days}_days', {}).get('successful', 0)
            
            success_rate = 0
            if total_completed > 0:
                success_rate = round((successful / total_completed) * 100, 1)
            
            # Combine stats
            return {
                'active_processes': process_stats.get('active_processes', 0),
                'pending_processes': process_stats.get('pending_processes', 0),
                'completed_last_24h': process_stats.get('historical', {}).get('last_24h', {}).get('total', 0),
                'success_rate': success_rate,
                'error_count_today': error_stats.get('today', 0),
                'attention_needed': self.get_processes_needing_attention_count(),
                'recent_successes': process_stats.get('historical', {}).get(f'last_{days}_days', {}).get('successful', 0),
                'recent_failures': process_stats.get('historical', {}).get(f'last_{days}_days', {}).get('failed', 0)
            }
        except Exception as e:
            logger.error(f"Error getting dashboard stats: {str(e)}")
            return {
                'active_processes': 0,
                'pending_processes': 0,
                'completed_last_24h': 0,
                'success_rate': 0,
                'error_count_today': 0,
                'attention_needed': 0,
                'recent_successes': 0,
                'recent_failures': 0
            }
    
    def get_recent_processes(self, 
                            days: int = 7, 
                            status: str = 'all', 
                            search_term: str = '',
                            page: int = 1,
                            per_page: int = 20) -> Dict[str, Any]:
        """Get recent processes with filtering."""
        try:
            # Convert page and per_page to integers if they're strings
            page = int(page) if isinstance(page, str) else page
            per_page = int(per_page) if isinstance(per_page, str) else per_page
            
            # Calculate date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            # Get processes from process control
            processes = self.process_control.get_processes(
                start_date=start_date,
                end_date=end_date,
                status=None if status == 'all' else status
            )
            
            # Apply search filter if provided
            if search_term:
                search_term = search_term.lower()
                filtered_processes = []
                for process in processes:
                    # Search in process_id, client_name, email_subject, etc.
                    searchable_fields = [
                        process.get('process_id', ''),
                        process.get('client_name', ''),
                        process.get('email_subject', ''),
                        process.get('email_from', ''),
                        process.get('notes', '')
                    ]
                    if any(search_term in str(field).lower() for field in searchable_fields):
                        filtered_processes.append(process)
                processes = filtered_processes
            
            # Calculate pagination
            total_records = len(processes)
            total_pages = (total_records + per_page - 1) // per_page
            
            # Adjust page if out of bounds
            if page < 1:
                page = 1
            if page > total_pages and total_pages > 0:
                page = total_pages
            
            # Get paginated results
            start_idx = (page - 1) * per_page
            end_idx = start_idx + per_page
            paginated_processes = processes[start_idx:end_idx]
            
            # Add additional info to each process
            for process in paginated_processes:
                process['documents_count'] = len(self.get_process_documents(process['process_id']))
                process['output_files_count'] = len(self.get_process_outputs(process['process_id']))
            
            return {
                'processes': paginated_processes,
                'pagination': {
                    'page': page,
                    'per_page': per_page,
                    'total_records': total_records,
                    'total_pages': total_pages
                }
            }
        except Exception as e:
            logger.error(f"Error getting recent processes: {str(e)}")
            return {
                'processes': [],
                'pagination': {
                    'page': page,
                    'per_page': per_page,
                    'total_records': 0,
                    'total_pages': 0
                }
            }
    
    def get_processes_needing_attention(self) -> List[Dict[str, Any]]:
        """Get processes that need manual review/attention."""
        try:
            # Get processes with error or manual_review status
            error_processes = self.process_control.get_processes(status=ProcessStatus.ERROR)
            review_processes = self.process_control.get_processes(status=ProcessStatus.MANUAL_REVIEW)
            
            # Combine and sort by timestamp (newest first)
            processes = error_processes + review_processes
            processes.sort(key=lambda x: x.get('last_updated', ''), reverse=True)
            
            # Add additional info to each process
            for process in processes:
                process['error_details'] = self.error_handler.get_process_errors(process['process_id'])
                if process.get('status') == ProcessStatus.ERROR:
                    process['attention_reason'] = 'Error detected'
                else:
                    process['attention_reason'] = 'Manual review required'
            
            return processes
        except Exception as e:
            logger.error(f"Error getting processes needing attention: {str(e)}")
            return []
    
    def get_processes_needing_attention_count(self) -> int:
        """Get count of processes needing attention."""
        try:
            # Get processes needing attention
            processes = self.get_processes_needing_attention()
            return len(processes)
        except Exception as e:
            logger.error(f"Error getting processes needing attention count: {str(e)}")
            return 0
    
    def get_process_details(self, process_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed information about a specific process."""
        try:
            # Get process from process control
            process = self.process_control.get_process(process_id)
            if not process:
                return None
            
            # Get additional information
            process['documents'] = self.get_process_documents(process_id)
            process['outputs'] = self.get_process_outputs(process_id)
            process['errors'] = self.error_handler.get_process_errors(process_id)
            process['timeline'] = self.get_process_timeline(process_id)
            
            return process
        except Exception as e:
            logger.error(f"Error getting process details for {process_id}: {str(e)}")
            return None
    
    def get_process_timeline(self, process_id: str) -> List[Dict[str, Any]]:
        """Get timeline events for a process."""
        try:
            # Get events from process control
            events = self.process_control.get_process_events(process_id)
            
            # Sort events by timestamp
            events.sort(key=lambda x: x.get('timestamp', ''))
            
            return events
        except Exception as e:
            logger.error(f"Error getting process timeline for {process_id}: {str(e)}")
            return []
    
    def get_process_documents(self, process_id: str) -> List[Dict[str, Any]]:
        """Get documents associated with a process."""
        try:
            # Get documents from process control
            documents = self.process_control.get_process_documents(process_id)
            
            # Add file size information
            for doc in documents:
                if doc.get('file_path') and os.path.exists(doc.get('file_path')):
                    doc['file_size'] = os.path.getsize(doc.get('file_path'))
                    doc['file_size_readable'] = self._format_file_size(doc['file_size'])
                else:
                    doc['file_size'] = 0
                    doc['file_size_readable'] = '0 B'
            
            return documents
        except Exception as e:
            logger.error(f"Error getting process documents for {process_id}: {str(e)}")
            return []
    
    def get_process_outputs(self, process_id: str) -> List[Dict[str, Any]]:
        """Get output files for a process."""
        try:
            # Get outputs from process control
            outputs = self.process_control.get_process_outputs(process_id)
            
            # Add file size information
            for output in outputs:
                if output.get('file_path') and os.path.exists(output.get('file_path')):
                    output['file_size'] = os.path.getsize(output.get('file_path'))
                    output['file_size_readable'] = self._format_file_size(output['file_size'])
                else:
                    output['file_size'] = 0
                    output['file_size_readable'] = '0 B'
            
            return outputs
        except Exception as e:
            logger.error(f"Error getting process outputs for {process_id}: {str(e)}")
            return []
    
    def get_file_info(self, file_id: str) -> Optional[Dict[str, Any]]:
        """Get information about a file for download."""
        try:
            # Check if it's a document or output file
            # Try to find in documents first
            file_info = self.process_control.get_document_by_id(file_id)
            
            if not file_info:
                # If not found in documents, try outputs
                file_info = self.process_control.get_output_by_id(file_id)
            
            if not file_info:
                return None
            
            # Extract directory and filename from file_path
            file_path = file_info.get('file_path', '')
            if not file_path or not os.path.exists(file_path):
                return None
            
            directory = os.path.dirname(file_path)
            filename = os.path.basename(file_path)
            
            return {
                'file_id': file_id,
                'process_id': file_info.get('process_id'),
                'filename': filename,
                'directory': directory,
                'file_type': file_info.get('file_type'),
                'file_size': os.path.getsize(file_path),
                'file_size_readable': self._format_file_size(os.path.getsize(file_path))
            }
        except Exception as e:
            logger.error(f"Error getting file info for {file_id}: {str(e)}")
            return None
    
    def get_process_stats(self, start_date_str: str, end_date_str: str) -> Dict[str, Any]:
        """Get process statistics for reporting."""
        try:
            # Parse dates
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d') + timedelta(days=1) - timedelta(seconds=1)
            
            # Get processes in date range
            processes = self.process_control.get_processes(
                start_date=start_date,
                end_date=end_date
            )
            
            # Calculate statistics
            total_processes = len(processes)
            status_counts = {}
            daily_counts = {}
            avg_processing_time = 0
            
            for process in processes:
                # Count by status
                status = process.get('status', 'unknown')
                status_counts[status] = status_counts.get(status, 0) + 1
                
                # Count by day
                created_at = process.get('created_at')
                if created_at:
                    day_key = created_at.split('T')[0]  # Extract date part
                    daily_counts[day_key] = daily_counts.get(day_key, 0) + 1
                
                # Calculate processing time for completed processes
                if process.get('status') == ProcessStatus.COMPLETED:
                    created_at = process.get('created_at')
                    completed_at = process.get('completed_at')
                    if created_at and completed_at:
                        try:
                            created_dt = datetime.fromisoformat(created_at)
                            completed_dt = datetime.fromisoformat(completed_at)
                            processing_time = (completed_dt - created_dt).total_seconds() / 60  # in minutes
                            avg_processing_time += processing_time
                        except:
                            pass
            
            # Calculate average processing time
            completed_count = status_counts.get(ProcessStatus.COMPLETED, 0)
            if completed_count > 0:
                avg_processing_time /= completed_count
            
            # Prepare daily data for charts
            daily_data = []
            current_date = start_date
            while current_date <= end_date:
                day_key = current_date.strftime('%Y-%m-%d')
                daily_data.append({
                    'date': day_key,
                    'count': daily_counts.get(day_key, 0)
                })
                current_date += timedelta(days=1)
            
            return {
                'total_processes': total_processes,
                'status_counts': status_counts,
                'daily_data': daily_data,
                'avg_processing_time': round(avg_processing_time, 1)
            }
        except Exception as e:
            logger.error(f"Error getting process stats: {str(e)}")
            return {
                'total_processes': 0,
                'status_counts': {},
                'daily_data': [],
                'avg_processing_time': 0
            }
    
    def get_document_stats(self, start_date_str: str, end_date_str: str) -> Dict[str, Any]:
        """Get document statistics for reporting."""
        try:
            # Parse dates
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d') + timedelta(days=1) - timedelta(seconds=1)
            
            # Get processes in date range
            processes = self.process_control.get_processes(
                start_date=start_date,
                end_date=end_date
            )
            
            # Initialize stats
            total_documents = 0
            doc_type_counts = {}
            document_sizes = []
            
            # Process each process's documents
            for process in processes:
                process_id = process.get('process_id')
                if not process_id:
                    continue
                
                # Get documents for this process
                documents = self.get_process_documents(process_id)
                total_documents += len(documents)
                
                for doc in documents:
                    # Count by document type
                    doc_type = doc.get('document_type', 'unknown')
                    doc_type_counts[doc_type] = doc_type_counts.get(doc_type, 0) + 1
                    
                    # Record document size
                    doc_size = doc.get('file_size', 0)
                    if doc_size > 0:
                        document_sizes.append(doc_size)
            
            # Calculate average document size
            avg_document_size = 0
            if document_sizes:
                avg_document_size = sum(document_sizes) / len(document_sizes)
            
            return {
                'total_documents': total_documents,
                'document_type_counts': doc_type_counts,
                'avg_document_size': self._format_file_size(avg_document_size),
                'avg_documents_per_process': round(total_documents / len(processes), 1) if processes else 0
            }
        except Exception as e:
            logger.error(f"Error getting document stats: {str(e)}")
            return {
                'total_documents': 0,
                'document_type_counts': {},
                'avg_document_size': '0 B',
                'avg_documents_per_process': 0
            }
    
    def get_error_stats(self, start_date_str: str, end_date_str: str) -> Dict[str, Any]:
        """Get error statistics for reporting."""
        try:
            # Parse dates
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d') + timedelta(days=1) - timedelta(seconds=1)
            
            # Get errors in date range
            errors = self.error_handler.get_errors(
                start_date=start_date,
                end_date=end_date
            )
            
            # Calculate statistics
            total_errors = len(errors)
            error_type_counts = {}
            stage_error_counts = {}
            daily_error_counts = {}
            
            for error in errors:
                # Count by error type
                error_type = error.get('error_type', 'unknown')
                error_type_counts[error_type] = error_type_counts.get(error_type, 0) + 1
                
                # Count by processing stage
                stage = error.get('process_stage', 'unknown')
                stage_error_counts[stage] = stage_error_counts.get(stage, 0) + 1
                
                # Count by day
                timestamp = error.get('timestamp')
                if timestamp:
                    day_key = timestamp.split('T')[0]  # Extract date part
                    daily_error_counts[day_key] = daily_error_counts.get(day_key, 0) + 1
            
            # Prepare daily data for charts
            daily_data = []
            current_date = start_date
            while current_date <= end_date:
                day_key = current_date.strftime('%Y-%m-%d')
                daily_data.append({
                    'date': day_key,
                    'count': daily_error_counts.get(day_key, 0)
                })
                current_date += timedelta(days=1)
            
            return {
                'total_errors': total_errors,
                'error_type_counts': error_type_counts,
                'stage_error_counts': stage_error_counts,
                'daily_data': daily_data
            }
        except Exception as e:
            logger.error(f"Error getting error stats: {str(e)}")
            return {
                'total_errors': 0,
                'error_type_counts': {},
                'stage_error_counts': {},
                'daily_data': []
            }
    
    def _format_file_size(self, size_bytes: int) -> str:
        """Format file size in human readable format."""
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes / 1024:.1f} KB"
        elif size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes / (1024 * 1024):.1f} MB"
        else:
            return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"