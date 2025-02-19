from flask import Blueprint, render_template, request, redirect, url_for, flash, current_app, send_from_directory, session
import os
import logging
from datetime import datetime, timedelta

from src.dashboard.services.dashboard_service import DashboardService
from src.dashboard.services.process_service import ProcessService
from src.utils.dependency_container import container
from src.utils.process_control import ProcessControl
from src.utils.process_control_interface import ProcessStatus, ProcessStage

logger = logging.getLogger(__name__)
main_bp = Blueprint('main', __name__)

# Get services
dashboard_service = DashboardService()
process_service = ProcessService()

@main_bp.route('/')
def index():
    """Dashboard homepage with process summary."""
    # Get filter parameters
    days = int(request.args.get('days', 7))
    status = request.args.get('status', 'all')
    search = request.args.get('search', '')
    
    # Get statistics
    stats = dashboard_service.get_dashboard_stats()
    
    # Get recent processes
    processes = dashboard_service.get_recent_processes(
        days=days,
        status=status,
        search_term=search
    )
    
    # Get processes needing attention
    attention_needed = dashboard_service.get_processes_needing_attention()
    
    return render_template(
        'dashboard.html',
        stats=stats,
        processes=processes,
        attention_needed=attention_needed,
        filter={
            'days': days,
            'status': status,
            'search': search
        }
    )

@main_bp.route('/process/<string:process_id>')
def process_details(process_id):
    """Display detailed information about a specific process."""
    # Get process details
    process = dashboard_service.get_process_details(process_id)
    if not process:
        flash('Process not found', 'error')
        return redirect(url_for('main.index'))
    
    # Get process timeline
    timeline = dashboard_service.get_process_timeline(process_id)
    
    # Get documents
    documents = dashboard_service.get_process_documents(process_id)
    
    # Get outputs
    outputs = dashboard_service.get_process_outputs(process_id)
    
    return render_template(
        'process_details.html',
        process=process,
        timeline=timeline,
        documents=documents,
        outputs=outputs
    )

@main_bp.route('/process/<string:process_id>/resume', methods=['POST'])
def resume_process(process_id):
    """Resume a paused process."""
    try:
        notes = request.form.get('notes', '')
        
        result = process_service.resume_process(
            process_id=process_id,
            user_id=session.get('user_id'),
            notes=notes
        )
        
        if result['success']:
            flash('Process resumed successfully', 'success')
        else:
            flash(f"Failed to resume process: {result['error']}", 'error')
            
    except Exception as e:
        logger.error(f"Error resuming process {process_id}: {str(e)}")
        flash(f"An error occurred: {str(e)}", 'error')
        
    return redirect(url_for('main.process_details', process_id=process_id))

@main_bp.route('/process/<string:process_id>/cancel', methods=['POST'])
def cancel_process(process_id):
    """Cancel a process."""
    try:
        reason = request.form.get('reason', '')
        
        result = process_service.cancel_process(
            process_id=process_id,
            user_id=session.get('user_id'),
            reason=reason
        )
        
        if result['success']:
            flash('Process cancelled successfully', 'success')
        else:
            flash(f"Failed to cancel process: {result['error']}", 'error')
            
    except Exception as e:
        logger.error(f"Error cancelling process {process_id}: {str(e)}")
        flash(f"An error occurred: {str(e)}", 'error')
        
    return redirect(url_for('main.process_details', process_id=process_id))

@main_bp.route('/download/<string:file_id>')
def download_file(file_id):
    """Download processed file."""
    try:
        file_info = dashboard_service.get_file_info(file_id)
        if not file_info:
            flash('File not found', 'error')
            return redirect(url_for('main.index'))
            
        # Log the download
        logger.info(f"User {session.get('user_id')} downloaded file {file_id}: {file_info['filename']}")
        
        return send_from_directory(
            directory=file_info['directory'],
            path=file_info['filename'],
            as_attachment=True
        )
        
    except Exception as e:
        logger.error(f"Error downloading file {file_id}: {str(e)}")
        flash(f"An error occurred: {str(e)}", 'error')
        return redirect(url_for('main.index'))

@main_bp.route('/reports')
def reports():
    """Display system reports and analytics."""
    # Get date range parameters
    start_date = request.args.get('start_date', 
                                (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'))
    end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))
    
    # Get reports
    process_stats = dashboard_service.get_process_stats(start_date, end_date)
    document_stats = dashboard_service.get_document_stats(start_date, end_date)
    error_stats = dashboard_service.get_error_stats(start_date, end_date)
    
    return render_template(
        'reports.html',
        process_stats=process_stats,
        document_stats=document_stats,
        error_stats=error_stats,
        start_date=start_date,
        end_date=end_date
    )