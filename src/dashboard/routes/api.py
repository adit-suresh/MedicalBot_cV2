from flask import Blueprint, jsonify, request, session, current_app
import logging
from datetime import datetime, timedelta
import os
import json

from src.dashboard.services.dashboard_service import DashboardService
from src.dashboard.services.process_service import ProcessService
from src.utils.dependency_container import container
from src.utils.process_control import ProcessControl

logger = logging.getLogger(__name__)
api_bp = Blueprint('api', __name__)

# Get services
dashboard_service = DashboardService()
process_service = ProcessService()

@api_bp.before_request
def check_api_auth():
    """Check API authentication."""
    # Skip authentication for public endpoints
    public_endpoints = ['api.login']
    if request.endpoint in public_endpoints:
        return

    # API key authentication
    api_key = request.headers.get('X-API-Key')
    if api_key and api_key == current_app.config.get('API_KEY'):
        return
    
    # Session authentication for browser-based API calls
    if 'user_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401

@api_bp.route('/login', methods=['POST'])
def login():
    """API login endpoint."""
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')
    
    if not username or not password:
        return jsonify({'success': False, 'error': 'Username and password required'}), 400
    
    # Import here to avoid circular imports
    from src.dashboard.routes.auth import verify_password, update_last_login
    
    if verify_password(username, password):
        # Generate API key (in a real app, this would be more secure)
        api_key = os.urandom(24).hex()
        
        # Update last login
        update_last_login(username)
        
        return jsonify({
            'success': True,
            'api_key': api_key,
            'user': username
        })
    else:
        return jsonify({'success': False, 'error': 'Invalid credentials'}), 401

@api_bp.route('/stats')
def get_stats():
    """Get dashboard statistics."""
    days = int(request.args.get('days', 7))
    stats = dashboard_service.get_dashboard_stats(days=days)
    return jsonify(stats)

@api_bp.route('/processes')
def get_processes():
    """Get list of processes with filtering."""
    # Get filter parameters
    days = int(request.args.get('days', 7))
    status = request.args.get('status', 'all')
    search = request.args.get('search', '')
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 20))
    
    # Get processes
    result = dashboard_service.get_recent_processes(
        days=days,
        status=status,
        search_term=search,
        page=page,
        per_page=per_page
    )
    
    return jsonify(result)

@api_bp.route('/process/<string:process_id>')
def get_process(process_id):
    """Get detailed information about a specific process."""
    process = dashboard_service.get_process_details(process_id)
    if not process:
        return jsonify({'error': 'Process not found'}), 404
        
    return jsonify(process)

@api_bp.route('/process/<string:process_id>/timeline')
def get_process_timeline(process_id):
    """Get timeline events for a process."""
    timeline = dashboard_service.get_process_timeline(process_id)
    return jsonify(timeline)

@api_bp.route('/process/<string:process_id>/documents')
def get_process_documents(process_id):
    """Get documents associated with a process."""
    documents = dashboard_service.get_process_documents(process_id)
    return jsonify(documents)

@api_bp.route('/process/<string:process_id>/resume', methods=['POST'])
def resume_process(process_id):
    """Resume a paused process."""
    data = request.get_json()
    notes = data.get('notes', '')
    
    result = process_service.resume_process(
        process_id=process_id,
        user_id=session.get('user_id', data.get('user_id')),
        notes=notes
    )
    
    if result['success']:
        return jsonify({'success': True})
    else:
        return jsonify({'success': False, 'error': result['error']}), 400

@api_bp.route('/process/<string:process_id>/cancel', methods=['POST'])
def cancel_process(process_id):
    """Cancel a process."""
    data = request.get_json()
    reason = data.get('reason', '')
    
    result = process_service.cancel_process(
        process_id=process_id,
        user_id=session.get('user_id', data.get('user_id')),
        reason=reason
    )
    
    if result['success']:
        return jsonify({'success': True})
    else:
        return jsonify({'success': False, 'error': result['error']}), 400

@api_bp.route('/processes/needing-attention')
def get_processes_needing_attention():
    """Get processes that need attention/manual review."""
    processes = dashboard_service.get_processes_needing_attention()
    return jsonify(processes)

@api_bp.route('/reports/process-stats')
def get_process_stats():
    """Get process statistics for reporting."""
    start_date = request.args.get('start_date', 
                                (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'))
    end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))
    
    stats = dashboard_service.get_process_stats(start_date, end_date)
    return jsonify(stats)

@api_bp.route('/reports/document-stats')
def get_document_stats():
    """Get document statistics for reporting."""
    start_date = request.args.get('start_date', 
                                (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'))
    end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))
    
    stats = dashboard_service.get_document_stats(start_date, end_date)
    return jsonify(stats)

@api_bp.route('/reports/error-stats')
def get_error_stats():
    """Get error statistics for reporting."""
    start_date = request.args.get('start_date', 
                                (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'))
    end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))
    
    stats = dashboard_service.get_error_stats(start_date, end_date)
    return jsonify(stats)

@api_bp.route('/health')
def health_check():
    """API health check endpoint."""
    # Check process control system
    process_control = container.resolve(ProcessControl)
    db_status = "ok"
    
    try:
        process_control.get_stats()
    except Exception as e:
        logger.error(f"Health check error: {str(e)}")
        db_status = "error"
    
    return jsonify({
        'status': 'ok' if db_status == 'ok' else 'degraded',
        'timestamp': datetime.now().isoformat(),
        'components': {
            'database': db_status,
            'api': 'ok'
        }
    })