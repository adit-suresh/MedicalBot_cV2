import os
import sys
from flask import Flask, render_template, jsonify, request
from datetime import datetime

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

# Now we can import our modules
from src.utils.process_control import ProcessControl, ProcessStatus
from src.utils.error_handler import ErrorHandler

app = Flask(__name__)
process_control = ProcessControl()
error_handler = ErrorHandler()

@app.route('/')
def dashboard():
    """Main dashboard view."""
    return render_template('dashboard.html')

@app.route('/api/processes')
def get_processes():
    """Get all processes."""
    processes = process_control.get_all_processes()
    return jsonify(processes)

@app.route('/api/process/<process_id>')
def get_process(process_id):
    """Get specific process details."""
    process = process_control.get_process_status(process_id)
    return jsonify(process)

@app.route('/api/process/<process_id>/control', methods=['POST'])
def control_process(process_id):
    """Control process (pause/resume/etc)."""
    action = request.json.get('action')
    if action == 'pause':
        process_control.pause_process(
            process_id, 
            reason="Manual pause from dashboard"
        )
    elif action == 'resume':
        process_control.resume_process(process_id)
    
    return jsonify({"status": "success"})

@app.route('/api/stats')
def get_stats():
    """Get system statistics."""
    error_stats = error_handler.get_error_stats()
    process_stats = process_control.get_stats()
    
    return jsonify({
        "errors": error_stats,
        "processes": process_stats
    })

@app.route('/api/errors')
def get_errors():
    """Get error logs."""
    return jsonify(error_handler.get_error_stats())

if __name__ == '__main__':
    # Verify paths
    print("Project root:", project_root)
    print("Python path:", sys.path)
    
    # Create necessary directories
    os.makedirs('data', exist_ok=True)
    
    # Initialize database if needed
    process_control._init_db()
    
    # Run the app
    app.run(debug=True, port=5000)