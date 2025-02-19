import os
import sys
import logging
from datetime import datetime
from flask import Flask, render_template, redirect, url_for, flash, request, session

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

# Import routes
from src.dashboard.routes.main import main_bp
from src.dashboard.routes.auth import auth_bp
from src.dashboard.routes.api import api_bp

# Import dependencies
from src.utils.dependency_container import container
from src.utils.process_control import ProcessControl
from src.database.db_manager import DatabaseManager
from src.config.app_config import configure_dependencies

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(project_root, 'logs', 'dashboard.log'))
    ]
)
logger = logging.getLogger(__name__)

def create_app(test_config=None):
    """Create and configure the Flask application."""
    # Create app instance
    app = Flask(__name__, 
                static_folder='static',
                template_folder='templates')
    
    # Load config
    if test_config is None:
        # Load the instance config, if it exists, when not testing
        app.config.from_mapping(
            SECRET_KEY=os.environ.get('SECRET_KEY', 'dev_key_only_for_development'),
            DATABASE=os.path.join(project_root, 'data', 'dashboard.db'),
            UPLOAD_FOLDER=os.path.join(project_root, 'data', 'uploads'),
            DOWNLOAD_FOLDER=os.path.join(project_root, 'data', 'processed'),
            DEBUG=os.environ.get('FLASK_DEBUG', 'False') == 'True',
            SESSION_COOKIE_SECURE=os.environ.get('FLASK_ENV', 'development') == 'production',
            SESSION_COOKIE_HTTPONLY=True,
            SESSION_COOKIE_SAMESITE='Lax',
            MAX_CONTENT_LENGTH=10 * 1024 * 1024,  # 10MB max upload size
        )
    else:
        # Load the test config if passed in
        app.config.from_mapping(test_config)

    # Ensure directories exist
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    os.makedirs(app.config['DOWNLOAD_FOLDER'], exist_ok=True)
    
    # Configure dependencies
    configure_dependencies()
    
    # Register blueprints
    app.register_blueprint(main_bp)
    app.register_blueprint(auth_bp, url_prefix='/auth')
    app.register_blueprint(api_bp, url_prefix='/api')
    
    # Global error handler
    @app.errorhandler(404)
    def page_not_found(e):
        return render_template('404.html'), 404
    
    @app.errorhandler(500)
    def server_error(e):
        logger.error(f"Server error: {str(e)}")
        return render_template('500.html'), 500
    
    # Global context processor
    @app.context_processor
    def inject_global_data():
        return {
            'current_year': datetime.now().year,
            'app_name': 'Medical Document Automation',
            'app_version': '1.0.0'
        }
    
    # Access control
    @app.before_request
    def require_login():
        allowed_routes = ['auth.login', 'auth.logout', 'static']
        if (request.endpoint not in allowed_routes and 
            not request.endpoint.startswith('api.') and
            'user_id' not in session and
            request.endpoint != 'auth.login'):
            return redirect(url_for('auth.login'))
    
    return app


if __name__ == '__main__':
    # Create app
    app = create_app()
    
    # Run the app
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))