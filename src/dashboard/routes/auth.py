from flask import Blueprint, render_template, request, redirect, url_for, flash, session
import logging
from datetime import datetime
import os
import hashlib
import json

logger = logging.getLogger(__name__)
auth_bp = Blueprint('auth', __name__)

def get_users():
    """Get users from JSON file, create default if doesn't exist."""
    users_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config', 'users.json')
    
    # Create default users file if it doesn't exist
    if not os.path.exists(users_file):
        os.makedirs(os.path.dirname(users_file), exist_ok=True)
        
        # Create admin user with hashed password
        default_password = "admin123"  # This should be changed in production
        salt = os.urandom(32).hex()
        hashed_password = hashlib.sha256((default_password + salt).encode()).hexdigest()
        
        default_users = {
            "admin": {
                "username": "admin",
                "name": "Administrator",
                "email": "admin@example.com",
                "password_hash": hashed_password,
                "salt": salt,
                "role": "admin",
                "created_at": datetime.now().isoformat(),
                "last_login": None
            }
        }
        
        with open(users_file, 'w') as f:
            json.dump(default_users, f, indent=2)
    
    # Read users from file
    try:
        with open(users_file, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading users file: {str(e)}")
        return {}

def save_users(users):
    """Save users to JSON file."""
    users_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config', 'users.json')
    try:
        with open(users_file, 'w') as f:
            json.dump(users, f, indent=2)
        return True
    except Exception as e:
        logger.error(f"Error saving users file: {str(e)}")
        return False

def verify_password(username, password):
    """Verify password for user."""
    users = get_users()
    if username not in users:
        return False
        
    user = users[username]
    salt = user.get('salt', '')
    password_hash = hashlib.sha256((password + salt).encode()).hexdigest()
    
    return password_hash == user['password_hash']

def update_last_login(username):
    """Update last login time for user."""
    users = get_users()
    if username in users:
        users[username]['last_login'] = datetime.now().isoformat()
        save_users(users)

@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    """Handle user login."""
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        
        if not username or not password:
            flash('Username and password are required', 'error')
            return render_template('login.html')
        
        if verify_password(username, password):
            users = get_users()
            user = users[username]
            
            # Store user info in session
            session['user_id'] = username
            session['user_name'] = user.get('name', username)
            session['user_role'] = user.get('role', 'user')
            
            # Update last login time
            update_last_login(username)
            
            # Log the login
            logger.info(f"User {username} logged in")
            
            # Redirect to dashboard
            return redirect(url_for('main.index'))
        else:
            flash('Invalid username or password', 'error')
            logger.warning(f"Failed login attempt for username: {username}")
    
    return render_template('login.html')

@auth_bp.route('/logout')
def logout():
    """Handle user logout."""
    # Log the logout
    if 'user_id' in session:
        logger.info(f"User {session['user_id']} logged out")
    
    # Clear session
    session.clear()
    
    flash('You have been logged out', 'info')
    return redirect(url_for('auth.login'))

@auth_bp.route('/profile')
def profile():
    """Display user profile page."""
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
        
    username = session['user_id']
    users = get_users()
    
    if username not in users:
        flash('User not found', 'error')
        return redirect(url_for('main.index'))
        
    user = users[username]
    
    return render_template('profile.html', user=user)

@auth_bp.route('/change_password', methods=['POST'])
def change_password():
    """Change user password."""
    if 'user_id' not in session:
        return redirect(url_for('auth.login'))
        
    current_password = request.form.get('current_password')
    new_password = request.form.get('new_password')
    confirm_password = request.form.get('confirm_password')
    
    if not current_password or not new_password or not confirm_password:
        flash('All fields are required', 'error')
        return redirect(url_for('auth.profile'))
    
    if new_password != confirm_password:
        flash('New passwords do not match', 'error')
        return redirect(url_for('auth.profile'))
    
    username = session['user_id']
    
    if not verify_password(username, current_password):
        flash('Current password is incorrect', 'error')
        return redirect(url_for('auth.profile'))
    
    # Update password
    users = get_users()
    user = users[username]
    
    # Generate new salt and hash
    salt = os.urandom(32).hex()
    password_hash = hashlib.sha256((new_password + salt).encode()).hexdigest()
    
    user['password_hash'] = password_hash
    user['salt'] = salt
    
    if save_users(users):
        flash('Password changed successfully', 'success')
        logger.info(f"User {username} changed their password")
    else:
        flash('Failed to change password', 'error')
    
    return redirect(url_for('auth.profile'))