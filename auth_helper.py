"""
User authentication helper for Profit Dashboard.
Handles credential verification using bcrypt password hashing.
"""

import json
import os
from typing import Optional, Dict, Any

try:
    import bcrypt
    _BCRYPT_AVAILABLE = True
except ImportError:
    _BCRYPT_AVAILABLE = False

CREDENTIALS_FILE = os.path.join(os.path.dirname(__file__), 'credentials.json')


def load_credentials() -> Dict[str, Any]:
    """Load credentials from credentials.json file."""
    if not os.path.exists(CREDENTIALS_FILE):
        return {"users": []}
    
    try:
        with open(CREDENTIALS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading credentials: {e}")
        return {"users": []}


def verify_user(email: str, password: str) -> Optional[Dict[str, Any]]:
    """
    Verify user credentials. Returns user dict if valid, None otherwise.
    
    Args:
        email: User email
        password: Plain text password
    
    Returns:
        User dict (without password hash) if credentials are valid, None otherwise
    """
    credentials = load_credentials()
    
    for user in credentials.get('users', []):
        if user.get('email', '').lower() == email.lower():
            # Verify password
            if _BCRYPT_AVAILABLE:
                stored_hash = user.get('password_hash', '')
                if stored_hash and bcrypt.checkpw(password.encode(), stored_hash.encode()):
                    # Return user info without password hash
                    return {
                        'email': user['email'],
                        'name': user.get('name', email),
                        'role': user.get('role', 'user')
                    }
            else:
                # Fallback: simple string comparison (NOT SECURE - only for dev)
                # This is a safety fallback if bcrypt isn't available
                import hashlib
                plain_hash = hashlib.sha256(password.encode()).hexdigest()
                if user.get('password_hash') == plain_hash:
                    return {
                        'email': user['email'],
                        'name': user.get('name', email),
                        'role': user.get('role', 'user')
                    }
    
    return None


def hash_password(password: str) -> str:
    """
    Hash a password for storage in credentials.json.
    
    Args:
        password: Plain text password
    
    Returns:
        Hashed password string
    """
    if _BCRYPT_AVAILABLE:
        return bcrypt.hashpw(password.encode(), bcrypt.gensalt(12)).decode()
    else:
        # Fallback: simple SHA256 (NOT SECURE - only for dev)
        import hashlib
        return hashlib.sha256(password.encode()).hexdigest()


def add_user(email: str, password: str, name: str = None, role: str = 'user') -> bool:
    """
    Add a new user to credentials.json.
    
    Args:
        email: User email
        password: Plain text password
        name: User display name (optional)
        role: User role (default: 'user')
    
    Returns:
        True if user was added, False otherwise
    """
    credentials = load_credentials()
    
    # Check if user already exists
    for user in credentials.get('users', []):
        if user.get('email', '').lower() == email.lower():
            return False  # User already exists
    
    # Add new user
    new_user = {
        'email': email,
        'password_hash': hash_password(password),
        'name': name or email.split('@')[0],
        'role': role
    }
    
    credentials['users'].append(new_user)
    
    # Save to file
    try:
        with open(CREDENTIALS_FILE, 'w', encoding='utf-8') as f:
            json.dump(credentials, f, indent=2)
        return True
    except Exception as e:
        print(f"Error saving credentials: {e}")
        return False


def get_test_hashes():
    """Generate test password hashes for documentation."""
    if not _BCRYPT_AVAILABLE:
        print("bcrypt not available; install with: pip install bcrypt")
        return
    
    test_passwords = {
        'admin@example.com': 'password123',  # Hash this as example
        'user@example.com': 'demo123'
    }
    
    print("Test password hashes:")
    for email, password in test_passwords.items():
        hashed = hash_password(password)
        print(f"  {email}: {hashed}")
