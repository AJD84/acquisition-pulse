#!/usr/bin/env python3
"""
Setup script to generate bcrypt password hashes for credentials.json.
Run this to create test users or update passwords.
"""

import json
import os

try:
    import bcrypt
    HAS_BCRYPT = True
except ImportError:
    HAS_BCRYPT = False
    print("Warning: bcrypt not installed. Install with: pip install bcrypt")


def generate_hash(password: str) -> str:
    """Generate bcrypt hash for a password."""
    if HAS_BCRYPT:
        return bcrypt.hashpw(password.encode(), bcrypt.gensalt(12)).decode()
    else:
        import hashlib
        return hashlib.sha256(password.encode()).hexdigest()


def setup_credentials():
    """Create default credentials.json with test users."""
    
    # Test users: email -> password
    test_users = {
        'admin@example.com': 'admin123',
        'user@example.com': 'user123',
        'demo@example.com': 'demo123'
    }
    
    credentials = {
        'users': [
            {
                'email': 'admin@example.com',
                'password_hash': generate_hash('admin123'),
                'name': 'Admin User',
                'role': 'admin'
            },
            {
                'email': 'user@example.com',
                'password_hash': generate_hash('user123'),
                'name': 'Demo User',
                'role': 'user'
            },
            {
                'email': 'demo@example.com',
                'password_hash': generate_hash('demo123'),
                'name': 'Demo Account',
                'role': 'user'
            }
        ]
    }
    
    # Write credentials.json
    with open('credentials.json', 'w', encoding='utf-8') as f:
        json.dump(credentials, f, indent=2)
    
    print("✅ credentials.json created with test users:")
    for user in credentials['users']:
        print(f"   - {user['email']} (role: {user['role']})")
    
    print("\nTest credentials:")
    for email, password in test_users.items():
        print(f"   - {email}: {password}")
    
    print("\n⚠️  Change these passwords in production!")
    print("   Edit credentials.json and run this script again to update.")


if __name__ == '__main__':
    setup_credentials()
