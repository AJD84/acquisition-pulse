#!/usr/bin/env python3
"""
Test script to verify Google Ads API setup
Run this after you get your Developer Token
"""

import os
import sys

def check_environment():
    """Check if all required environment variables are set"""
    print("🔍 Checking environment variables...")
    
    required = {
        "GOOGLE_CLIENT_ID": os.environ.get("GOOGLE_CLIENT_ID"),
        "GOOGLE_CLIENT_SECRET": os.environ.get("GOOGLE_CLIENT_SECRET"),
        "DASHBOARD_SECRET_KEY": os.environ.get("DASHBOARD_SECRET_KEY"),
        "GOOGLE_ADS_DEVELOPER_TOKEN": os.environ.get("GOOGLE_ADS_DEVELOPER_TOKEN"),
    }
    
    missing = []
    for key, value in required.items():
        if value:
            # Mask sensitive values
            if len(value) > 10:
                masked = value[:4] + "..." + value[-4:]
            else:
                masked = "***"
            print(f"  ✅ {key}: {masked}")
        else:
            print(f"  ❌ {key}: NOT SET")
            missing.append(key)
    
    if missing:
        print(f"\n❌ Missing required variables: {', '.join(missing)}")
        return False
    else:
        print("\n✅ All environment variables set!")
        return True

def check_packages():
    """Check if required packages are installed"""
    print("\n🔍 Checking required packages...")
    
    packages = [
        "fastapi",
        "uvicorn", 
        "cryptography",
        "requests",
        "google-ads"
    ]
    
    missing = []
    for pkg in packages:
        try:
            __import__(pkg.replace("-", "."))
            print(f"  ✅ {pkg}")
        except ImportError:
            print(f"  ❌ {pkg}: NOT INSTALLED")
            missing.append(pkg)
    
    if missing:
        print(f"\n❌ Missing packages: {', '.join(missing)}")
        print(f"   Run: pip install {' '.join(missing)}")
        return False
    else:
        print("\n✅ All packages installed!")
        return True

def check_backend():
    """Check if backend is running"""
    print("\n🔍 Checking backend status...")
    
    try:
        import requests
        response = requests.get("http://localhost:8000/health", timeout=2)
        if response.status_code == 200:
            print("  ✅ Backend is running on port 8000")
            return True
        else:
            print(f"  ❌ Backend responded with status {response.status_code}")
            return False
    except requests.exceptions.ConnectionError:
        print("  ❌ Backend is NOT running")
        print("     Start it with: uvicorn backend:app --reload --port 8000")
        return False
    except Exception as e:
        print(f"  ❌ Error checking backend: {e}")
        return False

def main():
    print("=" * 60)
    print("Google Ads API Setup Verification")
    print("=" * 60)
    
    checks = [
        check_environment(),
        check_packages(),
        check_backend()
    ]
    
    print("\n" + "=" * 60)
    if all(checks):
        print("✅ ALL CHECKS PASSED!")
        print("\nYou're ready to use the Google Ads API!")
        print("\nNext steps:")
        print("  1. Go to http://localhost:8501")
        print("  2. Click 'Connect Google Ads'")
        print("  3. Sign in with your Google account")
        print("  4. Real data should sync automatically!")
    else:
        print("❌ SOME CHECKS FAILED")
        print("\nFix the issues above and run this script again")
        sys.exit(1)
    print("=" * 60)

if __name__ == "__main__":
    main()
