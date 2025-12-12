"""State helpers for OAuth flows — HMAC-signed short-lived state tokens.

These helpers let the dashboard generate a short-lived signed `state` value
that encodes the user's email and a timestamp. The OAuth callback verifies
the signature and timestamp before storing the returned token against the
associated email. This avoids placing raw emails in redirect URLs.

Uses `DASHBOARD_HMAC_KEY` if present, otherwise falls back to
`DASHBOARD_SECRET_KEY`. If no key is configured the module will still
produce/verify tokens in a non-signed (base64) fallback mode but verification
will be weaker and tokens will only be accepted while unexpired.
"""
import os
import hmac
import hashlib
import base64
import time
from typing import Optional


def _get_key() -> Optional[bytes]:
    k = os.environ.get('DASHBOARD_HMAC_KEY') or os.environ.get('DASHBOARD_SECRET_KEY')
    if not k:
        return None
    return k.encode() if isinstance(k, str) else k


def make_state_token(email: str, ttl_seconds: int = 600) -> str:
    """Create a URL-safe state token for `email` valid for `ttl_seconds`.

    Returns a string suitable for embedding in an OAuth `state` parameter.
    The token format is: base64url(payload).hexsig  where payload = "email|ts".
    If no HMAC key is configured the function returns the base64(payload)
    without a signature (less secure).
    """
    ts = int(time.time())
    payload = f"{email}|{ts}"
    key = _get_key()
    b64 = base64.urlsafe_b64encode(payload.encode()).decode()
    if key:
        sig = hmac.new(key, payload.encode(), hashlib.sha256).hexdigest()
        return f"{b64}.{sig}"
    return b64


def verify_state_token(token: str, max_age_seconds: int = 600) -> Optional[str]:
    """Verify a state token and return the embedded email if valid.

    Returns the email string on success, or `None` if verification fails or
    the token is expired.
    """
    key = _get_key()
    try:
        if '.' in token and key:
            b64payload, sig = token.rsplit('.', 1)
            payload = base64.urlsafe_b64decode(b64payload.encode()).decode()
            expected = hmac.new(key, payload.encode(), hashlib.sha256).hexdigest()
            if not hmac.compare_digest(expected, sig):
                return None
        else:
            # unsigned fallback (we still accept it but only check expiry)
            payload = base64.urlsafe_b64decode(token.encode()).decode()

        email, ts_s = payload.split('|', 1)
        ts = int(ts_s)
        if int(time.time()) - ts > max_age_seconds:
            return None
        return email
    except Exception:
        return None
