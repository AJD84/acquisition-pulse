"""Simple OAuth redirect handler for ad platform connectors (dev only).

Run locally for development to complete the OAuth flow and store encrypted
tokens via the connector's `store_token_for_meta` helper.

Usage (dev):
  export DASHBOARD_SECRET_KEY="<fernet-key>"
  export META_CLIENT_ID="..."
  export META_CLIENT_SECRET="..."
  uvicorn connectors.oauth_server:app --reload --port 8888

Then configure your Facebook App to use redirect URI:
  http://localhost:8888/callback

This server is intentionally minimal and intended for local development/demo only.
Do not expose it in production without HTTPS and proper secrets management.
"""
import os
import logging
from typing import Optional

import requests
import urllib.parse

from connectors import meta_connector
try:
    from connectors import google_connector
except Exception:
    google_connector = None
try:
    from connectors import tiktok_connector
except Exception:
    tiktok_connector = None
from connectors import state as state_helper

# FastAPI is optional for in-process testing. Defer FastAPI imports so this
# module can be imported and the core helpers invoked even if `fastapi` is not
# installed in the environment. When FastAPI is available we expose the same
# HTTP routes by creating an `app` instance.
try:
    from fastapi import FastAPI, Request
    from fastapi.responses import HTMLResponse, RedirectResponse
    _FASTAPI_AVAILABLE = True
except Exception:
    FastAPI = None  # type: ignore
    Request = None  # type: ignore
    HTMLResponse = None  # type: ignore
    RedirectResponse = None  # type: ignore
    _FASTAPI_AVAILABLE = False

if _FASTAPI_AVAILABLE:
    app = FastAPI()

logger = logging.getLogger("oauth_server")


def _get_env(name: str) -> Optional[str]:
    return os.environ.get(name)


def _start_oauth_core(platform: Optional[str] = None, state: Optional[str] = None):
    """Start an OAuth flow for a given platform.

    Behavior:
    - If `USE_REAL_OAUTH` is truthy and the provider client id is configured,
      redirect to the provider authorization URL.
    - Otherwise render a tiny simulated consent page with a "Grant access"
      button that will call back to `/callback` with a simulated token.
    """
    use_real = str(os.environ.get('USE_REAL_OAUTH', '')).lower() in ('1', 'true', 'yes')
    plat = (platform or '').lower()

    # Platform-specific real-OAuth redirect builders
    if use_real:
        try:
            if plat in ('meta', 'facebook'):
                client_id = _get_env('META_CLIENT_ID')
                redirect_uri = _get_env('META_REDIRECT_URI') or 'http://localhost:8888/callback'
                if client_id:
                    auth_url = (
                        f"https://www.facebook.com/v17.0/dialog/oauth?client_id={client_id}"
                        f"&redirect_uri={urllib.parse.quote_plus(redirect_uri)}&scope=ads_read,ads_management"
                    )
                    if state:
                        auth_url = auth_url + f"&state={urllib.parse.quote_plus(state)}"
                    # Return a redirect URL string when running outside FastAPI.
                    return auth_url if not _FASTAPI_AVAILABLE else RedirectResponse(auth_url)

            if plat in ('google', 'google_ads'):
                client_id = _get_env('GOOGLE_CLIENT_ID')
                redirect = _get_env('OAUTH_REDIRECT_URI') or 'http://localhost:8888/callback'
                if client_id:
                    base = 'https://accounts.google.com/o/oauth2/v2/auth'
                    q = {
                        'client_id': client_id,
                        'redirect_uri': redirect,
                        'response_type': 'code',
                        'scope': os.environ.get('GOOGLE_OAUTH_SCOPE', 'https://www.googleapis.com/auth/adwords'),
                        'access_type': 'offline',
                    }
                    if state:
                        q['state'] = state
                    redirect_to = base + '?' + urllib.parse.urlencode(q)
                    return redirect_to if not _FASTAPI_AVAILABLE else RedirectResponse(redirect_to)

            if plat in ('tiktok',):
                client_id = _get_env('TIKTOK_CLIENT_ID')
                redirect = _get_env('OAUTH_REDIRECT_URI') or 'http://localhost:8888/callback'
                if client_id:
                    base = os.environ.get('TIKTOK_OAUTH_URL', 'https://ads.tiktok.com/marketing_api/auth')
                    q = {'app_id': client_id, 'redirect_uri': redirect}
                    if state:
                        q['state'] = state
                    redirect_to = base + '?' + urllib.parse.urlencode(q)
                    return redirect_to if not _FASTAPI_AVAILABLE else RedirectResponse(redirect_to)
        except Exception:
            pass

    # Otherwise render a small simulated consent page to let developers
    # click 'Grant access' which posts back a simulated token to /callback.
    safe_state = urllib.parse.quote_plus(state) if state else ''
    plat_label = platform or 'provider'
    simulated_token = f"SIMULATED_{(plat or 'prov').upper()}_TOKEN"
    # Build callback URL with token and state
    cb = '/callback'
    qs = []
    if platform:
        qs.append('platform=' + urllib.parse.quote_plus(platform))
    qs.append('token=' + urllib.parse.quote_plus(simulated_token))
    if state:
        qs.append('state=' + safe_state)
    cb_url = cb + '?' + '&'.join(qs)

    html = f"""
    <html>
      <head><title>Simulated OAuth Consent</title></head>
      <body>
        <h2>Simulated OAuth Consent for {plat_label}</h2>
        <p>This development server is simulating the OAuth consent flow.</p>
        <p><a href="{cb_url}">Grant access (simulate)</a></p>
        <p><a href="/">Cancel</a></p>
      </body>
    </html>
    """
    return html if not _FASTAPI_AVAILABLE else HTMLResponse(html)


def start_oauth(platform: Optional[str] = None, state: Optional[str] = None):
    """Public wrapper for starting OAuth flows.

    If FastAPI is available this function is registered as the route handler
    on `app`. Otherwise it behaves as a plain function returning a string
    (either an HTML page or a redirect URL) so tests and in-process callers
    can simulate the flow without FastAPI installed.
    """
    return _start_oauth_core(platform=platform, state=state)


def simulate_start_oauth(platform: Optional[str] = None, state: Optional[str] = None) -> str:
    """Simulation-only wrapper that always returns a string (HTML or redirect URL).
    
    Use this for in-process testing when you need predictable string output
    regardless of whether FastAPI is installed.
    """
    # Temporarily disable FastAPI responses
    global _FASTAPI_AVAILABLE
    original = _FASTAPI_AVAILABLE
    _FASTAPI_AVAILABLE = False
    try:
        result = _start_oauth_core(platform=platform, state=state)
        return result
    finally:
        _FASTAPI_AVAILABLE = original


if _FASTAPI_AVAILABLE:
    app.get('/start')(start_oauth)


def _callback_core(request, platform: Optional[str] = None, code: Optional[str] = None, token: Optional[str] = None, error: Optional[str] = None, state: Optional[str] = None):
    """Handle the OAuth callback, exchange `code` for a short-lived token then
    exchange to a long-lived token and store it encrypted using the connector helper.
    """
    if error:
        return (f"OAuth error: {error}", 400) if not _FASTAPI_AVAILABLE else HTMLResponse(f"OAuth error: {error}", status_code=400)
    plat = (platform or '').lower()

    # If a simulated token was provided by the dev server, use it directly
    long_token = None
    if token:
        long_token = token

    # If a real provider returned a code and this platform has exchange support,
    # attempt to exchange it. Start with Meta (Facebook) as before.
    if not long_token and code:
        if plat in ('meta', 'facebook'):
            client_id = _get_env('META_CLIENT_ID')
            client_secret = _get_env('META_CLIENT_SECRET')
            redirect_uri = _get_env('META_REDIRECT_URI') or 'http://localhost:8888/callback'

            if not client_id or not client_secret:
                return HTMLResponse("Missing client credentials in environment", status_code=500)

            # Exchange code for a short-lived token
            token_url = 'https://graph.facebook.com/v17.0/oauth/access_token'
            params = {
                'client_id': client_id,
                'redirect_uri': redirect_uri,
                'client_secret': client_secret,
                'code': code,
            }
            r = requests.get(token_url, params=params, timeout=10)
            if r.status_code != 200:
                logger.exception('Failed to exchange code for token: %s', r.text)
                try:
                    logging.getLogger('profit_dashboard').exception('OAuth token exchange failed: %s', r.text)
                except Exception:
                    pass
                return HTMLResponse(f'Failed to exchange code: {r.text}', status_code=500)

            data = r.json()
            short_lived = data.get('access_token')
            if not short_lived:
                return HTMLResponse('No access_token returned by provider', status_code=500)

            # Exchange for long-lived token
            exchange_url = 'https://graph.facebook.com/v17.0/oauth/access_token'
            params2 = {
                'grant_type': 'fb_exchange_token',
                'client_id': client_id,
                'client_secret': client_secret,
                'fb_exchange_token': short_lived,
            }
            r2 = requests.get(exchange_url, params=params2, timeout=10)
            if r2.status_code != 200:
                logger.exception('Failed to exchange to long-lived token: %s', r2.text)
                try:
                    logging.getLogger('profit_dashboard').exception('OAuth long-lived exchange failed: %s', r2.text)
                except Exception:
                    pass
                # fallback: try to store the short-lived token
                long_token = short_lived
            else:
                long_token = r2.json().get('access_token')

        elif plat in ('google', 'google_ads'):
            # For Google, real-code exchange support could be added here if desired.
            # For now, require `token` (simulated) or that the integrator sets
            # USE_REAL_OAUTH and proper client credentials; otherwise return an
            # informative response.
            if not token:
                return HTMLResponse('Google code exchange not implemented in dev server; use simulated token or configure real OAuth.', status_code=400)
        elif plat in ('tiktok',):
            if not token:
                return HTMLResponse('TikTok code exchange not implemented in dev server; use simulated token or configure real OAuth.', status_code=400)
        else:
            # Unknown platform — accept token only
            if not token:
                return HTMLResponse('Unknown platform and no token provided', status_code=400)

    if not long_token:
        return ("Missing token or code in callback", 400) if not _FASTAPI_AVAILABLE else HTMLResponse('Missing token or code in callback', status_code=400)

    # At this point `long_token` should be set either from a simulated token
    # or from earlier provider code exchange logic. Proceed to verify state
    # and persist the token to connector storage.

    # Validate and map `state` (signed token) to an email, if present.
    user_email = None
    if state:
        try:
            user_email = state_helper.verify_state_token(state)
            if not user_email:
                # If verification fails, fall back to using raw state as an email
                # only for development scenarios where HMAC key may not be provided.
                if '@' in state:
                    user_email = state
                else:
                    return HTMLResponse('Invalid or expired state token', status_code=400)
        except Exception:
            if '@' in state:
                user_email = state
            else:
                return HTMLResponse('Invalid state token', status_code=400)

    # Save token using connector helper (encrypted). If we resolved a user
    # email from the signed state, store the token for that user; otherwise
    # fall back to legacy single-slot storage.
    stored_ok = False
    rows_returned = None
    try:
        if plat in ('meta', 'facebook'):
            if user_email:
                stored_ok = meta_connector.store_token_for_meta_user(long_token, user_email=user_email)
            else:
                stored_ok = meta_connector.store_token_for_meta(long_token)
            # Trigger an immediate manual sync to fetch spend and persist CSVs
            try:
                df_new = meta_connector.fetch_spend_and_persist(long_token, user_email=user_email)
                if df_new is not None:
                    rows_returned = len(df_new)
            except Exception:
                pass

        elif plat in ('google', 'google_ads'):
            if google_connector is None:
                stored_ok = False
            else:
                if user_email:
                    stored_ok = google_connector.store_token_for_google_user(long_token, user_email=user_email)
                else:
                    stored_ok = google_connector.store_token_for_google_user(long_token)
                try:
                    df_new = google_connector.fetch_spend_and_persist(long_token, user_email=user_email)
                    if df_new is not None:
                        rows_returned = len(df_new)
                except Exception:
                    pass

        elif plat in ('tiktok',):
            if tiktok_connector is None:
                stored_ok = False
            else:
                if user_email:
                    stored_ok = tiktok_connector.store_token_for_tiktok_user(long_token, user_email=user_email)
                else:
                    stored_ok = tiktok_connector.store_token_for_tiktok_user(long_token)
                try:
                    df_new = tiktok_connector.fetch_spend_and_persist(long_token, user_email=user_email)
                    if df_new is not None:
                        rows_returned = len(df_new)
                except Exception:
                    pass

        else:
            # Unknown platform: attempt to store via meta connector as fallback
            try:
                stored_ok = meta_connector.store_token_for_meta_user(long_token, user_email=user_email) if user_email else meta_connector.store_token_for_meta(long_token)
            except Exception:
                stored_ok = False
    except Exception:
        stored_ok = False

    if not stored_ok:
        try:
            logging.getLogger('profit_dashboard').error('Token obtained but failed to store encrypted token via oauth_server callback.')
        except Exception:
            pass
        msg = 'Token obtained but failed to store encrypted token. Ensure DASHBOARD_SECRET_KEY is set and keyring is available if desired.'
        return (msg, 500) if not _FASTAPI_AVAILABLE else HTMLResponse(msg, status_code=500)

    msg = 'Token obtained and stored. You can close this window and return to the dashboard.'
    if user_email:
        msg += f' Stored for user: {user_email}.'
    if rows_returned is not None:
        msg += f' Manual sync fetched {rows_returned} rows and updated `data/spend.csv`.'
    return msg if not _FASTAPI_AVAILABLE else HTMLResponse(msg)


def callback(request=None, platform: Optional[str] = None, code: Optional[str] = None, token: Optional[str] = None, error: Optional[str] = None, state: Optional[str] = None):
    """Public wrapper for the OAuth callback.

    When FastAPI is available this function is registered as the `/callback`
    route. When not available it can be invoked directly by tests or the
    dashboard startup code to simulate a provider callback.
    """
    return _callback_core(request, platform=platform, code=code, token=token, error=error, state=state)


def simulate_callback(platform: Optional[str] = None, code: Optional[str] = None, token: Optional[str] = None, error: Optional[str] = None, state: Optional[str] = None) -> str:
    """Simulation-only wrapper that always returns a string message.
    
    Use this for in-process testing when you need predictable string output
    regardless of whether FastAPI is installed.
    """
    # Temporarily disable FastAPI responses
    global _FASTAPI_AVAILABLE
    original = _FASTAPI_AVAILABLE
    _FASTAPI_AVAILABLE = False
    try:
        result = _callback_core(None, platform=platform, code=code, token=token, error=error, state=state)
        # Result is either a string or a (string, status_code) tuple
        if isinstance(result, tuple):
            return result[0]
        return result
    finally:
        _FASTAPI_AVAILABLE = original


if _FASTAPI_AVAILABLE:
    app.get('/callback')(callback)
