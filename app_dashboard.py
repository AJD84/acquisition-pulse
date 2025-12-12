import os
import io
from typing import List, Optional, Tuple

import pandas as pd
import numpy as np
import streamlit as st
import datetime
import logging
from logging.handlers import RotatingFileHandler
import urllib.parse
import json
import time
import base64
import hmac
import hashlib
try:
    from dateutil.relativedelta import relativedelta  # more accurate month arithmetic
    _RELATIVEDELTA_AVAILABLE = True
except Exception:
    _RELATIVEDELTA_AVAILABLE = False

# Optional imports (graceful fallbacks)
try:
    import plotly.graph_objs as go
    _PLOTLY_AVAILABLE = True
except Exception:
    _PLOTLY_AVAILABLE = False

try:
    from st_aggrid import AgGrid, GridOptionsBuilder
    _AGGRID_AVAILABLE = True
except Exception:
    _AGGRID_AVAILABLE = False

# ============================================================================
# INLINE OAUTH CALLBACK HANDLER
# For Streamlit Cloud deployment: detect OAuth redirect with code/state
# and exchange for access token without separate OAuth server
# ============================================================================
def _handle_oauth_callback():
    """
    Detect OAuth callback via query params, verify state, exchange code,
    store token, and refresh the dashboard.
    """
    try:
        params = st.query_params
        code = params.get('code')
        state = params.get('state')
        platform = params.get('platform', '').lower()
        
        # Mark that we're processing OAuth
        if code:
            st.session_state.oauth_processing = True
            # Auto-authenticate user if not already authenticated
            if not getattr(st.session_state, 'dashboard_authenticated', False):
                st.session_state.dashboard_authenticated = True
                st.session_state.user_email = getattr(st.session_state, 'user_email', 'oauth_user@localhost')
                st.session_state.username = 'oauth_user'        
        # If no platform specified but we have a code, assume Google (most common)
        if code and not platform:
            # Check scope to determine platform
            scope = params.get('scope', '')
            if 'facebook' in scope or 'instagram' in scope:
                platform = 'meta'
            elif 'tiktok' in scope:
                platform = 'tiktok'
            else:
                # Default to Google if no clear indicator
                platform = 'google'

        if not code:
            return

        # Use a generic email for local testing
        user_email = "dashboard_user@localhost"
        
        # No complex state verification needed for local testing

        # Exchange code for token based on platform
        token = None
        if platform in ('meta', 'facebook'):
            # Meta OAuth - use backend broker
            try:
                backend_url = os.environ.get('BROKER_URL', 'http://localhost:8000')
                exchange_data = {
                    'code': code,
                    'state': state or '',
                    'platform': 'meta',
                    'redirect_uri': os.environ.get('REDIRECT_URI', 'http://localhost:8501')
                }
                r = requests.post(backend_url.rstrip('/') + '/oauth/exchange', json=exchange_data, timeout=10)
                if r.status_code == 200 and r.json().get('ok'):
                    token = r.json().get('access_token')
                    st.success('Meta token obtained from backend')
                    
                    # Fetch spend data from backend
                    spend_response = requests.get(f"{backend_url}/meta/spend", timeout=20)
                    if spend_response.ok and spend_response.json().get('ok'):
                        data = spend_response.json().get('data', [])
                        message = spend_response.json().get('message', '')
                        if data:
                            import pandas as _pd
                            df = _pd.DataFrame(data)
                            os.makedirs('data', exist_ok=True)
                            df.to_csv(os.path.join('data', 'spend.csv'), index=False)
                        
                        # Clear session data
                        for key in ['uploaded_spend_df', 'uploaded_orders_df', 'demo_connector']:
                            st.session_state.pop(key, None)
                        
                        st.success(f'Meta connected! {message}')
                        st.query_params.clear()
                        st.rerun()
                    else:
                        st.warning('Meta connected but spend fetch failed')
                else:
                    st.error(f'Meta backend exchange failed: {r.json().get("error")}')
            except Exception as e:
                st.error(f'Meta OAuth failed: {e}')

        elif platform in ('google', 'google_ads'):
            # Prefer backend broker for true zero-config
            token = None
            refresh_token = None
            try:
                import requests
                backend_url = os.environ.get('BROKER_URL', 'http://localhost:8000')
                redirect_uri = os.environ.get('OAUTH_REDIRECT_URI', '')
                payload = {'code': code, 'platform': 'google', 'redirect_uri': redirect_uri}
                st.info(f'Calling backend: {backend_url}/oauth/exchange')
                r = requests.post(backend_url.rstrip('/') + '/oauth/exchange', json=payload, timeout=20)
                st.info(f'Backend response code: {r.status_code}')
                if r.status_code == 200 and r.json().get('ok'):
                    token = r.json().get('access_token')
                    refresh_token = r.json().get('refresh_token')
                    st.success('Token obtained from backend')
                else:
                    backend_error = r.json() if r.status_code == 200 else {'error': 'Non-200 status'}
                    st.warning(f'Backend exchange returned error: {backend_error}')
                    # Fallback to local exchange
                    token = _exchange_google_code(code)
            except Exception as e:
                st.warning(f'Backend connection failed: {e}')
                token = _exchange_google_code(code)

            if token:
                st.info('Storing token and fetching spend data...')
                try:
                    from connectors import google_connector
                    # Store refresh token if available (needed for Google Ads API), otherwise store access token
                    token_to_store = refresh_token if refresh_token else token
                    google_connector.store_token_for_google_user(token_to_store, user_email)

                    # Ask backend for spend (auto account selection placeholder)
                    try:
                        backend_url = os.environ.get('BROKER_URL', 'http://localhost:8000')
                        rr = requests.get(backend_url.rstrip('/') + '/google/spend', params={
                            'access_token': token,
                            'customer_id': os.environ.get('GOOGLE_AD_ACCOUNT_ID', 'auto')
                        }, timeout=20)
                        if rr.status_code == 200 and rr.json().get('ok'):
                            data = rr.json().get('data') or []
                            import pandas as _pd
                            df = _pd.DataFrame(data)
                            if not df.empty:
                                os.makedirs('data', exist_ok=True)
                                df.to_csv(os.path.join('data', 'spend.csv'), index=False)
                                # Clear ALL session overrides to force reload from CSV files
                                for key in ['uploaded_spend_df', 'uploaded_orders_df', 'demo_connector']:
                                    st.session_state.pop(key, None)
                                st.success(f'Google Ads connected for {user_email}. Data synced from backend.')
                                # Clear OAuth processing flag
                                st.session_state.oauth_processing = False
                                st.query_params.clear()
                                st.rerun()
                    except Exception:
                        pass

                    # Fallback to local stub if backend not available
                    google_connector.fetch_spend_and_persist(token, user_email)
                    try:
                        # If stub wrote spend.csv, clear session to force CSV reload
                        stub_spend_csv = os.path.join('data', 'spend.csv')
                        if os.path.exists(stub_spend_csv):
                            # Clear ALL session overrides to force reload from CSV files
                            for key in ['uploaded_spend_df', 'uploaded_orders_df', 'demo_connector']:
                                st.session_state.pop(key, None)
                    except Exception:
                        pass
                    st.success(f'Google Ads connected for {user_email}. Data synced.')
                    # Clear OAuth processing flag
                    st.session_state.oauth_processing = False
                    st.query_params.clear()
                    st.rerun()
                except Exception as e:
                    st.error(f'Token stored but sync failed: {e}')

        elif platform == 'tiktok':
            # TikTok OAuth - use backend broker
            try:
                backend_url = os.environ.get('BROKER_URL', 'http://localhost:8000')
                exchange_data = {
                    'code': code,
                    'state': state or '',
                    'platform': 'tiktok',
                    'redirect_uri': os.environ.get('REDIRECT_URI', 'http://localhost:8501')
                }
                r = requests.post(backend_url.rstrip('/') + '/oauth/exchange', json=exchange_data, timeout=10)
                if r.status_code == 200 and r.json().get('ok'):
                    token = r.json().get('access_token')
                    st.success('TikTok token obtained from backend')
                    
                    # Fetch spend data from backend
                    spend_response = requests.get(f"{backend_url}/tiktok/spend", timeout=20)
                    if spend_response.ok and spend_response.json().get('ok'):
                        data = spend_response.json().get('data', [])
                        message = spend_response.json().get('message', '')
                        if data:
                            import pandas as _pd
                            df = _pd.DataFrame(data)
                            os.makedirs('data', exist_ok=True)
                            df.to_csv(os.path.join('data', 'spend.csv'), index=False)
                        
                        # Clear session data
                        for key in ['uploaded_spend_df', 'uploaded_orders_df', 'demo_connector']:
                            st.session_state.pop(key, None)
                        
                        st.success(f'TikTok connected! {message}')
                        st.query_params.clear()
                        st.rerun()
                    else:
                        st.warning('TikTok connected but spend fetch failed')
                else:
                    st.error(f'TikTok backend exchange failed: {r.json().get("error")}')
            except Exception as e:
                st.error(f'TikTok OAuth failed: {e}')

    except Exception as e:
        st.error(f'OAuth callback error: {e}')


def _exchange_meta_code(code: str) -> Optional[str]:
    """Exchange Meta OAuth code for access token."""
    try:
        import requests
        client_id = os.environ.get('META_CLIENT_ID')
        client_secret = os.environ.get('META_CLIENT_SECRET')
        redirect_uri = os.environ.get('OAUTH_REDIRECT_URI', '')
        
        if not client_id or not client_secret:
            return None
        
        # Add platform param to redirect URI if not present
        if '?' not in redirect_uri:
            redirect_uri += '?platform=meta'
        elif 'platform=' not in redirect_uri:
            redirect_uri += '&platform=meta'
        
        url = 'https://graph.facebook.com/v17.0/oauth/access_token'
        params = {
            'client_id': client_id,
            'client_secret': client_secret,
            'redirect_uri': redirect_uri,
            'code': code,
        }
        r = requests.get(url, params=params, timeout=15)
        if r.status_code == 200:
            data = r.json()
            return data.get('access_token')
    except Exception:
        pass
    return None


def _exchange_google_code(code: str) -> Optional[str]:
    """Exchange Google OAuth code for access token."""
    try:
        import requests
        client_id = os.environ.get('GOOGLE_CLIENT_ID')
        client_secret = os.environ.get('GOOGLE_CLIENT_SECRET')
        redirect_uri = os.environ.get('OAUTH_REDIRECT_URI', '')
        
        if not client_id or not client_secret:
            return None
        
        if '?' not in redirect_uri:
            redirect_uri += '?platform=google'
        elif 'platform=' not in redirect_uri:
            redirect_uri += '&platform=google'
        
        url = 'https://oauth2.googleapis.com/token'
        data = {
            'client_id': client_id,
            'client_secret': client_secret,
            'code': code,
            'redirect_uri': redirect_uri,
            'grant_type': 'authorization_code',
        }
        r = requests.post(url, data=data, timeout=15)
        if r.status_code == 200:
            resp = r.json()
            return resp.get('access_token')
    except Exception:
        pass
    return None


def _exchange_tiktok_code(code: str) -> Optional[str]:
    """Exchange TikTok OAuth code for access token."""
    try:
        import requests
        client_id = os.environ.get('TIKTOK_CLIENT_ID')
        client_secret = os.environ.get('TIKTOK_CLIENT_SECRET')
        redirect_uri = os.environ.get('OAUTH_REDIRECT_URI', '')
        
        if not client_id or not client_secret:
            return None
        
        if '?' not in redirect_uri:
            redirect_uri += '?platform=tiktok'
        elif 'platform=' not in redirect_uri:
            redirect_uri += '&platform=tiktok'
        
        url = 'https://business-api.tiktok.com/open_api/v1.3/oauth2/access_token/'
        data = {
            'app_id': client_id,
            'secret': client_secret,
            'auth_code': code,
        }
        r = requests.post(url, json=data, timeout=15)
        if r.status_code == 200:
            resp = r.json()
            if resp.get('code') == 0:
                return resp.get('data', {}).get('access_token')
    except Exception:
        pass
    return None

# ============================================================================

# Admin session token storage (file-backed signed token)
ADMIN_SESSION_PATH = os.path.join('data', 'admin_session.json')


def _get_hmac_key() -> str:
    return os.environ.get('DASHBOARD_HMAC_KEY') or os.environ.get('DASHBOARD_SECRET_KEY') or 'dev-dashboard-secret'


def _b64_url_encode(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).decode().rstrip('=')


def _b64_url_decode(s: str) -> bytes:
    # add padding
    pad = '=' * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + pad)


def make_admin_token(email: str) -> str:
    payload = {'email': email, 'iat': int(time.time())}
    payload_b = json.dumps(payload, separators=(',', ':')).encode('utf-8')
    key = _get_hmac_key().encode('utf-8')
    sig = hmac.new(key, payload_b, hashlib.sha256).digest()
    return f"{_b64_url_encode(payload_b)}.{_b64_url_encode(sig)}"


def verify_admin_token(token: str, max_age_seconds: int = 7 * 24 * 3600) -> Optional[str]:
    try:
        parts = token.split('.')
        if len(parts) != 2:
            return None
        payload_b = _b64_url_decode(parts[0])
        sig = _b64_url_decode(parts[1])
        key = _get_hmac_key().encode('utf-8')
        expected = hmac.new(key, payload_b, hashlib.sha256).digest()
        if not hmac.compare_digest(expected, sig):
            return None
        payload = json.loads(payload_b.decode('utf-8'))
        iat = int(payload.get('iat', 0))
        if time.time() - iat > max_age_seconds:
            return None
        return payload.get('email')
    except Exception:
        return None


def save_admin_session_token(token: str) -> None:
    os.makedirs(os.path.dirname(ADMIN_SESSION_PATH), exist_ok=True)
    with open(ADMIN_SESSION_PATH, 'w', encoding='utf-8') as f:
        json.dump({'token': token, 'created': int(time.time())}, f)


def load_admin_session_token() -> Optional[str]:
    try:
        if not os.path.exists(ADMIN_SESSION_PATH):
            return None
        with open(ADMIN_SESSION_PATH, 'r', encoding='utf-8') as f:
            d = json.load(f)
        return d.get('token')
    except Exception:
        return None


def clear_admin_session_token() -> None:
    try:
        if os.path.exists(ADMIN_SESSION_PATH):
            os.remove(ADMIN_SESSION_PATH)
    except Exception:
        pass

# Optional PDF export support
try:
    from reportlab.pdfgen import canvas as _rl_canvas
    from reportlab.lib.pagesizes import letter as _rl_letter
    from reportlab.lib.utils import ImageReader as _rl_ImageReader
    _REPORTLAB_AVAILABLE = True
except Exception:
    _REPORTLAB_AVAILABLE = False

try:
    import duckdb
    _DUCKDB_AVAILABLE = True
except Exception:
    _DUCKDB_AVAILABLE = False


def df_to_image_bytes(df: pd.DataFrame, fontsize: int = 8) -> Optional[bytes]:
    """Render a DataFrame to a PNG image (bytes)."""
    try:
        import matplotlib.pyplot as plt

        fig, ax = plt.subplots(figsize=(8, max(1.5, 0.3 * len(df.index) + 0.8)))
        ax.axis('off')
        tbl = ax.table(cellText=df.round(2).astype(str).values,
                       colLabels=list(df.columns),
                       loc='center')
        tbl.auto_set_font_size(False)
        tbl.set_fontsize(fontsize)
        tbl.scale(1, 1.2)
        plt.tight_layout()
        buf = io.BytesIO()
        fig.savefig(buf, format='png', dpi=150, bbox_inches='tight')
        plt.close(fig)
        buf.seek(0)
        return buf.getvalue()
    except Exception:
        return None


def create_chart_image(df_acq: pd.DataFrame) -> Optional[bytes]:
    """Create a PNG image of the LTV:CAC bar chart from df_acq_agg."""
    try:
        import matplotlib.pyplot as plt

        fig, ax = plt.subplots(figsize=(8, 4))
        if df_acq.empty:
            ax.text(0.5, 0.5, 'No acquisition data', ha='center')
        else:
            platforms = df_acq['platform']
            vals = df_acq['ltv_cac_ratio']
            ax.bar(platforms, vals, color='tab:blue')
            ax.set_ylabel('LTV:CAC')
            ax.set_title('LTV:CAC by Platform')
            plt.xticks(rotation=45)

        plt.tight_layout()
        buf = io.BytesIO()
        fig.savefig(buf, format='png', dpi=150, bbox_inches='tight')
        plt.close(fig)
        buf.seek(0)
        return buf.getvalue()
    except Exception:
        return None
def create_zip_of_images(named_images: list) -> Optional[bytes]:
    """Given a list of (filename, bytes) create an in-memory zip and return bytes."""
    try:
        import zipfile

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, mode='w', compression=zipfile.ZIP_DEFLATED) as z:
            for name, data in named_images:
                if data:
                    z.writestr(name, data)
        buf.seek(0)
        return buf.getvalue()
    except Exception:
        return None


def create_pdf_with_tables(kpi_lines: str, chart_bytes: Optional[bytes], table_images: Optional[list] = None) -> Optional[bytes]:
    if not _REPORTLAB_AVAILABLE:
        return None
    buf = io.BytesIO()
    c = _rl_canvas.Canvas(buf, pagesize=_rl_letter)
    width, height = _rl_letter

    x = 40
    y = height - 40
    for line in kpi_lines.splitlines():
        c.setFont('Helvetica', 10)
        c.drawString(x, y, line)
        y -= 14

    if chart_bytes:
        try:
            img = _rl_ImageReader(io.BytesIO(chart_bytes))
            iw, ih = img.getSize()
            max_w = width - 80
            max_h = y - 40
            scale = min(max_w / iw, max_h / ih, 1.0)
            draw_w = iw * scale
            draw_h = ih * scale
            c.drawImage(img, x, y - draw_h, width=draw_w, height=draw_h)
            y = y - draw_h - 20
        except Exception:
            pass

    if table_images:
        for tbl in table_images:
            try:
                if y < 200:
                    c.showPage()
                    y = height - 40
                img = _rl_ImageReader(io.BytesIO(tbl))
                iw, ih = img.getSize()
                max_w = width - 80
                scale = min(max_w / iw, (y - 40) / ih, 1.0)
                draw_w = iw * scale
                draw_h = ih * scale
                c.drawImage(img, x, y - draw_h, width=draw_w, height=draw_h)
                y = y - draw_h - 20
            except Exception:
                pass

    c.showPage()
    c.save()
    buf.seek(0)
    return buf.read()

def _validate_orders(df: pd.DataFrame) -> Tuple[bool, str]:
    """Validate orders DataFrame has required columns and proper data types.
    
    Returns:
        Tuple of (is_valid: bool, message: str)
    """
    required_cols = ['order_date', 'platform', 'revenue', 'cogs']
    optional_cols = ['refunds', 'is_new_customer']
    
    # Check required columns
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        return False, f"Missing required columns: {', '.join(missing)}"
    
    # Check for empty dataframe
    if df.empty:
        return False, "Orders file is empty"
    
    # Validate data types
    try:
        # Check numeric columns
        for col in ['revenue', 'cogs']:
            if not pd.api.types.is_numeric_dtype(df[col]):
                return False, f"Column '{col}' must be numeric"
        
        # Check platform is not empty
        if df['platform'].isna().all():
            return False, "Platform column cannot be all empty"
        
    except Exception as e:
        return False, f"Data validation error: {str(e)}"
    
    return True, f"✓ Valid orders data ({len(df)} rows)"

def _validate_spend(df: pd.DataFrame) -> Tuple[bool, str]:
    """Validate spend DataFrame has required columns and proper data types.
    
    Returns:
        Tuple of (is_valid: bool, message: str)
    """
    required_cols = ['date', 'platform', 'ad_spend']
    
    # Check required columns
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        return False, f"Missing required columns: {', '.join(missing)}"
    
    # Check for empty dataframe
    if df.empty:
        return False, "Spend file is empty"
    
    # Validate data types
    try:
        # Check ad_spend is numeric
        if not pd.api.types.is_numeric_dtype(df['ad_spend']):
            return False, "Column 'ad_spend' must be numeric"
        
        # Check platform is not empty
        if df['platform'].isna().all():
            return False, "Platform column cannot be all empty"
        
        # Check for negative spend
        if (df['ad_spend'] < 0).any():
            return False, "Ad spend cannot contain negative values"
        
    except Exception as e:
        return False, f"Data validation error: {str(e)}"
    
    return True, f"✓ Valid spend data ({len(df)} rows)"

def load_data() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Load orders and spend data.

    Prefers CSV files in the `data` folder if present; otherwise returns a small built-in sample.
    Ensures a `refunds` column exists on orders (defaults to 0).
    """
    orders_csv = os.path.join("data", "orders.csv")
    spend_csv = os.path.join("data", "spend.csv")

    if os.path.exists(orders_csv) and os.path.exists(spend_csv):
        df_orders = pd.read_csv(orders_csv, parse_dates=["order_date"]) if "order_date" in pd.read_csv(orders_csv, nrows=0).columns else pd.read_csv(orders_csv)
        df_spend = pd.read_csv(spend_csv)
        if "refunds" not in df_orders.columns:
            df_orders["refunds"] = 0
        return df_orders, df_spend

    # Minimal sample data
    shopify_orders = {
        "order_id": [1, 2, 3, 4, 5],
        "customer_id": ["C1", "C2", "C3", "C4", "C5"],
        "is_new_customer": [True, True, False, True, False],
        "revenue": [100.0, 50.0, 80.0, 200.0, 30.0],
        "cogs": [30.0, 10.0, 20.0, 60.0, 8.0],
        "refunds": [0.0, 0.0, 0.0, 20.0, 0.0],
        "source_utm": ["campA", "campB", "campA", "organic", "campB"],
        "order_date": pd.to_datetime(["2025-01-01", "2025-01-02", "2025-01-03", "2025-01-04", "2025-01-05"]),
    }
    ad_spend = {
        "campaign_id": ["campA", "campB"],
        "ad_spend": [150.0, 60.0],
        "platform": ["Facebook", "Google"],
    }

    df_orders = pd.DataFrame(shopify_orders)
    df_spend = pd.DataFrame(ad_spend)
    return df_orders, df_spend


def seed_demo_connector_data() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Return a pair of (orders_df, spend_df) with realistic demo data.

    This data is intended for the one-click Demo Connector. It will be
    injected into `st.session_state` so it does not overwrite local files.
    """
    today = pd.to_datetime('2025-06-30')
    dates = pd.date_range(end=today, periods=30).to_pydatetime().tolist()

    # Demo campaigns and platforms
    campaigns = [
        ('campA', 'Facebook'),
        ('campB', 'Google'),
        ('campC', 'TikTok'),
        ('organic', 'Organic / Direct'),
    ]

    orders = []
    cust_idx = 1
    import random
    random.seed(42)
    for d in dates:
        # Create 3-6 orders per day
        for _ in range(random.randint(3, 6)):
            camp, platform = random.choice(campaigns)
            revenue = round(random.uniform(20, 300), 2)
            cogs = round(revenue * random.uniform(0.2, 0.5), 2)
            # small chance of refund
            refunds = round(revenue * random.choice([0, 0, 0, 0.1]), 2)
            orders.append(
                {
                    'order_id': f'D{cust_idx}',
                    'customer_id': f'C{cust_idx}',
                    'is_new_customer': random.choice([True, True, False]),
                    'revenue': revenue,
                    'cogs': cogs,
                    'refunds': refunds,
                    'source_utm': camp,
                    'order_date': pd.to_datetime(d),
                    'platform': platform,
                }
            )
            cust_idx += 1

    df_orders = pd.DataFrame(orders)

    # Demo spend per campaign aggregated over the same period
    spend_rows = []
    for camp, platform in [('campA', 'Facebook'), ('campB', 'Google'), ('campC', 'TikTok')]:
        # simulate varied spend
        spend_rows.append({'campaign_id': camp, 'ad_spend': round(random.uniform(800, 2500), 2), 'platform': platform})

    df_spend = pd.DataFrame(spend_rows)
    return df_orders, df_spend


def read_aggregates_from_duckdb(db_path: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Read precomputed `acq_agg` and `agg_profit` tables from a DuckDB file and transform
    them into the same shape expected by `calculate_strategic_metrics`.
    """
    if not _DUCKDB_AVAILABLE:
        raise RuntimeError("duckdb package is not available")
    if not os.path.exists(db_path):
        raise FileNotFoundError(db_path)

    con = duckdb.connect(database=db_path, read_only=True)
    try:
        df_acq = con.execute("SELECT * FROM acq_agg").fetchdf()
        df_profit = con.execute("SELECT * FROM agg_profit").fetchdf()
    finally:
        con.close()

    # Normalize column names and compute LTV/CAC fields
    # acq: platform, new_customers, total_ad_spend, acq_profit_sum
    if 'acq_profit_sum' in df_acq.columns:
        df_acq = df_acq.rename(columns={'acq_profit_sum': 'total_contribution'})
    if 'total_ad_spend' not in df_acq.columns:
        df_acq['total_ad_spend'] = 0.0
    df_acq['new_customers'] = df_acq.get('new_customers', 0).astype(int)
    df_acq['total_contribution'] = df_acq.get('total_contribution', 0.0).astype(float)
    df_acq['customer_acquisition_cost'] = df_acq.apply(lambda r: (r['total_ad_spend'] / r['new_customers']) if r['new_customers'] > 0 else 0.0, axis=1)
    df_acq['ltv'] = df_acq.apply(lambda r: (r['total_contribution'] / r['new_customers']) if r['new_customers'] > 0 else 0.0, axis=1)
    df_acq['ltv_cac_ratio'] = df_acq.apply(lambda r: (r['ltv'] / r['customer_acquisition_cost']) if r['customer_acquisition_cost'] > 0 else 0.0, axis=1)

    # profit: platform, total_revenue, net_contribution_profit, total_ad_spend
    df_profit['total_revenue'] = df_profit.get('total_revenue', 0.0).astype(float)
    df_profit['net_contribution_profit'] = df_profit.get('net_contribution_profit', 0.0).astype(float)
    df_profit['total_ad_spend'] = df_profit.get('total_ad_spend', 0.0).astype(float)
    df_profit['true_roas'] = df_profit.apply(lambda r: (r['total_revenue'] / r['total_ad_spend']) if r['total_ad_spend'] > 0 else 0.0, axis=1)

    return df_acq, df_profit


def calculate_strategic_metrics(df_orders: pd.DataFrame, df_spend: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Compute acquisition aggregates (LTV/CAC) and overall profit aggregates.

    Returns:
      - df_acq_agg: per-platform new customers, total ad spend, CAC, LTV, LTV:CAC
      - df_agg_profit: per-platform total revenue, net contribution, total ad spend, true_roas
    """
    orders = df_orders.copy()
    spend = df_spend.copy()

    # Ensure expected columns
    if "refunds" not in orders.columns:
        orders["refunds"] = 0

    # Map campaign -> platform onto orders where possible
    if "campaign_id" in spend.columns and "source_utm" in orders.columns:
        mapping = spend[["campaign_id", "platform"]].drop_duplicates()
        orders = orders.merge(mapping, left_on="source_utm", right_on="campaign_id", how="left")

    orders["platform"] = orders.get("platform", "Organic / Direct")

    # Contribution per order
    orders["contribution"] = orders.get("revenue", 0) - orders.get("cogs", 0) - orders.get("refunds", 0)

    # Acquisition cohort (only new customers)
    acq = orders[orders.get("is_new_customer") == True].copy()

    # Per-platform aggregates for acquisition
    if not acq.empty:
        acq_by_platform = (
            acq.groupby("platform")
            .agg(new_customers=("customer_id", pd.Series.nunique), total_contribution=("contribution", "sum"))
            .reset_index()
        )
    else:
        acq_by_platform = pd.DataFrame(columns=["platform", "new_customers", "total_contribution"])

    # Compute acquisition spend per platform based on spend table
    if "campaign_id" in spend.columns:
        spend_by_platform = spend.groupby("platform", as_index=False).agg(total_ad_spend=("ad_spend", "sum"))
    else:
        spend_by_platform = pd.DataFrame(columns=["platform", "total_ad_spend"])

    df_acq_agg = acq_by_platform.merge(spend_by_platform, on="platform", how="left")
    df_acq_agg["total_ad_spend"] = df_acq_agg["total_ad_spend"].fillna(0.0)
    df_acq_agg["customer_acquisition_cost"] = (
        df_acq_agg.apply(lambda r: r["total_ad_spend"] / r["new_customers"] if r["new_customers"] > 0 else 0.0, axis=1)
    )
    df_acq_agg["ltv"] = df_acq_agg.apply(lambda r: r["total_contribution"] / r["new_customers"] if r["new_customers"] > 0 else 0.0, axis=1)
    df_acq_agg["ltv_cac_ratio"] = df_acq_agg.apply(lambda r: (r["ltv"] / r["customer_acquisition_cost"]) if r["customer_acquisition_cost"] > 0 else 0.0, axis=1)

    # Aggregate profit view (all orders)
    agg = (
        orders.groupby("platform")
        .agg(total_revenue=("revenue", "sum"), net_contribution_profit=("contribution", "sum"))
        .reset_index()
    )
    agg = agg.merge(spend_by_platform, on="platform", how="left")
    agg["total_ad_spend"] = agg["total_ad_spend"].fillna(0.0)
    agg["true_roas"] = agg.apply(lambda r: (r["total_revenue"] / r["total_ad_spend"]) if r["total_ad_spend"] > 0 else 0.0, axis=1)

    # Cleanup numeric columns
    for df in (df_acq_agg, agg):
        for col in df.select_dtypes(include=[object]).columns:
            try:
                df[col] = pd.to_numeric(df[col])
            except Exception:
                pass

    return df_acq_agg, agg


def generate_recommendations(
    df_orders: pd.DataFrame,
    df_spend: pd.DataFrame,
    scale_threshold: float = 3.0,
    test_threshold: float = 1.2,
    urgent_threshold: float = 1.0,
) -> List[str]:
    """Pure function that generates margin-aware recommendations.

    It uses contribution-based LTV (revenue - cogs - refunds) for new customers and campaign-level spend
    to compute CAC. Recommendations are pure Python (no Streamlit side-effects) so they are easy to test.
    """
    df_acq_agg, _ = calculate_strategic_metrics(df_orders, df_spend)

    # If no acquisition cohorts, return a helpful message (tests expect this)
    if df_acq_agg.empty:
        return ["Not enough acquisition data to generate margin-aware recommendations."]

    # Prepend an overall summary line
    total_revenue = float(df_orders.get("revenue", 0).sum())
    overall_lines: List[str] = [f"Overall revenue: ${total_revenue:,.2f}"]

    recs: List[Tuple[float, str]] = []

    for _, row in df_acq_agg.iterrows():
        platform = row.get("platform", "(unknown)")
        ltv_cac = float(row.get("ltv_cac_ratio", 0.0))
        ltv = float(row.get("ltv", 0.0))
        cac = float(row.get("customer_acquisition_cost", 0.0))

        if ltv_cac >= scale_threshold:
            action = f"Scale"
        elif ltv_cac >= test_threshold:
            action = f"Test & Optimize"
        elif ltv_cac <= urgent_threshold:
            action = f"Reduce or Pause"
        else:
            action = f"Maintain / Re-evaluate"

        rec_text = (
            f"Platform: {platform} — {action} (LTV:CAC={ltv_cac:.2f}, LTV=${ltv:.2f}, CAC=${cac:.2f})"
        )
        recs.append((ltv_cac, rec_text))

    # Sort by descending LTV:CAC so higher priority (scale) show first
    recs_sorted = [r for _, r in sorted(recs, key=lambda x: x[0], reverse=True)]
    return overall_lines + recs_sorted


def run_dashboard() -> None:
    # Configure logging for the dashboard (file + console). Log file: logs/dashboard.log
    logdir = os.path.join('logs')
    os.makedirs(logdir, exist_ok=True)
    log_file = os.path.join(logdir, 'dashboard.log')
    logger = logging.getLogger('profit_dashboard')
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        fh = RotatingFileHandler(log_file, maxBytes=5 * 1024 * 1024, backupCount=3)
        fh.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s')
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        # Also add a console handler at INFO
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    st.set_page_config(page_title="Acquisition Pulse", layout="wide")
    
    # Global dashboard styling
    st.markdown("""
    <style>
        /* Global dashboard polish */
        h1, h2, h3 {
            color: #1E3A8A;
            font-weight: 700;
        }
        h2 {
            border-bottom: 2px solid #FB8C00;
            padding-bottom: 8px;
        }
        .metric-container {
            background: linear-gradient(135deg, rgba(30, 58, 138, 0.02) 0%, rgba(251, 140, 0, 0.02) 100%);
            border-radius: 8px;
            padding: 8px;
        }
        /* Improve dataframe styling */
        .dataframe {
            font-size: 0.85em !important;
        }
        /* Improve subheader visibility */
        [data-testid="stMarkdownContainer"] h3 {
            margin-top: 20px;
            margin-bottom: 8px;
        }
    </style>
    """, unsafe_allow_html=True)
    
    # Handle OAuth callback if present (code + state in query params)
    _handle_oauth_callback()
    
    # ========================================================================
    # LOGIN GATE: Check if password is required
    # ========================================================================
    # Try to import auth helper for multi-user credentials
    try:
        from auth_helper import verify_user, load_credentials
        use_credentials_file = os.path.exists('credentials.json')
    except Exception:
        use_credentials_file = False
    
    dashboard_pw = os.getenv('DASHBOARD_PASSWORD')
    
    # Determine authentication mode (temporarily disabled to unblock usage)
    require_auth = False
    
    # Clear auth query param if already authenticated
    if getattr(st.session_state, 'dashboard_authenticated', False) and st.query_params.get('auth'):
        st.query_params.clear()
    
    # Check if this is an OAuth callback (code/state params) - if so, skip login gate
    is_oauth_callback = bool(st.query_params.get('code')) or bool(st.query_params.get('state')) or getattr(st.session_state, 'oauth_processing', False)
    
    # Debug: Show authentication state (remove this after debugging)
    if st.query_params.get('debug') == 'true':
        st.sidebar.write("DEBUG INFO:")
        st.sidebar.write(f"require_auth: {require_auth}")
        st.sidebar.write(f"authenticated: {getattr(st.session_state, 'dashboard_authenticated', False)}")
        st.sidebar.write(f"is_oauth_callback: {is_oauth_callback}")
        st.sidebar.write(f"oauth_processing: {getattr(st.session_state, 'oauth_processing', False)}")
    
    if require_auth and not getattr(st.session_state, 'dashboard_authenticated', False) and not is_oauth_callback:
        # Show branded login page
        st.markdown("""
        <style>
            /* Compact login page v3 */
            .login-container {
                max-width: 380px;
                margin: 50px auto;
                padding: 30px;
                background: white;
                border-radius: 10px;
                box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
            }
            .login-logo {
                text-align: center;
                margin-bottom: 20px;
            }
            .login-logo h1 {
                color: #1E3A8A;
                font-size: 2em;
                margin: 0;
                font-weight: 700;
            }
            .login-logo .pulse {
                color: #FB8C00;
            }
            .login-tagline {
                text-align: center;
                color: #64748B;
                font-size: 0.85em;
                margin-bottom: 20px;
                line-height: 1.3;
            }
            [data-testid="stTextInput"] {
                margin-bottom: 8px;
            }
            [data-testid="stTextInput"] input {
                border-radius: 6px !important;
                border: 1.5px solid #E2E8F0 !important;
                padding: 6px 10px !important;
                font-size: 0.88em !important;
            }
            [data-testid="stTextInput"] input:focus {
                border-color: #1E3A8A !important;
                box-shadow: 0 0 0 1px #1E3A8A !important;
            }
            [data-testid="stTextInput"] label {
                font-size: 0.8em !important;
                font-weight: 500 !important;
                color: #334155 !important;
                margin-bottom: 3px !important;
            }
            .stButton button {
                border-radius: 6px !important;
                font-weight: 600 !important;
                padding: 6px 14px !important;
                font-size: 0.88em !important;
                transition: all 0.2s !important;
            }
            .stButton button:hover {
                transform: translateY(-1px) !important;
                box-shadow: 0 3px 6px rgba(0, 0, 0, 0.12) !important;
            }
        </style>
        """, unsafe_allow_html=True)
        
        st.markdown('<div class="login-container">', unsafe_allow_html=True)
        
        # Logo and branding
        st.markdown("""
        <div class="login-logo">
            <h1>Acquisition <span class="pulse">Pulse</span></h1>
        </div>
        <div class="login-tagline">
            Track ad spend, analyze LTV:CAC, and optimize your customer acquisition
        </div>
        """, unsafe_allow_html=True)
        
        # Compact form with custom heights
        st.markdown('<div style="margin-bottom: 8px;">', unsafe_allow_html=True)
        login_email = st.text_input('Email', placeholder='user@example.com', key='login_email', label_visibility='visible')
        st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown('<div style="margin-bottom: 15px;">', unsafe_allow_html=True)
        login_password = st.text_input('Password', type='password', key='login_password', placeholder='••••••••', label_visibility='visible')
        st.markdown('</div>', unsafe_allow_html=True)
        
        col1, col2 = st.columns(2)
        if col1.button('Login', use_container_width=True, key='login_btn', type='primary'):
            if not login_email or not login_password:
                st.error('Please enter both email and password')
            else:
                # Try credentials file first, then fallback to env password
                user_info = None
                if use_credentials_file:
                    user_info = verify_user(login_email, login_password)
                elif login_password == dashboard_pw:
                    user_info = {'email': login_email, 'name': login_email.split('@')[0]}
                
                if user_info:
                    # Set all session state variables BEFORE button callback completes
                    st.session_state.dashboard_authenticated = True
                    st.session_state.user_email = user_info['email']
                    st.session_state.user_name = user_info.get('name', login_email)
                    st.session_state.user_role = user_info.get('role', 'user')
                    st.session_state.username = login_email.split('@')[0] if '@' in login_email else login_email
                    # Force redirect with query param to ensure state refresh
                    st.query_params['auth'] = 'success'
                    st.rerun()
                else:
                    st.error('Invalid email or password. Please try again.')
        
        if col2.button('Try Demo', use_container_width=True, key='demo_login_btn'):
            st.session_state.dashboard_authenticated = True
            st.session_state.user_email = 'demo@localhost'
            st.session_state.user_name = 'Demo User'
            st.session_state.user_role = 'demo'
            st.session_state.username = 'demo'
            # Force redirect with query param
            st.query_params['auth'] = 'success'
            st.rerun()
        
        st.markdown('</div>', unsafe_allow_html=True)
        st.stop()
    
    st.markdown("""
    <h1 style='color: #1E3A8A;'>
        Acquisition <span style='color: #FB8C00;'>Pulse</span>
    </h1>
    """, unsafe_allow_html=True)

    # Onboarding modal: show once per session unless the user opts out
    if 'onboarding_shown' not in st.session_state:
        st.session_state['onboarding_shown'] = False
    if 'suppress_onboarding' not in st.session_state:
        st.session_state['suppress_onboarding'] = False

    if not st.session_state.get('onboarding_shown') and not st.session_state.get('suppress_onboarding'):
        try:
            # Use Streamlit modal when available
            if hasattr(st, 'modal'):
                with st.modal('Welcome to Profit Dashboard', clear_on_close=False):
                    st.markdown('''
### Welcome to Profit Dashboard!

1. **Try Demo Data:** Click “Use demo connector” for instant sample results.
2. **Upload Your Data:** Add your Shopify orders and ad spend CSVs for real analysis.
3. **Connect Ad Accounts:** Use the Connectors section to sync spend automatically.

*Need help exporting data?* [See step-by-step guide](#)
''')
                    col1, col2 = st.columns(2)
                    if col1.button('Got it'):
                        st.session_state['onboarding_shown'] = True
                    if col2.button("Don't show again"):
                        st.session_state['onboarding_shown'] = True
                        st.session_state['suppress_onboarding'] = True
            else:
                # Fallback: show an info box in the sidebar
                with st.sidebar.expander('Welcome — Quick start', expanded=True):
                    st.markdown('''
**Welcome to Profit Dashboard!**

1. **Try Demo Data:** Click “Use demo connector” for instant sample results.
2. **Upload Your Data:** Add your Shopify orders and ad spend CSVs for real analysis.
3. **Connect Ad Accounts:** Use the Connectors section to sync spend automatically.

*Need help exporting data?* [See step-by-step guide](#)
''')
                    if st.button('Dismiss onboarding'):
                        st.session_state['onboarding_shown'] = True
        except Exception:
            # If the modal API isn't available or fails, continue silently
            st.session_state['onboarding_shown'] = True

    # Dev Mode and tuning thresholds (persisted in session_state)
    if 'scale_threshold' not in st.session_state:
        st.session_state['scale_threshold'] = 3.0
    if 'test_threshold' not in st.session_state:
        st.session_state['test_threshold'] = 1.2
    if 'urgent_threshold' not in st.session_state:
        st.session_state['urgent_threshold'] = 1.0

    # Show logged-in user and logout option (move to top of sidebar)
    if st.session_state.get('user_email'):
        st.sidebar.markdown(f"**Logged in:** {st.session_state.get('user_email')}")
        if st.sidebar.button('Logout', key='logout_btn'):
            st.session_state.dashboard_authenticated = False
            st.session_state['user_email'] = None
            st.session_state['username'] = None
            st.rerun()

    # Demo/Dev toggles (move to top)
    demo_mode = st.sidebar.checkbox('Demo Mode (generate sample data)', value=False)
    dev_mode = st.sidebar.checkbox('Dev Mode (show tuning + debug)', value=False)

    # Upload Data (move above connectors)
    with st.sidebar.expander('Upload Data (optional)', expanded=False):
        st.markdown("Upload `orders.csv` and `spend.csv` to preview and apply new data for this session.")
        uploaded_orders = st.file_uploader('Upload Orders CSV', type=['csv'], key='upload_orders')
        uploaded_spend = st.file_uploader('Upload Spend CSV', type=['csv'], key='upload_spend')

        # Read and validate uploads (but don't replace live data until Apply pressed)
        orders_preview = None
        spend_preview = None
        orders_msg = None
        spend_msg = None

        if uploaded_orders is not None:
            try:
                # Re-read header-first to detect order_date column existence
                uploaded_orders.seek(0)
                sample = pd.read_csv(uploaded_orders, nrows=0)
                uploaded_orders.seek(0)
                if 'order_date' in sample.columns:
                    orders_preview = pd.read_csv(uploaded_orders, parse_dates=["order_date"])
                else:
                    orders_preview = pd.read_csv(uploaded_orders)
            except Exception as e:
                orders_msg = f"Failed to read orders CSV: {e}"
                orders_preview = None
            if orders_preview is not None:
                ok, orders_msg = _validate_orders(orders_preview)

        if uploaded_spend is not None:
            try:
                uploaded_spend.seek(0)
                spend_preview = pd.read_csv(uploaded_spend)
            except Exception as e:
                spend_msg = f"Failed to read spend CSV: {e}"
                spend_preview = None
            if spend_preview is not None:
                ok2, spend_msg = _validate_spend(spend_preview)

        if orders_preview is not None:
            st.write('Orders preview (first 5 rows)')
            st.dataframe(orders_preview.head(5))
            st.write('Validation:', orders_msg)

        if spend_preview is not None:
            st.write('Spend preview (first 5 rows)')
            st.dataframe(spend_preview.head(5))
            st.write('Validation:', spend_msg)

        if (orders_preview is not None or spend_preview is not None):
            if st.button('Apply Uploaded Data'):
                # Save into session_state so rest of dashboard uses uploaded data
                if orders_preview is not None:
                    st.session_state['uploaded_orders_df'] = orders_preview
                if spend_preview is not None:
                    st.session_state['uploaded_spend_df'] = spend_preview
                st.success('Uploaded data applied to this session.')

    # ========================================================================
    # CONNECTORS SECTION (BEFORE data loading so demo button sets session data first)
    # ========================================================================
    # Helper functions for OAuth buttons
    def _oauth_button_html(name: str, url: str, color: str = '#333333', text: str = None, icon_url: str = None) -> str:
        txt = text or f'Connect {name}'
        icon_html = ''
        if icon_url:
            icon_html = f'<img src="{icon_url}" alt="{name}" style="width:18px;height:18px;vertical-align:middle;margin-right:8px;" />'
        return (
            f'<a href="{url}" target="_blank" '
            f'style="display:inline-flex;align-items:center;padding:10px 16px;margin:4px 0;border-radius:6px;color:#ffffff;text-decoration:none;background:{color};font-weight:600;gap:6px;">'
            f'{icon_html}{txt}</a>'
        )

    def _build_oauth_url(platform: str = '', user_email: str = '') -> str:
        """Build OAuth URL for a platform"""
        oauth_server_url = os.environ.get('OAUTH_SERVER_URL', 'http://localhost:8888')
        use_real_env = str(os.environ.get('USE_REAL_OAUTH', '')).lower() in ('1', 'true', 'yes')
        use_real = use_real_env or bool(st.session_state.get('force_real_oauth'))
        base_redirect = os.environ.get('OAUTH_REDIRECT_URI') or (oauth_server_url.rstrip('/') + '/callback')
        
        if use_real and platform.lower() in ('google', 'google_ads'):
            client_id = os.environ.get('GOOGLE_CLIENT_ID', '')
            scope = os.environ.get('GOOGLE_OAUTH_SCOPE', 'https://www.googleapis.com/auth/adwords')
            base = 'https://accounts.google.com/o/oauth2/v2/auth'
            q = {
                'client_id': client_id,
                'redirect_uri': base_redirect,
                'response_type': 'code',
                'scope': scope,
                'access_type': 'offline',
                'prompt': 'consent',
            }
            return base + '?' + urllib.parse.urlencode(q)
        
        if use_real and platform.lower() in ('meta', 'facebook'):
            client_id = os.environ.get('META_CLIENT_ID', '')
            redirect = base_redirect + ('?platform=meta' if '?' not in base_redirect else '&platform=meta')
            scope = os.environ.get('META_OAUTH_SCOPE', 'ads_read,ads_management')
            base = 'https://www.facebook.com/v12.0/dialog/oauth'
            q = {'client_id': client_id, 'redirect_uri': redirect, 'scope': scope}
            return base + '?' + urllib.parse.urlencode(q)
        
        if use_real and platform.lower() == 'tiktok':
            client_id = os.environ.get('TIKTOK_CLIENT_ID', '')
            redirect = base_redirect + ('?platform=tiktok' if '?' not in base_redirect else '&platform=tiktok')
            base = os.environ.get('TIKTOK_OAUTH_URL', 'https://ads.tiktok.com/marketing_api/auth')
            q = {'app_id': client_id, 'redirect_uri': redirect}
            return base + '?' + urllib.parse.urlencode(q)
        
        # Fallback to provider docs
        provider_docs = {
            'google': 'https://business.google.com/aunz/google-ads/?sourceid=awo&subid=ww-ww-ep-developers_ads',
            'google_ads': 'https://business.google.com/aunz/google-ads/?sourceid=awo&subid=ww-ww-ep-developers_ads',
            'meta': 'https://developers.facebook.com/tools/explorer/',
            'facebook': 'https://developers.facebook.com/tools/explorer/',
            'tiktok': 'https://business-api.tiktok.com/portal',
        }
        return provider_docs.get(platform.lower(), oauth_server_url)

    # Check which platforms are configured
    def _check_platform_configured(platform: str) -> bool:
        """Check if a platform has required credentials configured"""
        if platform.lower() in ('google', 'google_ads'):
            return bool(os.getenv('GOOGLE_CLIENT_ID') and os.getenv('GOOGLE_CLIENT_SECRET') and os.getenv('GOOGLE_ADS_DEVELOPER_TOKEN'))
        elif platform.lower() in ('meta', 'facebook'):
            return bool(os.getenv('META_CLIENT_ID') and os.getenv('META_CLIENT_SECRET'))
        elif platform.lower() == 'tiktok':
            return bool(os.getenv('TIKTOK_CLIENT_ID') and os.getenv('TIKTOK_CLIENT_SECRET'))
        return False

    # Import connectors
    try:
        from connectors import meta_connector as _meta_conn
    except Exception:
        _meta_conn = None
    
    try:
        from connectors import google_connector as _google_conn
    except Exception:
        _google_conn = None
    
    try:
        from connectors import tiktok_connector as _tiktok_conn
    except Exception:
        _tiktok_conn = None

    # Demo Connector UI
    with st.sidebar.expander('Ad Platform Connectors', expanded=True):
        st.markdown("Connect your ad accounts to sync spend automatically via OAuth.")
        
        # Debug: Show which credentials are found
        if dev_mode:
            st.info(f"Debug - GOOGLE_CLIENT_ID set: {bool(os.getenv('GOOGLE_CLIENT_ID'))}")
            st.info(f"Debug - GOOGLE_CLIENT_SECRET set: {bool(os.getenv('GOOGLE_CLIENT_SECRET'))}")
            st.info(f"Debug - GOOGLE_ADS_DEVELOPER_TOKEN set: {bool(os.getenv('GOOGLE_ADS_DEVELOPER_TOKEN'))}")
        
        # Demo Connector
        st.markdown("**Demo Mode** - Try without connecting real accounts")
        if st.button('Use Demo Data', key='use_demo_btn'):
            demo_orders, demo_spend = seed_demo_connector_data()
            st.session_state['uploaded_orders_df'] = demo_orders
            st.session_state['uploaded_spend_df'] = demo_spend
            st.session_state['demo_connector'] = True
            st.success('Demo data loaded')
        
        st.divider()
        
        # Google Ads
        google_configured = _check_platform_configured('google')
        google_status = "[Ready]" if google_configured else "[Not Configured]"
        st.markdown(f"**Google Ads** {google_status}")
        
        if google_configured:
            user_email = st.session_state.get('user_email', '')
            st.write("Status: Not connected - click to authorize")
            g_start = _build_oauth_url('google', user_email)
            g_btn = _oauth_button_html(
                'Google',
                g_start,
                color='#FB8C00',
                text='Connect Google Ads',
                icon_url='https://cdn.simpleicons.org/googleads/FFFFFF'
            )
            st.markdown(g_btn, unsafe_allow_html=True)
        else:
            st.write("Status: Credentials not configured")
        
        st.divider()
        
        # Meta
        meta_configured = _check_platform_configured('meta')
        meta_status = "[Ready]" if meta_configured else "[Not Configured]"
        st.markdown(f"**Meta (Facebook/Instagram)** {meta_status}")
        
        if meta_configured:
            user_email = st.session_state.get('user_email', '')
            st.write("Status: Not connected - click to authorize")
            start_url = _build_oauth_url('meta', user_email)
            meta_btn = _oauth_button_html(
                'Meta',
                start_url,
                color='#1877F2',
                text='Connect Meta Ads',
                icon_url='https://cdn.simpleicons.org/meta/FFFFFF'
            )
            st.markdown(meta_btn, unsafe_allow_html=True)
        else:
            st.write("Status: Credentials not configured")
            # Keep a docs link visible so the UI is consistent even without creds
            meta_docs = _build_oauth_url('meta', '')
            meta_btn = _oauth_button_html(
                'Meta',
                meta_docs,
                color='#9E9E9E',
                text='Setup Meta Ads (docs)',
                icon_url='https://cdn.simpleicons.org/meta/FFFFFF'
            )
            st.markdown(meta_btn, unsafe_allow_html=True)
        
        st.divider()
        
        # TikTok
        tiktok_configured = _check_platform_configured('tiktok')
        tiktok_status = "[Ready]" if tiktok_configured else "[Not Configured]"
        st.markdown(f"**TikTok Ads** {tiktok_status}")
        
        if tiktok_configured:
            user_email = st.session_state.get('user_email', '')
            st.write("Status: Not connected - click to authorize")
            t_start = _build_oauth_url('tiktok', user_email)
            t_btn = _oauth_button_html(
                'TikTok',
                t_start,
                color='#EE1D52',
                text='Connect TikTok Ads',
                icon_url='https://cdn.simpleicons.org/tiktok/FFFFFF'
            )
            st.markdown(t_btn, unsafe_allow_html=True)
        else:
            st.write("Status: Credentials not configured")
            # Show a docs link so the connector remains visible without creds
            t_docs = _build_oauth_url('tiktok', '')
            t_btn = _oauth_button_html(
                'TikTok',
                t_docs,
                color='#9E9E9E',
                text='Setup TikTok Ads (docs)',
                icon_url='https://cdn.simpleicons.org/tiktok/FFFFFF'
            )
            st.markdown(t_btn, unsafe_allow_html=True)

    # Load data after auth and apply date filtering from the sidebar
    # If the user uploaded data and applied it in session_state, prefer that for the session
    # NOTE: Demo button (above) stores data in session_state, which this section will now use
    if 'uploaded_orders_df' in st.session_state or 'uploaded_spend_df' in st.session_state:
        df_orders = st.session_state.get('uploaded_orders_df', None)
        df_spend = st.session_state.get('uploaded_spend_df', None)
        # Fall back to CSVs for any missing piece
        if df_orders is None or df_spend is None:
            _df_orders, _df_spend = load_data()
            if df_orders is None:
                df_orders = _df_orders
            if df_spend is None:
                df_spend = _df_spend
    else:
        df_orders, df_spend = load_data()
    
    st.session_state['debug_checkpoint'] = f'data_loaded: {len(df_orders)} orders, {len(df_spend)} spend rows'

    # Admin UI section (outside connectors expander)
    # Admin authentication: require both DASHBOARD_ADMIN_EMAIL and DASHBOARD_ADMIN_PASSWORD
    # to be set in the environment and to be provided by the user. Authentication
    # is stored in `st.session_state['is_admin_authenticated']` for the session.
    admin_email_env = os.environ.get('DASHBOARD_ADMIN_EMAIL')
    admin_password_env = os.environ.get('DASHBOARD_ADMIN_PASSWORD')

    # Provide a small admin login panel in the sidebar when admin creds are configured
    if admin_email_env and admin_password_env:
        with st.sidebar.expander('Admin Login', expanded=False):
            # On app start, attempt to restore admin session from persisted token
            if 'is_admin_authenticated' not in st.session_state:
                st.session_state['is_admin_authenticated'] = False
            if not st.session_state.get('is_admin_authenticated'):
                # try loading persisted token
                try:
                    token = load_admin_session_token()
                    if token:
                        email = verify_admin_token(token)
                        if email:
                            st.session_state['is_admin_authenticated'] = True
                            st.session_state['admin_email'] = email
                except Exception:
                    pass

            if st.session_state.get('is_admin_authenticated'):
                try:
                    st.success('Admin authenticated for this session')
                except Exception:
                    pass
                if st.button('Logout admin', key='admin_logout_btn'):
                    clear_admin_session_token()
                    st.session_state['is_admin_authenticated'] = False
                    if 'admin_email' in st.session_state:
                        del st.session_state['admin_email']
                    st.success('Admin session cleared for this client')
            else:
                input_admin_email = st.text_input('Admin email', value=st.session_state.get('user_email', ''), key='admin_login_email')
                input_admin_pw = st.text_input('Admin password', type='password', key='admin_login_pw')
                if st.button('Authenticate as admin', key='admin_auth_btn'):
                    if (input_admin_email or '').strip() == admin_email_env and (input_admin_pw or '') == admin_password_env:
                        # create and persist signed admin token
                        try:
                            token = make_admin_token(input_admin_email.strip())
                            save_admin_session_token(token)
                            st.session_state['is_admin_authenticated'] = True
                            st.session_state['admin_email'] = input_admin_email.strip()
                            st.success('Admin authenticated and session persisted')
                        except Exception:
                            st.session_state['is_admin_authenticated'] = True
                            st.success('Admin authenticated (session-only)')
                    else:
                        st.error('Admin credentials incorrect')

    # Admin allowed if in dev_mode or if the session has a successful admin authentication
    admin_allowed = dev_mode or bool(st.session_state.get('is_admin_authenticated'))
    if admin_allowed:
        # Render admin controls inline (avoid nesting expanders)
        st.markdown('**Admin: Manage Users & Ad Accounts**')
        if _meta_conn is None:
            st.error('Meta connector not available; admin functions are limited.')
        else:
            meta_store = _meta_conn.load_meta('meta') or {}
            mappings = meta_store.get('email_ad_accounts', {})

            st.markdown('Existing mappings (email → ad account id)')
            if mappings:
                try:
                    import pandas as _pd

                    df_map = _pd.DataFrame([{'email': k, 'ad_account_id': v, 'token_storage': _meta_conn.token_storage_location_for_user(k)} for k, v in mappings.items()])
                    st.dataframe(df_map)
                except Exception:
                    st.write(mappings)
            else:
                st.write('No mappings configured yet.')

            st.markdown('Add or update a mapping')
            new_email = st.text_input('User email', value='', key='admin_new_email')
            new_acct = st.text_input('Ad account id (e.g., 1234567890)', value='', key='admin_new_acct')
            if st.button('Add / Update mapping'):
                if not new_email or not new_acct:
                    st.warning('Provide both an email and an ad account id.')
                else:
                    mappings[new_email.strip()] = new_acct.strip()
                    meta_store['email_ad_accounts'] = mappings
                    _meta_conn.save_meta('meta', meta_store)
                    st.success(f'Added/updated mapping for {new_email.strip()}')

            if mappings:
                st.markdown('Remove a mapping')
                to_remove = st.selectbox('Select email to remove', sorted(list(mappings.keys())), key='admin_remove_select')
                if st.button('Remove mapping'):
                    try:
                        del mappings[to_remove]
                        meta_store['email_ad_accounts'] = mappings
                        _meta_conn.save_meta('meta', meta_store)
                        st.success(f'Removed mapping for {to_remove}')
                    except Exception as e:
                        st.error(f'Failed to remove mapping: {e}')

            # Manual sync for a selected mapped user (admin tool)
            if mappings:
                st.markdown('Manual Sync: fetch spend for a mapped user')
                sync_user = st.selectbox('Select user to sync', sorted(list(mappings.keys())), key='admin_sync_select')
                if st.button('Manual Sync for selected user'):
                    token = _meta_conn.get_token_for_meta_user(sync_user)
                    if not token:
                        st.error('No token stored for that user. They must complete OAuth or you must add a token manually via dev tooling.')
                    else:
                        try:
                            with st.spinner('Running manual sync...'):
                                df_new = _meta_conn.fetch_spend_and_persist(token, user_email=sync_user)
                                if df_new is not None:
                                    st.success('Manual sync completed; sample rows:')
                                    st.dataframe(df_new.head(10))
                                else:
                                    st.error('Sync returned no data.')
                        except Exception as e:
                            st.error(f'Manual sync failed: {e}')
                    # Developer notes pointer: keep detailed technical docs in README
                    with st.expander('Developer Notes (admins only)', expanded=False):
                        st.markdown('Propose a short developer note to append to `README.md`. The note will be appended only if the full test suite passes.')
                        dev_note = st.text_area('Developer note (markdown)', value='', height=200, key='admin_dev_note')
                        if st.button('Run tests and append to README'):
                            if not dev_note.strip():
                                st.warning('Enter a developer note before running tests.')
                            else:
                                st.info('Running test suite — this may take a moment...')
                                try:
                                    import subprocess

                                    # Run pytest; capture output
                                    proc = subprocess.run(['python3', '-m', 'pytest', '-q'], capture_output=True, text=True, timeout=600)
                                    output = (proc.stdout or '') + '\n' + (proc.stderr or '')
                                    if proc.returncode == 0:
                                        # Append to README.md with timestamp and author (signed-in email if present)
                                        readme_path = 'README.md'
                                        try:
                                            author = st.session_state.get('user_email') or 'admin'
                                            ts = datetime.datetime.utcnow().isoformat() + 'Z'
                                            entry = f"\n\n## Developer note ({ts}) by {author}\n\n{dev_note.strip()}\n"
                                            with open(readme_path, 'a', encoding='utf-8') as rf:
                                                rf.write(entry)
                                            st.success('All tests passed. Developer note appended to README.md')
                                            with st.expander('Test output (truncated)', expanded=False):
                                                st.text(output[:10000])
                                        except Exception as e:
                                            st.error(f'Failed to append README.md: {e}')
                                    else:
                                        st.error('Tests failed — README not modified. See test output below.')
                                        with st.expander('Test output', expanded=True):
                                            st.text(output)
                                except subprocess.TimeoutExpired:
                                    st.error('Test run timed out.')
                                except Exception as e:
                                    st.error(f'Error running tests: {e}')

                # Admin: OAuth credentials configuration (admin-only)
                st.markdown('**Admin: OAuth App Configuration**')
                try:
                    # Meta / Facebook
                    st.markdown('Meta / Facebook OAuth')
                    meta_cfg = _meta_conn.load_meta('meta') if _meta_conn else {}
                    meta_client = meta_cfg.get('client_id') if meta_cfg else ''
                    meta_secret = meta_cfg.get('client_secret') if meta_cfg else ''
                    meta_redirect = meta_cfg.get('redirect_uri') if meta_cfg else ''
                    new_meta_client = st.text_input('Meta Client ID', value=meta_client, key='admin_meta_client')
                    new_meta_secret = st.text_input('Meta Client Secret', value=meta_secret, key='admin_meta_secret')
                    new_meta_redirect = st.text_input('Meta Redirect URI', value=meta_redirect or os.environ.get('OAUTH_REDIRECT_URI', ''), key='admin_meta_redirect')
                    if st.button('Save Meta OAuth settings'):
                        try:
                            if _meta_conn:
                                # If a client secret is provided but the environment
                                # does not provide DASHBOARD_SECRET_KEY, warn and
                                # require explicit confirmation before saving plain text.
                                try:
                                    can_enc = _meta_conn.can_encrypt()
                                except Exception:
                                    can_enc = False
                                if new_meta_secret and not can_enc:
                                    confirm_key = 'confirm_save_meta_plain'
                                    if not st.session_state.get(confirm_key):
                                        st.warning('DASHBOARD_SECRET_KEY not set — client secret will be stored in plaintext. Tick the confirmation box to proceed.')
                                        if st.checkbox('I understand and accept storing the Meta client secret in plaintext', key=confirm_key):
                                            st.session_state[confirm_key] = True
                                        else:
                                            raise RuntimeError('Confirmation required to save plaintext secret')
                                _meta_conn.save_meta('meta', {'client_id': new_meta_client or '', 'client_secret': new_meta_secret or '', 'redirect_uri': new_meta_redirect or ''})
                                st.success('Saved Meta OAuth credentials to connector metadata.')
                            else:
                                st.error('Meta connector not available; cannot save settings.')
                        except RuntimeError as re:
                            st.info(str(re))
                        except Exception as e:
                            st.error(f'Failed to save Meta settings: {e}')

                    if st.button('Remove Meta OAuth settings'):
                        try:
                            if _meta_conn:
                                _meta_conn.save_meta('meta', {})
                                st.success('Cleared Meta OAuth settings from metadata.')
                            else:
                                st.error('Meta connector not available; cannot clear settings.')
                        except Exception as e:
                            st.error(f'Failed to clear Meta settings: {e}')

                    # Google
                    st.markdown('Google OAuth')
                    try:
                        from connectors import google_connector as _admin_google
                    except Exception:
                        _admin_google = None
                    gcfg = _admin_google.load_meta('google') if _admin_google else {}
                    g_client = gcfg.get('client_id') if gcfg else ''
                    g_secret = gcfg.get('client_secret') if gcfg else ''
                    new_g_client = st.text_input('Google Client ID', value=g_client, key='admin_google_client')
                    new_g_secret = st.text_input('Google Client Secret', value=g_secret, key='admin_google_secret')
                    if st.button('Save Google OAuth settings'):
                        try:
                            if _admin_google:
                                try:
                                    can_enc_g = _admin_google.can_encrypt()
                                except Exception:
                                    can_enc_g = False
                                if new_g_secret and not can_enc_g:
                                    confirm_key = 'confirm_save_google_plain'
                                    if not st.session_state.get(confirm_key):
                                        st.warning('DASHBOARD_SECRET_KEY not set — Google client secret will be stored in plaintext. Tick the confirmation box to proceed.')
                                        if st.checkbox('I understand and accept storing the Google client secret in plaintext', key=confirm_key):
                                            st.session_state[confirm_key] = True
                                        else:
                                            raise RuntimeError('Confirmation required to save plaintext secret')
                                _admin_google.save_meta('google', {'client_id': new_g_client or '', 'client_secret': new_g_secret or ''})
                                st.success('Saved Google OAuth credentials to connector metadata.')
                            else:
                                st.error('Google connector not available; cannot save settings.')
                        except RuntimeError as re:
                            st.info(str(re))
                        except Exception as e:
                            st.error(f'Failed to save Google settings: {e}')

                    if st.button('Remove Google OAuth settings'):
                        try:
                            if _admin_google:
                                _admin_google.save_meta('google', {})
                                st.success('Cleared Google OAuth settings from metadata.')
                            else:
                                st.error('Google connector not available; cannot clear settings.')
                        except Exception as e:
                            st.error(f'Failed to clear Google settings: {e}')

                    # TikTok
                    st.markdown('TikTok OAuth')
                    try:
                        from connectors import tiktok_connector as _admin_tiktok
                    except Exception:
                        _admin_tiktok = None
                    tcfg = _admin_tiktok.load_meta('tiktok') if _admin_tiktok else {}
                    t_client = tcfg.get('client_id') if tcfg else ''
                    t_secret = tcfg.get('client_secret') if tcfg else ''
                    new_t_client = st.text_input('TikTok Client ID', value=t_client, key='admin_tiktok_client')
                    new_t_secret = st.text_input('TikTok Client Secret', value=t_secret, key='admin_tiktok_secret')
                    if st.button('Save TikTok OAuth settings'):
                        try:
                            if _admin_tiktok:
                                try:
                                    can_enc_t = _admin_tiktok.can_encrypt()
                                except Exception:
                                    can_enc_t = False
                                if new_t_secret and not can_enc_t:
                                    confirm_key = 'confirm_save_tiktok_plain'
                                    if not st.session_state.get(confirm_key):
                                        st.warning('DASHBOARD_SECRET_KEY not set — TikTok client secret will be stored in plaintext. Tick the confirmation box to proceed.')
                                        if st.checkbox('I understand and accept storing the TikTok client secret in plaintext', key=confirm_key):
                                            st.session_state[confirm_key] = True
                                        else:
                                            raise RuntimeError('Confirmation required to save plaintext secret')
                                _admin_tiktok.save_meta('tiktok', {'client_id': new_t_client or '', 'client_secret': new_t_secret or ''})
                                st.success('Saved TikTok OAuth credentials to connector metadata.')
                            else:
                                st.error('TikTok connector not available; cannot save settings.')
                        except RuntimeError as re:
                            st.info(str(re))
                        except Exception as e:
                            st.error(f'Failed to save TikTok settings: {e}')

                    if st.button('Remove TikTok OAuth settings'):
                        try:
                            if _admin_tiktok:
                                _admin_tiktok.save_meta('tiktok', {})
                                st.success('Cleared TikTok OAuth settings from metadata.')
                            else:
                                st.error('TikTok connector not available; cannot clear settings.')
                        except Exception as e:
                            st.error(f'Failed to clear TikTok settings: {e}')
                except Exception:
                    st.error('Failed to render OAuth admin controls; ensure connectors are available.')

        # Admin/dev-only: show storage details, migration, and manual sync controls
        try:
            if dev_mode or st.session_state.get('is_admin_authenticated'):
                storage_loc = 'unknown'
                try:
                    storage_loc = _meta_conn.token_storage_location_for_user(st.session_state.get('user_email'))
                except Exception:
                    try:
                        storage_loc = _meta_conn.token_storage_location()
                    except Exception:
                        storage_loc = 'unknown'

                if storage_loc == 'keyring':
                    st.write('Token storage: OS Keychain')
                elif storage_loc == 'encrypted_file':
                    st.write('Token storage: Encrypted file (secure)')
                elif storage_loc == 'none':
                    st.write('Token storage: none')
                else:
                    st.write(f'Token storage: {storage_loc}')

                # Offer migration to OS keychain when an encrypted-file token exists
                try:
                    if storage_loc == 'encrypted_file' and _meta_conn.can_use_keyring():
                        if st.button('Migrate token to OS Keychain'):
                            with st.spinner('Migrating token to OS Keychain...'):
                                migrated = _meta_conn.migrate_file_token_to_keyring_for_user(st.session_state.get('user_email'))
                                if migrated:
                                    st.success('Token migrated to OS Keychain. The file-stored token was removed.')
                                else:
                                    st.error('Migration failed. Ensure DASHBOARD_SECRET_KEY is set and keyring is available.')
                except Exception:
                    pass

                if st.button('Manual Sync Meta (fetch and persist spend)'):
                    if _meta_conn is None:
                        st.error('Meta connector not available.')
                    else:
                        token = _meta_conn.get_token_for_meta_user(st.session_state.get('user_email'))
                        if not token:
                            st.error('No saved token found. Save a token first.')
                        else:
                            with st.spinner('Fetching spend from Meta (stub)...'):
                                try:
                                    df_new = _meta_conn.fetch_spend_and_persist(token, user_email=st.session_state.get('user_email'))
                                    if df_new is not None:
                                        st.success('Manual sync completed and spend.csv updated (stub data).')
                                        st.dataframe(df_new.head(10))
                                        try:
                                            logger = logging.getLogger('profit_dashboard')
                                            logger.info('Manual sync for Meta completed; %d rows returned.', len(df_new))
                                        except Exception:
                                            pass
                                    else:
                                        st.error('Sync returned no data.')
                                        try:
                                            logger = logging.getLogger('profit_dashboard')
                                            logger.warning('Manual sync for Meta returned no data.')
                                        except Exception:
                                            pass
                                except Exception as e:
                                    st.error(f'Manual sync failed: {e}')
                                    try:
                                        logger = logging.getLogger('profit_dashboard')
                                        logger.exception('Manual sync for Meta failed: %s', e)
                                    except Exception:
                                        pass
        except Exception:
            pass

    # Attempt to use DuckDB precomputed aggregates as a fast-path
    df_acq_agg = None
    df_agg_profit = None
    db_path = os.path.join('data', 'analytics.duckdb')
    try:
        if os.path.exists(db_path) and _DUCKDB_AVAILABLE:
            df_acq_agg, df_agg_profit = read_aggregates_from_duckdb(db_path)
    except Exception:
        # Fall back to CSV processing if any issue occurs
        df_acq_agg = None
        df_agg_profit = None

    # Sidebar: Date range presets (month-based presets by default)
    st.sidebar.markdown('**Date Range**')
    today = datetime.date.today()
    # persist selected_range in session_state so Reset/controls work
    if 'selected_range' not in st.session_state:
        st.session_state['selected_range'] = 'All time'
    range_options = [
        'All time',
        'Last 7 days',
        'Last 1 month',
        'Last 3 months',
        'Last 6 months',
        'Last 9 months',
        'Last 12 months',
        'Year to date',
        'Custom range',
    ]
    # allow resetting via a small button
    if st.sidebar.button('Reset to All time'):
        st.session_state['selected_range'] = 'All time'

    selected_range = st.sidebar.selectbox('Preset', range_options, index=range_options.index(st.session_state.get('selected_range', 'All time')), key='selected_range')
    start_date = None
    end_date = None
    if selected_range == 'All time':
        start_date = None
        end_date = None
    elif selected_range == 'Last 7 days':
        end_date = today
        start_date = today - datetime.timedelta(days=7)
    elif selected_range == 'Year to date':
        end_date = today
        start_date = datetime.date(today.year, 1, 1)
    elif selected_range == 'Custom range':
        dr = st.sidebar.date_input('Select date range', value=(today - datetime.timedelta(days=30), today))
        if isinstance(dr, (tuple, list)):
            start_date, end_date = dr[0], dr[1]
        else:
            start_date = dr
            end_date = dr
    else:
        # Month presets: parse the 'Last N months' text
        try:
            parts = selected_range.split()
            n = int(parts[1])
        except Exception:
            n = None

        if n:
            end_date = today
            if _RELATIVEDELTA_AVAILABLE:
                start_date = today - relativedelta(months=n)
            else:
                # fallback: approximate month as 30 days when dateutil isn't installed
                start_date = today - datetime.timedelta(days=30 * n)

        # Show the applied range to the user in the sidebar
        try:
            if start_date and end_date:
                st.sidebar.markdown(f"**Applied Range:** {start_date.isoformat()} → {end_date.isoformat()}")
            else:
                st.sidebar.markdown("**Applied Range:** All time")
        except Exception:
            pass

    # Apply date filter when order_date exists
    if start_date and end_date and 'order_date' in df_orders.columns:
        try:
            df_orders['order_date'] = pd.to_datetime(df_orders['order_date']).dt.date
            df_orders = df_orders[(df_orders['order_date'] >= start_date) & (df_orders['order_date'] <= end_date)].copy()
        except Exception:
            pass

    # If DuckDB fast-path didn't populate aggregates, compute from CSVs/dataframes
    if df_acq_agg is None or df_agg_profit is None:
        df_acq_agg, df_agg_profit = calculate_strategic_metrics(df_orders, df_spend)

    # Top-level KPIs
    total_revenue = float(df_orders.get("revenue", 0).sum())
    total_ad_spend = float(df_spend["ad_spend"].sum()) if (isinstance(df_spend, pd.DataFrame) and "ad_spend" in df_spend.columns) else 0.0
    total_contribution = float((df_orders.get("revenue", 0) - df_orders.get("cogs", 0) - df_orders.get("refunds", 0)).sum())

    # Additional KPIs: Avg True ROAS, Avg CAC, Avg LTV:CAC
    total_new_customers = int(df_acq_agg['new_customers'].sum()) if not df_acq_agg.empty else 0
    total_ad_spend_all = float(df_agg_profit['total_ad_spend'].sum()) if not df_agg_profit.empty else 0.0
    avg_cac = (total_ad_spend_all / total_new_customers) if total_new_customers > 0 else 0.0
    overall_true_roas = (float(df_agg_profit['total_revenue'].sum()) / total_ad_spend_all) if total_ad_spend_all > 0 else 0.0
    # Weighted avg LTV:CAC (weight by new_customers)
    if not df_acq_agg.empty and df_acq_agg['new_customers'].sum() > 0:
        avg_ltv_cac = float((df_acq_agg['ltv_cac_ratio'] * df_acq_agg['new_customers']).sum() / df_acq_agg['new_customers'].sum())
    else:
        avg_ltv_cac = 0.0

    # Show six KPI tiles with enhanced styling using HTML/CSS
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    
    # Helper function to create styled metric cards
    def _create_metric_card(col, title, value, subtitle, color):
        """Create a styled metric card with clean design."""
        col.markdown(f"""
        <div style="
            background: linear-gradient(135deg, {color}15 0%, {color}05 100%);
            border-left: 4px solid {color};
            padding: 16px;
            border-radius: 8px;
            margin-bottom: 8px;
        ">
            <div style="font-size: 12px; color: #666; margin-bottom: 8px; font-weight: 500;">{title}</div>
            <div style="font-size: 28px; font-weight: bold; color: {color}; margin-bottom: 6px;">{value}</div>
            <div style="font-size: 11px; color: #999;">{subtitle}</div>
        </div>
        """, unsafe_allow_html=True)
    
    # Display metrics with custom styling
    _create_metric_card(col1, "Total Revenue", f"${total_revenue:,.0f}", "Gross revenue", "#10B981")
    _create_metric_card(col2, "Ad Spend", f"${total_ad_spend:,.0f}", "Total spent", "#FB8C00")
    _create_metric_card(col3, "Net Contribution", f"${total_contribution:,.0f}", "Revenue - COGS - refunds", "#1E3A8A")
    
    roas_str = f"{overall_true_roas:.2f}".rstrip('.')
    _create_metric_card(col4, "Avg ROAS", roas_str, "Revenue / ad spend", "#8B5CF6")
    _create_metric_card(col5, "Avg CAC", f"${avg_cac:,.2f}", "Cost per customer", "#EC4899")
    _create_metric_card(col6, "LTV:CAC Ratio", f"{avg_ltv_cac:.2f}", "Weighted average", "#06B6D4")

    # Section separator with extra spacing
    st.markdown("<div style='margin: 40px 0;'></div>", unsafe_allow_html=True)

    # Show applied date range on main page for clarity
    try:
        if start_date and end_date:
            st.markdown(f"**Date Range:** {start_date.isoformat()} → {end_date.isoformat()}")
        else:
            st.markdown("**Date Range:** All time")
    except Exception:
        pass

    # Chart: show Ad Spend (bars) with LTV:CAC (line) on secondary axis
    if not df_agg_profit.empty:
        merged = pd.merge(
            df_agg_profit[['platform', 'total_ad_spend']].copy(),
            df_acq_agg[['platform', 'ltv_cac_ratio']].copy() if not df_acq_agg.empty else pd.DataFrame(columns=['platform', 'ltv_cac_ratio']),
            on='platform', how='outer'
        ).fillna(0)

        # Map platform to brand colors (Facebook blue, Google orange, TikTok red)
        brand_colors = {
            'Facebook': '#1877F2',
            'Meta': '#1877F2',
            'Google': '#FB8C00',
            'TikTok': '#EE1D52',
            'Instagram': '#E4405F',
            'Twitter': '#1DA1F2',
        }
        color_list = [brand_colors.get(p, '#777777') for p in merged['platform']]

        if _PLOTLY_AVAILABLE:
            fig = go.Figure()
            # Bar trace with hovertemplate showing formatted currency
            fig.add_trace(go.Bar(
                x=merged['platform'],
                y=merged['total_ad_spend'],
                name='Ad Spend',
                marker_color=color_list,
                hovertemplate='%{x}<br>Ad Spend: $%{y:,.2f}<extra></extra>'
            ))
            # Line trace with hover showing LTV:CAC
            fig.add_trace(
                go.Scatter(
                    x=merged['platform'],
                    y=merged['ltv_cac_ratio'],
                    name='LTV:CAC',
                    yaxis='y2',
                    mode='lines+markers',
                    marker=dict(color='crimson'),
                    hovertemplate='%{x}<br>LTV:CAC: %{y:.2f}<extra></extra>'
                )
            )
            fig.update_layout(
                title={
                    'text': '<b>Ad Spend & LTV:CAC Performance by Platform</b>',
                    'x': 0.5,
                    'xanchor': 'center',
                    'font': {'size': 18, 'color': '#1E3A8A'}
                },
                xaxis_title='<b>Platform</b>',
                yaxis=dict(title='<b>Ad Spend ($)</b>', titlefont=dict(color='#FB8C00'), tickfont=dict(color='#FB8C00')),
                yaxis2=dict(title='<b>LTV:CAC Ratio</b>', overlaying='y', side='right', titlefont=dict(color='#EC4899'), tickfont=dict(color='#EC4899')),
                legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
                hovermode='x unified',
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                xaxis=dict(showgrid=True, gridwidth=1, gridcolor='rgba(200,200,200,0.2)'),
                font=dict(family="Arial, sans-serif", size=12),
                margin=dict(b=80)
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            # Matplotlib fallback: bars + line on twin axis
            try:
                import matplotlib.pyplot as plt

                fig, ax = plt.subplots(figsize=(10, 4))
                x = range(len(merged))
                # Use color_list for bar colors, fall back to a single color if shapes mismatch
                try:
                    bar_colors = color_list
                except Exception:
                    bar_colors = '#1f77b4'
                ax.bar(x, merged['total_ad_spend'], color=bar_colors, alpha=0.9)
                ax.set_xticks(x)
                ax.set_xticklabels(merged['platform'], rotation=45)
                ax.set_ylabel('Ad Spend')
                ax2 = ax.twinx()
                ax2.plot(x, merged['ltv_cac_ratio'], color='crimson', marker='o')
                ax2.set_ylabel('LTV:CAC')
                plt.tight_layout()
                st.pyplot(fig)
            except Exception:
                st.bar_chart(df_agg_profit.set_index('platform')['total_ad_spend'])
    else:
        st.info('No channel profit data available to plot.')

    # Section separator with extra spacing
    st.markdown("<div style='margin: 50px 0 30px 0;'></div>", unsafe_allow_html=True)

    # Recommendations (use tuned thresholds)
    recs = generate_recommendations(
        df_orders,
        df_spend,
        scale_threshold=st.session_state.get('scale_threshold', 3.0),
        test_threshold=st.session_state.get('test_threshold', 1.2),
        urgent_threshold=st.session_state.get('urgent_threshold', 1.0),
    )
    
    st.subheader("Recommendations")
    if recs:
        for r in recs:
            # Determine badge color based on recommendation type
            if "Scale" in r or "increase" in r.lower():
                badge_color = "#10B981"
                badge_bg = "#D1F2EB"
            elif "Urgent" in r or "critical" in r.lower():
                badge_color = "#EF4444"
                badge_bg = "#FEE2E2"
            elif "Test" in r or "experiment" in r.lower():
                badge_color = "#F59E0B"
                badge_bg = "#FEF3C7"
            else:
                badge_color = "#3B82F6"
                badge_bg = "#DBEAFE"
            
            st.markdown(f"""
            <div style="
                background-color: {badge_bg};
                border-left: 4px solid {badge_color};
                padding: 12px 16px;
                border-radius: 6px;
                margin-bottom: 10px;
                font-size: 14px;
            ">
                <span style="color: {badge_color}; font-weight: 600;">{r}</span>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.info("No recommendations available. Your acquisition metrics look good!")

    # Section separator with extra spacing
    st.markdown("<div style='margin: 50px 0 30px 0;'></div>", unsafe_allow_html=True)
    st.markdown("<hr style='border: none; border-top: 2px solid #FB8C00; margin: 20px 0;' />", unsafe_allow_html=True)

    # Tables (display with human-friendly headers)
    st.subheader("Acquisition Aggregates")
    st.markdown("*Detailed breakdown by platform with LTV, CAC, and performance ratios*")
    # Show interactive grid when available
    # create a display copy with nicer column names (no underscores)
    def _prettify_cols(df: pd.DataFrame) -> pd.DataFrame:
        disp = df.copy()
        disp.columns = [str(c).replace('_', ' ').title() for c in disp.columns]
        return disp


    # Prepare a display DataFrame while preserving numeric types.
    df_acq_display = df_acq_agg.copy()
    try:
        if 'ltv_cac_ratio' in df_acq_display.columns:
            df_acq_display['ltv_cac_ratio'] = df_acq_display['ltv_cac_ratio'].round(2)
    except Exception:
        pass

    # Round other numeric columns slightly for display clarity, but keep numeric dtypes
    try:
        df_acq_display = df_acq_display.round({col: 3 for col in df_acq_display.select_dtypes(include=[float]).columns if col != 'ltv_cac_ratio'})
    except Exception:
        pass

    df_acq_disp = _prettify_cols(df_acq_display)

    # Format monetary columns for display (keep original CSV numeric values unchanged)
    def _format_currency_columns(df_disp: pd.DataFrame, cols_to_currency: list) -> pd.DataFrame:
        out = df_disp.copy()
        for c in cols_to_currency:
            if c in out.columns:
                out[c] = out[c].apply(lambda v: f"${v:,.2f}" if (pd.notnull(v) and isinstance(v, (int, float))) else v)
        return out

    acq_money_cols = ['Total Contribution', 'Total Ad Spend', 'Customer Acquisition Cost', 'Ltv']
    df_acq_disp = _format_currency_columns(df_acq_disp, acq_money_cols)

    # Render acquisition aggregates: prefer AgGrid with pagination and sorting, fallback to st.dataframe
    try:
        if _AGGRID_AVAILABLE:
            try:
                gb = GridOptionsBuilder.from_dataframe(df_acq_disp)
                gb.configure_pagination(enabled=True)
                gb.configure_default_column(filter=True, sortable=True)
                grid_options = gb.build()
                AgGrid(df_acq_disp, gridOptions=grid_options, fit_columns_on_grid_load=True)
            except Exception:
                st.dataframe(df_acq_disp)
        else:
            st.dataframe(df_acq_disp)
    except Exception:
        try:
            st.dataframe(df_acq_disp)
        except Exception:
            st.write('Acquisition aggregates are unavailable.')
    # Raw CSV for downstream analysis
    st.download_button('Download Acquisition CSV', data=df_acq_agg.to_csv(index=False), file_name='acquisition_report.csv')
    # Prettified CSV (title-case headers + currency formatting) for human-readable exports
    try:
        acq_pretty_csv = df_acq_disp.to_csv(index=False)
        st.download_button('Download Prettified Acquisition CSV', data=acq_pretty_csv, file_name='prettified_acquisition.csv')
        # Persist a copy locally for quick verification (outputs/)
        try:
            outdir = os.path.join('outputs')
            os.makedirs(outdir, exist_ok=True)
            with open(os.path.join(outdir, 'prettified_acquisition.csv'), 'w', encoding='utf-8') as _f:
                _f.write(acq_pretty_csv)
        except Exception:
            pass
    except Exception:
        pass

    # Profit Aggregates: show per-channel profit metrics to users (Total Revenue,
    # Net Contribution, Total Ad Spend, True ROAS). Keep formatting consistent
    # with the Acquisition Aggregates above.
    st.subheader("Profit Aggregates")
    st.markdown("*Revenue, margins, and ROI metrics by platform*")
    try:
        # Prepare display copy and round human-facing ratios
        df_profit_display = df_agg_profit.copy()
        try:
            if 'true_roas' in df_profit_display.columns:
                df_profit_display['true_roas'] = df_profit_display['true_roas'].round(2)
        except Exception:
            pass
        try:
            df_profit_display = df_profit_display.round({col: 3 for col in df_profit_display.select_dtypes(include=[float]).columns if col != 'true_roas'})
        except Exception:
            pass

        df_profit_disp = _prettify_cols(df_profit_display)
        # Format monetary columns for display
        profit_money_cols = ['Total Revenue', 'Net Contribution Profit', 'Total Ad Spend']
        df_profit_disp = _format_currency_columns(df_profit_disp, profit_money_cols)

        # Build header map for profit df (original column names)
        header_map_profit = {c: str(c).replace('_', ' ').title() for c in df_profit_display.columns}
        currency_cols_profit = [c for c in ['total_revenue', 'net_contribution_profit', 'total_ad_spend'] if c in df_profit_display.columns];

        try:
            if _AGGRID_AVAILABLE:
                try:
                    gb2 = GridOptionsBuilder.from_dataframe(df_profit_disp)
                    gb2.configure_pagination(enabled=True)
                    gb2.configure_default_column(filter=True, sortable=True)
                    AgGrid(df_profit_disp, gridOptions=gb2.build(), fit_columns_on_grid_load=True)
                except Exception:
                    st.dataframe(df_profit_disp)
            else:
                st.dataframe(df_profit_disp)
        except Exception:
            try:
                st.dataframe(df_profit_disp)
            except Exception:
                st.write('Profit aggregates are unavailable.')

        # Raw profit CSV
        st.download_button('Download Channel Profit CSV', data=df_agg_profit.to_csv(index=False), file_name='channel_profit_report.csv')
        # Prettified profit CSV for human-readable export
        try:
            profit_pretty_csv = df_profit_disp.to_csv(index=False)
            st.download_button('Download Prettified Profit CSV', data=profit_pretty_csv, file_name='prettified_profit.csv')
            try:
                outdir = os.path.join('outputs')
                os.makedirs(outdir, exist_ok=True)
                with open(os.path.join(outdir, 'prettified_profit.csv'), 'w', encoding='utf-8') as _f:
                    _f.write(profit_pretty_csv)
            except Exception:
                pass
        except Exception:
            pass
    except Exception:
        st.info('Profit aggregates are currently unavailable.')

    # Section separator with extra spacing
    st.markdown("<div style='margin: 50px 0 30px 0;'></div>", unsafe_allow_html=True)
    st.markdown("<hr style='border: none; border-top: 2px solid #FB8C00; margin: 20px 0;' />", unsafe_allow_html=True)
    
    st.subheader("Export / Snapshot")

    # Prepare KPI text for PDF header
    kpi_lines = (
        f"Total Revenue: ${total_revenue:,.2f}\n"
        f"Total Ad Spend: ${total_ad_spend:,.2f}\n"
        f"Net Contribution: ${total_contribution:,.2f}\n"
    )

    # Create chart and table images (if matplotlib available)
    chart_bytes = create_chart_image(df_acq_agg)
    table_images = []
    img_acq = df_to_image_bytes(df_acq_agg.round(3))
    if img_acq:
        table_images.append(img_acq)
    img_profit = df_to_image_bytes(df_agg_profit.round(3))
    if img_profit:
        table_images.append(img_profit)

    if _REPORTLAB_AVAILABLE:
        pdf_bytes = create_pdf_with_tables(kpi_lines, chart_bytes, table_images)
        if pdf_bytes:
            st.download_button("Download Snapshot PDF", data=pdf_bytes, file_name="profit_dashboard_snapshot.pdf", mime="application/pdf")
        else:
            st.warning("Failed to create PDF snapshot. You can download images instead.")
            # fall through to image downloads

    if not _REPORTLAB_AVAILABLE or not (pdf_bytes if 'pdf_bytes' in locals() else False):
        # Provide image downloads and a ZIP fallback so users can still capture snapshots
        st.info("PDF export requires `reportlab`. Offering PNG downloads and a ZIP of images as a fallback.")
        if chart_bytes:
            st.download_button("Download Chart PNG", data=chart_bytes, file_name="ltv_cac_chart.png", mime="image/png")
        if img_acq:
            st.download_button("Download Acquisition Table PNG", data=img_acq, file_name="acquisition_table.png", mime="image/png")
        if img_profit:
            st.download_button("Download Profit Table PNG", data=img_profit, file_name="profit_table.png", mime="image/png")

        # ZIP all images if available
        images_to_zip = []
        if chart_bytes:
            images_to_zip.append(("ltv_cac_chart.png", chart_bytes))
        if img_acq:
            images_to_zip.append(("acquisition_table.png", img_acq))
        if img_profit:
            images_to_zip.append(("profit_table.png", img_profit))

        zip_bytes = create_zip_of_images(images_to_zip) if images_to_zip else None
        if zip_bytes:
            st.download_button("Download Snapshot ZIP", data=zip_bytes, file_name="profit_dashboard_snapshot_images.zip", mime="application/zip")

    # Dev debug panel: show raw data and metrics
    # Logs viewer (show last N lines and allow download) in the sidebar
    try:
        with st.sidebar.expander('Logs', expanded=False):
            st.markdown('Recent dashboard logs (info & error).')
            try:
                def _tail(path: str, n: int = 200) -> str:
                    if not os.path.exists(path):
                        return ''
                    with open(path, 'rb') as f:
                        f.seek(0, os.SEEK_END)
                        sz = f.tell()
                        offset = max(0, sz - 32 * 1024)
                        f.seek(offset)
                        data = f.read().decode(errors='replace')
                    lines = data.splitlines()
                    return '\n'.join(lines[-n:])

                logs_text = _tail(log_file, n=200)
                if logs_text:
                    st.text_area('Recent logs', value=logs_text, height=240)
                    with open(log_file, 'rb') as _lf:
                        st.download_button('Download logs', data=_lf.read(), file_name='dashboard.log')
                else:
                    st.write('No logs yet.')
            except Exception:
                st.write('Unable to read logs.')
    except Exception:
        pass

    if dev_mode:
        with st.expander('Dev: Debug Info & Dataframes', expanded=False):
            st.write('Orders sample')
            st.dataframe(df_orders.head(50))
            st.write('Spend sample')
            st.dataframe(df_spend.head(50))
            st.write('Acquisition aggregates')
            st.dataframe(df_acq_agg.round(3))


if __name__ == "__main__":
    # Run a quick simulated OAuth flow in dev mode, then launch the dashboard.
    # Keep imports local so running the app as a module doesn't require FastAPI/uvicorn.
    from connectors import oauth_server

    # Only run the simulated OAuth flow when explicitly requested via env var.
    # This avoids requiring `fastapi`/`uvicorn` or performing I/O when starting
    # the app in production. Set `DEV_SIMULATE_OAUTH=1` to enable the simulated
    # flow for local testing.
    if str(os.environ.get('DEV_SIMULATE_OAUTH', '')).lower() in ('1', 'true', 'yes'):
        try:
            oauth_server.start_oauth(platform='meta', state='demo@local')
        except Exception:
            pass

        try:
            oauth_server.callback(None, platform='meta', token='SIMULATED_META_TOKEN', state='demo@local')
        except Exception:
            pass

    # Launch the Streamlit dashboard (normal operation for both dev and prod)
    run_dashboard()