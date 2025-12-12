"""Minimal Google Ads connector scaffold.

Patterned after `connectors/meta_connector.py` — provides token storage helpers
(prefer OS `keyring`, fallback to Fernet-encrypted file storage) and a
`fetch_spend_and_persist()` stub which attempts a real call when credentials
are provided but otherwise produces synthetic demo spend rows.

The runtime metadata file is `data/connectors_meta.json` and per-user tokens
are stored under the `google` platform key when Fernet fallback is used.
"""
import os
import json
from typing import Optional, Dict, List

try:
    from cryptography.fernet import Fernet
    _FERNET_AVAILABLE = True
except Exception:
    _FERNET_AVAILABLE = False
try:
    import keyring
    _KEYRING_AVAILABLE = True
except Exception:
    keyring = None
    _KEYRING_AVAILABLE = False

import pandas as pd
import logging

META_FILE = os.path.join('data', 'connectors_meta.json')


def _ensure_data_dir():
    os.makedirs('data', exist_ok=True)


def can_encrypt() -> bool:
    return _FERNET_AVAILABLE and bool(os.environ.get('DASHBOARD_SECRET_KEY'))


def can_use_keyring() -> bool:
    return _KEYRING_AVAILABLE


def encrypt_token(token: str) -> Optional[bytes]:
    if not _FERNET_AVAILABLE:
        return None
    key = os.environ.get('DASHBOARD_SECRET_KEY')
    if not key:
        return None
    f = Fernet(key.encode() if isinstance(key, str) else key)
    return f.encrypt(token.encode())


def decrypt_token(token_encrypted: bytes) -> Optional[str]:
    if not _FERNET_AVAILABLE:
        return None
    key = os.environ.get('DASHBOARD_SECRET_KEY')
    if not key:
        return None
    f = Fernet(key.encode() if isinstance(key, str) else key)
    try:
        return f.decrypt(token_encrypted).decode()
    except Exception:
        return None


def save_meta(platform: str, meta: Dict) -> None:
    _ensure_data_dir()
    data = {}
    if os.path.exists(META_FILE):
        try:
            with open(META_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception:
            data = {}
    # Encrypt client_secret if provided and encryption available
    try:
        if 'client_secret' in meta and can_encrypt():
            enc = encrypt_token(meta['client_secret'])
            if enc:
                meta['client_secret_enc'] = enc.decode('latin-1')
                try:
                    del meta['client_secret']
                except Exception:
                    pass
    except Exception:
        pass

    existing = data.get(platform, {})
    existing.update(meta)
    data[platform] = existing
    with open(META_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)


def load_meta(platform: str) -> Optional[Dict]:
    if not os.path.exists(META_FILE):
        return None
    try:
        with open(META_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        cfg = data.get(platform)
        try:
            if cfg and 'client_secret_enc' in cfg and can_encrypt():
                try:
                    dec = decrypt_token(cfg.get('client_secret_enc').encode('latin-1'))
                    if dec:
                        cfg['client_secret'] = dec
                except Exception:
                    pass
        except Exception:
            pass
        return cfg
    except Exception:
        return None


def store_token_for_google_user(token: str, user_email: Optional[str] = None) -> bool:
    email_key = user_email or 'default'
    if can_use_keyring():
        try:
            keyring.set_password('profit_dashboard', f'google_access_token_{email_key}', token)
            meta = load_meta('google') or {}
            meta.setdefault('last_sync', None)
            save_meta('google', meta)
            logging.getLogger('profit_dashboard').info('Stored Google token in OS keyring for %s.', email_key)
            return True
        except Exception:
            logging.getLogger('profit_dashboard').exception('Failed to store Google token in keyring; falling back.')

    if not can_encrypt():
        return False
    enc = encrypt_token(token)
    if not enc:
        return False
    meta = load_meta('google') or {}
    tokens = meta.get('tokens', {})
    tokens[email_key] = enc.decode('latin-1')
    meta['tokens'] = tokens
    meta.setdefault('last_sync', None)
    save_meta('google', meta)
    logging.getLogger('profit_dashboard').info('Stored Google token in encrypted file for %s.', email_key)
    return True


def get_token_for_google_user(user_email: Optional[str] = None) -> Optional[str]:
    email_key = user_email or 'default'
    if can_use_keyring():
        try:
            t = keyring.get_password('profit_dashboard', f'google_access_token_{email_key}')
            if t:
                return t
        except Exception:
            pass

    m = load_meta('google') or {}
    tokens = m.get('tokens', {})
    enc = tokens.get(email_key)
    if not enc:
        return None
    try:
        return decrypt_token(enc.encode('latin-1'))
    except Exception:
        return None


def token_storage_location_for_user(user_email: Optional[str] = None) -> str:
    email_key = user_email or 'default'
    if can_use_keyring():
        try:
            if keyring.get_password('profit_dashboard', f'google_access_token_{email_key}'):
                return 'keyring'
        except Exception:
            pass
    m = load_meta('google') or {}
    tokens = m.get('tokens', {})
    if tokens and tokens.get(email_key):
        return 'encrypted_file'
    return 'none'


def migrate_file_token_to_keyring_for_user(user_email: Optional[str] = None) -> bool:
    if not can_use_keyring():
        return False
    email_key = user_email or 'default'
    m = load_meta('google') or {}
    tokens = m.get('tokens', {})
    enc = tokens.get(email_key)
    if not enc:
        return False
    try:
        token = decrypt_token(enc.encode('latin-1'))
        if not token:
            return False
        keyring.set_password('profit_dashboard', f'google_access_token_{email_key}', token)
        try:
            del tokens[email_key]
            m['tokens'] = tokens
        except Exception:
            pass
        save_meta('google', m)
        logging.getLogger('profit_dashboard').info('Migrated Google token for %s to keyring.', email_key)
        return True
    except Exception:
        return False


def update_last_sync(platform: str) -> None:
    m = load_meta(platform) or {}
    import datetime

    m['last_sync'] = datetime.datetime.utcnow().isoformat() + 'Z'
    save_meta(platform, m)


def _fetch_google_ads_spend(access_token: str, customer_id: str, start_date: str, end_date: str) -> List[Dict]:
    """Fetch actual Google Ads spend data using the Google Ads API.
    
    Args:
        access_token: OAuth access token
        customer_id: Google Ads customer ID (format: 123-456-7890)
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
    
    Returns:
        List of dicts with campaign_id, ad_spend, platform, date
    """
    GoogleAdsException = None
    try:
        from google.ads.googleads.client import GoogleAdsClient
        from google.ads.googleads.errors import GoogleAdsException as GAException
        GoogleAdsException = GAException
        
        # Create Google Ads client with OAuth token
        credentials = {
            'developer_token': os.environ.get('GOOGLE_ADS_DEVELOPER_TOKEN'),
            'client_id': os.environ.get('GOOGLE_CLIENT_ID'),
            'client_secret': os.environ.get('GOOGLE_CLIENT_SECRET'),
            'refresh_token': access_token,  # Use the OAuth token as refresh token
            'use_proto_plus': True,
        }
        
        client = GoogleAdsClient.load_from_dict(credentials)
        ga_service = client.get_service('GoogleAdsService')
        
        # Remove dashes from customer ID
        customer_id_clean = customer_id.replace('-', '')
        
        # Query for campaign spend data
        query = f"""
            SELECT
                campaign.id,
                campaign.name,
                metrics.cost_micros,
                segments.date
            FROM campaign
            WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
                AND campaign.status = 'ENABLED'
            ORDER BY segments.date DESC
        """
        
        response = ga_service.search(customer_id=customer_id_clean, query=query)
        
        results = []
        for row in response:
            # Convert micros to actual currency (Google stores cost in micros)
            cost = row.metrics.cost_micros / 1_000_000.0
            results.append({
                'campaign_id': str(row.campaign.id),
                'campaign_name': row.campaign.name,
                'ad_spend': cost,
                'platform': 'Google',
                'date': row.segments.date
            })
        
        return results
        
    except Exception as ex:
        # Check if it's a GoogleAdsException
        if GoogleAdsException and isinstance(ex, GoogleAdsException):
            logging.getLogger('profit_dashboard').error(f'Google Ads API error: {ex}')
        else:
            logging.getLogger('profit_dashboard').error(f'Error fetching Google Ads data: {ex}')
        return []


def fetch_spend_and_persist(access_token: str = None, user_email: Optional[str] = None, write_to_csv: bool = True) -> Optional[pd.DataFrame]:
    """Attempt to fetch Google Ads spend or return a synthetic stub.

    The returned DataFrame uses the normalized schema expected by the dashboard:
    `campaign_id`, `ad_spend`, `platform`, `date`.
    """
    token = access_token or get_token_for_google_user(user_email)
    ad_account = os.environ.get('GOOGLE_AD_ACCOUNT_ID')
    if not ad_account:
        meta = load_meta('google') or {}
        try:
            if user_email:
                email_map = meta.get('email_ad_accounts', {}) or {}
                if email_map and email_map.get(user_email):
                    ad_account = email_map.get(user_email)
        except Exception:
            pass
        if not ad_account:
            ad_account = meta.get('ad_account_id')

    # Attempt real API call if we have token and customer ID
    if token and ad_account:
        try:
            # Get date range for last 30 days
            from datetime import datetime, timedelta
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            
            # Fetch real data
            data = _fetch_google_ads_spend(token, ad_account, start_date, end_date)
            
            if data:
                df = pd.DataFrame(data)
                if write_to_csv and not df.empty:
                    _ensure_data_dir()
                    out = os.path.join('data', 'spend.csv')
                    try:
                        df.to_csv(out, index=False)
                        update_last_sync('google')
                        logging.getLogger('profit_dashboard').info(f'Fetched {len(data)} rows from Google Ads API')
                    except Exception:
                        pass
                return df
        except Exception as ex:
            logging.getLogger('profit_dashboard').warning(f'Failed to fetch real Google Ads data: {ex}. Using synthetic data.')

    # Fallback synthetic data
    logging.getLogger('profit_dashboard').info('Using synthetic Google Ads data (no token or customer ID)')
    df = pd.DataFrame([
        {'campaign_id': 'g_campA', 'ad_spend': 500.0, 'platform': 'Google', 'date': '2025-01-01'},
        {'campaign_id': 'g_campB', 'ad_spend': 250.0, 'platform': 'Google', 'date': '2025-01-02'},
    ])
    if write_to_csv:
        _ensure_data_dir()
        out = os.path.join('data', 'spend.csv')
        try:
            df.to_csv(out, index=False)
            update_last_sync('google')
        except Exception:
            pass
    return df


def list_ad_accounts(access_token: str) -> List[dict]:
    """Fetch all Google Ads accounts accessible with this token.
    
    Returns list of dicts with 'id', 'name', 'customer_id' fields.
    Example: [{'id': '123-456-7890', 'name': 'My Google Ads', 'customer_id': '1234567890'}, ...]
    """
    GoogleAdsException = None
    try:
        from google.ads.googleads.client import GoogleAdsClient
        from google.ads.googleads.errors import GoogleAdsException as GAException
        GoogleAdsException = GAException
        
        # Create Google Ads client with OAuth token
        credentials = {
            'developer_token': os.environ.get('GOOGLE_ADS_DEVELOPER_TOKEN'),
            'client_id': os.environ.get('GOOGLE_CLIENT_ID'),
            'client_secret': os.environ.get('GOOGLE_CLIENT_SECRET'),
            'refresh_token': access_token,
            'use_proto_plus': True,
        }
        
        client = GoogleAdsClient.load_from_dict(credentials)
        customer_service = client.get_service('CustomerService')
        
        # Get accessible customers
        accessible_customers = customer_service.list_accessible_customers()
        
        accounts = []
        for customer_id in accessible_customers.resource_names:
            # Extract customer ID from resource name (format: "customers/1234567890")
            cid = customer_id.split('/')[-1]
            
            # Format with dashes for display
            formatted_id = f"{cid[:3]}-{cid[3:6]}-{cid[6:]}"
            
            # Get customer details
            try:
                ga_service = client.get_service('GoogleAdsService')
                query = """
                    SELECT
                        customer.id,
                        customer.descriptive_name
                    FROM customer
                    LIMIT 1
                """
                response = ga_service.search(customer_id=cid, query=query)
                
                for row in response:
                    accounts.append({
                        'id': formatted_id,
                        'name': row.customer.descriptive_name or f"Account {formatted_id}",
                        'customer_id': cid
                    })
                    break
            except Exception:
                # If we can't get details, still add the account
                accounts.append({
                    'id': formatted_id,
                    'name': f"Account {formatted_id}",
                    'customer_id': cid
                })
        
        return accounts
        
    except Exception as ex:
        # Check if it's a GoogleAdsException
        if GoogleAdsException and isinstance(ex, GoogleAdsException):
            logging.getLogger('profit_dashboard').error(f'Google Ads API error listing accounts: {ex}')
        else:
            logging.getLogger('profit_dashboard').error(f'Error listing Google Ads accounts: {ex}')
        return []
