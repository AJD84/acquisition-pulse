"""Minimal TikTok Ads connector scaffold.

Mirrors the token-storage and metadata patterns used by other connectors in
this project. Provides `store_token_for_tiktok_user`, `get_token_for_tiktok_user`,
and `fetch_spend_and_persist()` which returns a normalized spend DataFrame.
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
    # Encrypt client_secret if present and possible
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


def store_token_for_tiktok_user(token: str, user_email: Optional[str] = None) -> bool:
    email_key = user_email or 'default'
    if can_use_keyring():
        try:
            keyring.set_password('profit_dashboard', f'tiktok_access_token_{email_key}', token)
            meta = load_meta('tiktok') or {}
            meta.setdefault('last_sync', None)
            save_meta('tiktok', meta)
            logging.getLogger('profit_dashboard').info('Stored TikTok token in OS keyring for %s.', email_key)
            return True
        except Exception:
            logging.getLogger('profit_dashboard').exception('Failed to store TikTok token in keyring; falling back.')

    if not can_encrypt():
        return False
    enc = encrypt_token(token)
    if not enc:
        return False
    meta = load_meta('tiktok') or {}
    tokens = meta.get('tokens', {})
    tokens[email_key] = enc.decode('latin-1')
    meta['tokens'] = tokens
    meta.setdefault('last_sync', None)
    save_meta('tiktok', meta)
    logging.getLogger('profit_dashboard').info('Stored TikTok token in encrypted file for %s.', email_key)
    return True


def get_token_for_tiktok_user(user_email: Optional[str] = None) -> Optional[str]:
    email_key = user_email or 'default'
    if can_use_keyring():
        try:
            t = keyring.get_password('profit_dashboard', f'tiktok_access_token_{email_key}')
            if t:
                return t
        except Exception:
            pass

    m = load_meta('tiktok') or {}
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
            if keyring.get_password('profit_dashboard', f'tiktok_access_token_{email_key}'):
                return 'keyring'
        except Exception:
            pass
    m = load_meta('tiktok') or {}
    tokens = m.get('tokens', {})
    if tokens and tokens.get(email_key):
        return 'encrypted_file'
    return 'none'


def migrate_file_token_to_keyring_for_user(user_email: Optional[str] = None) -> bool:
    if not can_use_keyring():
        return False
    email_key = user_email or 'default'
    m = load_meta('tiktok') or {}
    tokens = m.get('tokens', {})
    enc = tokens.get(email_key)
    if not enc:
        return False
    try:
        token = decrypt_token(enc.encode('latin-1'))
        if not token:
            return False
        keyring.set_password('profit_dashboard', f'tiktok_access_token_{email_key}', token)
        try:
            del tokens[email_key]
            m['tokens'] = tokens
        except Exception:
            pass
        save_meta('tiktok', m)
        logging.getLogger('profit_dashboard').info('Migrated TikTok token for %s to keyring.', email_key)
        return True
    except Exception:
        return False


def update_last_sync(platform: str) -> None:
    m = load_meta(platform) or {}
    import datetime

    m['last_sync'] = datetime.datetime.utcnow().isoformat() + 'Z'
    save_meta(platform, m)


def fetch_spend_and_persist(access_token: str = None, user_email: Optional[str] = None, write_to_csv: bool = True) -> Optional[pd.DataFrame]:
    """Fetch TikTok spend or return a synthetic fallback.

    Returns DataFrame with columns `campaign_id`, `ad_spend`, `platform`, `date`.
    """
    token = access_token or get_token_for_tiktok_user(user_email)
    ad_account = os.environ.get('TIKTOK_AD_ACCOUNT_ID')
    if not ad_account:
        meta = load_meta('tiktok') or {}
        try:
            if user_email:
                email_map = meta.get('email_ad_accounts', {}) or {}
                if email_map and email_map.get(user_email):
                    ad_account = email_map.get(user_email)
        except Exception:
            pass
        if not ad_account:
            ad_account = meta.get('ad_account_id')

    # Attempt real TikTok Marketing API call
    if token and ad_account:
        try:
            import requests
            from datetime import datetime, timedelta
            
            # Get date range for last 30 days
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            
            # TikTok Marketing API endpoint for reports
            url = 'https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/'
            
            headers = {
                'Access-Token': token,
                'Content-Type': 'application/json',
            }
            
            payload = {
                'advertiser_id': ad_account,
                'report_type': 'BASIC',
                'data_level': 'AUCTION_CAMPAIGN',
                'dimensions': ['campaign_id', 'stat_time_day'],
                'metrics': ['spend'],
                'start_date': start_date,
                'end_date': end_date,
                'page': 1,
                'page_size': 1000,
            }
            
            r = requests.post(url, headers=headers, json=payload, timeout=20)
            
            if r.status_code == 200:
                j = r.json()
                if j.get('code') == 0:
                    data = []
                    for rec in j.get('data', {}).get('list', []):
                        dimensions = rec.get('dimensions', {})
                        metrics = rec.get('metrics', {})
                        
                        campaign_id = dimensions.get('campaign_id', '')
                        date = dimensions.get('stat_time_day', '')
                        spend = float(metrics.get('spend', 0.0))
                        
                        data.append({
                            'campaign_id': str(campaign_id),
                            'ad_spend': spend,
                            'platform': 'TikTok',
                            'date': date
                        })
                    
                    if data:
                        df = pd.DataFrame(data)
                        if write_to_csv:
                            _ensure_data_dir()
                            out = os.path.join('data', 'spend.csv')
                            try:
                                df.to_csv(out, index=False)
                                update_last_sync('tiktok')
                                logging.getLogger('profit_dashboard').info(f'Fetched {len(data)} rows from TikTok Marketing API')
                            except Exception:
                                pass
                        return df
                else:
                    logging.getLogger('profit_dashboard').warning(f'TikTok API error: code {j.get("code")} - {j.get("message")}')
            else:
                logging.getLogger('profit_dashboard').warning(f'TikTok API error: {r.status_code} - {r.text}')
        except Exception as ex:
            logging.getLogger('profit_dashboard').warning(f'Failed to fetch real TikTok data: {ex}. Using synthetic data.')

    # Synthetic fallback
    logging.getLogger('profit_dashboard').info('Using synthetic TikTok Ads data (no token or advertiser ID)')
    df = pd.DataFrame([
        {'campaign_id': 't_campA', 'ad_spend': 400.0, 'platform': 'TikTok', 'date': '2025-01-01'},
        {'campaign_id': 't_campB', 'ad_spend': 180.0, 'platform': 'TikTok', 'date': '2025-01-02'},
    ])
    if write_to_csv:
        _ensure_data_dir()
        out = os.path.join('data', 'spend.csv')
        try:
            df.to_csv(out, index=False)
            update_last_sync('tiktok')
        except Exception:
            pass
    return df


def list_ad_accounts(access_token: str) -> List[dict]:
    """Fetch all TikTok advertiser accounts accessible with this token.
    
    Returns list of dicts with 'id', 'name', 'advertiser_id' fields.
    Example: [{'id': '123456', 'name': 'My TikTok Ads', 'advertiser_id': '123456'}, ...]
    """
    try:
        import requests
        # TikTok Marketing API endpoint to list advertisers
        url = 'https://business-api.tiktok.com/open_api/v1.3/oauth2/advertiser/get/'
        headers = {
            'Access-Token': access_token,
        }
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code == 200:
            j = r.json()
            if j.get('code') == 0:
                accounts = []
                for adv in j.get('data', {}).get('list', []):
                    accounts.append({
                        'id': str(adv.get('advertiser_id', '')),
                        'name': adv.get('advertiser_name', 'Unnamed Account'),
                        'advertiser_id': str(adv.get('advertiser_id', '')),
                    })
                return accounts
        return []
    except Exception:
        return []
