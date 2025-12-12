"""Minimal Meta (Facebook) connector scaffold.

This module provides simple helpers to store an access token encrypted using
`cryptography.Fernet`, load/save connector metadata, and a small `fetch_spend`
stub which should be replaced by real Marketing API calls when credentials
and app setup are provided.

The storage file is `data/connectors_meta.json` and tokens are stored encrypted.
To decrypt/encrypt tokens a `DASHBOARD_SECRET_KEY` env var (a base64 Fernet key)
is expected. For local demos you can create one via:

  from cryptography.fernet import Fernet
  print(Fernet.generate_key().decode())

Keep the secret key out of source control.
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
    """Return True when the `keyring` library is available and appears usable.

    On macOS this will use the system Keychain, which is a safer default than
    storing tokens in a local file.
    """
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
    # If a client_secret is being saved and Fernet is available, encrypt it
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

    # Ensure tokens map exists for backward compatibility
    existing = data.get(platform, {})
    # Merge meta dictionaries so we don't overwrite unrelated keys
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
        # If client_secret_enc exists, attempt to decrypt and expose plaintext
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


def store_token_for_meta(token: str) -> bool:
    """Store a token for Meta.

    Prefer the OS keyring when available; fall back to encrypted file storage
    using `DASHBOARD_SECRET_KEY` if keyring isn't available.
    Returns True on success.
    """
    # Prefer keyring (OS keychain) when available
    # Legacy single-token storage preserved under key 'default'
    return store_token_for_meta_user(token, user_email=None)


def store_token_for_meta_user(token: str, user_email: Optional[str] = None) -> bool:
    """Store a token for Meta for a specific user (email)."""
    email_key = user_email or 'default'
    if can_use_keyring():
        try:
            keyring.set_password('profit_dashboard', f'meta_access_token_{email_key}', token)
            # keep metadata file for other meta settings (ad_account_id, last_sync)
            meta = load_meta('meta') or {}
            meta.setdefault('last_sync', None)
            # Do not write token into file when using keyring
            save_meta('meta', meta)
            try:
                logging.getLogger('profit_dashboard').info('Stored Meta token in OS keyring for %s.', email_key)
            except Exception:
                pass
            return True
        except Exception:
            try:
                logging.getLogger('profit_dashboard').exception('Failed to store token in keyring for %s, falling back to encrypted file.', email_key)
            except Exception:
                pass

    # Fallback: encrypted file storage (store per-user in meta['tokens'])
    if not can_encrypt():
        return False
    enc = encrypt_token(token)
    if not enc:
        return False
    meta = load_meta('meta') or {}
    tokens = meta.get('tokens', {})
    tokens[email_key] = enc.decode('latin-1')
    meta['tokens'] = tokens
    meta.setdefault('last_sync', None)
    save_meta('meta', meta)
    try:
        logging.getLogger('profit_dashboard').info('Stored Meta token in encrypted file for %s.', email_key)
    except Exception:
        pass
    return True

    # Fallback: encrypted file storage
    if not can_encrypt():
        return False
    enc = encrypt_token(token)
    if not enc:
        return False
    meta = load_meta('meta') or {}
    meta['access_token_encrypted'] = enc.decode('latin-1')
    meta.setdefault('last_sync', None)
    save_meta('meta', meta)
    try:
        logging.getLogger('profit_dashboard').info('Stored Meta token in encrypted file (fallback).')
    except Exception:
        pass
    return True


def get_token_for_meta() -> Optional[str]:
    # Try keyring first

    # Legacy single-token getter preserved
    return get_token_for_meta_user(user_email=None)


def get_token_for_meta_user(user_email: Optional[str] = None) -> Optional[str]:
    email_key = user_email or 'default'
    # Try keyring first
    if can_use_keyring():
        try:
            t = keyring.get_password('profit_dashboard', f'meta_access_token_{email_key}')
            if t:
                return t
        except Exception:
            pass

    # Fallback to encrypted-file token
    m = load_meta('meta')
    if not m:
        return None
    tokens = m.get('tokens', {})
    enc = tokens.get(email_key)
    if not enc:
        return None
    try:
        return decrypt_token(enc.encode('latin-1'))
    except Exception:
        return None

    # Fallback to encrypted-file token
    m = load_meta('meta')
    if not m or 'access_token_encrypted' not in m:
        return None
    try:
        enc = m['access_token_encrypted'].encode('latin-1')
        return decrypt_token(enc)
    except Exception:
        return None


def token_storage_location() -> str:
    """Return where the token is stored for the default slot: 'keyring', 'encrypted_file', or 'none'."""
    return token_storage_location_for_user(user_email=None)


def token_storage_location_for_user(user_email: Optional[str] = None) -> str:
    email_key = user_email or 'default'
    if can_use_keyring():
        try:
            if keyring.get_password('profit_dashboard', f'meta_access_token_{email_key}'):
                return 'keyring'
        except Exception:
            pass
    m = load_meta('meta')
    tokens = (m or {}).get('tokens', {})
    if tokens and tokens.get(email_key):
        return 'encrypted_file'
    return 'none'


def migrate_file_token_to_keyring() -> bool:
    """Migrate an encrypted file-stored token into the OS keyring (if possible).

    Returns True on success (token moved), False otherwise.
    """
    if not can_use_keyring():
        return False
    m = load_meta('meta') or {}
    enc = m.get('access_token_encrypted')
    if not enc:
        return False
    try:
        token = decrypt_token(enc.encode('latin-1'))
        if not token:
            return False
        keyring.set_password('profit_dashboard', 'meta_access_token', token)
        # Remove token from file
        try:
            del m['access_token_encrypted']
        except Exception:
            pass
        save_meta('meta', m)
        return True
    except Exception:
        return False
    """Migrate an encrypted file-stored token into the OS keyring for default slot (if possible)."""
    return migrate_file_token_to_keyring_for_user(user_email=None)


def migrate_file_token_to_keyring_for_user(user_email: Optional[str] = None) -> bool:
    if not can_use_keyring():
        return False
    email_key = user_email or 'default'
    m = load_meta('meta') or {}
    tokens = m.get('tokens', {})
    enc = tokens.get(email_key)
    if not enc:
        return False
    try:
        token = decrypt_token(enc.encode('latin-1'))
        if not token:
            return False
        keyring.set_password('profit_dashboard', f'meta_access_token_{email_key}', token)
        # Remove token from file
        try:
            del tokens[email_key]
            m['tokens'] = tokens
        except Exception:
            pass
        save_meta('meta', m)
        try:
            logging.getLogger('profit_dashboard').info('Migrated token for %s to keyring.', email_key)
        except Exception:
            pass
        return True
    except Exception:
        return False


def update_last_sync(platform: str) -> None:
    m = load_meta(platform) or {}
    import datetime

    m['last_sync'] = datetime.datetime.utcnow().isoformat() + 'Z'
    save_meta(platform, m)


def fetch_spend_and_persist(access_token: str = None, user_email: Optional[str] = None, write_to_csv: bool = True) -> Optional[pd.DataFrame]:
    """Fetch spend from Meta Marketing API if possible, otherwise return a synthetic stub.

    If `access_token` is None, this function will try to load a stored token
    via `get_token_for_meta()`. To perform a live fetch you should also provide
    an ad account id via the `META_AD_ACCOUNT_ID` environment variable or store
    it in the connector metadata under `ad_account_id`.

    The returned DataFrame uses the normalized schema expected by the dashboard:
    `campaign_id`, `ad_spend`, `platform`, `date`.
    """
    token = access_token or get_token_for_meta()
    ad_account = None
    # Try to read ad account from env (explicit override)
    ad_account = os.environ.get('META_AD_ACCOUNT_ID')
    if not ad_account:
        meta = load_meta('meta') or {}
        # First prefer a per-user mapping stored under meta['email_ad_accounts']
        try:
            if user_email:
                email_map = meta.get('email_ad_accounts', {}) or {}
                if email_map and email_map.get(user_email):
                    ad_account = email_map.get(user_email)
        except Exception:
            pass
        # Fallback to platform-level ad_account_id
        if not ad_account:
            ad_account = meta.get('ad_account_id')

    # If we have a token and an ad account, attempt a real API call
    if token and ad_account:
        try:
            import requests
            url = f'https://graph.facebook.com/v17.0/act_{ad_account}/insights'
            params = {
                'access_token': token,
                'fields': 'campaign_id,spend,date_start,date_stop',
                'level': 'campaign',
                'time_increment': '1',
            }
            r = requests.get(url, params=params, timeout=15)
            if r.status_code == 200:
                j = r.json()
                data = []
                for rec in j.get('data', []):
                    # Facebook returns date ranges; use date_start if present
                    date = rec.get('date_start') or rec.get('date')
                    campaign = rec.get('campaign_id') or rec.get('campaign_name')
                    spend = float(rec.get('spend', 0.0))
                    data.append({'campaign_id': campaign, 'ad_spend': spend, 'platform': 'Meta', 'date': date})
                df = pd.DataFrame(data)
                if df.empty:
                    logging.getLogger('profit_dashboard').warning('Meta API returned no data')
                    raise ValueError('No data returned from API')
                if write_to_csv:
                    _ensure_data_dir()
                    out = os.path.join('data', 'spend.csv')
                    try:
                        df.to_csv(out, index=False)
                        update_last_sync('meta')
                        logging.getLogger('profit_dashboard').info(f'Fetched {len(data)} rows from Meta Marketing API')
                    except Exception:
                        pass
                return df
            else:
                # API returned error; fall through to stub
                logging.getLogger('profit_dashboard').warning(f'Meta API error: {r.status_code} - {r.text}')
                pass
        except Exception as ex:
            # If anything fails, fallback to the synthetic stub below
            logging.getLogger('profit_dashboard').warning(f'Failed to fetch real Meta data: {ex}. Using synthetic data.')
            pass

    # Fallback synthetic data if live fetch isn't possible
    logging.getLogger('profit_dashboard').info('Using synthetic Meta Ads data (no token or ad account)')
    df = pd.DataFrame([
        {'campaign_id': 'campA', 'ad_spend': 120.0, 'platform': 'Meta', 'date': '2025-01-01'},
        {'campaign_id': 'campB', 'ad_spend': 90.0, 'platform': 'Meta', 'date': '2025-01-02'},
    ])
    if write_to_csv:
        _ensure_data_dir()
        out = os.path.join('data', 'spend.csv')
        try:
            df.to_csv(out, index=False)
            update_last_sync('meta')
        except Exception:
            pass
    return df


def list_ad_accounts(access_token: str) -> List[dict]:
    """Fetch all ad accounts accessible with this token.
    
    Returns list of dicts with 'id', 'name', 'account_id' fields.
    Example: [{'id': 'act_123', 'name': 'My Personal Ads', 'account_id': '123'}, ...]
    """
    try:
        import requests
        url = 'https://graph.facebook.com/v17.0/me/adaccounts'
        params = {
            'access_token': access_token,
            'fields': 'id,name,account_id',
        }
        r = requests.get(url, params=params, timeout=15)
        if r.status_code == 200:
            j = r.json()
            accounts = []
            for acc in j.get('data', []):
                # Facebook returns id as 'act_123456789', account_id as '123456789'
                accounts.append({
                    'id': acc.get('id', ''),
                    'name': acc.get('name', 'Unnamed Account'),
                    'account_id': acc.get('account_id', '').replace('act_', ''),
                })
            return accounts
        return []
    except Exception:
        return []
