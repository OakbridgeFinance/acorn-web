"""
token_manager.py
Manages OAuth tokens for multiple QBO companies stored in .env.
Handles token refresh automatically when tokens are near expiry.
"""

import os
import re
import sys
import shutil
import base64
import requests
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv, set_key


def _resolve_env_file() -> Path:
    """Locate the .env file. When frozen (PyInstaller), use a writable AppData
    location and seed it from any bundled .env on first run."""
    if getattr(sys, "frozen", False):
        appdata = os.environ.get("APPDATA") or os.path.expanduser("~")
        target_dir = Path(appdata) / "AcornLite"
        target_dir.mkdir(parents=True, exist_ok=True)
        target = target_dir / ".env"
        if not target.exists():
            # Seed from bundled .env if PyInstaller included one
            bundled = Path(getattr(sys, "_MEIPASS", "")) / ".env"
            if bundled.exists():
                try:
                    shutil.copyfile(bundled, target)
                except Exception:
                    target.touch()
            else:
                target.touch()
        return target
    return Path(__file__).parent / ".env"


ENV_FILE = _resolve_env_file()

TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
TOKEN_REFRESH_BUFFER_MINUTES = 5


def _load_env():
    load_dotenv(ENV_FILE, override=True)


def _sanitize(alias: str) -> str:
    """Convert alias to a safe uppercase env key prefix."""
    return re.sub(r"[^A-Z0-9]", "_", alias.upper())


def _key(alias: str, suffix: str) -> str:
    return f"QBO_{_sanitize(alias)}_{suffix}"


def _write(key: str, value: str):
    """Write a key-value pair to the .env file."""
    set_key(str(ENV_FILE), key, value, quote_mode="never")


def get_client_credentials() -> tuple[str, str]:
    _load_env()
    client_id = os.getenv("QBO_CLIENT_ID", "").strip()
    client_secret = os.getenv("QBO_CLIENT_SECRET", "").strip()
    if not client_id or not client_secret:
        raise ValueError(
            "QBO_CLIENT_ID and QBO_CLIENT_SECRET must be set in .env. "
            "Run setup_oauth.py to configure."
        )
    return client_id, client_secret


def get_environment() -> str:
    _load_env()
    return os.getenv("QBO_ENVIRONMENT", "production").lower()


def list_companies() -> list[dict]:
    """Return all configured companies found in the .env file."""
    _load_env()
    companies = []
    pattern = re.compile(r"^QBO_(.+)_REALM_ID$")
    for key, value in os.environ.items():
        match = pattern.match(key)
        if match and value:
            alias = match.group(1)
            company_name = os.getenv(f"QBO_{alias}_COMPANY_NAME", alias)
            companies.append(
                {
                    "alias": alias,
                    "company_name": company_name,
                    "realm_id": value,
                }
            )
    return sorted(companies, key=lambda c: c["alias"])


def get_company_tokens(alias: str) -> dict:
    """
    Return valid tokens for a company, refreshing automatically if needed.
    Raises ValueError if company is not configured.
    """
    _load_env()
    sanitized = _sanitize(alias)

    realm_id = os.getenv(_key(sanitized, "REALM_ID"), "").strip()
    if not realm_id:
        configured = [c["alias"] for c in list_companies()]
        hint = f" Configured companies: {configured}" if configured else " No companies configured yet."
        raise ValueError(
            f"Company alias '{alias}' not found in .env.{hint} "
            "Run setup_oauth.py to add it."
        )

    access_token = os.getenv(_key(sanitized, "ACCESS_TOKEN"), "").strip()
    refresh_token = os.getenv(_key(sanitized, "REFRESH_TOKEN"), "").strip()
    expiry_str = os.getenv(_key(sanitized, "TOKEN_EXPIRY"), "").strip()

    # Refresh if expired or close to expiry
    should_refresh = False
    if not access_token:
        should_refresh = True
    elif expiry_str:
        try:
            expiry = datetime.fromisoformat(expiry_str)
            if datetime.now() >= expiry - timedelta(minutes=TOKEN_REFRESH_BUFFER_MINUTES):
                should_refresh = True
        except ValueError:
            should_refresh = True

    if should_refresh:
        if not refresh_token:
            raise PermissionError(
                f"Access token for '{alias}' is expired and no refresh token found. "
                "Re-run setup_oauth.py to re-authorize."
            )
        access_token, refresh_token, expiry = _refresh_access_token(refresh_token)
        _save_tokens(sanitized, realm_id, access_token, refresh_token, expiry)

    return {"realm_id": realm_id, "access_token": access_token}


def _refresh_access_token(refresh_token: str) -> tuple[str, str, datetime]:
    """Use the refresh token to obtain a new access token."""
    client_id, client_secret = get_client_credentials()
    credentials = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()

    response = requests.post(
        TOKEN_URL,
        headers={
            "Authorization": f"Basic {credentials}",
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        },
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        },
        timeout=30,
    )

    if not response.ok:
        raise PermissionError(
            f"Token refresh failed ({response.status_code}): {response.text}"
        )

    data = response.json()
    new_access_token = data["access_token"]
    new_refresh_token = data.get("refresh_token", refresh_token)
    expires_in = data.get("expires_in", 3600)
    expiry = datetime.now() + timedelta(seconds=expires_in)

    return new_access_token, new_refresh_token, expiry


def _save_tokens(
    sanitized_alias: str,
    realm_id: str,
    access_token: str,
    refresh_token: str,
    expiry: datetime,
):
    """Write token values to .env file and reload."""
    _write(f"QBO_{sanitized_alias}_REALM_ID", realm_id)
    _write(f"QBO_{sanitized_alias}_ACCESS_TOKEN", access_token)
    _write(f"QBO_{sanitized_alias}_REFRESH_TOKEN", refresh_token)
    _write(f"QBO_{sanitized_alias}_TOKEN_EXPIRY", expiry.isoformat())
    _load_env()


def save_company(
    alias: str,
    realm_id: str,
    access_token: str,
    refresh_token: str,
    expiry: datetime,
    company_name: str = "",
):
    """Save all company credentials and tokens to .env file."""
    sanitized = _sanitize(alias)
    _save_tokens(sanitized, realm_id, access_token, refresh_token, expiry)
    if company_name:
        _write(f"QBO_{sanitized}_COMPANY_NAME", company_name)
    _load_env()
