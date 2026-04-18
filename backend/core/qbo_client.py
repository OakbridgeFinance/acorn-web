"""
qbo_client.py
Thin wrapper around the QuickBooks Online Reporting API.

All API calls require the caller to supply `access_token` and `realm_id`
explicitly. No module-level state — callers are responsible for passing
credentials through every call, which makes cross-user contamination
impossible under concurrent use.
"""

import os
import logging
from urllib.parse import quote

import requests

_logger = logging.getLogger(__name__)

QBO_API_HOSTS = {
    "production": "https://quickbooks.api.intuit.com",
    "sandbox":    "https://sandbox-quickbooks.api.intuit.com",
}
MINOR_VERSION = "75"


def _base_url() -> str:
    env = os.getenv("QBO_ENVIRONMENT", "production").lower()
    return QBO_API_HOSTS.get(env, QBO_API_HOSTS["production"])


def _check_for_fault(data: dict) -> None:
    """Raise a descriptive error if the QBO response contains a Fault."""
    if "Fault" in data:
        errors = data["Fault"].get("Error", [])
        messages = []
        for e in errors:
            msg    = e.get("Message", "Unknown error")
            detail = e.get("Detail", "")
            code   = e.get("code", "")
            messages.append(f"[{code}] {msg}: {detail}" if detail else f"[{code}] {msg}")
        raise RuntimeError("QBO API Error: " + " | ".join(messages))


def fetch_report(
    report_name: str,
    params: dict | None = None,
    *,
    access_token: str,
    realm_id: str,
    testing_migration: bool = False,
) -> dict:
    """
    Fetch a QBO report.

    Args:
        report_name:       QBO report endpoint name (e.g. "GeneralLedger").
        params:            Optional query parameters for the report.
        access_token:      Caller-supplied QBO access token.
        realm_id:          Caller-supplied QBO company realm id.
        testing_migration: Include `testing_migration` flag on the URL.

    Returns:
        The raw report JSON as a dict.
    """
    if not access_token or not realm_id:
        raise ValueError("fetch_report requires access_token and realm_id")

    url = f"{_base_url()}/v3/company/{realm_id}/reports/{report_name}"
    request_params = {"minorversion": MINOR_VERSION}
    if params:
        request_params.update(params)

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept":        "application/json",
    }

    # Build URL manually to avoid percent-encoding commas in values like
    # columns=tx_date,txn_type,... — QBO requires literal commas, not %2C.
    _parts = [f"{quote(str(k), safe='')}={quote(str(v), safe=',')}"
              for k, v in request_params.items()]
    if testing_migration:
        _parts.append("testing_migration")
    final_url = f"{url}?{'&'.join(_parts)}"

    response = requests.get(final_url, headers=headers, timeout=60)

    if response.status_code == 401:
        raise PermissionError("QBO authentication failed. Reconnect the company.")
    if response.status_code == 403:
        raise PermissionError("QBO access denied. Check app scopes.")
    if not response.ok:
        raise RuntimeError(
            f"QBO API request failed ({response.status_code}): {response.text[:500]}"
        )

    body = response.text.strip()
    if not body:
        raise RuntimeError(
            f"QBO API returned empty response body (status {response.status_code})"
        )
    try:
        data = response.json()
    except Exception as json_err:
        raise RuntimeError(
            f"QBO API returned non-JSON response "
            f"(status {response.status_code}): {body[:200]}"
        ) from json_err

    _check_for_fault(data)
    return data


def fetch_accounts(*, access_token: str, realm_id: str) -> list[dict]:
    """
    Fetch all accounts from the Chart of Accounts via the QBO query API.
    Returns a list of account dicts with Id, Name, AccountType,
    AccountSubType, and AcctNum.
    """
    if not access_token or not realm_id:
        raise ValueError("fetch_accounts requires access_token and realm_id")

    url = f"{_base_url()}/v3/company/{realm_id}/query"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept":        "application/json",
    }

    all_accounts: list[dict] = []
    start_position = 1
    page_size = 1000

    while True:
        params = {
            "query": (
                f"SELECT Id, Name, AccountType, AccountSubType, AcctNum, Active "
                f"FROM Account STARTPOSITION {start_position} MAXRESULTS {page_size}"
            ),
            "minorversion": MINOR_VERSION,
        }
        response = requests.get(url, headers=headers, params=params, timeout=30)
        if not response.ok:
            raise RuntimeError(
                f"QBO CoA fetch failed ({response.status_code}): {response.text[:500]}"
            )

        data = response.json()
        _check_for_fault(data)

        page = data.get("QueryResponse", {}).get("Account", [])
        all_accounts.extend(page)

        if len(page) < page_size:
            break
        start_position += page_size

    return all_accounts
