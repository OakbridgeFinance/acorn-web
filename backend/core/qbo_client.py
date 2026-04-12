"""
qbo_client.py
Thin wrapper around the QuickBooks Online Reporting API.
Handles authentication, base URLs, and error parsing.
"""

import requests
from token_manager import get_company_tokens, get_environment

QBO_API_HOSTS = {
    "production": "https://quickbooks.api.intuit.com",
    "sandbox": "https://sandbox-quickbooks.api.intuit.com",
}

MINOR_VERSION = "75"

# Web mode: when set, use these tokens instead of token_manager
_override_tokens: dict | None = None


def set_override_tokens(tokens: dict | None) -> None:
    """Set tokens directly (used by web backend to inject Supabase tokens)."""
    global _override_tokens
    _override_tokens = tokens


# Module-level flag: when True, all fetch_report calls include testing_migration
# Set via set_v2_test_mode() before generating reports.
_v2_test_mode: bool = False


def set_v2_test_mode(enabled: bool) -> None:
    """Enable/disable QBO API v2 test mode for all subsequent report calls."""
    global _v2_test_mode
    _v2_test_mode = enabled


def _base_url() -> str:
    env = get_environment()
    return QBO_API_HOSTS.get(env, QBO_API_HOSTS["production"])


def _check_for_fault(data: dict):
    """Raise a descriptive error if the QBO response contains a Fault."""
    if "Fault" in data:
        errors = data["Fault"].get("Error", [])
        messages = []
        for e in errors:
            msg = e.get("Message", "Unknown error")
            detail = e.get("Detail", "")
            code = e.get("code", "")
            messages.append(f"[{code}] {msg}: {detail}" if detail else f"[{code}] {msg}")
        raise RuntimeError("QBO API Error: " + " | ".join(messages))


def fetch_report(company_alias: str, report_name: str, params: dict | None = None,
                 testing_migration: bool = False) -> dict:
    """
    Fetch a QBO report for a given company alias.

    Args:
        company_alias: The alias used during setup (e.g., "ACME")
        report_name: QBO report endpoint name (e.g., "GeneralLedger", "ProfitAndLoss")
        params: Optional dict of query parameters for the report
        testing_migration: When True, append testing_migration to the URL
                          to enable QBO API v2 test mode responses.

    Returns:
        The raw report JSON as a dict
    """
    import logging as _logging
    _logger = _logging.getLogger(__name__)
    tokens = _override_tokens if _override_tokens is not None else get_company_tokens(company_alias)
    realm_id = tokens["realm_id"]
    access_token = tokens["access_token"]
    _logger.info(f"fetch_report: using {'override' if _override_tokens is not None else 'token_manager'} tokens, realm_id={realm_id}")

    url = f"{_base_url()}/v3/company/{realm_id}/reports/{report_name}"

    request_params = {"minorversion": MINOR_VERSION}
    if params:
        request_params.update(params)

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }

    # Build URL manually to avoid percent-encoding commas in values like
    # columns=tx_date,txn_type,... — QBO requires literal commas, not %2C.
    from urllib.parse import quote
    _parts = []
    for k, v in request_params.items():
        _parts.append(f"{quote(str(k), safe='')}={quote(str(v), safe=',')}")
    _use_v2_test = testing_migration or _v2_test_mode
    if _use_v2_test:
        _parts.append("testing_migration")
    final_url = f"{url}?{'&'.join(_parts)}"
    response = requests.get(final_url, headers=headers, timeout=60)

    if response.status_code == 401:
        raise PermissionError(
            f"Authentication failed for '{company_alias}'. "
            "The access token may have been revoked. Re-run setup_oauth.py."
        )

    if response.status_code == 403:
        raise PermissionError(
            f"Access denied for '{company_alias}'. "
            "Ensure the app has the 'com.intuit.quickbooks.accounting' scope."
        )

    if not response.ok:
        raise RuntimeError(
            f"QBO API request failed ({response.status_code}): {response.text[:500]}"
        )

    # Guard against empty or non-JSON response body
    body = response.text.strip()
    if not body:
        raise RuntimeError(
            f"QBO API returned empty response body "
            f"(status {response.status_code}, url: {response.url})"
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


def fetch_query(company_alias: str, sql: str, page_size: int = 1000) -> list[dict]:
    """
    Execute a QBO SQL query and return all matching records (auto-pages).

    The entity type is inferred from the FROM clause to extract the right key
    from the QueryResponse.  Returns a flat list of record dicts.
    """
    import re as _re

    tokens       = _override_tokens if _override_tokens is not None else get_company_tokens(company_alias)
    realm_id     = tokens["realm_id"]
    access_token = tokens["access_token"]
    url          = f"{_base_url()}/v3/company/{realm_id}/query"
    headers      = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }

    m           = _re.search(r"FROM\s+(\w+)", sql, _re.IGNORECASE)
    entity_name = m.group(1) if m else "Entity"

    all_records: list[dict] = []
    start = 1
    while True:
        paged = f"{sql} STARTPOSITION {start} MAXRESULTS {page_size}"
        resp  = requests.get(url, headers=headers,
                             params={"query": paged, "minorversion": MINOR_VERSION},
                             timeout=60)
        if not resp.ok:
            raise RuntimeError(
                f"QBO query failed ({resp.status_code}): {resp.text[:500]}"
            )
        data = resp.json()
        _check_for_fault(data)
        page = data.get("QueryResponse", {}).get(entity_name, [])
        all_records.extend(page)
        if len(page) < page_size:
            break
        start += page_size

    return all_records


def fetch_accounts(company_alias: str) -> list[dict]:
    """
    Fetch all accounts from the Chart of Accounts via the QBO query API.
    Returns a list of account dicts with Id, Name, AccountType,
    AccountSubType, and AcctNum (account number).
    """
    tokens = _override_tokens if _override_tokens is not None else get_company_tokens(company_alias)
    realm_id = tokens["realm_id"]
    access_token = tokens["access_token"]

    url = f"{_base_url()}/v3/company/{realm_id}/query"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
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
            break

        data = response.json()
        _check_for_fault(data)

        qr = data.get("QueryResponse", {})
        page = qr.get("Account", [])
        all_accounts.extend(page)

        if len(page) < page_size:
            break
        start_position += page_size

    return all_accounts
