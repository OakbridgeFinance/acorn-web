import os
import httpx
from datetime import datetime, timezone
from urllib.parse import unquote
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Any
from supabase import create_client
from backend.auth import get_current_user
from dotenv import load_dotenv

load_dotenv()


def _require_mapping_plan(user):
    """Raise 403 if user's plan doesn't include mapping."""
    plan = (user.app_metadata or {}).get("plan", "basic")
    if plan not in ("pro", "plus", "admin"):
        raise HTTPException(status_code=403, detail="Mapping requires a Pro or Plus plan")

router = APIRouter(prefix="/api/mapping", tags=["mapping"])

SUPABASE_URL         = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
QBO_API_BASE         = "https://quickbooks.api.intuit.com"


def get_supabase():
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


def get_tokens(user_id: str, realm_id: str) -> dict:
    supabase = get_supabase()
    result = supabase.table("qbo_tokens").select(
        "access_token, refresh_token"
    ).eq("user_id", user_id).eq("realm_id", realm_id).execute()
    if not result.data:
        raise HTTPException(status_code=404, detail="QBO connection not found")
    return result.data[0]


# ── Display-string helpers ───────────────────────────────────────────────────
# The frontend stores account assignments inside map.groups[].accounts as the
# string produced by getGLAccountName(displayName) — i.e. "{AcctNum} {Name}"
# (Name is the leaf, NOT FullyQualifiedName). The validation logic must build
# the same string server-side so set membership checks line up. See
# frontend/app.html getGLAccountName + the drag-drop write at
# mapsState.groups[gi].accounts.push({ name: glName, display_name: glName }).

def _account_display_string(account: dict) -> str:
    """Canonical display string for matching against map.accounts entries.

    "{AcctNum} {Name}" if AcctNum present, otherwise just "{Name}". Uses Name
    (leaf), not FullyQualifiedName — that's what the frontend stores.
    """
    acct_num = str(account.get("AcctNum") or "").strip()
    name     = str(account.get("Name") or "").strip()
    return f"{acct_num} {name}" if acct_num else name


def _format_account_for_response(account: dict) -> dict:
    """Format raw QBO Account record for API response payloads.

    `name` and `display` keep the existing get_coa shape (FullyQualifiedName
    for hierarchy display); `match_key` is the canonical string used for
    map membership comparisons.
    """
    acct_num   = str(account.get("AcctNum") or "").strip()
    name_fqn   = str(account.get("FullyQualifiedName") or account.get("Name") or "").strip()
    display_ui = f"{acct_num} {name_fqn}".strip() if acct_num else name_fqn
    return {
        "id":         account.get("Id", ""),
        "name":       name_fqn,
        "display":    display_ui,
        "match_key":  _account_display_string(account),
        "acct_num":   acct_num,
        "type":       account.get("AccountType", ""),
        "subtype":    account.get("AccountSubType", ""),
        "parent_ref": account.get("ParentRef", {}).get("value", "") if account.get("ParentRef") else "",
    }


def get_mapped_accounts(map_obj: dict) -> set[str]:
    """Flatten all assigned display strings across every group in a map."""
    mapped: set[str] = set()
    for group in (map_obj.get("groups") or []):
        for entry in (group.get("accounts") or []):
            if isinstance(entry, str):
                if entry:
                    mapped.add(entry)
            elif isinstance(entry, dict):
                # In-flight editor objects shouldn't reach the DB, but be lenient.
                s = entry.get("display_name") or entry.get("name") or entry.get("account_name")
                if s:
                    mapped.add(s)
    return mapped


# ── QBO COA fetch ────────────────────────────────────────────────────────────

async def _fetch_qbo_accounts(
    access_token: str, realm_id: str, include_inactive: bool = False,
) -> list[dict]:
    """Page through QBO's Account query and return raw records."""
    accounts: list[dict] = []
    start_position = 1
    page_size = 1000
    where_clause = "Active IN (true, false)" if include_inactive else "Active = true"

    async with httpx.AsyncClient() as client:
        while True:
            query = (
                f"SELECT Id, Name, FullyQualifiedName, AccountType, AccountSubType, "
                f"AcctNum, Active, ParentRef FROM Account WHERE {where_clause} "
                f"STARTPOSITION {start_position} MAXRESULTS {page_size}"
            )
            response = await client.get(
                f"{QBO_API_BASE}/v3/company/{realm_id}/query",
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Accept": "application/json",
                },
                params={"query": query, "minorversion": "75"},
            )
            if response.status_code != 200:
                raise HTTPException(
                    status_code=response.status_code,
                    detail=f"QBO API error: {response.text}",
                )
            data = response.json()
            batch = data.get("QueryResponse", {}).get("Account", [])
            if not batch:
                break
            accounts.extend(batch)
            if len(batch) < page_size:
                break
            start_position += page_size
    return accounts


@router.get("/coa/{realm_id}")
async def get_coa(
    realm_id: str,
    include_inactive: bool = False,
    user=Depends(get_current_user),
):
    """Fetch Chart of Accounts from QBO. Defaults to active-only for the editor;
    completeness validation passes include_inactive=true to match the GL
    extractor's behavior."""
    _require_mapping_plan(user)
    tokens = get_tokens(str(user.id), realm_id)
    accounts = await _fetch_qbo_accounts(
        tokens["access_token"], realm_id, include_inactive=include_inactive,
    )
    return {"accounts": [_format_account_for_response(a) for a in accounts]}


@router.get("/{realm_id}")
def get_mapping(realm_id: str, user=Depends(get_current_user)):
    """Get account mapping config for a company."""
    _require_mapping_plan(user)
    supabase = get_supabase()
    result = supabase.table("mappings").select("account_maps").eq(
        "user_id", str(user.id)
    ).eq("realm_id", realm_id).execute()
    if not result.data:
        return {"account_maps": []}
    return {"account_maps": result.data[0]["account_maps"] or []}


class MappingBody(BaseModel):
    account_maps: list[Any]


@router.post("/{realm_id}")
def save_mapping(realm_id: str, body: MappingBody, user=Depends(get_current_user)):
    """Save account mapping config for a company."""
    _require_mapping_plan(user)

    # Strip any legacy status keys left over from the draft/finalize era.
    cleaned_maps = []
    for m in body.account_maps:
        if isinstance(m, dict) and "status" in m:
            m = {k: v for k, v in m.items() if k != "status"}
        cleaned_maps.append(m)

    supabase = get_supabase()
    supabase.table("mappings").upsert({
        "user_id":      str(user.id),
        "realm_id":     realm_id,
        "account_maps": cleaned_maps,
        "updated_at":   datetime.now(timezone.utc).isoformat(),
    }, on_conflict="user_id,realm_id").execute()
    return {"saved": True}


@router.get("/{realm_id}/validate/{map_name}")
async def validate_map(realm_id: str, map_name: str, user=Depends(get_current_user)):
    """Report completeness of a single map vs. the current QBO COA (incl
    inactive). Read-only; returns unmapped + stale entries."""
    _require_mapping_plan(user)
    decoded_name = unquote(map_name)

    supabase = get_supabase()
    result = supabase.table("mappings").select("account_maps").eq(
        "user_id", str(user.id)
    ).eq("realm_id", realm_id).execute()
    all_maps = (result.data[0].get("account_maps") or []) if result.data else []
    map_obj = next((m for m in all_maps if isinstance(m, dict) and m.get("map_name") == decoded_name), None)
    if map_obj is None:
        raise HTTPException(status_code=404, detail=f'Map "{decoded_name}" not found')

    tokens = get_tokens(str(user.id), realm_id)
    accounts = await _fetch_qbo_accounts(
        tokens["access_token"], realm_id, include_inactive=True,
    )

    expected_by_key: dict[str, dict] = {}
    for a in accounts:
        key = _account_display_string(a)
        if key:
            expected_by_key[key] = a

    expected_keys = set(expected_by_key.keys())
    mapped_keys   = get_mapped_accounts(map_obj)

    unmapped_keys = expected_keys - mapped_keys
    stale_keys    = sorted(mapped_keys - expected_keys)

    unmapped_accounts = []
    for key in sorted(unmapped_keys):
        formatted = _format_account_for_response(expected_by_key[key])
        unmapped_accounts.append({
            "display":  formatted["match_key"],
            "name":     formatted["name"],
            "acct_num": formatted["acct_num"],
            "type":     formatted["type"],
            "subtype":  formatted["subtype"],
        })

    return {
        "map_name":              decoded_name,
        "is_complete":           not unmapped_accounts,
        "unmapped_accounts":     unmapped_accounts,
        "stale_assignments":     stale_keys,
        "total_qbo_accounts":    len(expected_keys),
        "total_mapped_accounts": len(mapped_keys),
    }
