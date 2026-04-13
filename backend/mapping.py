import os
import httpx
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Any
from supabase import create_client
from backend.auth import get_current_user
from dotenv import load_dotenv

load_dotenv()

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


@router.get("/coa/{realm_id}")
async def get_coa(realm_id: str, user=Depends(get_current_user)):
    """Fetch Chart of Accounts from QBO for mapping UI."""
    tokens = get_tokens(str(user.id), realm_id)
    access_token = tokens["access_token"]
    accounts = []
    start_position = 1
    page_size = 1000

    async with httpx.AsyncClient() as client:
        while True:
            query = (
                f"SELECT Id, Name, FullyQualifiedName, AccountType, AccountSubType, "
                f"AcctNum, Active, ParentRef FROM Account WHERE Active = true "
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
                    detail=f"QBO API error: {response.text}"
                )
            data = response.json()
            batch = data.get("QueryResponse", {}).get("Account", [])
            if not batch:
                break
            accounts.extend(batch)
            if len(batch) < page_size:
                break
            start_position += page_size

    formatted = []
    for a in accounts:
        acct_num = str(a.get("AcctNum", "") or "").strip()
        name     = str(a.get("FullyQualifiedName", a.get("Name", "")) or "").strip()
        display  = f"{acct_num} {name}".strip() if acct_num else name
        formatted.append({
            "id":         a.get("Id", ""),
            "name":       name,
            "display":    display,
            "acct_num":   acct_num,
            "type":       a.get("AccountType", ""),
            "subtype":    a.get("AccountSubType", ""),
            "parent_ref": a.get("ParentRef", {}).get("value", "") if a.get("ParentRef") else "",
        })

    return {"accounts": formatted}


@router.get("/totals/{realm_id}")
async def get_account_totals(
    realm_id: str,
    start_date: str,
    end_date: str,
    user=Depends(get_current_user),
):
    """Fetch account activity totals from QBO P&L and Balance Sheet for the mapping UI."""
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent / "core"))

    tokens = get_tokens(str(user.id), realm_id)
    access_token = tokens["access_token"]

    totals = {}

    async with httpx.AsyncClient() as client:
        # Fetch P&L totals
        try:
            pl_resp = await client.get(
                f"{QBO_API_BASE}/v3/company/{realm_id}/reports/ProfitAndLoss",
                headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
                params={
                    "start_date": start_date,
                    "end_date": end_date,
                    "accounting_method": "Accrual",
                    "minorversion": "75",
                },
                timeout=30,
            )
            if pl_resp.status_code == 200:
                from report_parser import parse_financial_statement
                pl_df = parse_financial_statement(pl_resp.json())
                if not pl_df.empty:
                    meta = {"Row_Type", "Indent_Level", "Account_Path", "Account", "Account_ID"}
                    num_cols = [c for c in pl_df.columns if c not in meta]
                    if num_cols:
                        first_col = num_cols[0]
                        for _, row in pl_df.iterrows():
                            if row.get("Row_Type") not in ("Data", "Section"):
                                continue
                            acct = str(row.get("Account", "") or "").strip()
                            if acct:
                                val = float(row.get(first_col, 0) or 0)
                                totals[acct] = val
        except Exception:
            pass

        # Fetch BS totals
        try:
            bs_resp = await client.get(
                f"{QBO_API_BASE}/v3/company/{realm_id}/reports/BalanceSheet",
                headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
                params={
                    "start_date": start_date,
                    "end_date": end_date,
                    "accounting_method": "Accrual",
                    "minorversion": "75",
                },
                timeout=30,
            )
            if bs_resp.status_code == 200:
                from report_parser import parse_financial_statement
                bs_df = parse_financial_statement(bs_resp.json())
                if not bs_df.empty:
                    meta = {"Row_Type", "Indent_Level", "Account_Path", "Account", "Account_ID"}
                    num_cols = [c for c in bs_df.columns if c not in meta]
                    if num_cols:
                        last_col = num_cols[-1]
                        for _, row in bs_df.iterrows():
                            if row.get("Row_Type") not in ("Data", "Section"):
                                continue
                            acct = str(row.get("Account", "") or "").strip()
                            if acct:
                                val = float(row.get(last_col, 0) or 0)
                                totals[acct] = val
        except Exception:
            pass

    return {"totals": totals}


@router.get("/{realm_id}")
def get_mapping(realm_id: str, user=Depends(get_current_user)):
    """Get account mapping config for a company."""
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
    supabase = get_supabase()
    supabase.table("mappings").upsert({
        "user_id":      str(user.id),
        "realm_id":     realm_id,
        "account_maps": body.account_maps,
        "updated_at":   datetime.utcnow().isoformat(),
    }, on_conflict="user_id,realm_id").execute()
    return {"saved": True}
