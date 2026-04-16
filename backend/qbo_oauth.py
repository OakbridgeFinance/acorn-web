import sys
from pathlib import Path

# Add core directory to path so gl_extractor and its dependencies can import token_manager
sys.path.insert(0, str(Path(__file__).parent / "core"))

import os
import httpx
import base64
import secrets
import urllib.parse
from datetime import datetime, timedelta, timezone
from fastapi import APIRouter, Request, Depends, HTTPException
from fastapi.responses import RedirectResponse, JSONResponse
from backend.auth import get_current_user
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

# ── Supabase schema for OAuth state storage (run once, manually) ─────────────
# create table if not exists qbo_oauth_states (
#   state       text primary key,
#   user_id     uuid not null references auth.users(id) on delete cascade,
#   created_at  timestamptz not null default now()
# );
# alter table qbo_oauth_states enable row level security;
# -- No policies are defined intentionally: with RLS enabled and no policies,
# -- only the service role (which bypasses RLS) can read or write this table,
# -- which is exactly what the OAuth flow needs.
# create index if not exists qbo_oauth_states_created_at_idx
#   on qbo_oauth_states (created_at);

OAUTH_STATE_TTL = timedelta(minutes=10)

router = APIRouter(prefix="/api/qbo", tags=["qbo"])

QBO_CLIENT_ID     = os.getenv("QBO_CLIENT_ID")
QBO_CLIENT_SECRET = os.getenv("QBO_CLIENT_SECRET")
QBO_REDIRECT_URI  = os.getenv("QBO_REDIRECT_URI")
QBO_SCOPES        = "com.intuit.quickbooks.accounting"
QBO_AUTH_URL      = "https://appcenter.intuit.com/connect/oauth2"
QBO_TOKEN_URL     = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
QBO_API_BASE      = "https://quickbooks.api.intuit.com"

SUPABASE_URL      = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")


def get_supabase():
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


@router.get("/auth-url")
def get_auth_url(user=Depends(get_current_user)):
    """Return the QBO OAuth authorization URL.

    Generates a random one-time state token, persists it bound to the
    requesting user, and includes it in the QBO authorize URL so the
    callback can verify the redirect originated from this flow.
    """
    state = secrets.token_urlsafe(32)
    supabase = get_supabase()
    supabase.table("qbo_oauth_states").insert({
        "state":   state,
        "user_id": str(user.id),
    }).execute()

    params = {
        "client_id":     QBO_CLIENT_ID,
        "response_type": "code",
        "scope":         QBO_SCOPES,
        "redirect_uri":  QBO_REDIRECT_URI,
        "state":         state,
    }
    url = QBO_AUTH_URL + "?" + urllib.parse.urlencode(params)
    return {"auth_url": url}


@router.get("/callback")
async def qbo_callback(code: str, realmId: str, state: str = ""):
    """Handle QBO OAuth callback — exchange code for tokens and store in Supabase."""
    if not state:
        raise HTTPException(status_code=400, detail="Missing state parameter")

    supabase = get_supabase()
    state_row = supabase.table("qbo_oauth_states").select(
        "user_id, created_at"
    ).eq("state", state).execute()

    if not state_row.data:
        raise HTTPException(status_code=400, detail="Invalid or expired state")

    record    = state_row.data[0]
    created_s = record["created_at"]
    # Supabase returns ISO-8601 with offset; tolerate trailing "Z".
    created   = datetime.fromisoformat(created_s.replace("Z", "+00:00"))
    now_utc   = datetime.now(timezone.utc)
    if now_utc - created > OAUTH_STATE_TTL:
        supabase.table("qbo_oauth_states").delete().eq("state", state).execute()
        raise HTTPException(status_code=400, detail="Invalid or expired state")

    user_id = record["user_id"]

    # Exchange authorization code for tokens
    credentials = base64.b64encode(
        f"{QBO_CLIENT_ID}:{QBO_CLIENT_SECRET}".encode()
    ).decode()

    async with httpx.AsyncClient() as client:
        token_response = await client.post(
            QBO_TOKEN_URL,
            headers={
                "Authorization": f"Basic {credentials}",
                "Content-Type":  "application/x-www-form-urlencoded",
                "Accept":        "application/json",
            },
            data={
                "grant_type":   "authorization_code",
                "code":         code,
                "redirect_uri": QBO_REDIRECT_URI,
            },
        )

    if token_response.status_code != 200:
        return JSONResponse(
            {"error": "Token exchange failed", "detail": token_response.text},
            status_code=400,
        )

    tokens = token_response.json()
    access_token  = tokens.get("access_token")
    refresh_token = tokens.get("refresh_token")
    expires_in    = tokens.get("expires_in", 3600)
    expires_at    = (datetime.utcnow() + timedelta(seconds=expires_in)).isoformat()

    # Fetch company name from QBO
    company_name = realmId  # fallback
    try:
        async with httpx.AsyncClient() as client:
            info_response = await client.get(
                f"{QBO_API_BASE}/v3/company/{realmId}/companyinfo/{realmId}",
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Accept": "application/json",
                },
                params={"minorversion": "75"},
            )
        if info_response.status_code == 200:
            company_name = (
                info_response.json()
                .get("CompanyInfo", {})
                .get("CompanyName", realmId)
            )
    except Exception:
        pass

    # Store tokens in Supabase (reuse the service-role client from above)
    supabase.table("qbo_tokens").upsert({
        "user_id":       user_id,
        "realm_id":      realmId,
        "company_name":  company_name,
        "access_token":  access_token,
        "refresh_token": refresh_token,
        "expires_at":    expires_at,
        "updated_at":    datetime.utcnow().isoformat(),
    }, on_conflict="user_id,realm_id").execute()

    # Consume the state row (one-time use) so it can't be replayed.
    supabase.table("qbo_oauth_states").delete().eq("state", state).execute()

    # Redirect back to app with success
    return RedirectResponse(url="/app.html?connected=true")


@router.get("/companies")
def list_companies(user=Depends(get_current_user)):
    """List connected QBO companies for the authenticated user."""
    supabase = get_supabase()
    result = supabase.table("qbo_tokens").select(
        "realm_id, company_name, updated_at"
    ).eq("user_id", str(user.id)).execute()
    return {"companies": result.data}


@router.delete("/companies/{realm_id}")
def remove_company(realm_id: str, user=Depends(get_current_user)):
    """Remove a QBO connection."""
    supabase = get_supabase()
    supabase.table("qbo_tokens").delete().eq(
        "user_id", str(user.id)
    ).eq("realm_id", realm_id).execute()
    return {"removed": True}


@router.post("/refresh-token/{realm_id}")
async def refresh_qbo_token(realm_id: str, user=Depends(get_current_user)):
    """Refresh the QBO access token for a company using the stored refresh token."""
    supabase = get_supabase()
    result = supabase.table("qbo_tokens").select(
        "refresh_token"
    ).eq("user_id", str(user.id)).eq("realm_id", realm_id).execute()

    if not result.data:
        raise HTTPException(status_code=404, detail="No QBO connection found")

    refresh_token = result.data[0]["refresh_token"]
    client_id     = os.getenv("QBO_CLIENT_ID", "")
    client_secret = os.getenv("QBO_CLIENT_SECRET", "")
    credentials   = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
            headers={
                "Authorization": f"Basic {credentials}",
                "Content-Type":  "application/x-www-form-urlencoded",
                "Accept":        "application/json",
            },
            data={
                "grant_type":    "refresh_token",
                "refresh_token": refresh_token,
            },
            timeout=30,
        )

    if resp.status_code != 200:
        raise HTTPException(status_code=400, detail=f"QBO token refresh failed: {resp.text}")

    new_tokens = resp.json()
    new_access  = new_tokens["access_token"]
    new_refresh = new_tokens.get("refresh_token", refresh_token)
    new_expiry  = (datetime.utcnow() + timedelta(seconds=new_tokens.get("expires_in", 3600))).isoformat()

    supabase.table("qbo_tokens").update({
        "access_token":  new_access,
        "refresh_token": new_refresh,
        "expires_at":    new_expiry,
        "updated_at":    datetime.utcnow().isoformat(),
    }).eq("user_id", str(user.id)).eq("realm_id", realm_id).execute()

    return {"refreshed": True, "expires_at": new_expiry}
