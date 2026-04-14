import sys
from pathlib import Path

# Add core directory to path so gl_extractor and its dependencies can import token_manager
sys.path.insert(0, str(Path(__file__).parent / "core"))

import os
import httpx
import base64
import urllib.parse
from datetime import datetime, timedelta
from fastapi import APIRouter, Request, Depends, HTTPException
from fastapi.responses import RedirectResponse, JSONResponse
from backend.auth import get_current_user
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

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
    """Return the QBO OAuth authorization URL."""
    params = {
        "client_id":     QBO_CLIENT_ID,
        "response_type": "code",
        "scope":         QBO_SCOPES,
        "redirect_uri":  QBO_REDIRECT_URI,
        "state":         str(user.id),
    }
    url = QBO_AUTH_URL + "?" + urllib.parse.urlencode(params)
    return {"auth_url": url}


@router.get("/callback")
async def qbo_callback(code: str, realmId: str, state: str = ""):
    """Handle QBO OAuth callback — exchange code for tokens and store in Supabase."""
    user_id = state  # we passed user_id as state

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

    # Store tokens in Supabase
    supabase = get_supabase()
    supabase.table("qbo_tokens").upsert({
        "user_id":       user_id,
        "realm_id":      realmId,
        "company_name":  company_name,
        "access_token":  access_token,
        "refresh_token": refresh_token,
        "expires_at":    expires_at,
        "updated_at":    datetime.utcnow().isoformat(),
    }, on_conflict="user_id,realm_id").execute()

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
