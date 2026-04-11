import os
import httpx
import base64
from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse, JSONResponse
from dotenv import load_dotenv

load_dotenv()

router = APIRouter(prefix="/api/qbo", tags=["qbo"])

QBO_CLIENT_ID     = os.getenv("QBO_CLIENT_ID")
QBO_CLIENT_SECRET = os.getenv("QBO_CLIENT_SECRET")
QBO_REDIRECT_URI  = os.getenv("QBO_REDIRECT_URI")
QBO_SCOPES        = "com.intuit.quickbooks.accounting"
QBO_AUTH_URL      = "https://appcenter.intuit.com/connect/oauth2"
QBO_TOKEN_URL     = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"


@router.get("/auth-url")
def get_auth_url():
    """Return the QBO OAuth authorization URL."""
    import urllib.parse
    params = {
        "client_id":     QBO_CLIENT_ID,
        "response_type": "code",
        "scope":         QBO_SCOPES,
        "redirect_uri":  QBO_REDIRECT_URI,
        "state":         "acornlite",
    }
    url = QBO_AUTH_URL + "?" + urllib.parse.urlencode(params)
    return {"auth_url": url}


@router.get("/callback")
async def qbo_callback(request: Request, code: str, realmId: str, state: str = ""):
    """Handle QBO OAuth callback — exchange code for tokens."""
    # Exchange authorization code for tokens
    credentials = base64.b64encode(
        f"{QBO_CLIENT_ID}:{QBO_CLIENT_SECRET}".encode()
    ).decode()

    async with httpx.AsyncClient() as client:
        response = await client.post(
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

    if response.status_code != 200:
        return JSONResponse(
            {"error": "Token exchange failed", "detail": response.text},
            status_code=400,
        )

    tokens = response.json()
    # For now just return tokens — will store in Supabase in next step
    return {
        "realm_id":      realmId,
        "access_token":  tokens.get("access_token"),
        "refresh_token": tokens.get("refresh_token"),
        "expires_in":    tokens.get("expires_in"),
    }


@router.get("/companies")
def list_companies():
    """List connected QBO companies for the current user."""
    # Placeholder — will read from Supabase in next step
    return {"companies": []}
