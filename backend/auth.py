import os
from fastapi import APIRouter, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

router = APIRouter(prefix="/api/auth", tags=["auth"])

SUPABASE_URL         = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
SUPABASE_ANON_KEY    = os.getenv("SUPABASE_ANON_KEY")

security = HTTPBearer()


def get_supabase_admin():
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


def get_supabase_anon():
    return create_client(SUPABASE_URL, SUPABASE_ANON_KEY)


class AuthRequest(BaseModel):
    email: str
    password: str


class RefreshRequest(BaseModel):
    refresh_token: str


def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Validate JWT token and return user."""
    supabase = get_supabase_admin()
    try:
        result = supabase.auth.get_user(credentials.credentials)
        return result.user
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid or expired token")


@router.post("/signup")
def signup(body: AuthRequest, user=Depends(get_current_user)):
    """Create a new user account (admin only)."""
    user_meta = user.user_metadata or {}
    if not user_meta.get("admin"):
        raise HTTPException(status_code=403, detail="Admin access required")
    supabase = get_supabase_anon()
    try:
        result = supabase.auth.sign_up({
            "email":    body.email,
            "password": body.password,
        })
        return {"message": "User created"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/login")
def login(body: AuthRequest):
    """Log in and return a session token."""
    supabase = get_supabase_anon()
    try:
        result = supabase.auth.sign_in_with_password({
            "email":    body.email,
            "password": body.password,
        })
        user_meta = result.user.user_metadata or {}
        return {
            "access_token":  result.session.access_token,
            "refresh_token": result.session.refresh_token,
            "user_id":       result.user.id,
            "email":         result.user.email,
            "plan":          user_meta.get("plan", "starter"),
        }
    except Exception as e:
        raise HTTPException(status_code=401, detail="Invalid email or password")


@router.post("/refresh")
def refresh_token(body: RefreshRequest):
    """Refresh an expired access token."""
    supabase = get_supabase_anon()
    try:
        result = supabase.auth.refresh_session(body.refresh_token)
        return {
            "access_token":  result.session.access_token,
            "refresh_token": result.session.refresh_token,
        }
    except Exception as e:
        raise HTTPException(status_code=401, detail="Invalid refresh token")
