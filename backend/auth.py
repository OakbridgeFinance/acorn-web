import os
from fastapi import APIRouter, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

# ── Plan / admin flags live in app_metadata, NOT user_metadata ───────────────
# Supabase clients can update their own user_metadata via the JS SDK
# (auth.updateUser), which would let any signed-in user self-promote to
# admin or pro if the gates read from there. app_metadata can only be
# written by the service role, so we read both `plan` and `admin` from
# app_metadata throughout the backend.
#
# To grant a plan or admin flag, run something like (in Supabase SQL):
#
#   update auth.users
#   set raw_app_meta_data = coalesce(raw_app_meta_data, '{}'::jsonb)
#                         || '{"plan": "admin"}'::jsonb
#   where email = 'someone@example.com';
#
#   update auth.users
#   set raw_app_meta_data = coalesce(raw_app_meta_data, '{}'::jsonb)
#                         || '{"admin": true}'::jsonb
#   where email = 'someone@example.com';
#
# One-time migration for existing users whose plan/admin currently lives
# in user_metadata (run once, then nothing else needed):
#
#   update auth.users
#   set raw_app_meta_data = coalesce(raw_app_meta_data, '{}'::jsonb)
#                         || jsonb_build_object('plan', raw_user_meta_data->>'plan')
#   where raw_user_meta_data ? 'plan'
#     and (raw_app_meta_data->>'plan') is distinct from (raw_user_meta_data->>'plan');
#
#   update auth.users
#   set raw_app_meta_data = coalesce(raw_app_meta_data, '{}'::jsonb)
#                         || jsonb_build_object('admin', raw_user_meta_data->'admin')
#   where raw_user_meta_data ? 'admin'
#     and (raw_app_meta_data->'admin') is distinct from (raw_user_meta_data->'admin');
#
# The user_metadata copies are intentionally left in place; they're just
# no longer authoritative.

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
    # Use the anon client: JWT verification doesn't need service-role
    # privileges, and using the admin client here would expand the blast
    # radius if this function ever grows beyond simple token validation.
    supabase = get_supabase_anon()
    try:
        result = supabase.auth.get_user(credentials.credentials)
        return result.user
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid or expired token")


@router.post("/signup")
def signup(body: AuthRequest, user=Depends(get_current_user)):
    """Create a new user account (admin only)."""
    app_meta = user.app_metadata or {}
    if not app_meta.get("admin"):
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
        app_meta = result.user.app_metadata or {}
        return {
            "access_token":  result.session.access_token,
            "refresh_token": result.session.refresh_token,
            "user_id":       result.user.id,
            "email":         result.user.email,
            "plan":          app_meta.get("plan", "starter"),
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
