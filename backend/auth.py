import os
import re
import time
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from fastapi import APIRouter, HTTPException, Depends, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

# ── Plan / admin flags live in app_metadata, NOT user_metadata ───────────────
# app_metadata can only be written by the service role.
#
# To grant a plan:
#   update auth.users
#   set raw_app_meta_data = coalesce(raw_app_meta_data, '{}'::jsonb)
#                         || '{"plan": "pro"}'::jsonb
#   where email = 'someone@example.com';

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


# ── Trial / plan helpers ─────────────────────────────────────────────────────

def _effective_plan(app_meta: dict) -> tuple[str, bool, int]:
    """Return (plan, is_trial, days_remaining) with trial expiry enforced."""
    plan = app_meta.get("plan", "basic")
    trial_expires = app_meta.get("trial_expires")

    if plan in ("pro", "plus") and trial_expires:
        try:
            expiry = datetime.fromisoformat(str(trial_expires).replace("Z", "+00:00"))
            now = datetime.now(timezone.utc)
            remaining = (expiry - now).days
            if remaining < 0:
                return ("basic", False, 0)
            return (plan, True, max(remaining, 0))
        except Exception:
            pass

    return (plan, False, 0)


# ── Rate limiting for signup ─────────────────────────────────────────────────

_signup_attempts: dict[str, list[float]] = defaultdict(list)
_SIGNUP_LIMIT = 5
_SIGNUP_WINDOW = 3600  # 1 hour


def _check_signup_rate(ip: str):
    now = time.time()
    attempts = _signup_attempts[ip]
    _signup_attempts[ip] = [t for t in attempts if now - t < _SIGNUP_WINDOW]
    if len(_signup_attempts[ip]) >= _SIGNUP_LIMIT:
        raise HTTPException(status_code=429, detail="Too many signup attempts. Try again later.")
    _signup_attempts[ip].append(now)


# ── Auth dependency ──────────────────────────────────────────────────────────

def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Validate JWT token and return user with effective plan."""
    supabase = get_supabase_anon()
    try:
        result = supabase.auth.get_user(credentials.credentials)
        user = result.user
        # Attach effective plan (with trial expiry check) to the user object
        app_meta = user.app_metadata or {}
        plan, is_trial, days = _effective_plan(app_meta)
        if not hasattr(user, '_acorn_plan'):
            user._acorn_plan = plan
            user._acorn_trial = is_trial
            user._acorn_trial_days = days
        return user
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid or expired token")


# ── Endpoints ────────────────────────────────────────────────────────────────

_EMAIL_RE = re.compile(r'^[^@\s]+@[^@\s]+\.[^@\s]+$')


@router.post("/signup")
def signup(body: AuthRequest, request: Request):
    """Create a new user account with a 7-day Pro trial."""
    # Validation
    if not body.email or not _EMAIL_RE.match(body.email):
        raise HTTPException(status_code=400, detail="Please enter a valid email address.")
    if not body.password or len(body.password) < 8:
        raise HTTPException(status_code=400, detail="Password must be at least 8 characters.")

    # Rate limit
    client_ip = request.client.host if request.client else "unknown"
    _check_signup_rate(client_ip)

    supabase_admin = get_supabase_admin()
    try:
        now = datetime.utcnow()
        result = supabase_admin.auth.admin.create_user({
            "email": body.email,
            "password": body.password,
            "email_confirm": True,
            "app_metadata": {
                "plan": "pro",
                "trial_start": now.isoformat(),
                "trial_expires": (now + timedelta(days=7)).isoformat(),
            },
        })
        return {"message": "Account created! Check your email to verify, then sign in."}
    except Exception as e:
        detail = str(e)
        if "already been registered" in detail.lower() or "already exists" in detail.lower():
            raise HTTPException(status_code=400, detail="An account with this email already exists.")
        raise HTTPException(status_code=400, detail=detail)


@router.post("/login")
def login(body: AuthRequest):
    """Log in and return a session token with plan/trial info."""
    supabase = get_supabase_anon()
    try:
        result = supabase.auth.sign_in_with_password({
            "email":    body.email,
            "password": body.password,
        })
        app_meta = result.user.app_metadata or {}
        plan, is_trial, days = _effective_plan(app_meta)
        return {
            "access_token":  result.session.access_token,
            "refresh_token": result.session.refresh_token,
            "user_id":       result.user.id,
            "email":         result.user.email,
            "plan":          plan,
            "trial":         is_trial,
            "trial_days_remaining": days,
        }
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid email or password")


@router.post("/refresh")
def refresh_token(body: RefreshRequest):
    """Refresh an expired access token, including updated plan/trial info."""
    supabase = get_supabase_anon()
    try:
        result = supabase.auth.refresh_session(body.refresh_token)
        app_meta = result.user.app_metadata or {}
        plan, is_trial, days = _effective_plan(app_meta)
        return {
            "access_token":  result.session.access_token,
            "refresh_token": result.session.refresh_token,
            "plan":          plan,
            "trial":         is_trial,
            "trial_days_remaining": days,
        }
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid refresh token")
