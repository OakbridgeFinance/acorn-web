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


class ResetPasswordRequest(BaseModel):
    email: str


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


# ── Rate limiting ────────────────────────────────────────────────────────────
# Per-IP in-memory limits. Adequate for single-worker deployments; move to
# Redis/DB if we ever scale out beyond one process.

_signup_attempts: dict[str, list[float]] = defaultdict(list)
_SIGNUP_LIMIT  = 5
_SIGNUP_WINDOW = 3600  # 1 hour

_login_attempts: dict[str, list[float]] = defaultdict(list)
_LOGIN_LIMIT  = 5
_LOGIN_WINDOW = 300  # 5 minutes

_reset_attempts: dict[str, list[float]] = defaultdict(list)
_RESET_LIMIT  = 3
_RESET_WINDOW = 3600  # 1 hour


def _sweep_rate_store(store: dict[str, list[float]], window: float, now: float) -> None:
    """Drop IPs whose entire window has elapsed so the dict can't grow unbounded."""
    stale = [ip for ip, timestamps in store.items()
             if not timestamps or (now - max(timestamps)) > window]
    for ip in stale:
        store.pop(ip, None)


def _check_rate(
    store: dict[str, list[float]],
    ip: str,
    limit: int,
    window: float,
    detail: str,
) -> None:
    now = time.time()
    _sweep_rate_store(store, window, now)
    store[ip] = [t for t in store[ip] if now - t < window]
    if len(store[ip]) >= limit:
        raise HTTPException(status_code=429, detail=detail)
    store[ip].append(now)


def _check_signup_rate(ip: str):
    _check_rate(
        _signup_attempts, ip, _SIGNUP_LIMIT, _SIGNUP_WINDOW,
        "Too many signup attempts. Try again later.",
    )


def _check_login_rate(ip: str):
    _check_rate(
        _login_attempts, ip, _LOGIN_LIMIT, _LOGIN_WINDOW,
        "Too many login attempts. Please try again in a few minutes.",
    )


def _check_reset_rate(ip: str):
    _check_rate(
        _reset_attempts, ip, _RESET_LIMIT, _RESET_WINDOW,
        "Too many reset attempts. Try again later.",
    )


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

_PASSWORD_POLICY_MSG = (
    "Password must be at least 8 characters with at least one uppercase "
    "letter, one lowercase letter, and one number."
)


def _password_ok(pw: str) -> bool:
    return (
        isinstance(pw, str)
        and len(pw) >= 8
        and any(c.isupper() for c in pw)
        and any(c.islower() for c in pw)
        and any(c.isdigit() for c in pw)
    )


# Duplicate-user detection. supabase-py wraps Auth errors with an HTTP status
# code (422) and/or an `error_code` like "email_exists" / "user_already_exists".
# We try each signal in order, with message substrings as the last resort for
# older/alternate Supabase versions.
_DUPLICATE_ERROR_CODES = {"email_exists", "user_already_exists"}
_DUPLICATE_SUBSTRINGS = (
    "already been registered",
    "already registered",
    "already exists",
    "user already exists",
    "duplicate key",
)


def _looks_like_duplicate_user(err: Exception) -> bool:
    code = getattr(err, "code", None) or getattr(err, "error_code", None)
    if isinstance(code, str) and code.lower() in _DUPLICATE_ERROR_CODES:
        return True
    status = getattr(err, "status", None) or getattr(err, "status_code", None)
    msg = str(err).lower()
    if status in (409, 422) and any(s in msg for s in _DUPLICATE_SUBSTRINGS):
        return True
    return any(s in msg for s in _DUPLICATE_SUBSTRINGS)


@router.post("/signup")
def signup(body: AuthRequest, request: Request):
    """Create a new user account with a 7-day Pro trial."""
    # Validation
    if not body.email or not _EMAIL_RE.match(body.email):
        raise HTTPException(status_code=400, detail="Please enter a valid email address.")
    if not _password_ok(body.password or ""):
        raise HTTPException(status_code=400, detail=_PASSWORD_POLICY_MSG)

    # Rate limit
    client_ip = request.client.host if request.client else "unknown"
    _check_signup_rate(client_ip)

    supabase_admin = get_supabase_admin()
    try:
        now = datetime.now(timezone.utc)
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
        if _looks_like_duplicate_user(e):
            raise HTTPException(
                status_code=400,
                detail="An account with this email already exists.",
            )
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/login")
def login(body: AuthRequest, request: Request):
    """Log in and return a session token with plan/trial info."""
    client_ip = request.client.host if request.client else "unknown"
    _check_login_rate(client_ip)

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


@router.post("/reset-password")
def reset_password(body: ResetPasswordRequest, request: Request):
    """Send a password-reset email via Supabase's built-in recovery flow.

    Always returns a generic success message regardless of whether the email
    exists, so attackers can't use this endpoint to enumerate accounts.
    """
    client_ip = request.client.host if request.client else "unknown"
    _check_reset_rate(client_ip)

    # Basic format validation — don't bother Supabase with garbage input.
    if not body.email or not _EMAIL_RE.match(body.email):
        raise HTTPException(status_code=400, detail="Please enter a valid email address.")

    try:
        supabase_admin = get_supabase_admin()
        # generate_link creates a recovery link; Supabase's Auth service emails
        # it to the user automatically when the "Enable email confirmations"
        # template is configured.
        supabase_admin.auth.admin.generate_link({
            "type":  "recovery",
            "email": body.email,
        })
    except Exception:
        # Swallow — revealing whether this succeeded leaks account existence.
        pass

    return {
        "message": "If an account exists with that email, a reset link has been sent.",
    }
