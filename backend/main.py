import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, RedirectResponse
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Acorn API")

_cors_env = os.getenv("CORS_ALLOWED_ORIGINS", "").strip()
if _cors_env:
    allowed_origins = [o.strip() for o in _cors_env.split(",") if o.strip()]
else:
    allowed_origins = ["https://acorn.oakbridgefinance.com"]
logger.info(f"CORS allowed origins: {allowed_origins}")

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Let import errors crash startup — a silently-missing router is worse than
# a visible boot failure in the logs.
from backend.auth import router as auth_router
from backend.qbo_oauth import router as qbo_router
from backend.reports import router as reports_router
from backend.mapping import router as mapping_router

app.include_router(auth_router)
app.include_router(qbo_router)
app.include_router(reports_router)
app.include_router(mapping_router)

FRONTEND = Path(__file__).parent.parent / "frontend"


@app.on_event("startup")
def _startup_sweeps():
    """Boot-time cleanups.

    1. Flip pending/running jobs to failed. Reports run in in-process threads,
       so a process restart drops the worker but leaves the DB row stuck at
       status='running', making the UI poll forever.
    2. Delete expired QBO OAuth state rows (older than 1 hour). States are
       consumed on callback but abandoned flows accumulate otherwise.
    """
    from datetime import datetime, timezone, timedelta
    from supabase import create_client

    try:
        sb = create_client(
            os.getenv("SUPABASE_URL"),
            os.getenv("SUPABASE_SERVICE_KEY"),
        )
    except Exception as e:
        logger.warning(f"Startup sweep skipped — Supabase client init failed: {e}")
        return

    try:
        sb.table("jobs").update({
            "status":     "failed",
            "error":      "Server restart",
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }).in_("status", ["pending", "running"]).execute()
    except Exception as e:
        logger.warning(f"Orphan-job recovery sweep failed: {e}")

    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        sb.table("qbo_oauth_states").delete().lt("created_at", cutoff).execute()
    except Exception as e:
        logger.warning(f"OAuth state cleanup failed: {e}")

@app.get("/health")
def health():
    return {"status": "ok", "service": "acorn"}

@app.get("/")
def root():
    return RedirectResponse(url="/login.html")

@app.get("/login.html")
def login_page():
    return FileResponse(FRONTEND / "login.html")

@app.get("/app.html")
def app_page():
    return FileResponse(FRONTEND / "app.html")

@app.get("/style.css")
def styles():
    return FileResponse(FRONTEND / "style.css", media_type="text/css")

@app.get("/Logo-F23-transparent.png")
def logo_image():
    return FileResponse(FRONTEND / "Logo-F23-transparent.png", media_type="image/png")

@app.get("/favicon.png")
def favicon_png():
    return FileResponse(FRONTEND / "favicon.png", media_type="image/png")
