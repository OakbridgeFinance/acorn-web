from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, RedirectResponse
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Acorn API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

import traceback

try:
    from backend.auth import router as auth_router
    app.include_router(auth_router)
    logger.info("Auth router loaded")
except Exception as e:
    logger.error(f"Failed to load auth router: {e}")
    logger.error(traceback.format_exc())

try:
    from backend.qbo_oauth import router as qbo_router
    app.include_router(qbo_router)
    logger.info("QBO router loaded")
except Exception as e:
    logger.error(f"Failed to load qbo router: {e}")
    logger.error(traceback.format_exc())

try:
    from backend.reports import router as reports_router
    app.include_router(reports_router)
    logger.info("Reports router loaded")
except Exception as e:
    logger.error(f"Failed to load reports router: {e}")
    logger.error(traceback.format_exc())

FRONTEND = Path(__file__).parent.parent / "frontend"
logger.info(f"Frontend path: {FRONTEND}")
logger.info(f"Frontend exists: {FRONTEND.exists()}")

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
