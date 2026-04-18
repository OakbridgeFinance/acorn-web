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

try:
    from backend.mapping import router as mapping_router
    app.include_router(mapping_router)
    logger.info("Mapping router loaded")
except Exception as e:
    logger.error(f"Failed to load mapping router: {e}")
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

@app.get("/Logo-F23-transparent.png")
def logo_image():
    return FileResponse(FRONTEND / "Logo-F23-transparent.png", media_type="image/png")

@app.get("/favicon.svg")
def favicon_svg():
    return FileResponse(FRONTEND / "favicon.svg", media_type="image/svg+xml")

@app.get("/favicon.png")
def favicon_png():
    return FileResponse(FRONTEND / "favicon.png", media_type="image/png")
