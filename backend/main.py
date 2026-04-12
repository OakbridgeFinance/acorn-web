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

FRONTEND = Path(__file__).parent.parent / "frontend"
logger.info(f"Frontend path: {FRONTEND}")
logger.info(f"Frontend exists: {FRONTEND.exists()}")

@app.get("/health")
def health():
    return {"status": "ok", "service": "acorn"}

@app.get("/")
def root():
    logger.info("Root route hit")
    return RedirectResponse(url="/login.html")

@app.get("/login.html")
def login_page():
    logger.info(f"Login page requested, file exists: {(FRONTEND / 'login.html').exists()}")
    return FileResponse(FRONTEND / "login.html")

@app.get("/app.html")
def app_page():
    return FileResponse(FRONTEND / "app.html")

@app.get("/style.css")
def styles():
    return FileResponse(FRONTEND / "style.css", media_type="text/css")
