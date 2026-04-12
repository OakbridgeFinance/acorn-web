from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse
from pathlib import Path
from backend.qbo_oauth import router as qbo_router
from backend.auth import router as auth_router
from backend.reports import router as reports_router

app = FastAPI(title="Acorn API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth_router)
app.include_router(qbo_router)
app.include_router(reports_router)

@app.get("/health")
def health():
    return {"status": "ok", "service": "acorn"}

@app.get("/")
def root():
    return RedirectResponse(url="/login.html")

# Serve frontend static files
frontend_path = Path(__file__).parent.parent / "frontend"
if frontend_path.exists():
    app.mount("/", StaticFiles(directory=str(frontend_path), html=True), name="frontend")
