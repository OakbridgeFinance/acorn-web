from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from backend.qbo_oauth import router as qbo_router
from backend.auth import router as auth_router

app = FastAPI(title="Acorn Lite API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth_router)
app.include_router(qbo_router)

@app.get("/health")
def health():
    return {"status": "ok", "service": "acorn"}
