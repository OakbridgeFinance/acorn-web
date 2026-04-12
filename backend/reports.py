import sys
from pathlib import Path

# Add core directory to path so gl_extractor and its dependencies can import token_manager
sys.path.insert(0, str(Path(__file__).parent / "core"))

import os
import threading
import tempfile
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from supabase import create_client
from backend.auth import get_current_user
from backend.jobs import create_job, update_job, get_job, get_user_jobs
from dotenv import load_dotenv

load_dotenv()

router = APIRouter(prefix="/api/reports", tags=["reports"])

SUPABASE_URL         = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")


def get_supabase():
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


class GenerateRequest(BaseModel):
    realm_id:   str
    start_date: str
    end_date:   str
    dimension:  str = "none"


def run_report_job(job_id: str, user_id: str, realm_id: str,
                   start_date: str, end_date: str, dimension: str):
    """Run in a background thread — fetches QBO data and generates Excel file."""
    try:
        update_job(job_id, status="running")

        # Import core modules
        import sys
        sys.path.insert(0, str(Path(__file__).parent / "core"))
        from gl_extractor import generate_lite

        # Get tokens from Supabase
        supabase = get_supabase()
        token_result = supabase.table("qbo_tokens").select(
            "access_token, refresh_token"
        ).eq("user_id", user_id).eq("realm_id", realm_id).execute()

        if not token_result.data:
            update_job(job_id, status="failed", error="No QBO connection found")
            return

        tokens = token_result.data[0]

        # Write tokens to a temp env so qbo_client can use them
        # (we'll refactor this properly later)
        os.environ["QBO_ACCESS_TOKEN"]  = tokens["access_token"]
        os.environ["QBO_REFRESH_TOKEN"] = tokens["refresh_token"]
        os.environ["QBO_REALM_ID"]      = realm_id

        # Generate report to a temp file
        with tempfile.TemporaryDirectory() as tmpdir:
            result = generate_lite(
                alias=realm_id,
                start_date=start_date,
                end_date=end_date,
                output_mode="new",
                output_folder=tmpdir,
                dimension=dimension,
            )

            # Upload to Supabase storage
            file_path = result["path"]
            file_name = Path(file_path).name
            storage_path = f"{user_id}/{job_id}/{file_name}"

            with open(file_path, "rb") as f:
                supabase.storage.from_("reports").upload(
                    storage_path,
                    f.read(),
                    {"content-type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"}
                )

            # Get signed download URL (valid 1 hour)
            url_result = supabase.storage.from_("reports").create_signed_url(
                storage_path, 3600
            )
            file_url = url_result["signedURL"]

            update_job(job_id, status="complete", file_url=file_url)

    except Exception as e:
        update_job(job_id, status="failed", error=str(e))


@router.post("/generate")
def generate_report(body: GenerateRequest, user=Depends(get_current_user)):
    """Kick off a report generation job."""
    job = create_job(
        user_id=str(user.id),
        realm_id=body.realm_id,
        start_date=body.start_date,
        end_date=body.end_date,
        dimension=body.dimension,
    )

    # Run in background thread so request returns immediately
    thread = threading.Thread(
        target=run_report_job,
        args=(job["id"], str(user.id), body.realm_id,
              body.start_date, body.end_date, body.dimension),
        daemon=True,
    )
    thread.start()

    return {"job_id": job["id"], "status": "pending"}


@router.get("/job/{job_id}")
def get_job_status(job_id: str, user=Depends(get_current_user)):
    """Poll for job status."""
    job = get_job(job_id)
    if not job or job["user_id"] != str(user.id):
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@router.get("/history")
def job_history(user=Depends(get_current_user)):
    """Get recent jobs for the current user."""
    return {"jobs": get_user_jobs(str(user.id))}
