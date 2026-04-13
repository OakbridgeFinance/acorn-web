import sys
from pathlib import Path

# Add core directory to path so gl_extractor and its dependencies can import token_manager
sys.path.insert(0, str(Path(__file__).parent / "core"))

import os
import logging
import threading
import tempfile
from fastapi import APIRouter, Depends, HTTPException

logger = logging.getLogger(__name__)
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
    realm_id:     str
    start_date:   str
    end_date:     str
    dimension:    str = "none"
    selected_map: str = ""


def run_report_job(job_id: str, user_id: str, realm_id: str,
                   start_date: str, end_date: str, dimension: str):
    """Run in a background thread — fetches QBO data and generates Excel file."""
    try:
        update_job(job_id, status="running")

        # Import core modules
        import sys
        sys.path.insert(0, str(Path(__file__).parent / "core"))
        from gl_extractor import generate_lite

        # Get tokens and company name from Supabase
        supabase = get_supabase()
        token_result = supabase.table("qbo_tokens").select(
            "access_token, refresh_token, company_name"
        ).eq("user_id", user_id).eq("realm_id", realm_id).execute()

        if not token_result.data:
            update_job(job_id, status="failed", error="No QBO connection found")
            return

        tokens = token_result.data[0]
        company_name = tokens.get("company_name", realm_id)
        access_token  = tokens["access_token"]
        refresh_token = tokens["refresh_token"]

        # Check if access token is expired and refresh if needed
        import re as _re
        from datetime import datetime, timedelta
        expires_at_str = tokens.get("expires_at", "")
        if expires_at_str:
            try:
                expires_at = datetime.fromisoformat(expires_at_str.replace("Z", "+00:00"))
                if datetime.utcnow().replace(tzinfo=expires_at.tzinfo) >= expires_at - timedelta(minutes=5):
                    # Token expired or near expiry — refresh it
                    import base64, httpx
                    client_id = os.getenv("QBO_CLIENT_ID", "")
                    client_secret = os.getenv("QBO_CLIENT_SECRET", "")
                    credentials = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
                    refresh_resp = httpx.post(
                        "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
                        headers={
                            "Authorization": f"Basic {credentials}",
                            "Content-Type": "application/x-www-form-urlencoded",
                            "Accept": "application/json",
                        },
                        data={
                            "grant_type": "refresh_token",
                            "refresh_token": refresh_token,
                        },
                        timeout=30,
                    )
                    if refresh_resp.status_code == 200:
                        new_tokens = refresh_resp.json()
                        access_token  = new_tokens["access_token"]
                        refresh_token = new_tokens.get("refresh_token", refresh_token)
                        new_expiry = (datetime.utcnow() + timedelta(seconds=new_tokens.get("expires_in", 3600))).isoformat()
                        # Update Supabase with refreshed tokens
                        supabase.table("qbo_tokens").update({
                            "access_token":  access_token,
                            "refresh_token": refresh_token,
                            "expires_at":    new_expiry,
                        }).eq("user_id", user_id).eq("realm_id", realm_id).execute()
            except Exception:
                pass  # proceed with existing token

        # Inject Supabase tokens directly into qbo_client
        import qbo_client
        logger.info(f"Setting override tokens for realm_id={realm_id}, access_token starts with: {access_token[:20]}")
        qbo_client.set_override_tokens({
            "realm_id":     realm_id,
            "access_token": access_token,
            "refresh_token": refresh_token,
        })
        qbo_client.get_environment = lambda: "production"
        logger.info("Override tokens set successfully")

        # Clean company name for filename
        clean_name = _re.sub(r'[^\w]', '_', company_name).strip('_').upper()
        file_name = f"{clean_name}_{start_date[:7]}_{end_date[:7]}.xlsx"

        # Create a progress function that updates the job in Supabase
        def progress_fn(msg):
            msg = str(msg).strip()
            if msg and not msg.startswith('[progress]'):
                update_job(job_id, progress=msg)

        # Generate report to a temp file
        with tempfile.TemporaryDirectory() as tmpdir:
            result = generate_lite(
                alias=realm_id,
                start_date=start_date,
                end_date=end_date,
                output_mode="new",
                output_folder=tmpdir,
                file_name=file_name,
                dimension=dimension,
                progress_fn=progress_fn,
            )

            # Upload to Supabase storage
            file_path = result["path"]
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
    finally:
        try:
            logger.info("Clearing override tokens in finally block")
            qbo_client.set_override_tokens(None)
        except Exception:
            pass


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
