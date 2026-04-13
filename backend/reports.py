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
    realm_id:      str
    start_date:    str
    end_date:      str
    dimension:     str = "none"
    selected_maps: list[str] = []


def run_report_job(job_id: str, user_id: str, realm_id: str,
                   start_date: str, end_date: str, dimension: str,
                   selected_maps: list[str] | None = None):
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
        company_name = tokens.get("company_name", "") or realm_id
        logger.info(f"company_name from tokens: '{company_name}'")
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

            file_path = result["path"]

            # Append mapping columns if maps were selected
            if selected_maps:
                try:
                    logger.info(f"selected_maps received: {selected_maps}")
                    mapping_result = supabase.table("mappings").select("account_maps").eq(
                        "user_id", user_id
                    ).eq("realm_id", realm_id).execute()

                    account_maps = []
                    if mapping_result.data:
                        account_maps = mapping_result.data[0].get("account_maps", [])
                    logger.info(f"account_maps from Supabase: {len(account_maps)} maps")

                    maps_to_apply = [m for m in account_maps if m.get("map_name", "") in selected_maps]
                    logger.info(f"maps_to_apply after filter: {[m.get('map_name') for m in maps_to_apply]}")

                    if maps_to_apply:
                        progress_fn(f"  Applying {len(maps_to_apply)} mapping(s)...")
                        import openpyxl as _ox
                        from openpyxl.styles import Font, PatternFill
                        from openpyxl.utils import get_column_letter
                        import re as _re2

                        wb = _ox.load_workbook(file_path)
                        HEADER_FILL = PatternFill("solid", fgColor="336699")
                        HEADER_FONT = Font(bold=True, color="FFFFFF")

                        for tab_name in ("IS GL Detail", "BS GL Detail"):
                            if tab_name not in wb.sheetnames:
                                continue
                            ws = wb[tab_name]
                            if ws.max_row < 2:
                                continue

                            logger.info(f"Processing tab {tab_name}, max_row={ws.max_row}")
                            header = [ws.cell(row=1, column=ci).value for ci in range(1, ws.max_column + 1)]
                            try:
                                acct_col_idx = header.index("Account Name") + 1
                            except ValueError:
                                logger.info(f"  Account Name column not found in {tab_name}, header: {header}")
                                continue

                            sample_names = []
                            for ri in range(2, min(8, ws.max_row + 1)):
                                val = ws.cell(row=ri, column=acct_col_idx).value
                                if val:
                                    sample_names.append(repr(str(val)))
                            logger.info(f"  Account Name samples in {tab_name}: {sample_names}")

                            for m in maps_to_apply:
                                map_name = m.get("map_name", "")

                                # Build lookup: account_name string -> (group_name, section)
                                lookup = {}
                                for grp in m.get("groups", []):
                                    group_name = grp.get("group_name", "")
                                    section    = grp.get("pl_section") or grp.get("bs_section") or ""
                                    for acct in grp.get("accounts", []):
                                        if isinstance(acct, dict):
                                            acct_name = acct.get("account_name", "").strip()
                                        else:
                                            acct_name = str(acct).strip()
                                        if acct_name:
                                            lookup[acct_name] = (group_name, section)

                                logger.info(f"  Map '{map_name}': lookup has {len(lookup)} entries")
                                logger.info(f"  lookup keys sample: {list(lookup.keys())[:5]}")

                                next_col = ws.max_column + 1
                                grp_col = next_col
                                sec_col = next_col + 1

                                c = ws.cell(row=1, column=grp_col, value=f"{map_name} - Account Group")
                                c.font = HEADER_FONT
                                c.fill = HEADER_FILL
                                c = ws.cell(row=1, column=sec_col, value=f"{map_name} - Statement Section")
                                c.font = HEADER_FONT
                                c.fill = HEADER_FILL
                                ws.column_dimensions[get_column_letter(grp_col)].width = 24
                                ws.column_dimensions[get_column_letter(sec_col)].width = 22

                                # Write values — exact string match, log first 20 rows for debug
                                for ri in range(2, ws.max_row + 1):
                                    cell_val = ws.cell(row=ri, column=acct_col_idx).value
                                    if not cell_val:
                                        continue
                                    acct_name = str(cell_val).strip()
                                    if ri <= 21:
                                        logger.info(f"  row {ri}: '{acct_name}' -> {lookup.get(acct_name, 'NO MATCH')}")
                                    match = lookup.get(acct_name)
                                    if match:
                                        ws.cell(row=ri, column=grp_col, value=match[0])
                                        ws.cell(row=ri, column=sec_col, value=match[1])

                        wb.save(file_path)
                        progress_fn(f"  Mapping columns appended.")
                except Exception as e:
                    progress_fn(f"  WARNING: Could not apply mappings — {e}")

            # Upload to Supabase storage
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
              body.start_date, body.end_date, body.dimension,
              body.selected_maps),
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


@router.post("/job/{job_id}/cancel")
def cancel_job(job_id: str, user=Depends(get_current_user)):
    """Mark a job as cancelled."""
    job = get_job(job_id)
    if not job or job["user_id"] != str(user.id):
        raise HTTPException(status_code=404, detail="Job not found")
    update_job(job_id, status="failed", error="Cancelled by user")
    return {"cancelled": True}


@router.get("/history")
def job_history(user=Depends(get_current_user)):
    """Get recent jobs for the current user."""
    return {"jobs": get_user_jobs(str(user.id))}
