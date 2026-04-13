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
        import sys
        sys.path.insert(0, str(Path(__file__).parent / "core"))
        from gl_extractor import generate_lite
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
        import re as _re
        from datetime import datetime, timedelta
        expires_at_str = tokens.get("expires_at", "")
        if expires_at_str:
            try:
                expires_at = datetime.fromisoformat(expires_at_str.replace("Z", "+00:00"))
                if datetime.utcnow().replace(tzinfo=expires_at.tzinfo) >= expires_at - timedelta(minutes=5):
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
                        supabase.table("qbo_tokens").update({
                            "access_token":  access_token,
                            "refresh_token": refresh_token,
                            "expires_at":    new_expiry,
                        }).eq("user_id", user_id).eq("realm_id", realm_id).execute()
            except Exception:
                pass
        import qbo_client
        logger.info(f"Setting override tokens for realm_id={realm_id}, access_token starts with: {access_token[:20]}")
        qbo_client.set_override_tokens({
            "realm_id":     realm_id,
            "access_token": access_token,
            "refresh_token": refresh_token,
        })
        qbo_client.get_environment = lambda: "production"
        logger.info("Override tokens set successfully")
        clean_name = _re.sub(r'[^\w]', '_', company_name).strip('_').upper()
        file_name = f"{clean_name}_{start_date[:7]}_{end_date[:7]}.xlsx"
        def progress_fn(msg):
            msg = str(msg).strip()
            if msg and not msg.startswith('[progress]'):
                update_job(job_id, progress=msg)
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

            # ── Append mapping columns ────────────────────────────────────
            if selected_maps:
                try:
                    logger.info(f"selected_maps received: {selected_maps}")
                    mapping_result = supabase.table("mappings").select("account_maps").eq(
                        "user_id", user_id
                    ).eq("realm_id", realm_id).execute()
                    account_maps = []
                    if mapping_result.data:
                        account_maps = mapping_result.data[0].get("account_maps", [])
                    maps_to_apply = [m for m in account_maps if m.get("map_name", "") in selected_maps]
                    logger.info(f"maps_to_apply: {[m.get('map_name') for m in maps_to_apply]}")

                    if maps_to_apply:
                        progress_fn(f"  Applying {len(maps_to_apply)} mapping(s)...")
                        import openpyxl as _ox
                        from openpyxl.styles import Font, PatternFill, Alignment
                        from openpyxl.utils import get_column_letter
                        from copy import copy as _copy
                        import re as _re_map

                        wb = _ox.load_workbook(file_path)

                        # Hide gridlines on all tabs
                        for sn in wb.sheetnames:
                            wb[sn].sheet_view.showGridLines = False

                        MAP_HEADER_FILL = PatternFill("solid", fgColor="336699")
                        MAP_HEADER_FONT = Font(bold=True, color="FFFFFF")
                        PLAIN_FONT      = Font(name="Calibri", size=11, bold=False)
                        PLAIN_FILL      = PatternFill("solid", fgColor="FFFFFF")
                        PLAIN_ALIGN     = Alignment(horizontal="left", vertical="bottom")

                        def _build_lookup(m):
                            lookup = {}
                            for grp in m.get("groups", []):
                                group_name = grp.get("group_name", "")
                                section = grp.get("pl_section") or grp.get("bs_section") or ""
                                for acct in grp.get("accounts", []):
                                    acct_name = (acct.get("account_name", "") if isinstance(acct, dict) else str(acct)).strip()
                                    if acct_name:
                                        lookup[acct_name] = (group_name, section)
                            return lookup

                        num_maps     = len(maps_to_apply)
                        num_new_cols = num_maps * 2

                        # Log Validation formula BEFORE any changes
                        if "Validation" in wb.sheetnames:
                            ws_val = wb["Validation"]
                            sample = ws_val.cell(row=9, column=4).value
                            logger.info(f"Validation D9 BEFORE: {repr(sample)}")

                        # ── IS GL Detail and BS GL Detail — append at end ─────
                        for tab_name in ("IS GL Detail", "BS GL Detail"):
                            if tab_name not in wb.sheetnames:
                                continue
                            ws = wb[tab_name]
                            if ws.max_row < 2:
                                continue
                            header = [ws.cell(row=1, column=ci).value for ci in range(1, ws.max_column + 1)]
                            try:
                                acct_col_idx = header.index("Account Name") + 1
                            except ValueError:
                                logger.warning(f"  No 'Account Name' col in {tab_name}, header={header[:8]}")
                                continue
                            next_col = ws.max_column + 1
                            for map_idx, m in enumerate(maps_to_apply):
                                map_name = m.get("map_name", "")
                                lookup   = _build_lookup(m)
                                grp_col  = next_col + (map_idx * 2)
                                sec_col  = next_col + (map_idx * 2) + 1

                                # Header row
                                c = ws.cell(row=1, column=grp_col, value=f"{map_name} - Account Group")
                                c.font = MAP_HEADER_FONT
                                c.fill = MAP_HEADER_FILL
                                ws.column_dimensions[get_column_letter(grp_col)].width = 24

                                c = ws.cell(row=1, column=sec_col, value=f"{map_name} - Statement Section")
                                c.font = MAP_HEADER_FONT
                                c.fill = MAP_HEADER_FILL
                                ws.column_dimensions[get_column_letter(sec_col)].width = 22

                                matched = 0
                                for ri in range(2, ws.max_row + 1):
                                    cell_val = ws.cell(row=ri, column=acct_col_idx).value
                                    if not cell_val:
                                        continue
                                    acct_name = str(cell_val).strip()
                                    # Always set plain formatting on new columns
                                    for col in (grp_col, sec_col):
                                        tgt = ws.cell(row=ri, column=col)
                                        tgt.font      = PLAIN_FONT
                                        tgt.fill      = PLAIN_FILL
                                        tgt.alignment = PLAIN_ALIGN
                                    match = lookup.get(acct_name)
                                    if match:
                                        ws.cell(row=ri, column=grp_col, value=match[0])
                                        ws.cell(row=ri, column=sec_col, value=match[1])
                                        matched += 1
                                logger.info(f"  {tab_name} map '{map_name}': {matched} rows matched of {ws.max_row-1}")

                        # ── BS Balances, P&L, Balance Sheet — insert after col A ──
                        # Record P&L Total column BEFORE inserting
                        pl_total_col_before = None
                        if "P&L" in wb.sheetnames:
                            ws_pl = wb["P&L"]
                            pl_hdr = [ws_pl.cell(row=1, column=ci).value for ci in range(1, ws_pl.max_column + 1)]
                            logger.info(f"P&L header before insert: {pl_hdr[:5]}...{pl_hdr[-3:]}")
                            try:
                                pl_total_col_before = pl_hdr.index("Total") + 1  # 1-based col number
                                logger.info(f"P&L Total col before insert: {pl_total_col_before} ({get_column_letter(pl_total_col_before)})")
                            except ValueError:
                                logger.warning("P&L 'Total' column not found in header")

                        # Record BS last data column BEFORE inserting
                        bs_last_col_before = None
                        if "Balance Sheet" in wb.sheetnames:
                            ws_bs = wb["Balance Sheet"]
                            bs_max = ws_bs.max_column
                            bs_last_col_before = bs_max  # last col before insert
                            logger.info(f"Balance Sheet max col before insert: {bs_max} ({get_column_letter(bs_max)})")

                        for tab_name in ("BS Balances", "P&L", "Balance Sheet"):
                            if tab_name not in wb.sheetnames:
                                continue
                            ws = wb[tab_name]
                            if ws.max_row < 2:
                                continue

                            # Insert all map columns at once after col A
                            ws.insert_cols(2, num_new_cols)

                            for map_idx, m in enumerate(maps_to_apply):
                                map_name = m.get("map_name", "")
                                lookup   = _build_lookup(m)
                                grp_col  = 2 + (map_idx * 2)
                                sec_col  = 2 + (map_idx * 2) + 1

                                # Header
                                c = ws.cell(row=1, column=grp_col, value=f"{map_name} - Account Group")
                                c.font = MAP_HEADER_FONT
                                c.fill = MAP_HEADER_FILL
                                ws.column_dimensions[get_column_letter(grp_col)].width = 24

                                c = ws.cell(row=1, column=sec_col, value=f"{map_name} - Statement Section")
                                c.font = MAP_HEADER_FONT
                                c.fill = MAP_HEADER_FILL
                                ws.column_dimensions[get_column_letter(sec_col)].width = 22

                                matched = 0
                                for ri in range(2, ws.max_row + 1):
                                    src      = ws.cell(row=ri, column=1)
                                    cell_val = src.value
                                    if not cell_val:
                                        continue
                                    acct_name = str(cell_val).strip()

                                    # Copy font+fill from col A for consistent row formatting
                                    for col in (grp_col, sec_col):
                                        tgt = ws.cell(row=ri, column=col)
                                        if src.has_style:
                                            tgt.font = _copy(src.font)
                                            tgt.fill = _copy(src.fill)
                                        tgt.alignment = Alignment(horizontal="left")

                                    match = lookup.get(acct_name)
                                    if match:
                                        ws.cell(row=ri, column=grp_col, value=match[0])
                                        ws.cell(row=ri, column=sec_col, value=match[1])
                                        matched += 1
                                logger.info(f"  {tab_name} map '{map_name}': {matched} rows matched")

                        # ── Patch Validation XLOOKUP formulas ────────────────────
                        # After insert_cols, the Total col in P&L shifted right by num_new_cols
                        # After insert_cols, the last data col in Balance Sheet shifted right by num_new_cols
                        if "Validation" in wb.sheetnames and num_new_cols > 0:
                            ws_val = wb["Validation"]

                            # Calculate new column letters after shift
                            new_pl_total_col = None
                            if pl_total_col_before is not None:
                                new_pl_total_col = get_column_letter(pl_total_col_before + num_new_cols)
                                logger.info(f"P&L Total col after insert: {new_pl_total_col}")

                            new_bs_last_col = None
                            if bs_last_col_before is not None:
                                new_bs_last_col = get_column_letter(bs_last_col_before + num_new_cols)
                                logger.info(f"Balance Sheet last col after insert: {new_bs_last_col}")

                            patched = 0
                            for ri in range(1, ws_val.max_row + 1):
                                cell    = ws_val.cell(row=ri, column=4)
                                formula = cell.value
                                if not formula or not isinstance(formula, str) or not formula.startswith("="):
                                    continue
                                original = formula
                                # Replace P&L column range references
                                if new_pl_total_col and "'P&L'!" in formula:
                                    formula = _re_map.sub(
                                        r"'P&L'!([A-Z]+):([A-Z]+)",
                                        f"'P&L'!{new_pl_total_col}:{new_pl_total_col}",
                                        formula
                                    )
                                # Replace Balance Sheet column range references
                                if new_bs_last_col and "'Balance Sheet'!" in formula:
                                    formula = _re_map.sub(
                                        r"'Balance Sheet'!([A-Z]+):([A-Z]+)",
                                        f"'Balance Sheet'!{new_bs_last_col}:{new_bs_last_col}",
                                        formula
                                    )
                                if formula != original:
                                    cell.value = formula
                                    patched += 1

                            logger.info(f"Validation: patched {patched} formulas")
                            # Log sample after patching
                            sample = ws_val.cell(row=9, column=4).value
                            logger.info(f"Validation D9 AFTER: {repr(sample)}")

                        wb.save(file_path)
                        progress_fn(f"  Mapping columns appended.")

                except Exception as e:
                    import traceback
                    logger.error(f"Mapping failed: {e}")
                    logger.error(traceback.format_exc())
                    progress_fn(f"  WARNING: Could not apply mappings — {e}")

            # Upload to Supabase storage
            storage_path = f"{user_id}/{job_id}/{file_name}"
            with open(file_path, "rb") as f:
                supabase.storage.from_("reports").upload(
                    storage_path,
                    f.read(),
                    {"content-type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"}
                )
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