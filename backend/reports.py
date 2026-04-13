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
                    logger.info(f"Applying maps: {selected_maps}")
                    mapping_result = supabase.table("mappings").select("account_maps").eq(
                        "user_id", user_id
                    ).eq("realm_id", realm_id).execute()
                    account_maps = (mapping_result.data or [{}])[0].get("account_maps", []) if mapping_result.data else []
                    maps_to_apply = [m for m in account_maps if m.get("map_name", "") in selected_maps]

                    if maps_to_apply:
                        import openpyxl as _ox
                        from openpyxl.styles import Font, PatternFill, Alignment
                        from openpyxl.utils import get_column_letter, column_index_from_string
                        import re as _re_map
                        from copy import copy as _copy

                        wb = _ox.load_workbook(file_path)

                        for sn in wb.sheetnames:
                            wb[sn].sheet_view.showGridLines = False

                        HDR_FONT = Font(bold=True, color="FFFFFF")
                        HDR_FILL = PatternFill("solid", fgColor="1F4E79")

                        def build_lookup(m):
                            out = {}
                            for grp in m.get("groups", []):
                                gname = grp.get("group_name", "")
                                sect  = grp.get("pl_section") or grp.get("bs_section") or ""
                                for a in grp.get("accounts", []):
                                    name = (a.get("account_name","") if isinstance(a,dict) else str(a)).strip()
                                    if name:
                                        out[name] = (gname, sect)
                            return out

                        def _patch_col_refs(formula, tab_name, num_inserted, insert_pos=2):
                            """Shift column references >= insert_pos in formulas referencing tab_name."""
                            def _shift(match):
                                col_letter = match.group(1)
                                sep = match.group(2)
                                col_idx = column_index_from_string(col_letter)
                                if col_idx >= insert_pos:
                                    col_idx += num_inserted
                                return get_column_letter(col_idx) + sep
                            escaped = _re_map.escape(tab_name)
                            pattern = _re_map.compile(rf"'{escaped}'!([A-Z]+)([:0-9])")
                            return pattern.sub(
                                lambda m: f"'{tab_name}'!" + _shift(m),
                                formula
                            )

                        n = len(maps_to_apply)
                        nc = n * 2  # number of new columns

                        # ── GL Detail tabs — insert after Account Name ──────
                        for tab in ("IS GL Detail", "BS GL Detail"):
                            if tab not in wb.sheetnames:
                                continue
                            ws = wb[tab]
                            hdr = [ws.cell(1, c).value for c in range(1, ws.max_column+1)]
                            if "Account Name" not in hdr:
                                continue
                            acct_col = hdr.index("Account Name") + 1
                            ws.insert_cols(acct_col + 1, nc)
                            base = acct_col + 1

                            for mi, m in enumerate(maps_to_apply):
                                lkp  = build_lookup(m)
                                gc   = base + mi*2
                                sc   = base + mi*2 + 1
                                mname = m.get("map_name","")

                                ws.cell(1, gc, f"{mname} - Account Group").font = HDR_FONT
                                ws.cell(1, gc).fill = HDR_FILL
                                ws.cell(1, sc, f"{mname} - Statement Section").font = HDR_FONT
                                ws.cell(1, sc).fill = HDR_FILL
                                ws.column_dimensions[get_column_letter(gc)].width = 24
                                ws.column_dimensions[get_column_letter(sc)].width = 22

                                for ri in range(2, ws.max_row+1):
                                    v = ws.cell(ri, acct_col).value
                                    if not v:
                                        continue
                                    match = lkp.get(str(v).strip())
                                    if match:
                                        ws.cell(ri, gc, match[0])
                                        ws.cell(ri, sc, match[1])

                        # ── P&L, Balance Sheet, BS Balances — insert after col A
                        pl_total_before = None
                        if "P&L" in wb.sheetnames:
                            ph = [wb["P&L"].cell(1,c).value for c in range(1, wb["P&L"].max_column+1)]
                            if "Total" in ph:
                                pl_total_before = ph.index("Total") + 1

                        bs_last_before = None
                        if "Balance Sheet" in wb.sheetnames:
                            bs_last_before = wb["Balance Sheet"].max_column

                        for tab in ("BS Balances", "P&L", "Balance Sheet"):
                            if tab not in wb.sheetnames:
                                continue
                            ws = wb[tab]
                            ws.insert_cols(2, nc)

                            for mi, m in enumerate(maps_to_apply):
                                lkp   = build_lookup(m)
                                gc    = 2 + mi*2
                                sc    = 2 + mi*2 + 1
                                mname = m.get("map_name","")

                                h1 = ws.cell(1,1)
                                for col in (gc, sc):
                                    c = ws.cell(1, col)
                                    c.font = _copy(h1.font) if h1.has_style else HDR_FONT
                                    c.fill = _copy(h1.fill) if h1.has_style else HDR_FILL
                                ws.cell(1, gc, f"{mname} - Account Group")
                                ws.cell(1, sc, f"{mname} - Statement Section")
                                ws.column_dimensions[get_column_letter(gc)].width = 24
                                ws.column_dimensions[get_column_letter(sc)].width = 22

                                for ri in range(2, ws.max_row+1):
                                    v = ws.cell(ri, 1).value
                                    if not v:
                                        continue
                                    src = ws.cell(ri, 1)
                                    for col in (gc, sc):
                                        tgt = ws.cell(ri, col)
                                        if src.has_style:
                                            tgt.font = _copy(src.font)
                                            tgt.fill = _copy(src.fill)
                                    match = lkp.get(str(v).strip())
                                    if match:
                                        ws.cell(ri, gc, match[0])
                                        ws.cell(ri, sc, match[1])

                        # ── Patch Validation formulas ───────────────────────
                        if "Validation" in wb.sheetnames:
                            ws_v = wb["Validation"]
                            new_pl_col = get_column_letter(pl_total_before + nc) if pl_total_before else None
                            new_bs_col = get_column_letter(bs_last_before  + nc) if bs_last_before  else None
                            patched = 0

                            for ri in range(1, ws_v.max_row+1):
                                # Patch col D (QBO Report Value)
                                cell = ws_v.cell(ri, 4)
                                f = cell.value
                                if f and isinstance(f, str) and f.startswith("="):
                                    orig = f
                                    # P&L: only replace LAST col ref (return array)
                                    if new_pl_col and "'P&L'!" in f:
                                        refs = list(_re_map.finditer(r"'P&L'!([A-Z]+):([A-Z]+)", f))
                                        if refs:
                                            last = refs[-1]
                                            f = f[:last.start()] + f"'P&L'!{new_pl_col}:{new_pl_col}" + f[last.end():]
                                    # Balance Sheet: only replace LAST col ref
                                    if new_bs_col and "'Balance Sheet'!" in f:
                                        refs = list(_re_map.finditer(r"'Balance Sheet'!([A-Z]+):([A-Z]+)", f))
                                        if refs:
                                            last = refs[-1]
                                            f = f[:last.start()] + f"'Balance Sheet'!{new_bs_col}:{new_bs_col}" + f[last.end():]
                                    if f != orig:
                                        cell.value = f
                                        patched += 1

                                # Patch col C (GL Value Live) — BS Balances refs
                                cell_c = ws_v.cell(ri, 3)
                                fc = cell_c.value
                                if fc and isinstance(fc, str) and fc.startswith("=") and "'BS Balances'!" in fc:
                                    orig_c = fc
                                    refs = list(_re_map.finditer(r"'BS Balances'!([A-Z]+):([A-Z]+)", fc))
                                    for ref in reversed(refs):
                                        old_idx = column_index_from_string(ref.group(1))
                                        if old_idx >= 2:
                                            new_letter = get_column_letter(old_idx + nc)
                                            fc = fc[:ref.start()] + f"'BS Balances'!{new_letter}:{new_letter}" + fc[ref.end():]
                                    # Also patch IS GL Detail refs in col C
                                    if fc != orig_c:
                                        cell_c.value = fc
                                        patched += 1

                                # Patch IS GL Detail refs in col C
                                if fc and isinstance(fc, str) and fc.startswith("=") and "'IS GL Detail'!" in fc:
                                    fc2 = cell_c.value or fc
                                    orig_c2 = fc2
                                    refs = list(_re_map.finditer(r"'IS GL Detail'!([A-Z]+):([A-Z]+)", fc2))
                                    for ref in reversed(refs):
                                        old_idx = column_index_from_string(ref.group(1))
                                        if old_idx > acct_col if 'acct_col' in dir() else old_idx >= 2:
                                            new_letter = get_column_letter(old_idx + nc)
                                            fc2 = fc2[:ref.start()] + f"'IS GL Detail'!{new_letter}:{new_letter}" + fc2[ref.end():]
                                    if fc2 != orig_c2:
                                        cell_c.value = fc2
                                        patched += 1

                            logger.info(f"Validation: patched {patched} formulas")

                        wb.save(file_path)
                        progress_fn("  Mapping columns appended.")

                except Exception as e:
                    import traceback
                    logger.error(f"Mapping error: {e}\n{traceback.format_exc()}")
                    progress_fn(f"  WARNING: Mapping failed — {e}")

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
    return {"jobs": get_user_jobs(str(user.id))}# rebuild
