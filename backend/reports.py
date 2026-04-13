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
        from datetime import datetime, timedelta
        import re as _re

        # Always refresh QBO token before running — don't rely on expires_at
        try:
            import base64, httpx as _httpx
            client_id     = os.getenv("QBO_CLIENT_ID", "")
            client_secret = os.getenv("QBO_CLIENT_SECRET", "")
            credentials   = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
            refresh_resp  = _httpx.post(
                "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
                headers={
                    "Authorization": f"Basic {credentials}",
                    "Content-Type":  "application/x-www-form-urlencoded",
                    "Accept":        "application/json",
                },
                data={
                    "grant_type":    "refresh_token",
                    "refresh_token": refresh_token,
                },
                timeout=30,
            )
            if refresh_resp.status_code == 200:
                new_tokens    = refresh_resp.json()
                access_token  = new_tokens["access_token"]
                refresh_token = new_tokens.get("refresh_token", refresh_token)
                new_expiry    = (datetime.utcnow() + timedelta(
                    seconds=new_tokens.get("expires_in", 3600)
                )).isoformat()
                supabase.table("qbo_tokens").update({
                    "access_token":  access_token,
                    "refresh_token": refresh_token,
                    "expires_at":    new_expiry,
                    "updated_at":    datetime.utcnow().isoformat(),
                }).eq("user_id", user_id).eq("realm_id", realm_id).execute()
                logger.info(f"QBO token refreshed successfully")
            else:
                logger.warning(f"QBO token refresh failed: {refresh_resp.status_code} — proceeding with existing token")
        except Exception as _re_err:
            logger.warning(f"QBO token refresh error: {_re_err} — proceeding with existing token")

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
                    account_maps = (mapping_result.data[0].get("account_maps", []) if mapping_result.data else [])
                    maps_to_apply = [m for m in account_maps if m.get("map_name", "") in selected_maps]
                    if maps_to_apply:
                        import openpyxl as _ox
                        from openpyxl.styles import Font, PatternFill, Alignment
                        from openpyxl.utils import get_column_letter
                        from openpyxl.formatting.rule import CellIsRule
                        from collections import defaultdict
                        from copy import copy as _copy

                        wb = _ox.load_workbook(file_path)
                        for sn in wb.sheetnames:
                            wb[sn].sheet_view.showGridLines = False

                        HDR_FONT = Font(bold=True, color="FFFFFF")
                        HDR_FILL = PatternFill("solid", fgColor="1F4E79")
                        GL_HDR_FONT = Font(bold=True)
                        GL_HDR_FILL = PatternFill(fill_type=None)

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

                        n = len(maps_to_apply)

                        # ── ALL three tabs — append at end ──────────────────
                        for tab in ("IS GL Detail", "BS GL Detail", "BS Balances"):
                            if tab not in wb.sheetnames:
                                continue
                            ws = wb[tab]
                            hdr = [ws.cell(1, c).value for c in range(1, ws.max_column+1)]
                            acct_col = None
                            for candidate in ("Account Name", "Account"):
                                if candidate in hdr:
                                    acct_col = hdr.index(candidate) + 1
                                    break
                            if not acct_col:
                                continue
                            base = ws.max_column + 1

                            for mi, m in enumerate(maps_to_apply):
                                lkp  = build_lookup(m)
                                gc   = base + mi*2
                                sc   = base + mi*2 + 1
                                mname = m.get("map_name","")

                                ws.cell(1, gc, f"{mname} - Account Group").font = GL_HDR_FONT
                                ws.cell(1, gc).fill = GL_HDR_FILL
                                ws.cell(1, sc, f"{mname} - Statement Section").font = GL_HDR_FONT
                                ws.cell(1, sc).fill = GL_HDR_FILL
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

                        # ── Map Summary tab ─────────────────────────────────
                        def write_map_summary(wb, maps_to_apply):
                            NUM_FMT = "#,##0.00"
                            BOLD = Font(bold=True)
                            BOLD_LARGE = Font(bold=True, size=12)
                            SEC_FILL = PatternFill("solid", fgColor="D9E1F2")
                            SUBTOT_FILL = PatternFill("solid", fgColor="EEF2F7")
                            RED_FONT = Font(bold=True, color="9C0006")
                            RED_FILL = PatternFill("solid", fgColor="FFC7CE")
                            GREEN_FONT = Font(bold=True, color="276221")
                            GREEN_FILL = PatternFill("solid", fgColor="C6EFCE")

                            if "Map Summary" in wb.sheetnames:
                                del wb["Map Summary"]
                            ws = wb.create_sheet("Map Summary")
                            ws.sheet_view.showGridLines = False
                            cr = 1

                            for m in maps_to_apply:
                                map_name = m.get("map_name", "")

                                # ── IS Section ──────────────────────────────
                                if "IS GL Detail" not in wb.sheetnames:
                                    continue
                                ws_is = wb["IS GL Detail"]
                                hdr_is = [ws_is.cell(1, c).value for c in range(1, ws_is.max_column+1)]
                                grp_label = f"{map_name} - Account Group"
                                sec_label = f"{map_name} - Statement Section"
                                if grp_label not in hdr_is:
                                    continue
                                grp_col = hdr_is.index(grp_label) + 1
                                sec_col = hdr_is.index(sec_label) + 1 if sec_label in hdr_is else None
                                month_col = (hdr_is.index("Month") + 1) if "Month" in hdr_is else None
                                amount_col = (hdr_is.index("Amount") + 1) if "Amount" in hdr_is else None
                                if not month_col or not amount_col:
                                    continue

                                # Collect months — normalize to YYYY-MM string keys
                                from datetime import datetime as _dt2
                                month_set = []
                                _month_display = {}  # key → display label
                                for ri in range(2, ws_is.max_row+1):
                                    mv = ws_is.cell(ri, month_col).value
                                    if mv is None:
                                        continue
                                    if hasattr(mv, 'strftime'):
                                        ml = mv.strftime("%Y-%m")
                                    else:
                                        try: ml = _dt2.strptime(str(mv)[:7], "%Y-%m").strftime("%Y-%m")
                                        except: ml = str(mv)[:7]
                                    if ml not in month_set:
                                        month_set.append(ml)
                                    if ml not in _month_display:
                                        try: _month_display[ml] = _dt2.strptime(ml, "%Y-%m").strftime("%b %Y")
                                        except: _month_display[ml] = ml
                                month_labels = sorted(month_set)

                                # Aggregate IS — use normalized month keys
                                is_agg = defaultdict(lambda: defaultdict(float))
                                is_group_sec = {}
                                for ri in range(2, ws_is.max_row+1):
                                    grp = ws_is.cell(ri, grp_col).value
                                    if not grp:
                                        continue
                                    mv = ws_is.cell(ri, month_col).value
                                    if mv is None:
                                        continue
                                    if hasattr(mv, 'strftime'):
                                        ml = mv.strftime("%Y-%m")
                                    else:
                                        try: ml = _dt2.strptime(str(mv)[:7], "%Y-%m").strftime("%Y-%m")
                                        except: ml = str(mv)[:7]
                                    amt = float(ws_is.cell(ri, amount_col).value or 0)
                                    is_agg[grp][ml] += amt
                                    if sec_col and grp not in is_group_sec:
                                        is_group_sec[grp] = ws_is.cell(ri, sec_col).value or ""

                                IS_ORDER = ["Revenue","COS","Cost of Goods Sold","Operating Expenses","Other","Other Income","Other Expense"]
                                def is_sort(g):
                                    s = is_group_sec.get(g, "")
                                    try: return IS_ORDER.index(s)
                                    except ValueError: return 99
                                sorted_is = sorted(is_agg.keys(), key=is_sort)

                                # Write IS
                                ws.cell(cr, 1, f"{map_name} \u2014 Income Statement").font = BOLD_LARGE
                                cr += 1
                                ws.cell(cr, 1, "Group").font = HDR_FONT; ws.cell(cr, 1).fill = HDR_FILL
                                ws.cell(cr, 2, "Section").font = HDR_FONT; ws.cell(cr, 2).fill = HDR_FILL
                                for ci, ml in enumerate(month_labels, 3):
                                    c = ws.cell(cr, ci, _month_display.get(ml, ml))
                                    c.font = HDR_FONT; c.fill = HDR_FILL; c.alignment = Alignment(horizontal="center")
                                tc = len(month_labels) + 3
                                ws.cell(cr, tc, "Total").font = HDR_FONT; ws.cell(cr, tc).fill = HDR_FILL
                                cr += 1

                                ni = defaultdict(float)
                                for grp in sorted_is:
                                    ws.cell(cr, 1, grp)
                                    ws.cell(cr, 2, is_group_sec.get(grp, ""))
                                    rt = 0.0
                                    for ci, ml in enumerate(month_labels, 3):
                                        v = is_agg[grp].get(ml, 0.0)
                                        ws.cell(cr, ci, v).number_format = NUM_FMT
                                        rt += v; ni[ml] += v
                                    ws.cell(cr, tc, rt).number_format = NUM_FMT
                                    cr += 1

                                ni_row = cr
                                ws.cell(cr, 1, "Net Income").font = BOLD
                                for ci, ml in enumerate(month_labels, 3):
                                    c = ws.cell(cr, ci, ni[ml]); c.number_format = NUM_FMT; c.font = BOLD
                                ws.cell(cr, tc, sum(ni.values())).number_format = NUM_FMT
                                ws.cell(cr, tc).font = BOLD
                                cr += 1

                                qbo_row = cr
                                ws.cell(cr, 1, "Net Income \u2014 QBO").font = Font(italic=True)
                                if "P&L" in wb.sheetnames:
                                    ws_pl = wb["P&L"]
                                    pl_hdr = [ws_pl.cell(1, c).value for c in range(1, ws_pl.max_column+1)]
                                    for pri in range(2, ws_pl.max_row+1):
                                        lbl = str(ws_pl.cell(pri, 1).value or "").strip().lower()
                                        if lbl in ("net income", "net earnings"):
                                            for ci, ml in enumerate(month_labels, 3):
                                                ml_s = _month_display.get(ml, ml)
                                                for pci, ph in enumerate(pl_hdr):
                                                    if str(ph or "") == ml_s or ml_s in str(ph or ""):
                                                        pv = ws_pl.cell(pri, pci+1).value
                                                        c = ws.cell(cr, ci, float(pv or 0))
                                                        c.number_format = NUM_FMT; c.font = Font(italic=True)
                                                        break
                                            break
                                cr += 1

                                diff_row = cr
                                ws.cell(cr, 1, "Difference (should be zero)").font = BOLD
                                for ci in range(3, tc+1):
                                    cl = get_column_letter(ci)
                                    ws.cell(cr, ci, f"={cl}{ni_row}-{cl}{qbo_row}").number_format = NUM_FMT
                                dr = f"C{diff_row}:{get_column_letter(tc)}{diff_row}"
                                ws.conditional_formatting.add(dr, CellIsRule(operator="notEqual", formula=["0"], font=RED_FONT, fill=RED_FILL))
                                ws.conditional_formatting.add(dr, CellIsRule(operator="equal", formula=["0"], font=GREEN_FONT, fill=GREEN_FILL))
                                cr += 2

                                # ── BS Section ──────────────────────────────
                                if "BS Balances" not in wb.sheetnames:
                                    continue
                                ws_bsb = wb["BS Balances"]
                                hdr_bsb = [ws_bsb.cell(1, c).value for c in range(1, ws_bsb.max_column+1)]
                                if grp_label not in hdr_bsb:
                                    continue
                                bs_gc = hdr_bsb.index(grp_label) + 1
                                bs_sc = hdr_bsb.index(sec_label) + 1 if sec_label in hdr_bsb else None
                                bs_dc = (hdr_bsb.index("Date") + 1) if "Date" in hdr_bsb else None
                                bs_bc = (hdr_bsb.index("Ending Balance") + 1) if "Ending Balance" in hdr_bsb else None
                                if not bs_dc or not bs_bc:
                                    continue

                                bs_months = []
                                _bs_display = {}
                                for ri in range(2, ws_bsb.max_row+1):
                                    dv = ws_bsb.cell(ri, bs_dc).value
                                    if dv is None:
                                        continue
                                    ml = dv.strftime("%Y-%m-%d") if hasattr(dv, 'strftime') else str(dv)
                                    if ml not in bs_months:
                                        bs_months.append(ml)
                                    if ml not in _bs_display:
                                        _bs_display[ml] = dv.strftime("%b %d, %Y") if hasattr(dv, 'strftime') else str(dv)

                                bs_agg = defaultdict(lambda: defaultdict(float))
                                bs_gs = {}
                                for ri in range(2, ws_bsb.max_row+1):
                                    grp = ws_bsb.cell(ri, bs_gc).value
                                    if not grp: continue
                                    dv = ws_bsb.cell(ri, bs_dc).value
                                    if dv is None: continue
                                    ml = dv.strftime("%Y-%m-%d") if hasattr(dv, 'strftime') else str(dv)
                                    bal = float(ws_bsb.cell(ri, bs_bc).value or 0)
                                    bs_agg[grp][ml] += bal
                                    if bs_sc and grp not in bs_gs:
                                        bs_gs[grp] = ws_bsb.cell(ri, bs_sc).value or ""

                                BS_ORDER = ["Current Assets","Fixed Assets","Other Assets","Current Liabilities","Long-term Liabilities","Equity"]
                                def bs_sort(g):
                                    s = bs_gs.get(g, "")
                                    try: return BS_ORDER.index(s)
                                    except ValueError: return 99
                                sorted_bs = sorted(bs_agg.keys(), key=bs_sort)

                                ws.cell(cr, 1, f"{map_name} \u2014 Balance Sheet").font = BOLD_LARGE
                                cr += 1
                                ws.cell(cr, 1, "Group").font = HDR_FONT; ws.cell(cr, 1).fill = HDR_FILL
                                ws.cell(cr, 2, "Section").font = HDR_FONT; ws.cell(cr, 2).fill = HDR_FILL
                                for ci, ml in enumerate(bs_months, 3):
                                    label = _bs_display.get(ml, ml)
                                    c = ws.cell(cr, ci, label)
                                    c.font = HDR_FONT; c.fill = HDR_FILL; c.alignment = Alignment(horizontal="center")
                                cr += 1

                                cur_sec = None
                                sec_tots = defaultdict(lambda: defaultdict(float))
                                for grp in sorted_bs:
                                    sec = bs_gs.get(grp, "")
                                    if sec != cur_sec:
                                        if cur_sec is not None:
                                            ws.cell(cr, 1, f"Total {cur_sec}").font = BOLD
                                            ws.cell(cr, 1).fill = SUBTOT_FILL
                                            for ci, ml in enumerate(bs_months, 3):
                                                c = ws.cell(cr, ci, sec_tots[cur_sec][ml])
                                                c.number_format = NUM_FMT; c.font = BOLD; c.fill = SUBTOT_FILL
                                            cr += 1
                                        cur_sec = sec
                                        ws.cell(cr, 1, sec).font = BOLD; ws.cell(cr, 1).fill = SEC_FILL
                                        for ci in range(2, len(bs_months)+3): ws.cell(cr, ci).fill = SEC_FILL
                                        cr += 1
                                    ws.cell(cr, 1, f"  {grp}"); ws.cell(cr, 2, sec)
                                    for ci, ml in enumerate(bs_months, 3):
                                        v = bs_agg[grp].get(ml, 0.0)
                                        ws.cell(cr, ci, v).number_format = NUM_FMT
                                        sec_tots[sec][ml] += v
                                    cr += 1
                                if cur_sec:
                                    ws.cell(cr, 1, f"Total {cur_sec}").font = BOLD; ws.cell(cr, 1).fill = SUBTOT_FILL
                                    for ci, ml in enumerate(bs_months, 3):
                                        c = ws.cell(cr, ci, sec_tots[cur_sec][ml])
                                        c.number_format = NUM_FMT; c.font = BOLD; c.fill = SUBTOT_FILL
                                    cr += 2

                            ws.column_dimensions["A"].width = 28; ws.column_dimensions["B"].width = 20
                            for ci in range(3, ws.max_column+1):
                                ws.column_dimensions[get_column_letter(ci)].width = 14
                            ws.freeze_panes = "C2"

                        write_map_summary(wb, maps_to_apply)
                        wb.save(file_path)
                        progress_fn("  Mapping columns appended.")

                except Exception as e:
                    import traceback
                    logger.error(f"Mapping error: {e}\n{traceback.format_exc()}")
                    progress_fn(f"  WARNING: Mapping failed \u2014 {e}")

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
