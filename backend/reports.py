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
                        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
                        from openpyxl.utils import get_column_letter, column_index_from_string
                        from openpyxl.formatting.rule import CellIsRule
                        import re as _re_map
                        from copy import copy as _copy
                        from datetime import datetime as _dt
                        import calendar as _cal

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

                        n = len(maps_to_apply)
                        nc = n * 2

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

                        # ── BS Balances only — insert after col A ───────────
                        for tab in ("BS Balances",):
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

                        # ── Patch Validation col C — BS Balances refs only ──
                        if "Validation" in wb.sheetnames:
                            ws_v = wb["Validation"]
                            patched = 0
                            for ri in range(1, ws_v.max_row+1):
                                cell_c = ws_v.cell(ri, 3)
                                fc = cell_c.value
                                if not fc or not isinstance(fc, str) or not fc.startswith("="):
                                    continue
                                orig_c = fc
                                if "'BS Balances'!" in fc:
                                    refs = list(_re_map.finditer(r"'BS Balances'!([A-Z]+):([A-Z]+)", fc))
                                    for ref in reversed(refs):
                                        old_idx = column_index_from_string(ref.group(1))
                                        if old_idx >= 2:
                                            new_letter = get_column_letter(old_idx + nc)
                                            fc = fc[:ref.start()] + f"'BS Balances'!{new_letter}:{new_letter}" + fc[ref.end():]
                                if "'IS GL Detail'!" in fc:
                                    refs = list(_re_map.finditer(r"'IS GL Detail'!([A-Z]+):([A-Z]+)", fc))
                                    for ref in reversed(refs):
                                        old_idx = column_index_from_string(ref.group(1))
                                        if old_idx >= 6:
                                            new_letter = get_column_letter(old_idx + nc)
                                            fc = fc[:ref.start()] + f"'IS GL Detail'!{new_letter}:{new_letter}" + fc[ref.end():]
                                if fc != orig_c:
                                    cell_c.value = fc
                                    patched += 1
                            logger.info(f"Validation: patched {patched} formulas")

                        # ── Map Summary tab ─────────────────────────────────
                        def write_map_summary(wb, maps_to_apply):
                            NUM_FMT = "#,##0.00"
                            BOLD = Font(bold=True)
                            SEC_FILL = PatternFill("solid", fgColor="D9E1F2")
                            SUBTOT_FILL = PatternFill("solid", fgColor="EEF2F7")
                            RED_FONT = Font(bold=True, color="9C0006")
                            RED_FILL = PatternFill("solid", fgColor="FFC7CE")
                            GREEN_FONT = Font(bold=True, color="276221")
                            GREEN_FILL = PatternFill("solid", fgColor="C6EFCE")

                            ws = wb.create_sheet("Map Summary")
                            ws.sheet_view.showGridLines = False

                            # Get month labels from P&L
                            month_labels = []
                            if "P&L" in wb.sheetnames:
                                ws_pl = wb["P&L"]
                                for ci in range(2, ws_pl.max_column + 1):
                                    v = ws_pl.cell(1, ci).value
                                    if v and str(v) not in ("Total", ""):
                                        month_labels.append(str(v))
                            if not month_labels:
                                ws.cell(1, 1, "No P&L data available")
                                return

                            # Build IS data from IS GL Detail
                            is_data = {}
                            is_order = []
                            if "IS GL Detail" in wb.sheetnames:
                                ws_is = wb["IS GL Detail"]
                                hdr = [ws_is.cell(1, c).value for c in range(1, ws_is.max_column+1)]
                                month_col = (hdr.index("Month") + 1) if "Month" in hdr else None
                                amount_col = (hdr.index("Amount") + 1) if "Amount" in hdr else None
                                mgc = {}
                                for ci, h in enumerate(hdr):
                                    if h and str(h).endswith(" - Account Group"):
                                        mn = str(h).replace(" - Account Group", "")
                                        sl = f"{mn} - Statement Section"
                                        if sl in hdr:
                                            mgc[mn] = (ci+1, hdr.index(sl)+1)
                                if month_col and amount_col and mgc:
                                    for ri in range(2, ws_is.max_row+1):
                                        mv = ws_is.cell(ri, month_col).value
                                        av = ws_is.cell(ri, amount_col).value
                                        if not mv or not av:
                                            continue
                                        if hasattr(mv, 'strftime'):
                                            ml = mv.strftime("%b %Y")
                                        else:
                                            try: ml = _dt.strptime(str(mv)[:7], "%Y-%m").strftime("%b %Y")
                                            except: ml = str(mv)
                                        if ml not in month_labels:
                                            continue
                                        amt = float(av or 0)
                                        for mn, (gc, sc) in mgc.items():
                                            grp = ws_is.cell(ri, gc).value or ""
                                            sec = ws_is.cell(ri, sc).value or ""
                                            if not grp:
                                                continue
                                            key = (mn, grp, sec)
                                            if key not in is_data:
                                                is_data[key] = {m: 0.0 for m in month_labels}
                                                is_order.append(key)
                                            is_data[key][ml] = is_data[key].get(ml, 0.0) + amt

                            # Build BS data from BS Balances
                            bs_data = {}
                            bs_order = []
                            bs_months = []
                            if "BS Balances" in wb.sheetnames:
                                ws_bs = wb["BS Balances"]
                                hdr_bs = [ws_bs.cell(1, c).value for c in range(1, ws_bs.max_column+1)]
                                date_col = None
                                bal_col = None
                                for ci, h in enumerate(hdr_bs):
                                    if h == "Date": date_col = ci + 1
                                    if h == "Ending Balance": bal_col = ci + 1
                                mgc_bs = {}
                                for ci, h in enumerate(hdr_bs):
                                    if h and str(h).endswith(" - Account Group"):
                                        mn = str(h).replace(" - Account Group", "")
                                        sl = f"{mn} - Statement Section"
                                        if sl in hdr_bs:
                                            mgc_bs[mn] = (ci+1, hdr_bs.index(sl)+1)
                                if date_col and bal_col and mgc_bs:
                                    seen = []
                                    for ri in range(2, ws_bs.max_row+1):
                                        dv = ws_bs.cell(ri, date_col).value
                                        if dv:
                                            if hasattr(dv, 'strftime'):
                                                ml = dv.strftime("%b %Y")
                                            else:
                                                ml = str(dv)
                                            if ml not in seen:
                                                seen.append(ml)
                                    bs_months = seen
                                    for ri in range(2, ws_bs.max_row+1):
                                        dv = ws_bs.cell(ri, date_col).value
                                        bv = ws_bs.cell(ri, bal_col).value
                                        if not dv:
                                            continue
                                        if hasattr(dv, 'strftime'):
                                            ml = dv.strftime("%b %Y")
                                        else:
                                            ml = str(dv)
                                        bal = float(bv or 0)
                                        for mn, (gc, sc) in mgc_bs.items():
                                            grp = ws_bs.cell(ri, gc).value or ""
                                            sec = ws_bs.cell(ri, sc).value or ""
                                            if not grp:
                                                continue
                                            key = (mn, grp, sec)
                                            if key not in bs_data:
                                                bs_data[key] = {m: 0.0 for m in bs_months}
                                                bs_order.append(key)
                                            bs_data[key][ml] = bal

                            # Write summary
                            cr = 1
                            for mname in [m.get("map_name","") for m in maps_to_apply]:
                                # IS section
                                ws.cell(cr, 1, f"{mname} \u2014 Income Statement").font = Font(bold=True, size=13)
                                cr += 1
                                ws.cell(cr, 1, "Account Group").font = HDR_FONT
                                ws.cell(cr, 1).fill = HDR_FILL
                                ws.cell(cr, 2, "Section").font = HDR_FONT
                                ws.cell(cr, 2).fill = HDR_FILL
                                for ci, ml in enumerate(month_labels, 3):
                                    c = ws.cell(cr, ci, ml)
                                    c.font = HDR_FONT; c.fill = HDR_FILL
                                    c.alignment = Alignment(horizontal="center")
                                tc = len(month_labels) + 3
                                ws.cell(cr, tc, "Total").font = HDR_FONT
                                ws.cell(cr, tc).fill = HDR_FILL
                                cr += 1

                                ni_map = {ml: 0.0 for ml in month_labels}
                                for key in [k for k in is_order if k[0] == mname]:
                                    monthly = is_data[key]
                                    ws.cell(cr, 1, key[1])
                                    ws.cell(cr, 2, key[2])
                                    rt = 0.0
                                    for ci, ml in enumerate(month_labels, 3):
                                        v = monthly.get(ml, 0.0)
                                        ws.cell(cr, ci, v).number_format = NUM_FMT
                                        rt += v; ni_map[ml] = ni_map.get(ml, 0.0) + v
                                    ws.cell(cr, tc, rt).number_format = NUM_FMT
                                    cr += 1

                                ni_row = cr
                                ws.cell(cr, 1, "Net Income").font = BOLD
                                for ci, ml in enumerate(month_labels, 3):
                                    c = ws.cell(cr, ci, ni_map.get(ml, 0.0))
                                    c.number_format = NUM_FMT; c.font = BOLD
                                ws.cell(cr, tc, sum(ni_map.values())).number_format = NUM_FMT
                                ws.cell(cr, tc).font = BOLD
                                cr += 1

                                qbo_row = cr
                                ws.cell(cr, 1, "Net Income \u2014 QBO P&L").font = Font(italic=True)
                                if "P&L" in wb.sheetnames:
                                    ws_pl = wb["P&L"]
                                    pl_hdr = [ws_pl.cell(1, c).value for c in range(1, ws_pl.max_column+1)]
                                    for pri in range(2, ws_pl.max_row+1):
                                        lbl = str(ws_pl.cell(pri, 1).value or "").strip().lower()
                                        if lbl in ("net income", "net earnings"):
                                            for ci, ml in enumerate(month_labels, 3):
                                                try:
                                                    pc = pl_hdr.index(ml) + 1
                                                    pv = ws_pl.cell(pri, pc).value
                                                    c = ws.cell(cr, ci, float(pv or 0))
                                                    c.number_format = NUM_FMT; c.font = Font(italic=True)
                                                except (ValueError, TypeError): pass
                                            break
                                cr += 1

                                diff_row = cr
                                ws.cell(cr, 1, "Difference (should be zero)").font = BOLD
                                for ci in range(3, tc+1):
                                    cl = get_column_letter(ci)
                                    ws.cell(cr, ci, f"={cl}{ni_row}-{cl}{qbo_row}").number_format = NUM_FMT
                                cr += 2

                                # Conditional formatting on diff row
                                dr = f"C{diff_row}:{get_column_letter(tc)}{diff_row}"
                                ws.conditional_formatting.add(dr, CellIsRule(operator="notEqual", formula=["0"], font=RED_FONT, fill=RED_FILL))
                                ws.conditional_formatting.add(dr, CellIsRule(operator="equal", formula=["0"], font=GREEN_FONT, fill=GREEN_FILL))

                                # BS section
                                if bs_months:
                                    ws.cell(cr, 1, f"{mname} \u2014 Balance Sheet").font = Font(bold=True, size=13)
                                    cr += 1
                                    ws.cell(cr, 1, "Account Group").font = HDR_FONT
                                    ws.cell(cr, 1).fill = HDR_FILL
                                    ws.cell(cr, 2, "Section").font = HDR_FONT
                                    ws.cell(cr, 2).fill = HDR_FILL
                                    for ci, ml in enumerate(bs_months, 3):
                                        c = ws.cell(cr, ci, ml)
                                        c.font = HDR_FONT; c.fill = HDR_FILL; c.alignment = Alignment(horizontal="center")
                                    cr += 1

                                    sec_groups = {}
                                    for key in [k for k in bs_order if k[0] == mname]:
                                        sec = key[2]
                                        if sec not in sec_groups: sec_groups[sec] = []
                                        sec_groups[sec].append((key[1], bs_data[key]))

                                    for sec, groups in sec_groups.items():
                                        ws.cell(cr, 1, sec).font = BOLD
                                        ws.cell(cr, 1).fill = SEC_FILL
                                        for ci in range(2, len(bs_months)+3): ws.cell(cr, ci).fill = SEC_FILL
                                        cr += 1
                                        st = {ml: 0.0 for ml in bs_months}
                                        for grp, monthly in groups:
                                            ws.cell(cr, 1, f"  {grp}")
                                            for ci, ml in enumerate(bs_months, 3):
                                                v = monthly.get(ml, 0.0)
                                                ws.cell(cr, ci, v).number_format = NUM_FMT
                                                st[ml] = st.get(ml, 0.0) + v
                                            cr += 1
                                        ws.cell(cr, 1, f"Total {sec}").font = BOLD
                                        ws.cell(cr, 1).fill = SUBTOT_FILL
                                        for ci, ml in enumerate(bs_months, 3):
                                            c = ws.cell(cr, ci, st.get(ml, 0.0))
                                            c.number_format = NUM_FMT; c.font = BOLD; c.fill = SUBTOT_FILL
                                        cr += 1
                                    cr += 1

                            ws.column_dimensions["A"].width = 30
                            ws.column_dimensions["B"].width = 20
                            for ci in range(3, ws.max_column+1):
                                ws.column_dimensions[get_column_letter(ci)].width = 14
                            ws.freeze_panes = "C2"

                        write_map_summary(wb, maps_to_apply)
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
