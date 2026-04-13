import sys
from pathlib import Path
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
        tokens        = token_result.data[0]
        company_name  = tokens.get("company_name", "") or realm_id
        access_token  = tokens["access_token"]
        refresh_token = tokens["refresh_token"]
        logger.info(f"company_name from tokens: '{company_name}'")

        from datetime import datetime, timedelta
        import re as _re

        # Always refresh QBO token before running
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
                data={"grant_type": "refresh_token", "refresh_token": refresh_token},
                timeout=30,
            )
            if refresh_resp.status_code == 200:
                nt            = refresh_resp.json()
                access_token  = nt["access_token"]
                refresh_token = nt.get("refresh_token", refresh_token)
                new_expiry    = (datetime.utcnow() + timedelta(
                    seconds=nt.get("expires_in", 3600))).isoformat()
                supabase.table("qbo_tokens").update({
                    "access_token":  access_token,
                    "refresh_token": refresh_token,
                    "expires_at":    new_expiry,
                    "updated_at":    datetime.utcnow().isoformat(),
                }).eq("user_id", user_id).eq("realm_id", realm_id).execute()
                logger.info("QBO token refreshed successfully")
            else:
                logger.warning(f"QBO token refresh failed: {refresh_resp.status_code}")
        except Exception as _rfe:
            logger.warning(f"QBO token refresh error: {_rfe}")

        import qbo_client
        qbo_client.set_override_tokens({
            "realm_id":      realm_id,
            "access_token":  access_token,
            "refresh_token": refresh_token,
        })
        qbo_client.get_environment = lambda: "production"
        logger.info(f"Override tokens set for realm_id={realm_id}")

        clean_name = _re.sub(r'[^\w]', '_', company_name).strip('_').upper()
        file_name  = f"{clean_name}_{start_date[:7]}_{end_date[:7]}.xlsx"

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

            # ── Append mapping columns ────────────────────────────────
            if selected_maps:
                try:
                    logger.info(f"Applying maps: {selected_maps}")
                    mapping_result = supabase.table("mappings").select("account_maps").eq(
                        "user_id", user_id
                    ).eq("realm_id", realm_id).execute()
                    account_maps  = (mapping_result.data[0].get("account_maps", [])
                                     if mapping_result.data else [])
                    maps_to_apply = [m for m in account_maps
                                     if m.get("map_name", "") in selected_maps]

                    if maps_to_apply:
                        import openpyxl as _ox
                        from openpyxl.styles import Font, PatternFill, Alignment
                        from openpyxl.utils import get_column_letter
                        from openpyxl.formatting.rule import CellIsRule
                        from collections import defaultdict
                        from datetime import datetime as _dt
                        import calendar as _cal

                        wb = _ox.load_workbook(file_path)

                        for sn in wb.sheetnames:
                            wb[sn].sheet_view.showGridLines = False

                        # Shared styles
                        HDR_FONT    = Font(bold=True, color="FFFFFF")
                        HDR_FILL    = PatternFill("solid", fgColor="1F4E79")
                        GL_HDR_FONT = Font(bold=True)
                        GL_HDR_FILL = PatternFill(fill_type=None)
                        BOLD        = Font(bold=True)
                        BOLD_LG     = Font(bold=True, size=12)
                        SEC_FILL    = PatternFill("solid", fgColor="D9E1F2")
                        SUBTOT_FILL = PatternFill("solid", fgColor="EEF2F7")
                        NUM_FMT     = "#,##0.00"
                        RED_FONT    = Font(bold=True, color="9C0006")
                        RED_FILL    = PatternFill("solid", fgColor="FFC7CE")
                        GRN_FONT    = Font(bold=True, color="276221")
                        GRN_FILL    = PatternFill("solid", fgColor="C6EFCE")

                        def build_lookup(m):
                            out = {}
                            for grp in m.get("groups", []):
                                gname = grp.get("group_name", "")
                                sect  = grp.get("pl_section") or grp.get("bs_section") or ""
                                for a in grp.get("accounts", []):
                                    name = (a.get("account_name", "") if isinstance(a, dict)
                                            else str(a)).strip()
                                    if name:
                                        out[name] = (gname, sect)
                            return out

                        # ── Append to IS GL Detail, BS GL Detail, BS Balances ──
                        for tab in ("IS GL Detail", "BS GL Detail", "BS Balances"):
                            if tab not in wb.sheetnames:
                                continue
                            ws  = wb[tab]
                            hdr = [ws.cell(1, c).value for c in range(1, ws.max_column + 1)]
                            acct_col = None
                            for candidate in ("Account Name", "Account"):
                                if candidate in hdr:
                                    acct_col = hdr.index(candidate) + 1
                                    break
                            if not acct_col:
                                continue
                            base = ws.max_column + 1
                            for mi, m in enumerate(maps_to_apply):
                                lkp   = build_lookup(m)
                                gc    = base + mi * 2
                                sc    = base + mi * 2 + 1
                                mname = m.get("map_name", "")
                                ws.cell(1, gc, f"{mname} - Account Group").font = GL_HDR_FONT
                                ws.cell(1, gc).fill = GL_HDR_FILL
                                ws.cell(1, sc, f"{mname} - Statement Section").font = GL_HDR_FONT
                                ws.cell(1, sc).fill = GL_HDR_FILL
                                ws.column_dimensions[get_column_letter(gc)].width = 24
                                ws.column_dimensions[get_column_letter(sc)].width = 22
                                for ri in range(2, ws.max_row + 1):
                                    v = ws.cell(ri, acct_col).value
                                    if not v:
                                        continue
                                    match = lkp.get(str(v).strip())
                                    if match:
                                        ws.cell(ri, gc, match[0])
                                        ws.cell(ri, sc, match[1])

                        # ── Map Summary tab ────────────────────────────────
                        if "Map Summary" in wb.sheetnames:
                            del wb["Map Summary"]
                        ws_sum = wb.create_sheet("Map Summary")
                        ws_sum.sheet_view.showGridLines = False
                        cr = 1  # current row

                        for m in maps_to_apply:
                            map_name  = m.get("map_name", "")
                            grp_label = f"{map_name} - Account Group"
                            sec_label = f"{map_name} - Statement Section"

                            # ── IS Section ──────────────────────────────────
                            if "IS GL Detail" not in wb.sheetnames:
                                continue
                            ws_is  = wb["IS GL Detail"]
                            hdr_is = [ws_is.cell(1, c).value
                                      for c in range(1, ws_is.max_column + 1)]

                            if grp_label not in hdr_is:
                                logger.warning(f"'{grp_label}' not in IS GL Detail headers")
                                continue

                            is_grp_col_i  = hdr_is.index(grp_label) + 1
                            is_grp_col_l  = get_column_letter(is_grp_col_i)
                            is_mon_col_i  = (hdr_is.index("Month") + 1
                                             if "Month" in hdr_is else None)
                            is_mon_col_l  = get_column_letter(is_mon_col_i) if is_mon_col_i else None
                            is_amt_col_i  = (hdr_is.index("Amount") + 1
                                             if "Amount" in hdr_is else None)
                            is_amt_col_l  = get_column_letter(is_amt_col_i) if is_amt_col_i else None

                            if not is_mon_col_i or not is_amt_col_i:
                                continue

                            # Collect unique months
                            month_keys    = []
                            month_display = {}
                            month_dates   = {}
                            for ri in range(2, ws_is.max_row + 1):
                                mv = ws_is.cell(ri, is_mon_col_i).value
                                if mv is None:
                                    continue
                                if hasattr(mv, 'strftime'):
                                    mk = mv.strftime("%Y-%m")
                                    if mk not in month_dates:
                                        month_dates[mk] = mv
                                else:
                                    try:
                                        mk = _dt.strptime(str(mv)[:7], "%Y-%m").strftime("%Y-%m")
                                    except Exception:
                                        mk = str(mv)[:7]
                                if mk not in month_keys:
                                    month_keys.append(mk)
                                if mk not in month_display:
                                    try:
                                        month_display[mk] = _dt.strptime(mk, "%Y-%m").strftime("%b %Y")
                                    except Exception:
                                        month_display[mk] = mk
                            month_keys = sorted(month_keys)
                            num_mo     = len(month_keys)
                            tot_col    = num_mo + 3

                            # IS groups from mapping definition, sorted by section
                            IS_ORDER = ["Revenue", "COS", "Cost of Goods Sold",
                                        "Operating Expenses", "Other Income",
                                        "Other Expense", "Other"]
                            is_groups = []
                            seen_ig   = set()
                            for grp in m.get("groups", []):
                                gname = grp.get("group_name", "")
                                stmt  = grp.get("statement", "IS")
                                sec   = grp.get("pl_section", "") or grp.get("bs_section", "")
                                if gname and stmt == "IS" and gname not in seen_ig:
                                    seen_ig.add(gname)
                                    is_groups.append((gname, sec))

                            is_groups.sort(key=lambda g: IS_ORDER.index(g[1])
                                           if g[1] in IS_ORDER else 99)

                            # Title
                            ws_sum.cell(cr, 1,
                                f"{map_name} \u2014 Income Statement").font = BOLD_LG
                            cr += 1

                            # Header
                            ws_sum.cell(cr, 1, "Group").font = HDR_FONT
                            ws_sum.cell(cr, 1).fill = HDR_FILL
                            ws_sum.cell(cr, 2, "Section").font = HDR_FONT
                            ws_sum.cell(cr, 2).fill = HDR_FILL
                            for ci, mk in enumerate(month_keys, 3):
                                c = ws_sum.cell(cr, ci, month_display.get(mk, mk))
                                c.font = HDR_FONT
                                c.fill = HDR_FILL
                                c.alignment = Alignment(horizontal="center")
                            ws_sum.cell(cr, tot_col, "Total").font = HDR_FONT
                            ws_sum.cell(cr, tot_col).fill = HDR_FILL
                            cr += 1

                            # Data rows with SUMIFS
                            data_rows = []
                            for gname, sec in is_groups:
                                ws_sum.cell(cr, 1, gname)
                                ws_sum.cell(cr, 2, sec)
                                data_rows.append(cr)
                                for ci, mk in enumerate(month_keys, 3):
                                    mv = month_dates.get(mk)
                                    if mv and hasattr(mv, 'year'):
                                        date_f = f"DATE({mv.year},{mv.month},{mv.day})"
                                    else:
                                        try:
                                            d      = _dt.strptime(mk, "%Y-%m")
                                            last   = _cal.monthrange(d.year, d.month)[1]
                                            date_f = f"DATE({d.year},{d.month},{last})"
                                        except Exception:
                                            date_f = f'"{mk}"'
                                    f = (f"=SUMIFS('IS GL Detail'!${is_amt_col_l}:${is_amt_col_l},"
                                         f"'IS GL Detail'!${is_grp_col_l}:${is_grp_col_l},"
                                         f'"{gname}",'
                                         f"'IS GL Detail'!${is_mon_col_l}:${is_mon_col_l},"
                                         f"{date_f})")
                                    ws_sum.cell(cr, ci, f).number_format = NUM_FMT
                                # Row total
                                sl = get_column_letter(3)
                                el = get_column_letter(2 + num_mo)
                                ws_sum.cell(cr, tot_col,
                                    f"=SUM({sl}{cr}:{el}{cr})").number_format = NUM_FMT
                                cr += 1

                            # Net Income
                            ni_row = cr
                            ws_sum.cell(cr, 1, "Net Income").font = BOLD
                            for ci in range(3, tot_col + 1):
                                cl   = get_column_letter(ci)
                                refs = "+".join(f"{cl}{r}" for r in data_rows) if data_rows else "0"
                                c = ws_sum.cell(cr, ci, f"={refs}")
                                c.number_format = NUM_FMT
                                c.font = BOLD
                            cr += 1

                            # QBO Net Income from P&L
                            qbo_ni_row = cr
                            ws_sum.cell(cr, 1,
                                "Net Income \u2014 QBO P&L").font = Font(italic=True)
                            if "P&L" in wb.sheetnames:
                                ws_pl  = wb["P&L"]
                                pl_hdr = [ws_pl.cell(1, c).value
                                          for c in range(1, ws_pl.max_column + 1)]
                                for pri in range(2, ws_pl.max_row + 1):
                                    lbl = str(ws_pl.cell(pri, 1).value or "").strip().lower()
                                    if lbl in ("net income", "net earnings"):
                                        for ci, mk in enumerate(month_keys, 3):
                                            ml_s = month_display.get(mk, mk)
                                            for pci, ph in enumerate(pl_hdr):
                                                if str(ph or "").strip() == ml_s:
                                                    c = ws_sum.cell(cr, ci,
                                                        f"='P&L'!{get_column_letter(pci+1)}{pri}")
                                                    c.number_format = NUM_FMT
                                                    c.font = Font(italic=True)
                                                    break
                                        try:
                                            ptc = pl_hdr.index("Total") + 1
                                            c = ws_sum.cell(cr, tot_col,
                                                f"='P&L'!{get_column_letter(ptc)}{pri}")
                                            c.number_format = NUM_FMT
                                            c.font = Font(italic=True)
                                        except ValueError:
                                            pass
                                        break
                            cr += 1

                            # Difference row
                            diff_row = cr
                            ws_sum.cell(cr, 1, "Difference (should be zero)").font = BOLD
                            for ci in range(3, tot_col + 1):
                                cl = get_column_letter(ci)
                                ws_sum.cell(cr, ci,
                                    f"={cl}{ni_row}-{cl}{qbo_ni_row}").number_format = NUM_FMT
                            dr = f"C{diff_row}:{get_column_letter(tot_col)}{diff_row}"
                            ws_sum.conditional_formatting.add(dr,
                                CellIsRule(operator="notEqual", formula=["0"],
                                           font=RED_FONT, fill=RED_FILL))
                            ws_sum.conditional_formatting.add(dr,
                                CellIsRule(operator="equal", formula=["0"],
                                           font=GRN_FONT, fill=GRN_FILL))
                            cr += 2

                            # ── BS Section ──────────────────────────────────
                            if "BS Balances" not in wb.sheetnames:
                                continue
                            ws_bsb  = wb["BS Balances"]
                            hdr_bsb = [ws_bsb.cell(1, c).value
                                       for c in range(1, ws_bsb.max_column + 1)]

                            if grp_label not in hdr_bsb:
                                logger.warning(f"'{grp_label}' not in BS Balances headers")
                                continue

                            bs_grp_col_i = hdr_bsb.index(grp_label) + 1
                            bs_grp_col_l = get_column_letter(bs_grp_col_i)
                            bs_mon_col_i = (hdr_bsb.index("Month") + 1
                                            if "Month" in hdr_bsb else None)
                            bs_mon_col_l = get_column_letter(bs_mon_col_i) if bs_mon_col_i else None
                            bs_bal_col_i = (hdr_bsb.index("Ending Balance") + 1
                                            if "Ending Balance" in hdr_bsb else None)
                            bs_bal_col_l = get_column_letter(bs_bal_col_i) if bs_bal_col_i else None

                            if not bs_mon_col_i or not bs_bal_col_i:
                                logger.warning("Month or Ending Balance col missing in BS Balances")
                                continue

                            # Collect BS month-end dates
                            bs_month_keys    = []
                            bs_month_display = {}
                            bs_month_dates   = {}
                            for ri in range(2, ws_bsb.max_row + 1):
                                mv = ws_bsb.cell(ri, bs_mon_col_i).value
                                if mv is None:
                                    continue
                                if hasattr(mv, 'strftime'):
                                    mk   = mv.strftime("%Y-%m-%d")
                                    disp = mv.strftime("%b %Y")
                                    if mk not in bs_month_dates:
                                        bs_month_dates[mk] = mv
                                else:
                                    mk   = str(mv)
                                    disp = str(mv)
                                if mk not in bs_month_keys:
                                    bs_month_keys.append(mk)
                                if mk not in bs_month_display:
                                    bs_month_display[mk] = disp
                            bs_month_keys = sorted(bs_month_keys)
                            num_bs_mo     = len(bs_month_keys)

                            # BS groups from mapping definition
                            BS_ORDER  = ["Current Assets", "Fixed Assets", "Other Assets",
                                         "Current Liabilities", "Long-term Liabilities", "Equity"]
                            bs_groups = []
                            seen_bg   = set()
                            for grp in m.get("groups", []):
                                gname = grp.get("group_name", "")
                                stmt  = grp.get("statement", "")
                                sec   = grp.get("pl_section", "") or grp.get("bs_section", "")
                                if gname and stmt == "BS" and gname not in seen_bg:
                                    seen_bg.add(gname)
                                    bs_groups.append((gname, sec))

                            bs_groups.sort(key=lambda g: BS_ORDER.index(g[1])
                                           if g[1] in BS_ORDER else 99)

                            # Title
                            ws_sum.cell(cr, 1,
                                f"{map_name} \u2014 Balance Sheet").font = BOLD_LG
                            cr += 1

                            # BS header
                            ws_sum.cell(cr, 1, "Group").font = HDR_FONT
                            ws_sum.cell(cr, 1).fill = HDR_FILL
                            ws_sum.cell(cr, 2, "Section").font = HDR_FONT
                            ws_sum.cell(cr, 2).fill = HDR_FILL
                            for ci, mk in enumerate(bs_month_keys, 3):
                                c = ws_sum.cell(cr, ci, bs_month_display.get(mk, mk))
                                c.font = HDR_FONT
                                c.fill = HDR_FILL
                                c.alignment = Alignment(horizontal="center")
                            cr += 1

                            # BS data rows — grouped by section, SUMIFS formulas
                            cur_sec  = None
                            sec_rows = defaultdict(list)  # section → list of data row numbers

                            for gname, sec in bs_groups:
                                if sec != cur_sec:
                                    cur_sec = sec
                                    # Section header row
                                    c = ws_sum.cell(cr, 1, sec)
                                    c.font = BOLD
                                    c.fill = SEC_FILL
                                    for ci in range(2, num_bs_mo + 3):
                                        ws_sum.cell(cr, ci).fill = SEC_FILL
                                    cr += 1

                                sec_rows[sec].append(cr)
                                ws_sum.cell(cr, 1, f"  {gname}")
                                ws_sum.cell(cr, 2, sec)

                                for ci, mk in enumerate(bs_month_keys, 3):
                                    mv = bs_month_dates.get(mk)
                                    if mv and hasattr(mv, 'year'):
                                        date_f = f"DATE({mv.year},{mv.month},{mv.day})"
                                    else:
                                        try:
                                            d      = _dt.strptime(mk[:10], "%Y-%m-%d")
                                            date_f = f"DATE({d.year},{d.month},{d.day})"
                                        except Exception:
                                            date_f = f'"{mk}"'
                                    f = (f"=SUMIFS('BS Balances'!${bs_bal_col_l}:${bs_bal_col_l},"
                                         f"'BS Balances'!${bs_grp_col_l}:${bs_grp_col_l},"
                                         f'"{gname}",'
                                         f"'BS Balances'!${bs_mon_col_l}:${bs_mon_col_l},"
                                         f"{date_f})")
                                    ws_sum.cell(cr, ci, f).number_format = NUM_FMT
                                cr += 1

                            # Section subtotals (written after all groups)
                            for sec, rows in sec_rows.items():
                                if not rows:
                                    continue
                                c = ws_sum.cell(cr, 1, f"Total {sec}")
                                c.font = BOLD
                                c.fill = SUBTOT_FILL
                                ws_sum.cell(cr, 2).fill = SUBTOT_FILL
                                for ci in range(3, num_bs_mo + 3):
                                    cl   = get_column_letter(ci)
                                    refs = "+".join(f"{cl}{r}" for r in rows)
                                    c = ws_sum.cell(cr, ci, f"={refs}")
                                    c.number_format = NUM_FMT
                                    c.font = BOLD
                                    c.fill = SUBTOT_FILL
                                cr += 1

                            cr += 1  # spacer between maps

                        # Column widths for Map Summary
                        ws_sum.column_dimensions["A"].width = 28
                        ws_sum.column_dimensions["B"].width = 20
                        for ci in range(3, ws_sum.max_column + 1):
                            ws_sum.column_dimensions[get_column_letter(ci)].width = 14
                        ws_sum.freeze_panes = "C2"

                        wb.save(file_path)
                        progress_fn("  Mapping columns and Map Summary written.")

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
    return {"jobs": get_user_jobs(str(user.id))}