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
    realm_id:          str
    start_date:        str
    end_date:          str
    dimension:         str = "none"
    selected_maps:      list[str] = []
    include_gl_detail:  bool = False
    include_portal_data: bool = False

def run_report_job(job_id: str, user_id: str, realm_id: str,
                   start_date: str, end_date: str, dimension: str,
                   selected_maps: list[str] | None = None,
                   include_gl_detail: bool = False,
                   include_portal_data: bool = False):
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
                include_gl_detail=include_gl_detail,
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
                        HDR_FONT    = Font(name="Arial", size=10, bold=True, color="FFFFFF")
                        HDR_FILL    = PatternFill("solid", fgColor="337E8D")
                        GL_HDR_FONT = Font(name="Arial", size=10, bold=True, color="FFFFFF")
                        GL_HDR_FILL = PatternFill("solid", fgColor="337E8D")
                        BOLD        = Font(name="Arial", size=10, bold=True)
                        BOLD_LG     = Font(name="Arial", size=10, bold=True)
                        SEC_FILL    = PatternFill("solid", fgColor="D9E1F2")
                        SUBTOT_FILL = PatternFill("solid", fgColor="EEF2F7")
                        NUM_FMT     = '#,##0.00_);(#,##0.00);"-"??;@'
                        RED_FONT    = Font(name="Arial", size=10, bold=True)
                        RED_FILL    = PatternFill("solid", fgColor="FFC7CE")
                        GRN_FONT    = Font(name="Arial", size=10, bold=True)
                        GRN_FILL    = PatternFill("solid", fgColor="C6EFCE")
                        LINK_FONT   = Font(name="Arial", size=10, color="276221")

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
                        for tab in ("IS GL Summary", "BS GL Summary", "IS GL Detail", "BS GL Detail", "BS Balances"):
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
                            if "IS GL Summary" not in wb.sheetnames:
                                continue
                            ws_is  = wb["IS GL Summary"]
                            hdr_is = [ws_is.cell(1, c).value
                                      for c in range(1, ws_is.max_column + 1)]

                            # Resolve IS GL Summary columns dynamically
                            def _cl(hdrs, name):
                                for i, h in enumerate(hdrs):
                                    if str(h or "").strip() == name:
                                        return (i + 1, get_column_letter(i + 1))
                                return (None, None)

                            is_amt_col_i, is_amt_col_l = _cl(hdr_is, "Amount")
                            is_mon_col_i, is_mon_col_l = _cl(hdr_is, "Month")
                            _, is_sec_col_l = _cl(hdr_is, sec_label)
                            _, is_grp_col_l = _cl(hdr_is, grp_label)

                            if not is_mon_col_i or not is_amt_col_i or not is_sec_col_l:
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
                            tot_col    = num_mo + 2  # months start at col 2

                            # IS groups from mapping definition, sorted by section
                            IS_ORDER = ["Revenue", "COS", "Cost of Goods Sold",
                                        "Sales & Marketing", "Operating Expenses",
                                        "Other Income", "Other Expense", "Other"]
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

                            # Header — col A blank, then month labels, then Total
                            ws_sum.cell(cr, 1, "").font = HDR_FONT
                            ws_sum.cell(cr, 1).fill = HDR_FILL
                            for ci, mk in enumerate(month_keys, 2):
                                c = ws_sum.cell(cr, ci, month_display.get(mk, mk))
                                c.font = HDR_FONT
                                c.fill = HDR_FILL
                                c.alignment = Alignment(horizontal="center")
                            ws_sum.cell(cr, tot_col, "Total").font = HDR_FONT
                            ws_sum.cell(cr, tot_col).fill = HDR_FILL
                            cr += 1

                            # Aggregate by section — one row per P&L section
                            INCOME_SECS  = {"Revenue", "Other Income"}
                            EXPENSE_SECS = {"COS", "Cost of Goods Sold", "Sales & Marketing", "Operating Expenses", "Other Expense", "Other"}

                            section_order = []
                            seen_secs = set()
                            for gname, sec in is_groups:
                                if sec and sec not in seen_secs:
                                    seen_secs.add(sec)
                                    section_order.append(sec)
                            section_order.sort(key=lambda s: IS_ORDER.index(s) if s in IS_ORDER else 99)

                            data_rows = []  # [(row_number, section_name)]
                            for sec in section_order:
                                ws_sum.cell(cr, 1, sec)
                                data_rows.append((cr, sec))

                                for ci, mk in enumerate(month_keys, 2):
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
                                    formula = (
                                        f"=SUMIFS('IS GL Summary'!${is_amt_col_l}:${is_amt_col_l},"
                                        f"'IS GL Summary'!${is_sec_col_l}:${is_sec_col_l},\"{sec}\","
                                        f"'IS GL Summary'!${is_mon_col_l}:${is_mon_col_l},{date_f})"
                                    )
                                    ws_sum.cell(cr, ci, formula).number_format = NUM_FMT

                                sl = get_column_letter(2)
                                el = get_column_letter(1 + num_mo)
                                ws_sum.cell(cr, tot_col, f"=SUM({sl}{cr}:{el}{cr})").number_format = NUM_FMT
                                cr += 1

                            # Net Income — income sections minus expense sections
                            ni_row = cr
                            ws_sum.cell(cr, 1, "Net Income").font = BOLD
                            for ci in range(2, tot_col + 1):
                                cl       = get_column_letter(ci)
                                inc_refs = "+".join(f"{cl}{r}" for r, s in data_rows if s in INCOME_SECS)
                                exp_refs = "+".join(f"{cl}{r}" for r, s in data_rows if s in EXPENSE_SECS)
                                if inc_refs and exp_refs:
                                    formula = f"={inc_refs}-({exp_refs})"
                                elif inc_refs:
                                    formula = f"={inc_refs}"
                                else:
                                    formula = "=0"
                                c = ws_sum.cell(cr, ci, formula)
                                c.number_format = NUM_FMT
                                c.font = BOLD
                            cr += 1

                            # QBO Net Income from P&L
                            qbo_ni_row = cr
                            ws_sum.cell(cr, 1,
                                "Net Income \u2014 QBO P&L").font = Font(name="Arial", size=10, italic=True)
                            if "P&L" in wb.sheetnames:
                                ws_pl  = wb["P&L"]
                                pl_hdr = [ws_pl.cell(1, c).value
                                          for c in range(1, ws_pl.max_column + 1)]
                                for pri in range(2, ws_pl.max_row + 1):
                                    lbl = str(ws_pl.cell(pri, 1).value or "").strip().lower()
                                    if lbl in ("net income", "net earnings"):
                                        for ci, mk in enumerate(month_keys, 2):
                                            ml_s = month_display.get(mk, mk)
                                            for pci, ph in enumerate(pl_hdr):
                                                if str(ph or "").strip() == ml_s:
                                                    c = ws_sum.cell(cr, ci,
                                                        f"='P&L'!{get_column_letter(pci+1)}{pri}")
                                                    c.number_format = NUM_FMT
                                                    c.font = LINK_FONT
                                                    break
                                        try:
                                            ptc = pl_hdr.index("Total") + 1
                                            c = ws_sum.cell(cr, tot_col,
                                                f"='P&L'!{get_column_letter(ptc)}{pri}")
                                            c.number_format = NUM_FMT
                                            c.font = LINK_FONT
                                        except ValueError:
                                            pass
                                        break
                            cr += 1

                            # Difference row — black font, conditional fill only
                            diff_row = cr
                            ws_sum.cell(cr, 1, "Difference (should be zero)").font = Font(name="Arial", size=10, bold=True)
                            for ci in range(2, tot_col + 1):
                                cl = get_column_letter(ci)
                                c = ws_sum.cell(cr, ci, f"={cl}{ni_row}-{cl}{qbo_ni_row}")
                                c.number_format = NUM_FMT
                                c.font = Font(name="Arial", size=10)
                            dr = f"B{diff_row}:{get_column_letter(tot_col)}{diff_row}"
                            ws_sum.conditional_formatting.add(dr,
                                CellIsRule(operator="notEqual", formula=["0"],
                                           font=RED_FONT, fill=RED_FILL))
                            ws_sum.conditional_formatting.add(dr,
                                CellIsRule(operator="equal", formula=["0"],
                                           font=GRN_FONT, fill=GRN_FILL))
                            cr += 2

                            # ── BS Section ──────────────────────────────────
                            if "BS GL Summary" not in wb.sheetnames:
                                continue
                            ws_bsg  = wb["BS GL Summary"]
                            hdr_bsg = [ws_bsg.cell(1, c).value
                                       for c in range(1, ws_bsg.max_column + 1)]

                            bs_amt_col_i, bs_amt_col_l = _cl(hdr_bsg, "Amount")
                            bs_mon_col_i, bs_mon_col_l = _cl(hdr_bsg, "Month")
                            _, bs_sec_col_l = _cl(hdr_bsg, sec_label)

                            if not bs_mon_col_i or not bs_amt_col_l or not bs_sec_col_l:
                                continue

                            # Collect BS month-end dates
                            bs_month_keys    = []
                            bs_month_display = {}
                            bs_month_dates   = {}
                            for ri in range(2, ws_bsg.max_row + 1):
                                mv = ws_bsg.cell(ri, bs_mon_col_i).value
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

                            # BS header — col A blank, then month labels
                            ws_sum.cell(cr, 1, "").font = HDR_FONT
                            ws_sum.cell(cr, 1).fill = HDR_FILL
                            for ci, mk in enumerate(bs_month_keys, 2):
                                c = ws_sum.cell(cr, ci, bs_month_display.get(mk, mk))
                                c.font = HDR_FONT
                                c.fill = HDR_FILL
                                c.alignment = Alignment(horizontal="center")
                            cr += 1

                            # Build section → groups lookup
                            bs_sec_groups = {}
                            for gname, sec in bs_groups:
                                if sec not in bs_sec_groups:
                                    bs_sec_groups[sec] = []
                                bs_sec_groups[sec].append(gname)

                            ASSET_SECS = ["Current Assets", "Fixed Assets", "Other Assets"]
                            LIAB_SECS  = ["Current Liabilities", "Long-term Liabilities"]
                            EQ_SECS    = ["Equity"]

                            def _bs_date_f(mk):
                                mv = bs_month_dates.get(mk)
                                if mv and hasattr(mv, 'year'):
                                    return f"DATE({mv.year},{mv.month},{mv.day})"
                                try:
                                    d = _dt.strptime(mk[:10], "%Y-%m-%d")
                                    return f"DATE({d.year},{d.month},{d.day})"
                                except Exception:
                                    return f'"{mk}"'

                            def write_bs_block(sections_list, total_label):
                                nonlocal cr
                                sec_data_rows = []
                                for sec in sections_list:
                                    if sec not in bs_sec_groups:
                                        continue
                                    ws_sum.cell(cr, 1, sec)
                                    sec_data_rows.append(cr)
                                    for ci, mk in enumerate(bs_month_keys, 2):
                                        date_f = _bs_date_f(mk)
                                        formula = (
                                            f"=SUMIFS('BS GL Summary'!${bs_amt_col_l}:${bs_amt_col_l},"
                                            f"'BS GL Summary'!${bs_sec_col_l}:${bs_sec_col_l},\"{sec}\","
                                            f"'BS GL Summary'!${bs_mon_col_l}:${bs_mon_col_l},{date_f})"
                                        )
                                        ws_sum.cell(cr, ci, formula).number_format = NUM_FMT
                                    cr += 1

                                if not sec_data_rows:
                                    return

                                # Total row
                                total_row = cr
                                ws_sum.cell(cr, 1, total_label).font = BOLD
                                for ci in range(2, num_bs_mo + 2):
                                    cl = get_column_letter(ci)
                                    refs = "+".join(f"{cl}{r}" for r in sec_data_rows)
                                    ws_sum.cell(cr, ci, f"={refs}").number_format = NUM_FMT
                                    ws_sum.cell(cr, ci).font = BOLD
                                cr += 1

                                # BS GL Summary reference row — same SUMIFS, serves as cross-check
                                bs_ref_row = cr
                                ws_sum.cell(cr, 1, f"{total_label} \u2014 per BS GL Summary").font = LINK_FONT
                                for ci, mk in enumerate(bs_month_keys, 2):
                                    date_f = _bs_date_f(mk)
                                    # Sum all sections in this block
                                    sec_parts = [
                                        f"SUMIFS('BS GL Summary'!${bs_amt_col_l}:${bs_amt_col_l},"
                                        f"'BS GL Summary'!${bs_sec_col_l}:${bs_sec_col_l},\"{s}\","
                                        f"'BS GL Summary'!${bs_mon_col_l}:${bs_mon_col_l},{date_f})"
                                        for s in sections_list if s in bs_sec_groups
                                    ]
                                    formula = "=" + "+".join(sec_parts) if sec_parts else "=0"
                                    c = ws_sum.cell(cr, ci, formula)
                                    c.number_format = NUM_FMT
                                    c.font = LINK_FONT
                                cr += 1

                                # Difference row — black font, conditional fill only
                                diff_row = cr
                                ws_sum.cell(cr, 1, "Difference (should be zero)").font = Font(name="Arial", size=10, bold=True)
                                for ci in range(2, num_bs_mo + 2):
                                    cl = get_column_letter(ci)
                                    c = ws_sum.cell(cr, ci, f"={cl}{total_row}-{cl}{bs_ref_row}")
                                    c.number_format = NUM_FMT
                                    c.font = Font(name="Arial", size=10)
                                dr = f"B{diff_row}:{get_column_letter(num_bs_mo + 1)}{diff_row}"
                                ws_sum.conditional_formatting.add(dr,
                                    CellIsRule(operator="notEqual", formula=["0"], font=RED_FONT, fill=RED_FILL))
                                ws_sum.conditional_formatting.add(dr,
                                    CellIsRule(operator="equal", formula=["0"], font=GRN_FONT, fill=GRN_FILL))
                                cr += 2

                            write_bs_block(ASSET_SECS, "Total Assets")
                            write_bs_block(LIAB_SECS, "Total Liabilities")
                            write_bs_block(EQ_SECS, "Total Equity")

                        # Column widths for Map Summary
                        ws_sum.column_dimensions["A"].width = 28
                        for ci in range(2, ws_sum.max_column + 1):
                            ws_sum.column_dimensions[get_column_letter(ci)].width = 14
                        ws_sum.freeze_panes = "B2"

                        wb.save(file_path)
                        progress_fn("  Mapping columns and Map Summary written.")

                except Exception as e:
                    import traceback
                    logger.error(f"Mapping error: {e}\n{traceback.format_exc()}")
                    progress_fn(f"  WARNING: Mapping failed — {e}")

            # Add portal flat tabs if requested
            if include_portal_data:
                try:
                    progress_fn("  Building portal data tabs...")
                    import openpyxl as _ox_p
                    from backend.portal_prep import build_portal_flat_tabs
                    wb_p = _ox_p.load_workbook(file_path)

                    # Read IS GL Summary and BS GL Summary rows from the workbook
                    def _read_tab_rows(wb, tab_name):
                        if tab_name not in wb.sheetnames:
                            return []
                        ws = wb[tab_name]
                        return [[ws.cell(r, c).value for c in range(1, ws.max_column+1)]
                                for r in range(1, ws.max_row+1)]

                    is_sum = _read_tab_rows(wb_p, "IS GL Summary")
                    bs_bal = _read_tab_rows(wb_p, "BS Balances")
                    p_is, p_bs = build_portal_flat_tabs(is_sum, bs_bal)

                    if p_is:
                        from openpyxl.styles import Font as _Fp, PatternFill as _PFp, Alignment as _Alp
                        from openpyxl.utils import get_column_letter as _gclp
                        from datetime import datetime as _dtp, date as _datep
                        for tab_name, rows in [("Portal_IS_Flat", p_is), ("Portal_BS_Flat", p_bs)]:
                            if not rows: continue
                            ws = wb_p.create_sheet(tab_name)
                            ws.sheet_view.showGridLines = False
                            hf = _Fp(name="Arial", size=10, bold=True, color="FFFFFF")
                            hb = _PFp("solid", fgColor="337E8D")
                            pf = _Fp(name="Arial", size=10)
                            for ci, v in enumerate(rows[0], 1):
                                c = ws.cell(1, ci, v); c.font = hf; c.fill = hb
                            for ri, row in enumerate(rows[1:], 2):
                                for ci, v in enumerate(row, 1):
                                    if isinstance(v, _dtp): v = v.date()
                                    c = ws.cell(ri, ci, v); c.font = pf
                                    if isinstance(v, _datep):
                                        c.number_format = "M/D/YYYY"
                                    elif isinstance(v, (int, float)):
                                        c.number_format = '#,##0.00_);(#,##0.00);"-"??;@'
                            # Autofit column widths
                            for col_cells in ws.columns:
                                mx = 0
                                cl = _gclp(col_cells[0].column)
                                for cell in col_cells:
                                    try:
                                        cl2 = len(str(cell.value)) if cell.value is not None else 0
                                        if cl2 > mx: mx = cl2
                                    except: pass
                                ws.column_dimensions[cl].width = min(mx + 4, 60)
                            ws.freeze_panes = "A2"

                    wb_p.save(file_path)
                    progress_fn("  Portal data tabs added.")
                except Exception as pe:
                    logger.warning(f"Portal tab build failed: {pe}")
                    progress_fn(f"  WARNING: Portal tabs failed — {pe}")

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
              body.selected_maps, body.include_gl_detail,
              body.include_portal_data),
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


