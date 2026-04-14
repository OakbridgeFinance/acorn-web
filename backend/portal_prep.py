"""Portal flat tab builder — produces Portal_IS_Flat and Portal_BS_Flat."""
from collections import defaultdict
from datetime import datetime, date
import re as _re


def _clean_date(val):
    """Strip time component from any date/datetime value."""
    if isinstance(val, datetime):
        return val.date()
    return val


def _find_col(header, *names):
    for name in names:
        for i, h in enumerate(header):
            if str(h or "").strip().lower() == name.lower():
                return i
    return -1


def _classify_section(sec_val):
    """Classify a BS section name into asset/liability/equity."""
    s = str(sec_val or "").strip().lower()
    if any(k in s for k in ("asset", "receivable", "bank")):
        return "asset"
    if any(k in s for k in ("liabilit", "payable", "credit card")):
        return "liability"
    if "equity" in s:
        return "equity"
    return ""


def build_portal_flat_tabs(is_summary_rows, bs_balances_rows):
    """Build Portal_IS_Flat and Portal_BS_Flat."""
    is_flat = _build_is_flat(is_summary_rows)
    bs_flat = _build_bs_flat(bs_balances_rows)
    return is_flat, bs_flat


def _build_is_flat(rows):
    if not rows or len(rows) < 2:
        return []
    header = rows[0]
    i_acct = _find_col(header, "Account Name")
    i_month = _find_col(header, "Month")
    i_amt = _find_col(header, "Amount")
    i_dim = _find_col(header, "Class", "Location", "Department")
    dim_name = header[i_dim] if i_dim >= 0 else None

    mgc = [(i, h) for i, h in enumerate(header) if str(h or "").endswith(" - Account Group")]
    msc = [(i, h) for i, h in enumerate(header) if str(h or "").endswith(" - Statement Section")]

    out_hdr = ["Account", "Row Type"]
    if dim_name:
        out_hdr += ["Dimension Type", "Dimension Value"]
    out_hdr += ["Date", "Amount"]
    for _, h in mgc: out_hdr.append(h)
    for _, h in msc: out_hdr.append(h)

    agg = defaultdict(float)
    meta = {}
    for row in rows[1:]:
        acct = str(row[i_acct] or "") if i_acct >= 0 else ""
        month = _clean_date(row[i_month]) if i_month >= 0 else ""
        amt = float(row[i_amt] or 0) if i_amt >= 0 else 0.0
        dim = str(row[i_dim] or "Total") if i_dim >= 0 else "Total"
        key = (acct, month, dim)
        agg[key] += amt
        if key not in meta:
            meta[key] = {
                "gv": [str(row[i] or "") if i < len(row) else "" for i, _ in mgc],
                "sv": [str(row[i] or "") if i < len(row) else "" for i, _ in msc],
            }

    out = [out_hdr]
    for (acct, month, dim), amt in sorted(agg.items(), key=lambda x: (str(x[0][1]), str(x[0][0]))):
        m = meta[(acct, month, dim)]
        r = [acct, "Subtotal"]
        if dim_name:
            r += [dim_name if dim != "Total" else "", dim]
        r += [month, amt]
        r += m["gv"]
        r += m["sv"]
        out.append(r)

    # SectionTotal rows per Statement Section
    for si, (_, sh) in enumerate(msc):
        sec_tots = defaultdict(float)
        for row in out[1:]:
            if row[1] != "Subtotal": continue
            if dim_name:
                dim_val = row[3]
                dt = row[4]
                amt = float(row[5] or 0)
                sv = row[6 + len(mgc) + si] if len(row) > 6 + len(mgc) + si else ""
            else:
                dim_val = "Total"
                dt = row[2]
                amt = float(row[3] or 0)
                sv = row[4 + len(mgc) + si] if len(row) > 4 + len(mgc) + si else ""
            if not sv: continue
            sec_tots[(sv, dt, dim_val)] += amt
        for (sv, dt, dim_val), total in sorted(sec_tots.items()):
            r = [sv, "SectionTotal"]
            if dim_name:
                r += ["", dim_val]
            r += [dt, total]
            r += [""] * len(mgc)
            r += [""] * len(msc)
            out.append(r)

    return out


def _build_bs_flat(bs_balances_rows):
    """
    Build Portal_BS_Flat from BS Balances tab rows.
    BS Balances format: Account | Account Type | Account Subtype | {map cols} | Date | Ending Balance
    This is long format — one row per account per month-end date.
    """
    if not bs_balances_rows or len(bs_balances_rows) < 2:
        return []

    header = bs_balances_rows[0]
    i_acct = _find_col(header, "Account")
    i_date = _find_col(header, "Date")
    i_bal = _find_col(header, "Ending Balance")

    if i_acct < 0 or i_bal < 0:
        return []

    mgc = [(i, h) for i, h in enumerate(header) if str(h or "").endswith(" - Account Group")]
    msc = [(i, h) for i, h in enumerate(header) if str(h or "").endswith(" - Statement Section")]
    n_grp = len(mgc)
    n_sec = len(msc)

    out_header = ["Account", "Row Type", "Date", "Balance"]
    for _, h in mgc: out_header.append(h)
    for _, h in msc: out_header.append(h)

    out_rows = [out_header]

    # Read each row — one per account per month-end
    agg = defaultdict(float)
    meta = {}
    for row in bs_balances_rows[1:]:
        acct = str(row[i_acct] or "").strip() if i_acct >= 0 else ""
        dt = _clean_date(row[i_date]) if i_date >= 0 else ""
        bal = float(row[i_bal] or 0) if i_bal >= 0 else 0.0
        if not acct:
            continue
        key = (acct, dt)
        agg[key] += bal
        if key not in meta:
            meta[key] = {
                "gv": [str(row[i] or "") if i < len(row) else "" for i, _ in mgc],
                "sv": [str(row[i] or "") if i < len(row) else "" for i, _ in msc],
            }

    # Subtotal rows
    for (acct, dt), bal in sorted(agg.items(), key=lambda x: (str(x[0][1]), str(x[0][0]))):
        m = meta[(acct, dt)]
        r = [acct, "Subtotal", dt, bal]
        r += m["gv"]
        r += m["sv"]
        out_rows.append(r)

    # SectionTotal rows — sum balances by Statement Section per date
    sec_totals = defaultdict(float)
    for row in out_rows[1:]:
        if row[1] != "Subtotal": continue
        dt = row[2]
        bal = float(row[3] or 0)
        for si, (_, _) in enumerate(msc):
            sv = row[4 + n_grp + si] if len(row) > 4 + n_grp + si else ""
            if str(sv or "").strip():
                sec_totals[(str(sv).strip(), dt)] += bal

    for (sv, dt), total in sorted(sec_totals.items(), key=lambda x: (str(x[0][1]), str(x[0][0]))):
        r = [sv, "SectionTotal", dt, total]
        r += [""] * n_grp
        r += [sv] + [""] * max(0, n_sec - 1)
        out_rows.append(r)

    # GrandTotal rows
    bucket_totals = defaultdict(float)
    for row in out_rows[1:]:
        if row[1] != "SectionTotal": continue
        sv = str(row[0] or "").strip()
        dt = row[2]
        bal = float(row[3] or 0)
        bucket = _classify_section(sv)
        if bucket:
            bucket_totals[(bucket, dt)] += bal

    BUCKET_LABELS = {"asset": "Total Assets", "liability": "Total Liabilities", "equity": "Total Equity"}
    for (bucket, dt), total in sorted(bucket_totals.items(), key=lambda x: (str(x[0][1]), x[0][0])):
        label = BUCKET_LABELS.get(bucket, f"Total {bucket.title()}")
        r = [label, "GrandTotal", dt, total]
        r += [""] * n_grp
        r += [""] * n_sec
        out_rows.append(r)

    # Total Liabilities & Equity
    liab_eq = defaultdict(float)
    for row in out_rows[1:]:
        if row[1] != "GrandTotal": continue
        if str(row[0]) in ("Total Liabilities", "Total Equity"):
            liab_eq[row[2]] += float(row[3] or 0)
    for dt, total in sorted(liab_eq.items(), key=lambda x: str(x[0])):
        r = ["Total Liabilities & Equity", "GrandTotal", dt, total]
        r += [""] * n_grp
        r += [""] * n_sec
        out_rows.append(r)

    return out_rows
