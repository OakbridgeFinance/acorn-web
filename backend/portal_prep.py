"""Portal flat tab builder — produces Portal_IS_Flat and Portal_BS_Flat from GL Summary tabs."""
from collections import defaultdict


def build_portal_flat_tabs(is_summary_rows, bs_summary_rows):
    """Build Portal_IS_Flat and Portal_BS_Flat from IS/BS GL Summary rows."""
    is_flat = _build_is_flat(is_summary_rows)
    bs_flat = _build_bs_flat(bs_summary_rows)
    return is_flat, bs_flat


def _find_col(header, *names):
    for name in names:
        for i, h in enumerate(header):
            if str(h or "").strip().lower() == name.lower():
                return i
    return -1


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
        month = row[i_month] if i_month >= 0 else ""
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
    for (acct, month, dim), amt in sorted(agg.items()):
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
                date = row[4]
                amt = float(row[5] or 0)
                sv = row[6 + len(mgc) + si] if len(row) > 6 + len(mgc) + si else ""
            else:
                dim_val = "Total"
                date = row[2]
                amt = float(row[3] or 0)
                sv = row[4 + len(mgc) + si] if len(row) > 4 + len(mgc) + si else ""
            if not sv: continue
            sec_tots[(sv, date, dim_val)] += amt
        for (sv, date, dim_val), total in sorted(sec_tots.items()):
            r = [sv, "SectionTotal"]
            if dim_name:
                r += ["", dim_val]
            r += [date, total]
            r += [""] * len(mgc)
            r += [""] * len(msc)
            out.append(r)

    return out


def _build_bs_flat(rows):
    if not rows or len(rows) < 2:
        return []
    header = rows[0]
    i_acct = _find_col(header, "Account Name")
    i_month = _find_col(header, "Month")
    i_amt = _find_col(header, "Amount")

    mgc = [(i, h) for i, h in enumerate(header) if str(h or "").endswith(" - Account Group")]
    msc = [(i, h) for i, h in enumerate(header) if str(h or "").endswith(" - Statement Section")]

    out_hdr = ["Account", "Row Type", "Date", "Balance"]
    for _, h in mgc: out_hdr.append(h)
    for _, h in msc: out_hdr.append(h)

    agg = defaultdict(float)
    meta = {}
    for row in rows[1:]:
        acct = str(row[i_acct] or "") if i_acct >= 0 else ""
        month = row[i_month] if i_month >= 0 else ""
        amt = float(row[i_amt] or 0) if i_amt >= 0 else 0.0
        key = (acct, month)
        agg[key] += amt
        if key not in meta:
            meta[key] = {
                "gv": [str(row[i] or "") if i < len(row) else "" for i, _ in mgc],
                "sv": [str(row[i] or "") if i < len(row) else "" for i, _ in msc],
            }

    out = [out_hdr]
    for (acct, month), bal in sorted(agg.items()):
        m = meta[(acct, month)]
        r = [acct, "Subtotal", month, bal]
        r += m["gv"]
        r += m["sv"]
        out.append(r)

    # SectionTotal
    sec_tots = defaultdict(float)
    for row in out[1:]:
        if row[1] != "Subtotal": continue
        date = row[2]
        sv = row[4 + len(mgc)] if len(row) > 4 + len(mgc) else ""
        if not sv: continue
        sec_tots[(sv, date)] += float(row[3] or 0)
    for (sv, date), total in sorted(sec_tots.items()):
        r = [sv, "SectionTotal", date, total]
        r += [""] * len(mgc)
        r += [""] * len(msc)
        out.append(r)

    return out
