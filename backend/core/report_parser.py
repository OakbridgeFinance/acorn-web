"""
report_parser.py
Parses QuickBooks Online report JSON responses into flat pandas DataFrames.

QBO reports use a hierarchical row structure (Sections > Data rows > Summaries).
Each parser flattens this into a table with one row per line item or transaction.
"""

import pandas as pd
from typing import Any


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _extract_columns(report_data: dict) -> list[str]:
    """Extract ordered column names from report metadata."""
    raw = report_data.get("Columns", {}).get("Column", [])
    names = []
    for col in raw:
        title = col.get("ColTitle", "").strip()
        col_type = col.get("ColType", "")
        # Use title if available, fall back to ColType
        names.append(title if title else col_type)
    return names


def _col_value(col_data_item: dict) -> str:
    return col_data_item.get("value", "")


def _col_id(col_data_item: dict) -> str:
    return col_data_item.get("id", "")


# ---------------------------------------------------------------------------
# General Ledger parser
# ---------------------------------------------------------------------------

def parse_general_ledger(report_data: dict) -> pd.DataFrame:
    """
    Parse a GeneralLedger report into a flat transaction-level DataFrame.

    Each row represents one posted transaction line.
    Columns include: Account, Account_ID, plus all GL report columns
    (Date, Transaction Type, No., Name, Memo/Description, Split, Amount, Balance).
    Transaction IDs are captured where QBO provides them.
    """
    columns = _extract_columns(report_data)
    rows = report_data.get("Rows", {}).get("Row", [])
    flat_rows: list[dict] = []

    def _process(row_list: list, account: str = "", account_id: str = ""):
        for row in row_list:
            row_type = row.get("type", "")

            if row_type == "Section":
                header_data = row.get("Header", {}).get("ColData", [{}])
                acc = _col_value(header_data[0]) if header_data else account
                acc_id = _col_id(header_data[0]) if header_data else account_id
                nested = row.get("Rows", {}).get("Row", [])
                if nested:
                    _process(nested, acc, acc_id)
                elif row.get("ColData"):
                    # v2-safe: Section with ColData but no nested rows = data row
                    col_data = row["ColData"]
                    entry: dict[str, Any] = {
                        "Account": acc,
                        "Account_ID": acc_id,
                    }
                    for i, col in enumerate(col_data):
                        col_name = columns[i] if i < len(columns) else f"Col_{i}"
                        entry[col_name] = _col_value(col)
                        txn_id = _col_id(col)
                        if txn_id and col_name in ("Date", "Transaction Type", "No."):
                            entry.setdefault("Txn_ID", txn_id)
                    flat_rows.append(entry)

            elif row_type == "Data":
                col_data = row.get("ColData", [])
                entry2: dict[str, Any] = {
                    "Account": account,
                    "Account_ID": account_id,
                }
                for i, col in enumerate(col_data):
                    col_name = columns[i] if i < len(columns) else f"Col_{i}"
                    entry2[col_name] = _col_value(col)
                    txn_id = _col_id(col)
                    if txn_id and col_name in ("Date", "Transaction Type", "No."):
                        entry2.setdefault("Txn_ID", txn_id)

                flat_rows.append(entry2)
            # Skip Summary rows — transaction-level data only for GL

    _process(rows)

    if not flat_rows:
        base_cols = ["Account", "Account_ID"] + columns
        return pd.DataFrame(columns=base_cols)

    df = pd.DataFrame(flat_rows)
    return _coerce_numerics(df, skip=["Account", "Account_ID", "Txn_ID"])


# ---------------------------------------------------------------------------
# TransactionListDetail parser
# ---------------------------------------------------------------------------

def _tld_cell_value(cell: dict) -> str:
    """
    Extract the display value from a TLD ColData cell.

    QBO reference columns (class_ref, department_ref, location_ref) sometimes put
    the display name in a nested structure rather than the top-level "value" key:
      {"value": "", "id": "5", "ref": {"name": "Marketing"}}
    or just:
      {"value": "Marketing", "id": "5"}

    We try in order: top-level "value" → nested "ref.name" → nested "ref.value".
    """
    val = cell.get("value", "")
    if val:
        return val
    ref = cell.get("ref", {})
    if isinstance(ref, dict):
        return ref.get("name", "") or ref.get("value", "") or ""
    return ""


def parse_transaction_list_detail(report_data: dict) -> pd.DataFrame:
    """
    Parse a TransactionListDetail report into a flat transaction-level DataFrame.

    Each Data row represents one transaction split line.  Class and Location arrive
    as reference columns (class_ref, department_ref, location_ref) whose values may
    be in a nested "ref" structure inside the ColData cell.

    Split transactions may also have a nested "lines" array; we unroll these into
    separate rows, inheriting header-level fields (date, type, doc_num, etc.) and
    overriding with line-level values where present, so that per-line class/location
    is captured correctly.
    """
    raw_cols = report_data.get("Columns", {}).get("Column", [])
    col_types: list[str] = []
    _seen_types: dict[str, int] = {}
    for col in raw_cols:
        col_type  = col.get("ColType",  "").strip()
        col_title = col.get("ColTitle", "").strip()
        # Prefer ColType for unique internal names (tx_date, klass_name, etc.)
        # but fall back to ColTitle when ColType is generic (String, Money, etc.)
        # to avoid duplicate keys that overwrite data in the row dict.
        key = col_type if col_type else col_title
        if key in ("String", "Money", "Number", "Date"):
            # Generic ColType — use ColTitle instead for a unique key
            key = col_title if col_title else f"{col_type}_{len(col_types)}"
        elif key in _seen_types:
            # Duplicate ColType — use ColTitle to disambiguate
            key = col_title if col_title else f"{key}_{_seen_types[key]}"
        _seen_types[key] = _seen_types.get(key, 0) + 1
        col_types.append(key)

    rows = report_data.get("Rows", {}).get("Row", [])
    flat_rows: list[dict] = []

    # Reference column names that may carry class/location data
    _REF_COLS = {"class_ref", "department_ref", "location_ref",
                 "class", "class_name", "klass_name",
                 "department", "department_name", "dept_name", "location_ref"}

    def _parse_col_data(col_data: list) -> dict:
        entry: dict[str, Any] = {}
        for i, cell in enumerate(col_data):
            key = col_types[i] if i < len(col_types) else f"col_{i}"
            entry[key] = _tld_cell_value(cell)
            txn_id = cell.get("id", "")
            if txn_id and "Txn_ID" not in entry:
                entry["Txn_ID"] = txn_id
        return entry

    for row in rows:
        # v2-safe: accept both "Data" (v1) and "Section" (v2) row types
        if row.get("type") not in ("Data", "Section"):
            continue

        header = _parse_col_data(row.get("ColData", []))

        # Check for nested Line arrays (split transactions).
        # Each line may have its own class/location overriding the header value.
        lines = row.get("lines") or row.get("Lines") or row.get("Line") or []
        if lines:
            for line in lines:
                line_col_data = line.get("ColData", [])
                if line_col_data:
                    line_entry = dict(header)
                    parsed = _parse_col_data(line_col_data)
                    # Only override header fields when the line value is non-blank
                    for k, v in parsed.items():
                        if v or k in _REF_COLS:
                            line_entry[k] = v
                    flat_rows.append(line_entry)
                else:
                    # Line has no ColData — just inherit header values
                    flat_rows.append(dict(header))
        else:
            flat_rows.append(header)

    if not flat_rows:
        return pd.DataFrame(columns=col_types)

    df = pd.DataFrame(flat_rows)
    return _coerce_numerics(df, skip=["Txn_ID"])


# ---------------------------------------------------------------------------
# Financial statement parser (P&L, Balance Sheet, Trial Balance)
# ---------------------------------------------------------------------------

def parse_financial_statement(report_data: dict) -> pd.DataFrame:
    """
    Parse P&L, Balance Sheet, or Trial Balance into a flat DataFrame.

    Each row represents a line item (account, subtotal, or grand total).
    Includes structural metadata columns:
      - Row_Type: Header | Data | Summary | GrandTotal
      - Indent_Level: 0-based depth in the account hierarchy
      - Account_Path: full path, e.g. "Income > Services > Consulting"
      - Account_ID: QBO internal ID when available
    """
    columns = _extract_columns(report_data)
    report_rows = report_data.get("Rows", {}).get("Row", [])
    flat_rows: list[dict] = []

    def _process(row_list: list, path: list[str], depth: int):
        for row in row_list:
            row_type = row.get("type", "")

            if row_type == "Section":
                header = row.get("Header", {})
                header_data = header.get("ColData", [{}])
                section_name = _col_value(header_data[0]) if header_data else ""
                section_id = _col_id(header_data[0]) if header_data else ""
                new_path = path + [section_name] if section_name else path

                nested = row.get("Rows", {}).get("Row", [])
                summary_data = row.get("Summary", {}).get("ColData", [])

                if nested:
                    # Multi-row section: emit header, recurse, emit summary
                    if len(header_data) > 1:
                        row_dict = _make_row(
                            "Header", header_data, columns, new_path, section_id, depth
                        )
                        flat_rows.append(row_dict)

                    _process(nested, new_path, depth + 1)

                    if summary_data:
                        row_dict = _make_row(
                            "Summary", summary_data, columns, new_path, "", depth
                        )
                        flat_rows.append(row_dict)
                else:
                    # Leaf section — no sub-rows.  QBO uses this pattern for
                    # individual Balance Sheet accounts (investments, loans, etc.)
                    # that have no sub-accounts, and for computed subtotals like
                    # "Net Operating Income" and "Gross Profit" which have no
                    # Header — only a Summary.  Emit a single Data row.
                    if summary_data:
                        # Prefer the header name (avoids "Total …" prefix in
                        # summary ColData[0]).  For headerless sections like Net
                        # Operating Income, fall back to the summary label.
                        if _col_value(header_data[0]) or _col_id(header_data[0]):
                            name_cell = header_data[0]
                        else:
                            name_cell = summary_data[0]
                            # Patch new_path so Account_Path reflects the label
                            label_str = _col_value(name_cell)
                            if label_str and label_str not in new_path:
                                new_path = path + [label_str]
                        synthetic = [name_cell] + list(summary_data[1:])
                        row_dict = _make_row(
                            "Data", synthetic, columns, new_path, section_id, depth
                        )
                        flat_rows.append(row_dict)
                    elif len(header_data) > 1:
                        # No summary — emit as a header row (rare edge case)
                        row_dict = _make_row(
                            "Header", header_data, columns, new_path, section_id, depth
                        )
                        flat_rows.append(row_dict)

            elif row_type == "Data":
                col_data = row.get("ColData", [])
                row_dict = _make_row("Data", col_data, columns, path, "", depth)
                flat_rows.append(row_dict)

            elif row_type == "GrandTotal" or row_type == "grandtotal":
                col_data = row.get("ColData", [])
                row_dict = _make_row("GrandTotal", col_data, columns, ["Net Income"], "", 0)
                flat_rows.append(row_dict)

    _process(report_rows, [], 0)

    if not flat_rows:
        meta_cols = ["Row_Type", "Indent_Level", "Account_Path", "Account", "Account_ID"]
        return pd.DataFrame(columns=meta_cols + columns)

    df = pd.DataFrame(flat_rows)
    skip_cols = ["Row_Type", "Account_Path", "Account", "Account_ID"]
    return _coerce_numerics(df, skip=skip_cols)


def _make_row(
    row_type: str,
    col_data: list[dict],
    columns: list[str],
    path: list[str],
    entity_id: str,
    depth: int,
) -> dict:
    """Build a single flat row dict from ColData."""
    account = path[-1] if path else ""
    account_path = " > ".join(filter(None, path))

    row: dict[str, Any] = {
        "Row_Type": row_type,
        "Indent_Level": depth,
        "Account_Path": account_path,
        "Account": account,
        "Account_ID": entity_id,
    }

    for i, col in enumerate(col_data):
        col_name = columns[i] if i < len(columns) else f"Col_{i}"
        # For Data rows, the first column is usually the account name — skip it
        # since we already captured it from the section hierarchy.
        if row_type == "Data" and i == 0 and col_name in ("", "Account", "ACCOUNT_TYPE"):
            acc_val = _col_value(col)
            acc_id = _col_id(col)
            if acc_val:
                row["Account"] = acc_val
                row["Account_Path"] = (
                    " > ".join(filter(None, path[:-1] + [acc_val]))
                    if path else acc_val
                )
            if acc_id:
                row["Account_ID"] = acc_id
            continue

        row[col_name] = _col_value(col)
        # Capture QBO entity IDs for Data rows
        item_id = _col_id(col)
        if item_id and row_type == "Data":
            row[f"{col_name}_ID"] = item_id

    return row


# ---------------------------------------------------------------------------
# Numeric coercion
# ---------------------------------------------------------------------------

def _coerce_numerics(df: pd.DataFrame, skip: list[str] | None = None) -> pd.DataFrame:
    """Attempt to convert string columns that look like numbers to float."""
    skip = set(skip or [])
    df = df.copy()
    for col in df.columns:
        if col in skip or col.endswith("_ID"):
            continue
        # pandas 3.x uses StringDtype; older pandas uses object — accept both
        if not (df[col].dtype == object or pd.api.types.is_string_dtype(df[col])):
            continue
        converted = (
            df[col]
            .astype(str)
            .str.replace(",", "", regex=False)
            .str.strip()
        )
        numeric = pd.to_numeric(converted, errors="coerce")
        # Count only non-empty strings in the denominator — empty-string rows
        # (e.g. Balance Sheet section headers) would otherwise drag the ratio
        # below the threshold and prevent numeric columns from being coerced.
        non_empty = (converted != "") & (converted != "nan") & converted.notna()
        if non_empty.sum() > 0 and (numeric[non_empty].notna().sum() / non_empty.sum()) > 0.7:
            df[col] = numeric
    return df
