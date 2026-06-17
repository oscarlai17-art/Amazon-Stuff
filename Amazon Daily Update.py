"""
Amazon Vendor Data -> Google Sheets Uploader
=============================================
Reads the latest file from each Amazon-Data subfolder and uploads
it to the corresponding tab in Google Sheets.

Traffic uploads differently: each daily CSV in Amazon-Data/traffic/ gets
appended as new rows to "Traffic raw" (long format), and a matching date
column with a SUMIFS formula is inserted into "Traffic trend".

Usage:
    python "Amazon Daily Update.py"

Requirements:
    pip install gspread google-auth pandas xlrd openpyxl
"""

import os
import re
import glob
import datetime
import numpy as np
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# ── Config ─────────────────────────────────────────────────────────────────────
CREDENTIALS_FILE = "C:/Users/makep/Downloads/amazon-494102-3bd915b4a36e.json"
SPREADSHEET_ID   = "1zhlqL2tqKvI70h0OQ_V46erwwLA9ztp0PjkJ3B7BgSI"
DATA_ROOT        = "C:/Users/makep/Documents/Amazon-Data"


def _po_tab_name():
    """Derive 'Line Items <Month>' from the latest PO file's name, falling back to current month."""
    files = (glob.glob(os.path.join(DATA_ROOT, "purchase-orders", "*.xls")) +
             glob.glob(os.path.join(DATA_ROOT, "purchase-orders", "*.xlsx")))
    if files:
        latest = max(files, key=os.path.getmtime)
        m = re.search(r"_(\d{4})-(\d{2})-\d{2}", os.path.basename(latest))
        if m:
            month_name = datetime.date(int(m.group(1)), int(m.group(2)), 1).strftime("%B")
            return f"Line Items {month_name}"
    return f"Line Items {datetime.datetime.now().strftime('%B')}"


FOLDER_MAP = {
    "purchase-orders":   (_po_tab_name(), "Line Items", 0),
    "Last 2 days Sales": ("Last 2 days",  None,         1),
    "inventory":         ("Inventory raw", None,        1),
    "Top 100":           ("Top 100",       None,        0),
}

TRAFFIC_FOLDER    = os.path.join(DATA_ROOT, "traffic")
TRAFFIC_RAW_SHEET = "Traffic raw"

SALES_FOLDER    = os.path.join(DATA_ROOT, "Sales")
SALES_RAW_SHEET = "Unit sold raw"


def get_latest_file(folder_path):
    files = (
        glob.glob(os.path.join(folder_path, "*.csv")) +
        glob.glob(os.path.join(folder_path, "*.xls")) +
        glob.glob(os.path.join(folder_path, "*.xlsx"))
    )
    if not files:
        return None
    return max(files, key=os.path.getmtime)


def coerce_numeric(df):
    for col in df.columns:
        if df[col].dtype == object:
            converted = pd.to_numeric(df[col], errors="coerce")
            original_null = df[col].isna() | (df[col].astype(str).str.strip() == "")
            new_null = converted.isna()
            if (new_null & ~original_null).sum() == 0:
                df[col] = converted
    return df


def read_file(file_path, sheet_name=None, skiprows=0):
    ext = os.path.splitext(file_path)[1].lower()
    if ext == ".csv":
        df = pd.read_csv(file_path, skiprows=skiprows, encoding="utf-8-sig")
    else:
        kwargs = {"sheet_name": sheet_name} if sheet_name else {"sheet_name": 0}
        if skiprows:
            kwargs["skiprows"] = skiprows
        df = pd.read_excel(file_path, **kwargs)
    return coerce_numeric(df)


def extract_hyperlinks_from_xlsx(file_path, col_idx, skiprows=0):
    """Return a list of hyperlink URLs from a specific column in an xlsx file."""
    import openpyxl
    wb = openpyxl.load_workbook(file_path)
    ws = wb.active
    urls = []
    for i, row in enumerate(ws.iter_rows(min_row=skiprows + 2)):  # +2: 1-indexed + skip header
        cell = row[col_idx]
        if cell.hyperlink:
            urls.append(cell.hyperlink.target)
        else:
            urls.append(None)
    return urls


def clean_value(val):
    if val is None:
        return ""
    if isinstance(val, float) and np.isnan(val):
        return ""
    if isinstance(val, np.integer):
        return int(val)
    if isinstance(val, np.floating):
        return float(val)
    if hasattr(val, "isoformat"):
        return val.isoformat()
    return val


def upload_to_sheet(ws, df, url_col_idx=None):
    headers = df.columns.tolist()
    rows = []
    for row in df.itertuples(index=False, name=None):
        cells = []
        for i, cell in enumerate(row):
            val = clean_value(cell)
            if url_col_idx is not None and i == url_col_idx and isinstance(val, str) and val.startswith("http"):
                val = f'=HYPERLINK("{val}","{val}")'
            cells.append(val)
        rows.append(cells)
    ws.clear()
    input_option = "USER_ENTERED" if url_col_idx is not None else "RAW"
    ws.update([headers] + rows, value_input_option=input_option)



def parse_daily_date(filename):
    """Extract MM/DD from filenames containing Daily_M-D-YYYY."""
    match = re.search(r"Daily_(\d+)-(\d+)-\d{4}", filename)
    if match:
        month, day = int(match.group(1)), int(match.group(2))
        return f"{month:02d}/{day:02d}"
    return None


def upload_sales(sh):
    try:
        ws = sh.worksheet(SALES_RAW_SHEET)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=SALES_RAW_SHEET, rows=10000, cols=5)
        ws.update(range_name="A1:E1",
                  values=[["Date", "ASIN", "Ordered Revenue", "Ordered Units", "Shipped Units"]],
                  value_input_option="RAW")
        print(f"  Created '{SALES_RAW_SHEET}' sheet")

    existing_dates = set(ws.col_values(1)[1:])

    files = sorted(glob.glob(os.path.join(SALES_FOLDER, "*.csv")))

    for f in files:
        date_str = parse_daily_date(os.path.basename(f))
        if not date_str:
            continue
        if date_str in existing_dates:
            print(f"  {date_str} already uploaded — skipping")
            continue

        df = pd.read_csv(f, skiprows=1, encoding="utf-8-sig")
        if "ASIN" not in df.columns:
            print(f"  Unexpected columns in {os.path.basename(f)} — skipping")
            continue

        rows = []
        for _, row in df.iterrows():
            asin = str(row["ASIN"]).strip()
            if not asin or asin == "nan":
                continue
            # Strip $ and commas from revenue
            rev_raw = str(row.get("Ordered Revenue", "")).replace("$", "").replace(",", "").strip()
            try:
                revenue = float(rev_raw)
            except ValueError:
                revenue = 0.0
            ordered  = int(row["Ordered Units"])  if pd.notna(row.get("Ordered Units"))  else 0
            shipped  = int(row["Shipped Units"])  if pd.notna(row.get("Shipped Units"))  else 0
            import math
            revenue = 0.0 if (isinstance(revenue, float) and math.isnan(revenue)) else revenue
            rows.append([date_str, asin, revenue, ordered, shipped])

        if rows:
            ws.append_rows(rows, value_input_option="RAW")
            existing_dates.add(date_str)
            print(f"  Appended {len(rows)} rows for {date_str} to '{SALES_RAW_SHEET}'")

    print("  Unit sold raw up to date.")


def upload_traffic(sh):
    # ── Ensure "Traffic raw" sheet exists ──────────────────────────────────────
    try:
        raw_ws = sh.worksheet(TRAFFIC_RAW_SHEET)
    except gspread.exceptions.WorksheetNotFound:
        raw_ws = sh.add_worksheet(title=TRAFFIC_RAW_SHEET, rows=10000, cols=3)
        raw_ws.update(range_name="A1:C1", values=[["Date", "ASIN", "Page Views"]],
                      value_input_option="RAW")
        print(f"  Created '{TRAFFIC_RAW_SHEET}' sheet")

    existing_dates = set(raw_ws.col_values(1)[1:])   # skip header row

    # ── Process each CSV file (sorted = chronological order) ──────────────────
    files = sorted(glob.glob(os.path.join(TRAFFIC_FOLDER, "*.csv")))

    for f in files:
        date_str = parse_daily_date(os.path.basename(f))
        if not date_str:
            continue
        if date_str in existing_dates:
            print(f"  {date_str} already uploaded — skipping")
            continue

        df = pd.read_csv(f, skiprows=1, encoding="utf-8-sig")
        if "ASIN" not in df.columns or "Featured Offer Page Views" not in df.columns:
            print(f"  Unexpected columns in {os.path.basename(f)} — skipping")
            continue

        rows = []
        for _, row in df.iterrows():
            asin = str(row["ASIN"]).strip()
            if not asin or asin == "nan":
                continue
            views = row["Featured Offer Page Views"]
            views_int = int(str(views).replace(',', '')) if pd.notna(views) else 0
            rows.append([date_str, asin, views_int])

        if rows:
            raw_ws.append_rows(rows, value_input_option="RAW")
            existing_dates.add(date_str)
            print(f"  Appended {len(rows)} rows for {date_str} to '{TRAFFIC_RAW_SHEET}'")

    print("  Traffic raw up to date.")


def col_num_to_letter(n):
    result = ""
    while n:
        n, r = divmod(n - 1, 26)
        result = chr(65 + r) + result
    return result


_MONTH_NAMES = {
    "01": "January", "02": "February", "03": "March",    "04": "April",
    "05": "May",     "06": "June",     "07": "July",     "08": "August",
    "09": "September","10": "October", "11": "November", "12": "December",
}

# Per-sheet config: trailing column header and number of leading fixed columns
_TREND_CONFIG = {
    "Traffic trend":   ("L7D",   4),
    "Unit Sold trend": ("L7D",   4),
    "Revenue trend":   ("L7D",   4),
    "ASP trend":       ("L7D",   4),
    "CVR trend":       ("L7D",   4),
    "PO list trend":   ("Total", 7),
}


def _make_formula(sheet_name, col_letter, col_1based, row, date=""):
    if sheet_name == "Traffic trend":
        return f"=IFERROR(SUMIFS('Traffic raw'!$C:$C,'Traffic raw'!$A:$A,INDIRECT(ADDRESS(1,COLUMN())),'Traffic raw'!$B:$B,$B{row}),0)"
    if sheet_name == "Unit Sold trend":
        return f"=IFERROR(SUMIFS('Unit sold raw'!$D:$D,'Unit sold raw'!$A:$A,INDIRECT(ADDRESS(1,COLUMN())),'Unit sold raw'!$B:$B,$B{row}),0)"
    if sheet_name == "Revenue trend":
        return f"=IFERROR(SUMIFS('Unit sold raw'!$C:$C,'Unit sold raw'!$A:$A,INDIRECT(ADDRESS(1,COLUMN())),'Unit sold raw'!$B:$B,$B{row}),0)"
    if sheet_name == "ASP trend":
        return (f"=Iferror(xlookup($B{row},'Revenue trend'!$B:$B,'Revenue trend'!{col_letter}:{col_letter},\"\")"
                f"/xlookup($B{row},'Unit Sold trend'!$B:$B,'Unit Sold trend'!{col_letter}:{col_letter},\"\"),\"\")")
    if sheet_name == "CVR trend":
        unit_col = col_num_to_letter(col_1based - 4)
        return f"=Iferror('Unit Sold trend'!{unit_col}{row}/'Traffic trend'!{col_letter}{row},\"\")"
    if sheet_name == "PO list trend":
        # Fully dynamic: INDIRECT(ADDRESS(1,COLUMN())) reads the date from the header row,
        # INDIRECT(ADDRESS(ROW(),1)) reads the ASIN from col A of the current row.
        # The month name is derived from the date header so the formula never needs updating.
        month_fn = 'TEXT(DATE(2000,VALUE(LEFT(INDIRECT(ADDRESS(1,COLUMN())),2)),1),"MMMM")'
        src = f'"\'Line Items "& {month_fn} &"\'"'
        return (f'=IFERROR(SUMIFS('
                f'INDIRECT({src}&"!$N:$N"),'
                f'INDIRECT({src}&"!$C:$C"),'
                f'INDIRECT(ADDRESS(1,COLUMN())),'
                f'INDIRECT({src}&"!$G:$G"),'
                f'INDIRECT(ADDRESS(ROW(),1))),0)')
    return ""


def _raw_dates_for(sh, sheet_name):
    if sheet_name == "Traffic trend":
        return sorted(set(sh.worksheet("Traffic raw").col_values(1)[1:]))
    if sheet_name == "PO list trend":
        # Use Order dates from Line Items tabs (column C), not Traffic raw
        dates = set()
        today = datetime.date.today()
        months_to_check = set()
        for offset in range(3):  # current + previous 2 months
            d = datetime.date(today.year, today.month, 1) - datetime.timedelta(days=30 * offset)
            months_to_check.add(d.strftime("%B"))
        for month in months_to_check:
            try:
                li_ws = sh.worksheet(f"Line Items {month}")
                vals = li_ws.col_values(3)[1:]  # col C = Order date, skip header
                dates.update(v for v in vals if v.strip() and re.match(r"\d{2}/\d{2}", v))
            except gspread.exceptions.WorksheetNotFound:
                pass
        return sorted(dates)
    return sorted(set(sh.worksheet("Unit sold raw").col_values(1)[1:]))


def rebuild_po_list_trend(sh):
    """Rewrite every date-column formula in PO list trend — only if the formula is outdated."""
    trend_ws   = sh.worksheet("PO list trend")
    headers    = trend_ws.row_values(1)
    if "Total" not in headers:
        print("  'PO list trend' missing 'Total' column — skipping rebuild.")
        return
    total_idx  = headers.index("Total")   # 0-based
    fixed_cols = 7
    num_data_rows = len(trend_ws.col_values(1)) - 1

    first_1based = fixed_cols + 1          # col 8 = H
    last_1based  = total_idx               # last date col (0-based total_idx = 1-based last date)
    n_date_cols  = last_1based - first_1based + 1

    if n_date_cols <= 0 or num_data_rows <= 0:
        print("  'PO list trend' has no date columns to rebuild.")
        return

    # Check first data cell — skip rebuild if already uses correct INDIRECT pattern
    first_letter = col_num_to_letter(first_1based)
    sample = trend_ws.acell(f"{first_letter}2", value_render_option="FORMULA").value or ""
    if "INDIRECT(ADDRESS(ROW(),1))" in sample:
        print("  PO list trend formulas already up to date — skipping rebuild.")
        return

    formula = _make_formula("PO list trend", "", 0, 0)
    values  = [[formula] * n_date_cols for _ in range(num_data_rows)]

    last_letter  = col_num_to_letter(last_1based)
    trend_ws.update(
        values,
        f"{first_letter}2:{last_letter}{num_data_rows + 1}",
        value_input_option="USER_ENTERED",
    )
    print(f"  Rebuilt {n_date_cols} PO list trend date columns with corrected formula.")


def update_trend_sheet(sh, sheet_name):
    raw_dates = _raw_dates_for(sh, sheet_name)
    trend_ws  = sh.worksheet(sheet_name)
    trailing_col, fixed_cols = _TREND_CONFIG[sheet_name]

    headers      = trend_ws.row_values(1)
    trailing_idx = headers.index(trailing_col)     # 0-based
    existing     = set(headers[fixed_cols:trailing_idx])

    new_dates = [d for d in raw_dates if d not in existing]
    if not new_dates:
        print(f"  '{sheet_name}' already up to date.")
        return

    num_data_rows = len(trend_ws.col_values(1)) - 1

    sh.batch_update({
        "requests": [{
            "insertDimension": {
                "range": {
                    "sheetId": trend_ws.id,
                    "dimension": "COLUMNS",
                    "startIndex": trailing_idx,
                    "endIndex": trailing_idx + len(new_dates),
                },
                "inheritFromBefore": True,
            }
        }]
    })

    for i, date in enumerate(new_dates):
        col_1based = trailing_idx + i + 1
        col_letter = col_num_to_letter(col_1based)
        trend_ws.update_cell(1, col_1based, date)
        values = [[_make_formula(sheet_name, col_letter, col_1based, r + 2, date)]
                  for r in range(num_data_rows)]
        trend_ws.update(
            values,
            f"{col_letter}2:{col_letter}{num_data_rows + 1}",
            value_input_option="USER_ENTERED",
        )
        print(f"  [{sheet_name}] Added column: {date}")

    print(f"  '{sheet_name}' updated with {len(new_dates)} new date(s).")


def main():
    creds = Credentials.from_service_account_file(
        CREDENTIALS_FILE,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)

    # ── Standard folder uploads ────────────────────────────────────────────────
    for folder, (tab_name, excel_sheet, skiprows) in FOLDER_MAP.items():
        folder_path = os.path.join(DATA_ROOT, folder)
        file_path = get_latest_file(folder_path)
        if not file_path:
            print(f"No file found in {folder}/ — skipping.")
            continue
        print(f"Reading: {os.path.basename(file_path)} -> '{tab_name}'")
        try:
            df = read_file(file_path, sheet_name=excel_sheet, skiprows=skiprows)
            # Format Order date as MM/DD so PO list trend SUMIFS can match against date headers
            if tab_name.startswith("Line Items") and "Order date" in df.columns:
                df["Order date"] = pd.to_datetime(
                    df["Order date"].astype(str), errors="coerce"
                ).dt.strftime("%m/%d").fillna("")
            url_col = 6 if tab_name == "Top 100" else None
            if tab_name == "Top 100" and file_path.endswith(".xlsx"):
                urls = extract_hyperlinks_from_xlsx(file_path, col_idx=url_col, skiprows=skiprows)
                url_col_name = df.columns[url_col]
                df[url_col_name] = urls[:len(df)]
            try:
                ws = sh.worksheet(tab_name)
            except gspread.exceptions.WorksheetNotFound:
                ws = sh.add_worksheet(title=tab_name, rows=10000, cols=len(df.columns))
                print(f"  Created new tab '{tab_name}'")
            upload_to_sheet(ws, df, url_col_idx=url_col)
            print(f"  Uploaded {len(df)} rows x {len(df.columns)} cols to '{tab_name}'")
        except Exception as e:
            print(f"  Failed: {e}")

    # ── Sales upload (append-only) ─────────────────────────────────────────────
    print(f"\nProcessing sales files -> '{SALES_RAW_SHEET}'")
    try:
        upload_sales(sh)
    except Exception as e:
        print(f"  Sales upload failed: {e}")

    # ── Traffic upload (append-only) ───────────────────────────────────────────
    print(f"\nProcessing traffic files -> '{TRAFFIC_RAW_SHEET}'")
    try:
        upload_traffic(sh)
    except Exception as e:
        print(f"  Traffic upload failed: {e}")

    # ── Fix all PO list trend formulas (corrects column refs + date format) ──────
    print("\nRebuilding PO list trend formulas...")
    try:
        rebuild_po_list_trend(sh)
    except Exception as e:
        print(f"  PO list trend rebuild failed: {e}")

    # ── Trend sheet column updates ─────────────────────────────────────────────
    print("\nUpdating trend sheets...")
    for trend_name in ["Traffic trend", "Unit Sold trend", "Revenue trend", "ASP trend", "CVR trend", "PO list trend"]:
        try:
            update_trend_sheet(sh, trend_name)
        except Exception as e:
            print(f"  '{trend_name}' update failed: {e}")

    print("\nAll done!")


if __name__ == "__main__":
    main()
