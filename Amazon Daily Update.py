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
import numpy as np
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# ── Config ─────────────────────────────────────────────────────────────────────
CREDENTIALS_FILE = "C:/Users/makep/Downloads/amazon-494102-3bd915b4a36e.json"
SPREADSHEET_ID   = "1zhlqL2tqKvI70h0OQ_V46erwwLA9ztp0PjkJ3B7BgSI"
DATA_ROOT        = "C:/Users/makep/Documents/Amazon-Data"

FOLDER_MAP = {
    "purchase-orders":   ("Line Items April", "Line Items", 0),
    "Last 2 days Sales": ("Last 2 days",      None,         1),
    "inventory":         ("Inventory raw",    None,         1),
    "Top 100":           ("Top 100",          None,         0),
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


def upload_to_sheet(ws, df):
    headers = df.columns.tolist()
    rows = [
        [clean_value(cell) for cell in row]
        for row in df.itertuples(index=False, name=None)
    ]
    ws.clear()
    ws.update([headers] + rows)



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
            rows.append([date_str, asin, int(views) if pd.notna(views) else 0])

        if rows:
            raw_ws.append_rows(rows, value_input_option="RAW")
            existing_dates.add(date_str)
            print(f"  Appended {len(rows)} rows for {date_str} to '{TRAFFIC_RAW_SHEET}'")

    print("  Traffic raw up to date.")


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
            ws = sh.worksheet(tab_name)
            upload_to_sheet(ws, df)
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

    print("\nAll done!")


if __name__ == "__main__":
    main()
