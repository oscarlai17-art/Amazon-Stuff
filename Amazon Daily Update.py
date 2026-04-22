"""
Amazon Vendor Data -> Google Sheets Uploader
=============================================
Reads the latest file from each Amazon-Data subfolder and uploads
it to the corresponding tab in Google Sheets.

Folder -> Sheet mapping:
    purchase-orders/    -> "Line Items April"
    Last 2 days Sales/  -> "Last 2 days"
    inventory/          -> "Inventory raw"
    traffic/            -> (add your sheet tab name here)

Usage:
    python "Amazon Daily Update.py"

Requirements:
    pip install gspread google-auth pandas xlrd openpyxl
"""

import os
import glob
import numpy as np
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# ── Config ─────────────────────────────────────────────────────────────────────
CREDENTIALS_FILE = "C:/Users/makep/Downloads/amazon-494102-3bd915b4a36e.json"
SPREADSHEET_ID   = "1zhlqL2tqKvI70h0OQ_V46erwwLA9ztp0PjkJ3B7BgSI"
DATA_ROOT        = "C:/Users/makep/Documents/Amazon-Data"

# Map each subfolder to: (sheet_tab_name, excel_sheet_name_or_None)
# excel_sheet_name: name of the tab inside the Excel/XLS file to read (None = first sheet)
FOLDER_MAP = {
    "purchase-orders":   ("Line Items April", "Line Items"),
    "Last 2 days Sales": ("Last 2 days",      None),
    "inventory":         ("Inventory raw",    None),
}


def get_latest_file(folder_path: str) -> str | None:
    files = (
        glob.glob(os.path.join(folder_path, "*.csv")) +
        glob.glob(os.path.join(folder_path, "*.xls")) +
        glob.glob(os.path.join(folder_path, "*.xlsx"))
    )
    if not files:
        return None
    return max(files, key=os.path.getmtime)


def read_file(file_path: str, sheet_name=None) -> pd.DataFrame:
    ext = os.path.splitext(file_path)[1].lower()
    if ext == ".csv":
        return pd.read_csv(file_path)
    else:
        kwargs = {"sheet_name": sheet_name} if sheet_name else {"sheet_name": 0}
        return pd.read_excel(file_path, **kwargs)


def clean_value(val):
    """Convert a cell value to a Google Sheets compatible type, preserving numbers."""
    if val is None:
        return ""
    if isinstance(val, float) and np.isnan(val):
        return ""
    if isinstance(val, np.integer):
        return int(val)
    if isinstance(val, np.floating):
        return float(val)
    return val


def upload_to_sheet(ws: gspread.Worksheet, df: pd.DataFrame):
    headers = df.columns.tolist()
    rows = [
        [clean_value(cell) for cell in row]
        for row in df.itertuples(index=False, name=None)
    ]
    ws.clear()
    ws.update([headers] + rows)


def main():
    creds = Credentials.from_service_account_file(
        CREDENTIALS_FILE,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)

    for folder, (tab_name, excel_sheet) in FOLDER_MAP.items():
        folder_path = os.path.join(DATA_ROOT, folder)
        file_path = get_latest_file(folder_path)

        if not file_path:
            print(f"No file found in {folder}/ — skipping.")
            continue

        print(f"Reading: {os.path.basename(file_path)} -> '{tab_name}'")
        try:
            df = read_file(file_path, sheet_name=excel_sheet)
            ws = sh.worksheet(tab_name)
            upload_to_sheet(ws, df)
            print(f"  Uploaded {len(df)} rows x {len(df.columns)} cols to '{tab_name}'")
        except Exception as e:
            print(f"  Failed: {e}")

    print("\nAll done!")


if __name__ == "__main__":
    main()
