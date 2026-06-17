"""
Proportion Sheet Updater
========================
Reads inventory, PO cost, traffic, and sales data, then writes a structured
GPU -> CPU+RAM tier breakdown (Tier 1 = lowest vendor cost) to the Proportion sheet.
"""

import re
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

CREDENTIALS_FILE = "C:/Users/makep/Documents/Amazon-Stuff/amazon-494102-3bd915b4a36e.json"
SPREADSHEET_ID   = "1zhlqL2tqKvI70h0OQ_V46erwwLA9ztp0PjkJ3B7BgSI"
PROPORTION_SHEET = "Proportion"


# ── Spec parsers ──────────────────────────────────────────────────────────────

def parse_gpu(title):
    m = re.search(
        r'(RTX\s*\d{4}(?:\s*Ti)?(?:\s*\d+GB)?|RX\s*\d{4}[A-Z]*(?:\s*XT)?)',
        title, re.IGNORECASE
    )
    if not m:
        return "Unknown"
    gpu = re.sub(r'\s+', ' ', m.group(1).upper().strip())
    # Normalize spacing e.g. "RTX5060" -> "RTX 5060"
    gpu = re.sub(r'(RTX|RX)(\d)', r'\1 \2', gpu)
    return gpu


def parse_cpu(title):
    patterns = [
        r'(Intel\s+(?:Core\s+)?(?:Ultra\s+)?[i579]\d?[-\s]\d{3,5}[A-Z0-9]*)',
        r'((?:AMD\s+)?Ryzen\s+[579]\s+\d{4}[A-Z0-9]*)',
        r'(Ryzen\s+[579]\s+\d{4}[A-Z0-9]*)',
    ]
    for pat in patterns:
        m = re.search(pat, title, re.IGNORECASE)
        if m:
            cpu = re.sub(r'\s+', ' ', m.group(1).strip())
            cpu = re.sub(r'^AMD\s+', '', cpu, flags=re.IGNORECASE)
            return cpu
    return "Unknown"


def parse_ram(title):
    m = re.search(r'(\d+)\s*GB\s+(DDR[45])', title, re.IGNORECASE)
    if m:
        return f"{m.group(1)}GB {m.group(2).upper()}"
    return "Unknown"


def clean_price(val):
    if not val:
        return None
    try:
        return float(str(val).replace('$', '').replace(',', '').strip())
    except ValueError:
        return None


# ── Main ──────────────────────────────────────────────────────────────────────

def update_proportion():
    creds = Credentials.from_service_account_file(
        CREDENTIALS_FILE,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)

    print("Reading sheets...")

    # ── Inventory ─────────────────────────────────────────────────────────────
    inv_rows = sh.worksheet("Inventory raw").get_all_records()
    inv = pd.DataFrame(inv_rows)[["ASIN", "Product Title", "Model Number",
                                   "Sellable On Hand Units", "Replenishment Code"]]
    inv.columns = ["asin", "title", "sku", "inventory", "status"]
    inv["inventory"] = pd.to_numeric(inv["inventory"], errors="coerce").fillna(0).astype(int)

    # ── Vendor cost from PO Line Items ────────────────────────────────────────
    po_rows = sh.worksheet("Line Items April").get_all_records()
    po = pd.DataFrame(po_rows)[["ASIN", "Cost"]]
    po.columns = ["asin", "cost"]
    po["cost"] = pd.to_numeric(po["cost"], errors="coerce")
    # Take the most recent (last) cost per ASIN
    po_cost = po.dropna(subset=["cost"]).groupby("asin")["cost"].last().reset_index()

    # ── Traffic raw ───────────────────────────────────────────────────────────
    tr_rows = sh.worksheet("Traffic raw").get_all_records()
    tr = pd.DataFrame(tr_rows)
    tr["Page Views"] = pd.to_numeric(tr["Page Views"], errors="coerce").fillna(0)
    dates_sorted = sorted(tr["Date"].unique())
    l7d_dates  = set(dates_sorted[-7:])
    ll7d_dates = set(dates_sorted[-14:-7])
    tr_l7d  = tr[tr["Date"].isin(l7d_dates)].groupby("ASIN")["Page Views"].sum().reset_index()
    tr_ll7d = tr[tr["Date"].isin(ll7d_dates)].groupby("ASIN")["Page Views"].sum().reset_index()
    tr_l7d.columns  = ["asin", "l7d_traffic"]
    tr_ll7d.columns = ["asin", "ll7d_traffic"]

    # ── Sales raw ─────────────────────────────────────────────────────────────
    sl_rows = sh.worksheet("Unit sold raw").get_all_records()
    sl = pd.DataFrame(sl_rows)
    sl["Ordered Units"] = pd.to_numeric(sl["Ordered Units"], errors="coerce").fillna(0)
    sdates_sorted = sorted(sl["Date"].unique())
    sl_l7d_dates  = set(sdates_sorted[-7:])
    sl_ll7d_dates = set(sdates_sorted[-14:-7])
    sl_l7d  = sl[sl["Date"].isin(sl_l7d_dates)].groupby("ASIN")["Ordered Units"].sum().reset_index()
    sl_ll7d = sl[sl["Date"].isin(sl_ll7d_dates)].groupby("ASIN")["Ordered Units"].sum().reset_index()
    sl_l7d.columns  = ["asin", "l7d_units"]
    sl_ll7d.columns = ["asin", "ll7d_units"]

    # ── Join everything ───────────────────────────────────────────────────────
    df = inv.merge(po_cost, on="asin", how="left")
    df = df.merge(tr_l7d,  on="asin", how="left")
    df = df.merge(tr_ll7d, on="asin", how="left")
    df = df.merge(sl_l7d,  on="asin", how="left")
    df = df.merge(sl_ll7d, on="asin", how="left")
    df[["l7d_traffic","ll7d_traffic","l7d_units","ll7d_units"]] = \
        df[["l7d_traffic","ll7d_traffic","l7d_units","ll7d_units"]].fillna(0).astype(int)

    # ── Parse specs ───────────────────────────────────────────────────────────
    df["gpu"] = df["title"].apply(parse_gpu)
    df["cpu"] = df["title"].apply(parse_cpu)
    df["ram"] = df["title"].apply(parse_ram)
    df["active"] = df["status"].str.upper().str.contains("NP|ACTIVE|^$", na=True)

    # ── Group by GPU + CPU + RAM ───────────────────────────────────────────────
    grp = df.groupby(["gpu", "cpu", "ram"], as_index=False).agg(
        sku_count   = ("asin",        "count"),
        min_cost    = ("cost",        "min"),
        median_cost = ("cost",        "median"),
        max_cost    = ("cost",        "max"),
        l7d_traffic = ("l7d_traffic", "sum"),
        ll7d_traffic= ("ll7d_traffic","sum"),
        l7d_units   = ("l7d_units",   "sum"),
        ll7d_units  = ("ll7d_units",  "sum"),
        inventory   = ("inventory",   "sum"),
    )

    # ── Assign tiers within each GPU group (by median cost) ───────────────────
    def assign_tiers(sub):
        sub = sub.sort_values("median_cost", na_position="last").reset_index(drop=True)
        n = len(sub)
        if n == 1:
            sub["tier"] = "Tier 1"
        elif n == 2:
            sub["tier"] = ["Tier 1", "Tier 3"]
        else:
            labels = []
            for i in range(n):
                pct = i / (n - 1)
                if pct < 0.4:
                    labels.append("Tier 1")
                elif pct < 0.7:
                    labels.append("Tier 2")
                else:
                    labels.append("Tier 3")
            sub["tier"] = labels
        return sub

    tier_frames = []
    for gpu_name, sub in grp.groupby("gpu"):
        tier_frames.append(assign_tiers(sub.copy()))
    grp = pd.concat(tier_frames, ignore_index=True)

    # Sort: GPU groups by total L7D traffic desc, combos by median cost asc
    gpu_order = (grp.groupby("gpu")["l7d_traffic"].sum()
                    .sort_values(ascending=False).index.tolist())
    grp["_gpu_rank"] = grp["gpu"].map({g: i for i, g in enumerate(gpu_order)})
    grp = grp.sort_values(["_gpu_rank", "median_cost"]).drop(columns="_gpu_rank")

    # ── Format dif % ──────────────────────────────────────────────────────────
    def pct_dif(a, b):
        if b == 0:
            return "" if a == 0 else "N/A"
        return f"{(a - b) / b * 100:.1f}%"

    # ── Build output rows ─────────────────────────────────────────────────────
    headers = [
        "GPU", "CPU", "RAM", "SKU Count", "Tier",
        "Min Cost", "Median Cost", "Max Cost",
        "L7D Traffic", "LL7D Traffic", "Traffic Dif",
        "L7D Units", "LL7D Units", "Unit Sold Dif",
        "Inventory"
    ]

    out = [headers]
    last_gpu = None
    for _, row in grp.iterrows():
        gpu_label = row["gpu"] if row["gpu"] != last_gpu else ""
        last_gpu = row["gpu"]

        def fmt_cost(v):
            return f"${v:,.2f}" if pd.notna(v) else ""

        out.append([
            gpu_label,
            row["cpu"],
            row["ram"],
            int(row["sku_count"]),
            row["tier"],
            fmt_cost(row["min_cost"]),
            fmt_cost(row["median_cost"]),
            fmt_cost(row["max_cost"]),
            int(row["l7d_traffic"]),
            int(row["ll7d_traffic"]),
            pct_dif(row["l7d_traffic"], row["ll7d_traffic"]),
            int(row["l7d_units"]),
            int(row["ll7d_units"]),
            pct_dif(row["l7d_units"], row["ll7d_units"]),
            int(row["inventory"]),
        ])

    # ── Write to sheet ────────────────────────────────────────────────────────
    ws = sh.worksheet(PROPORTION_SHEET)
    ws.clear()
    ws.update(out, value_input_option="RAW")
    print(f"Proportion sheet updated: {len(out) - 1} GPU+CPU+RAM combinations.")


if __name__ == "__main__":
    update_proportion()
