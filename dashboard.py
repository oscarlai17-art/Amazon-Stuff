"""
Amazon Gaming PC — Main Sales Dashboard
Run with: streamlit run dashboard.py
"""
import gspread
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from google.oauth2.service_account import Credentials

st.set_page_config(
    page_title="Amazon Gaming PC Dashboard",
    page_icon="🖥️",
    layout="wide",
)

CREDS    = "C:/Users/makep/Downloads/amazon-494102-3bd915b4a36e.json"
SHEET_ID = "1zhlqL2tqKvI70h0OQ_V46erwwLA9ztp0PjkJ3B7BgSI"

def gc_connect():
    creds = Credentials.from_service_account_file(
        CREDS, scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
    )
    return gspread.authorize(creds)

def to_num(v, default=0.0):
    try:
        return float(str(v).replace(",", "").replace("$", "").replace("%", "").strip())
    except:
        return default

# ── Load data ─────────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_all():
    import re
    from datetime import datetime, timedelta

    gc = gc_connect()
    sh = gc.open_by_key(SHEET_ID)

    # 1. Weekly dashboard — current-week KPIs (source of truth)
    wd_rows  = sh.worksheet("Weekly dashboard").get_all_values()
    cur_cw   = wd_rows[1][1]   # e.g. "05/30-06/05"
    cur_pw   = wd_rows[1][2]
    kpis = {}
    label_map = {
        "Total Revenue":   "revenue",  "Total Traffic":   "traffic",
        "Total Unit sold": "units",    "Overal CVR":      "cvr",
        "ASP":             "asp",      "Clicks":          "ad_clicks",
        "Spend":           "ad_spend", "Revenue":         "ad_revenue",
        "Acos":            "acos",
    }
    for row in wd_rows:
        key = row[0].strip()
        if key in label_map:
            kpis[label_map[key]] = {"cw": to_num(row[1]), "pw": to_num(row[2]), "wow": to_num(row[3])}

    # 2. WoW Dashboard — per-SKU verified data (current week)
    ws_wow   = sh.worksheet("WoW Dashboard")
    wow_raw  = ws_wow.get_all_values()
    wow_hdrs = wow_raw[0]
    df = pd.DataFrame(wow_raw[1:], columns=wow_hdrs)
    df = df[df["SKU"].str.strip() != ""].copy()
    num_skip = {"SKU","ASIN","CPU","GPU","RAM","Notes","Tier","Sellable + Open PO","URL"}
    for col in wow_hdrs:
        if col not in num_skip:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(",","").str.replace("%","").str.replace("$",""),
                errors="coerce")

    cw_units_col = next((h for h in wow_hdrs if h.startswith("CW Units Total")), None)
    pw_units_col = next((h for h in wow_hdrs if h.startswith("PW Units Total")), None)
    cw_traf_col  = next((h for h in wow_hdrs if h.startswith("CW Traffic Total")), None)
    pw_traf_col  = next((h for h in wow_hdrs if h.startswith("PW Traffic Total")), None)
    cw_uorg_col  = next((h for h in wow_hdrs if h.startswith("CW Units Organic")), None)
    cw_upaid_col = next((h for h in wow_hdrs if h.startswith("CW Units Paid")), None)
    cw_torg_col  = next((h for h in wow_hdrs if h.startswith("CW Traffic Organic")), None)
    cw_tpaid_col = next((h for h in wow_hdrs if h.startswith("CW Traffic Paid")), None)
    pw_torg_col  = next((h for h in wow_hdrs if h.startswith("PW Traffic Organic")), None)
    pw_tpaid_col = next((h for h in wow_hdrs if h.startswith("PW Traffic Paid")), None)
    df["Rev_CW"] = df[cw_units_col] * df["MSRP"]
    df["Rev_PW"] = df[pw_units_col] * df["MSRP"]

    cols = {"cw_units": cw_units_col, "pw_units": pw_units_col,
            "cw_traf": cw_traf_col,   "pw_traf":  pw_traf_col,
            "cw_uorg": cw_uorg_col,   "cw_upaid": cw_upaid_col,
            "cw_torg": cw_torg_col,   "cw_tpaid": cw_tpaid_col,
            "pw_torg": pw_torg_col,   "pw_tpaid": pw_tpaid_col}

    # 3. Unit Sold trend + Traffic trend — daily data for date picker
    #    Filtered to verified SKU set (WoW Dashboard) so totals are accurate
    valid_skus = set(df["SKU"].str.strip())

    def load_trend_daily(tab):
        ws    = sh.worksheet(tab)
        hdrs  = ws.row_values(1)
        rows  = ws.get_all_values()[1:]
        # date cols only (MM/DD format, exclude L7D LL7D Highest etc.)
        dcols = [h for h in hdrs if re.match(r"^\d{2}/\d{2}$", h)]
        data  = []
        for r in rows:
            if not r[0].strip() or r[0].strip() not in valid_skus:
                continue
            entry = {"SKU": r[0].strip(), "GPU": r[3].strip() if len(r) > 3 else ""}
            for d in dcols:
                entry[d] = to_num(r[hdrs.index(d)]) if hdrs.index(d) < len(r) else 0.0
            data.append(entry)
        return pd.DataFrame(data), dcols

    unit_daily, date_cols = load_trend_daily("Unit Sold trend")
    traf_daily, traf_dcols = load_trend_daily("Traffic trend")

    # Parse date cols to real dates (assume current year, roll back if future)
    def parse_date(s):
        for yr in [2026, 2025]:
            try:
                d = datetime.strptime(f"{s}/{yr}", "%m/%d/%Y")
                if d <= datetime.now():
                    return d
            except:
                pass
        return None

    # Cap traffic data to the latest unit sold date.
    # Amazon unit sold reports often lag traffic by 1–2 days; keeping them
    # in sync ensures every week's metrics cover the exact same date range.
    if date_cols:
        _unit_dts  = [parse_date(d) for d in date_cols if parse_date(d)]
        _max_unit  = max(_unit_dts) if _unit_dts else None
        if _max_unit:
            _extra_traf = [d for d in traf_dcols
                           if parse_date(d) and parse_date(d) > _max_unit]
            if _extra_traf:
                traf_daily = traf_daily.drop(
                    columns=[c for c in _extra_traf if c in traf_daily.columns])

    parsed = [(d, parse_date(d)) for d in date_cols if parse_date(d)]
    parsed = sorted(parsed, key=lambda x: x[1])

    # Build list of selectable 7-day weeks (most recent first)
    weeks = []
    if parsed:
        all_dates = [p[1] for p in parsed]
        end       = all_dates[-1]
        while end - timedelta(days=6) >= all_dates[0]:
            start     = end - timedelta(days=6)
            cw_dcols  = [p[0] for p in parsed if start <= p[1] <= end]
            pw_dcols  = [p[0] for p in parsed if start - timedelta(days=7) <= p[1] < start]
            if cw_dcols:
                label = f"{start.strftime('%m/%d')}-{end.strftime('%m/%d')}"
                weeks.append({"label": label, "cw": cw_dcols, "pw": pw_dcols})
            end -= timedelta(days=7)

    # MSRP lookup per SKU for revenue estimate
    msrp_map = df.set_index("SKU")["MSRP"].to_dict()

    # 4. Ads product CW / PW — per-ASIN ad clicks, spend, sales
    asin_to_sku = df.set_index("ASIN")["SKU"].to_dict()

    def load_ads(tab):
        try:
            ws   = sh.worksheet(tab)
            rows = ws.get_all_values()
            hdrs = rows[0]
            ad   = pd.DataFrame(rows[1:], columns=hdrs)
            ad   = ad[ad["Product ID"].str.strip() != ""].copy()
            for col in ["Clicks", "Ad spend", "Ad sales", "Ad units sold", "Impressions"]:
                if col in ad.columns:
                    ad[col] = pd.to_numeric(
                        ad[col].astype(str).str.replace(",", ""), errors="coerce"
                    ).fillna(0)
            agg = ad.groupby("Product ID")[["Clicks", "Ad spend", "Ad sales", "Ad units sold"]].sum().reset_index()
            agg.rename(columns={"Product ID": "ASIN"}, inplace=True)
            agg["SKU"] = agg["ASIN"].map(asin_to_sku)
            return agg
        except Exception:
            return pd.DataFrame(columns=["ASIN", "SKU", "Clicks", "Ad spend", "Ad sales", "Ad units sold"])

    ads_cw = load_ads("Ads product CW")
    ads_pw = load_ads("Ads product PW")

    return df, kpis, cur_cw, cur_pw, cols, unit_daily, traf_daily, weeks, msrp_map, ads_cw, ads_pw

df, kpis, CW, PW, C, unit_daily, traf_daily, WEEKS, MSRP_MAP, ads_cw, ads_pw = load_all()

@st.cache_data(ttl=300)
def load_inventory():
    gc = gc_connect()
    sh = gc.open_by_key(SHEET_ID)

    # Inventory by Status — sellable, aging, open PO per SKU
    ws  = sh.worksheet("Inventory by Status")
    raw = ws.get_all_values()
    inv = pd.DataFrame(raw[1:], columns=raw[0])
    inv = inv[inv["SKU"].str.strip() != ""].copy()
    for col in ["Skytech Available","AMZ Open PO","AMZ sellable","AMZ aging",
                "L7D sell-thru","LL7D sell thru","Trafic L7D","Inv weeks","SUM of L2D sales"]:
        if col in inv.columns:
            inv[col] = pd.to_numeric(inv[col].astype(str).str.replace(",",""), errors="coerce")

    # Inventory health report — summary KPIs
    ws2   = sh.worksheet("Inventory health report")
    ih    = ws2.get_all_values()
    health = {}
    for row in ih:
        if row[0].strip() in ("Aging","Non-aging","Total"):
            health[row[0].strip()] = {
                "value": row[1], "units": to_num(row[3]),
                "runrate_7d": to_num(row[5]), "runrate_14d": to_num(row[7]),
                "wow": row[9],
            }

    # PO lines — read directly from local purchase-orders folder
    import glob as _glob
    PO_FOLDER = "C:/Users/makep/Documents/Amazon-Data/purchase-orders"
    po_files  = sorted(
        _glob.glob(f"{PO_FOLDER}/*.xls") + _glob.glob(f"{PO_FOLDER}/*.xlsx"),
        key=lambda f: f,
        reverse=True,   # most recent file name first
    )
    po_frames = []
    for fpath in po_files:
        try:
            po = pd.read_excel(fpath, sheet_name=0)
            po_frames.append(po)
        except Exception:
            pass

    po = pd.concat(po_frames, ignore_index=True) if po_frames else pd.DataFrame()
    if not po.empty:
        # Merchant SKU is blank in Amazon exports — fall back to Model number
        if "Merchant SKU" in po.columns and "Model number" in po.columns:
            po["Merchant SKU"] = (
                po["Merchant SKU"].astype(str).str.strip()
                .replace("", pd.NA)
                .fillna(po["Model number"].astype(str).str.strip())
            )
        # Numeric quantity / cost columns
        for col in ["Requested quantity","Accepted quantity","Remaining quantity",
                    "Received quantity","Cancelled quantity","Total accepted cost"]:
            if col in po.columns:
                po[col] = pd.to_numeric(po[col].astype(str).str.replace(",",""), errors="coerce").fillna(0)
        # Clean up ISO datetime strings to plain dates
        for col in ["Window start","Window end","Expected date","Order date"]:
            if col in po.columns:
                po[col] = pd.to_datetime(po[col], errors="coerce").dt.strftime("%m/%d/%Y").fillna("")
        # Keep only lines with remaining quantity > 0
        if "Remaining quantity" in po.columns:
            po = po[po["Remaining quantity"] > 0]

    return inv, health, po

inv_df, inv_health, po_df = load_inventory()

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🖥️ Gaming PC Dashboard")
    st.markdown("---")
    gpu_opts = sorted(df["GPU"].dropna().unique())
    sel_gpu  = st.multiselect("GPU", gpu_opts, default=list(gpu_opts))
    st.markdown("---")

    msrp_min = int(df["MSRP"].dropna().min() or 0)
    msrp_max = int(df["MSRP"].dropna().max() or 9999)
    st.markdown("**MSRP Range**")
    msrp_range = st.slider(
        "MSRP ($)",
        min_value=msrp_min,
        max_value=msrp_max,
        value=(msrp_min, msrp_max),
        step=50,
        format="$%d",
        label_visibility="collapsed",
    )
    st.caption(f"${msrp_range[0]:,} – ${msrp_range[1]:,}")

    st.markdown("---")
    tier_opts = sorted([x for x in df["Tier"].dropna().unique() if str(x).strip()])
    sel_tier  = st.multiselect("Tier", tier_opts, default=list(tier_opts)) if tier_opts else []

    st.markdown("---")
    # Week picker
    week_labels = [w["label"] for w in WEEKS]
    st.markdown("**Week**")
    sel_week_label = st.selectbox("Week", week_labels, index=0, label_visibility="collapsed")
    sel_week = next(w for w in WEEKS if w["label"] == sel_week_label)
    is_current_week = (sel_week_label == week_labels[0])

    st.markdown("---")
    if st.button("🔄 Refresh data"):
        st.cache_data.clear()
        st.rerun()
    st.caption(f"CW: **{sel_week_label}**")
    st.caption("KPIs & breakdowns from trend sheets × MSRP.\nReflects active filters.")

# ── Apply filters + compute metrics for selected week ─────────────────────────
mask = df["GPU"].isin(sel_gpu)
if sel_tier:
    mask &= df["Tier"].isin(sel_tier)
mask &= df["MSRP"].between(msrp_range[0], msrp_range[1], inclusive="both") | df["MSRP"].isna()
dff = df[mask].copy()


# KPI totals: always compute from trend data for the filtered SKU set
# so all numbers (cards + charts) reflect the active GPU / MSRP / Tier filters.
if True:
    _cw_d = sel_week["cw"]
    _pw_d = sel_week["pw"]
    _skus = set(dff["SKU"].str.strip())

    def week_sum(trend_df, dcols, skus):
        sub = trend_df[trend_df["SKU"].isin(skus)]
        cols_present = [c for c in dcols if c in sub.columns]
        return sub[cols_present].sum().sum() if cols_present else 0

    cw_units = week_sum(unit_daily, _cw_d, _skus)
    pw_units = week_sum(unit_daily, _pw_d, _skus)
    cw_traf  = week_sum(traf_daily, _cw_d, _skus)
    pw_traf  = week_sum(traf_daily, _pw_d, _skus)

    def week_rev(trend_df, dcols, skus):
        sub = trend_df[trend_df["SKU"].isin(skus)].copy()
        cols_present = [c for c in dcols if c in sub.columns]
        if not cols_present:
            return 0
        sub["_units"] = sub[cols_present].sum(axis=1)
        sub["_msrp"]  = sub["SKU"].map(MSRP_MAP).fillna(0)
        return (sub["_units"] * sub["_msrp"]).sum()

    cw_rev = week_rev(unit_daily, _cw_d, _skus)
    pw_rev = week_rev(unit_daily, _pw_d, _skus)

    def pct(a, b): return (a - b) / b * 100 if b else 0
    asp_cw = cw_rev / cw_units if cw_units else 0
    asp_pw = pw_rev / pw_units if pw_units else 0
    cvr_cw = cw_units / cw_traf * 100 if cw_traf else 0
    cvr_pw = pw_units / pw_traf * 100 if pw_traf else 0

    # Ads totals filtered to active SKU set
    _ads_cw_f = ads_cw[ads_cw["SKU"].isin(_skus)] if not ads_cw.empty else ads_cw
    _ads_pw_f = ads_pw[ads_pw["SKU"].isin(_skus)] if not ads_pw.empty else ads_pw
    ad_clicks_cw  = _ads_cw_f["Clicks"].sum()        if not _ads_cw_f.empty else 0
    ad_clicks_pw  = _ads_pw_f["Clicks"].sum()         if not _ads_pw_f.empty else 0
    ad_spend_cw   = _ads_cw_f["Ad spend"].sum()       if not _ads_cw_f.empty else 0
    ad_spend_pw   = _ads_pw_f["Ad spend"].sum()        if not _ads_pw_f.empty else 0
    ad_sales_cw   = _ads_cw_f["Ad sales"].sum()       if not _ads_cw_f.empty else 0
    ad_sales_pw   = _ads_pw_f["Ad sales"].sum()        if not _ads_pw_f.empty else 0
    acos_cw = ad_spend_cw / ad_sales_cw * 100 if ad_sales_cw else 0
    acos_pw = ad_spend_pw / ad_sales_pw * 100 if ad_sales_pw else 0
    roas_cw = ad_sales_cw / ad_spend_cw if ad_spend_cw else 0
    roas_pw = ad_sales_pw / ad_spend_pw if ad_spend_pw else 0

    active_kpis = {
        "revenue":   {"cw": cw_rev,       "pw": pw_rev,       "wow": pct(cw_rev,      pw_rev)},
        "units":     {"cw": cw_units,      "pw": pw_units,      "wow": pct(cw_units,    pw_units)},
        "traffic":   {"cw": cw_traf,       "pw": pw_traf,       "wow": pct(cw_traf,     pw_traf)},
        "asp":       {"cw": asp_cw,        "pw": asp_pw,        "wow": pct(asp_cw,      asp_pw)},
        "cvr":       {"cw": cvr_cw,        "pw": cvr_pw,        "wow": pct(cvr_cw,      cvr_pw)},
        "ad_clicks": {"cw": ad_clicks_cw,  "pw": ad_clicks_pw,  "wow": pct(ad_clicks_cw, ad_clicks_pw)},
        "ad_spend":  {"cw": ad_spend_cw,   "pw": ad_spend_pw,   "wow": pct(ad_spend_cw,  ad_spend_pw)},
        "ad_revenue":{"cw": ad_sales_cw,   "pw": ad_sales_pw,   "wow": pct(ad_sales_cw,  ad_sales_pw)},
        "acos":      {"cw": acos_cw,       "pw": acos_pw,       "wow": acos_cw - acos_pw},
        "roas":      {"cw": roas_cw,       "pw": roas_pw,       "wow": roas_cw - roas_pw},
    }

# Always rebuild per-SKU dff from trend sheets so every GPU comparison chart
# compares the selected week against its immediately preceding week.
_cw_dcols = sel_week["cw"]
_pw_dcols  = sel_week["pw"]
_valid_skus = set(dff["SKU"].str.strip())

_sub_u = unit_daily[unit_daily["SKU"].isin(_valid_skus)].copy()
_cw_p  = [c for c in _cw_dcols if c in _sub_u.columns]
_pw_p  = [c for c in _pw_dcols if c in _sub_u.columns]
_sub_u["_cw_units"] = _sub_u[_cw_p].sum(axis=1) if _cw_p else 0
_sub_u["_pw_units"] = _sub_u[_pw_p].sum(axis=1) if _pw_p else 0

_sub_t = traf_daily[traf_daily["SKU"].isin(_valid_skus)].copy()
_cw_pt = [c for c in _cw_dcols if c in _sub_t.columns]
_pw_pt = [c for c in _pw_dcols if c in _sub_t.columns]
_sub_t["_cw_traf"] = _sub_t[_cw_pt].sum(axis=1) if _cw_pt else 0
_sub_t["_pw_traf"] = _sub_t[_pw_pt].sum(axis=1) if _pw_pt else 0

_extra_cols = [c for c in ["Margin NS"] if c in dff.columns]
hist = dff[["SKU","GPU","CPU","RAM","ASIN","MSRP","Margin DS","Tier"] + _extra_cols].copy()
hist = hist.merge(_sub_u[["SKU","_cw_units","_pw_units"]], on="SKU", how="left")
hist = hist.merge(_sub_t[["SKU","_cw_traf","_pw_traf"]],  on="SKU", how="left")
for _col in ["_cw_units","_pw_units","_cw_traf","_pw_traf"]:
    hist[_col] = hist[_col].fillna(0)
hist["Rev_CW"] = hist["_cw_units"] * hist["MSRP"]
hist["Rev_PW"] = hist["_pw_units"] * hist["MSRP"]
hist["WoW Units Total %"]   = ((hist["_cw_units"] - hist["_pw_units"]) / hist["_pw_units"].replace(0, pd.NA)) * 100
hist["WoW Traffic Total %"] = ((hist["_cw_traf"]  - hist["_pw_traf"])  / hist["_pw_traf"].replace(0, pd.NA))  * 100
hist[C["cw_units"]] = hist["_cw_units"]
hist[C["pw_units"]] = hist["_pw_units"]
hist[C["cw_traf"]]  = hist["_cw_traf"]
hist[C["pw_traf"]]  = hist["_pw_traf"]
# For the current week, pull organic/paid counts + WoW % columns from WoW Dashboard.
# For historical weeks these columns don't exist in trend data so default to 0.
if is_current_week:
    _split_cols = [c for c in [C["cw_uorg"], C["cw_upaid"], C["cw_torg"], C["cw_tpaid"],
                                C["pw_torg"], C["pw_tpaid"]] if c and c in df.columns]
    _wow_extra  = [c for c in ["WoW Units Organic %", "WoW Traffic Organic %",
                                "WoW Units Paid %",   "WoW Traffic Paid %"] if c in df.columns]
    _carry = _split_cols + _wow_extra
    if _carry:
        hist = hist.merge(df[["SKU"] + _carry], on="SKU", how="left")
        for _c in _carry:
            hist[_c] = hist[_c].fillna(0)
    for _c in [C["cw_uorg"], C["cw_upaid"], C["cw_torg"], C["cw_tpaid"],
               C["pw_torg"], C["pw_tpaid"]]:
        if _c and _c not in hist.columns:
            hist[_c] = 0
else:
    hist[C["cw_uorg"]]  = 0
    hist[C["cw_upaid"]] = 0
    hist[C["cw_torg"]]  = 0
    hist[C["cw_tpaid"]] = 0
    if C["pw_torg"]:  hist[C["pw_torg"]]  = 0
    if C["pw_tpaid"]: hist[C["pw_tpaid"]] = 0

# Merge per-SKU ads data so top-table and future charts can show ad metrics
if not ads_cw.empty:
    _ads_cw_skus = ads_cw[["SKU","Clicks","Ad spend","Ad sales"]].rename(
        columns={"Clicks":"Ad Clicks CW","Ad spend":"Ad Spend CW","Ad sales":"Ad Sales CW"})
    hist = hist.merge(_ads_cw_skus, on="SKU", how="left")
    for _c in ["Ad Clicks CW","Ad Spend CW","Ad Sales CW"]:
        hist[_c] = hist[_c].fillna(0)
if not ads_pw.empty:
    _ads_pw_skus = ads_pw[["SKU","Clicks","Ad spend","Ad sales"]].rename(
        columns={"Clicks":"Ad Clicks PW","Ad spend":"Ad Spend PW","Ad sales":"Ad Sales PW"})
    hist = hist.merge(_ads_pw_skus, on="SKU", how="left")
    for _c in ["Ad Clicks PW","Ad Spend PW","Ad Sales PW"]:
        hist[_c] = hist[_c].fillna(0)

dff = hist.copy()

CW_LABEL = sel_week_label
_pw_start = sel_week["pw"]
PW_LABEL  = _pw_start[0] + "-" + _pw_start[-1] if _pw_start else "prev week"

tab_sales, tab_inv = st.tabs(["📊 Sales", "📦 Inventory"])

with tab_sales:
    # ── Header ────────────────────────────────────────────────────────────────────
    st.markdown(f"""
    <div style="background:linear-gradient(90deg,#1a2e5a,#2e6ea6);padding:18px 28px;border-radius:10px;margin-bottom:8px;">
      <h2 style="color:white;margin:0;font-size:26px;">🖥️ Amazon Gaming PC — Weekly Performance</h2>
      <p style="color:#b8d9f5;margin:5px 0 0;font-size:14px;">
        Current week: <b>{CW_LABEL}</b> &nbsp;·&nbsp; vs Previous week: <b>{PW_LABEL}</b>
        &nbsp;·&nbsp; {len(dff)} SKUs
      </p>
    </div>
    """, unsafe_allow_html=True)
    st.markdown("")

    # ── KPI Row — 5 columns, each with its own dropdown ──────────────────────────
    def kv(key, sub="cw"): return active_kpis.get(key, {}).get(sub, 0)
    def kpct(key): return active_kpis.get(key, {}).get("wow", 0)

    KPI_OPTIONS = {
        "💰 Total Revenue":  {"val": lambda: f"${kv('revenue'):,.0f}",    "delta": lambda: f"{kpct('revenue'):+.1f}% WoW"},
        "📦 Units Sold":     {"val": lambda: f"{kv('units'):,.0f}",       "delta": lambda: f"{kpct('units'):+.1f}% WoW"},
        "👁️ Traffic":        {"val": lambda: f"{kv('traffic'):,.0f}",     "delta": lambda: f"{kpct('traffic'):+.1f}% WoW"},
        "💵 ASP":            {"val": lambda: f"${kv('asp'):,.2f}",        "delta": lambda: f"${kv('asp')-kv('asp','pw'):+.2f} vs PW"},
        "🎯 CVR":            {"val": lambda: f"{kv('cvr'):.2f}%",         "delta": lambda: f"{kv('cvr')-kv('cvr','pw'):+.2f}pp WoW"},
        "📢 Ad Clicks":      {"val": lambda: f"{kv('ad_clicks'):,.0f}",   "delta": lambda: f"{kpct('ad_clicks'):+.1f}% WoW"},
        "💸 Ad Spend":       {"val": lambda: f"${kv('ad_spend'):,.0f}",   "delta": lambda: f"{kpct('ad_spend'):+.1f}% WoW"},
        "📈 Ad Revenue":     {"val": lambda: f"${kv('ad_revenue'):,.0f}", "delta": lambda: f"{kpct('ad_revenue'):+.1f}% WoW"},
        "🎯 ACoS":           {"val": lambda: f"{kv('acos'):.2f}%",        "delta": lambda: f"{kv('acos')-kv('acos','pw'):+.2f}pp WoW"},
        "📊 Ad Spend/Unit":  {"val": lambda: f"${kv('ad_spend')/kv('units') if kv('units') else 0:,.2f}", "delta": lambda: ""},
        "💹 ROAS":           {"val": lambda: f"{kv('roas'):.2f}x",
                              "delta": lambda: f"{kv('roas')-kv('roas','pw'):+.2f} vs PW"},
    }

    KPI_LABELS  = list(KPI_OPTIONS.keys())
    DEFAULTS    = ["💰 Total Revenue", "📦 Units Sold", "👁️ Traffic", "💵 ASP", "🎯 CVR"]

    c1, c2, c3, c4, c5 = st.columns(5)
    kpi_cols = [c1, c2, c3, c4, c5]

    for i, col in enumerate(kpi_cols):
        with col:
            chosen = st.selectbox(
                f"KPI {i+1}",
                KPI_LABELS,
                index=KPI_LABELS.index(DEFAULTS[i]),
                key=f"kpi_slot_{i}",
                label_visibility="collapsed",
            )
            kpi = KPI_OPTIONS[chosen]
            delta_val = kpi["delta"]()
            st.metric(label=chosen, value=kpi["val"](), delta=delta_val if delta_val else None)

    st.markdown("---")

    # ── Daily Trend: Traffic / Units / ASP ───────────────────────────────────────
    from datetime import datetime as _dt, timedelta as _td

    st.markdown("#### Daily Trend")
    _trend_window = st.radio(
        "Window",
        ["Last 30d", "Last 60d", "Last 90d", "All time"],
        index=1,
        horizontal=True,
        key="trend_window",
        label_visibility="collapsed",
    )

    # Build daily totals filtered to active SKU set
    _trend_skus = set(dff["SKU"].str.strip())

    def _parse(s):
        for yr in [2026, 2025]:
            try:
                d = _dt.strptime(f"{s}/{yr}", "%m/%d/%Y")
                if d <= _dt.now():
                    return d
            except Exception:
                pass
        return None

    # All unit date columns sorted by real date
    _all_unit_dcols = sorted(
        [(c, _parse(c)) for c in unit_daily.columns if _parse(c)],
        key=lambda x: x[1]
    )
    _all_traf_dcols = sorted(
        [(c, _parse(c)) for c in traf_daily.columns if _parse(c)],
        key=lambda x: x[1]
    )

    # Apply window filter
    _now = _dt.now()
    _window_days = {"Last 30d": 30, "Last 60d": 60, "Last 90d": 90, "All time": 9999}[_trend_window]
    _cutoff = _now - _td(days=_window_days)
    _unit_range = [(c, d) for c, d in _all_unit_dcols if d >= _cutoff]
    _traf_range = [(c, d) for c, d in _all_traf_dcols if d >= _cutoff]

    # Sum per day across filtered SKUs
    _u_sub = unit_daily[unit_daily["SKU"].isin(_trend_skus)]
    _t_sub = traf_daily[traf_daily["SKU"].isin(_trend_skus)]

    _trend_rows = []
    for col, dt in _unit_range:
        u = _u_sub[col].sum() if col in _u_sub.columns else 0
        # revenue per SKU = units × MSRP
        rev = (_u_sub[col] * _u_sub["SKU"].map(MSRP_MAP).fillna(0)).sum() if col in _u_sub.columns else 0
        asp = rev / u if u else 0
        t_col_match = next((c for c, d in _traf_range if d.date() == dt.date()), None)
        t = _t_sub[t_col_match].sum() if t_col_match and t_col_match in _t_sub.columns else 0
        _trend_rows.append({"date": dt, "units": u, "traffic": t, "asp": asp, "revenue": rev})

    _trend_df = pd.DataFrame(_trend_rows).sort_values("date")

    if not _trend_df.empty:
        from plotly.subplots import make_subplots

        _fig_trend = make_subplots(
            rows=3, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.06,
            subplot_titles=("Traffic (daily)", "Units Sold (daily)", "ASP (daily)"),
        )

        _fig_trend.add_trace(
            go.Scatter(x=_trend_df["date"], y=_trend_df["traffic"],
                       mode="lines", name="Traffic",
                       line=dict(color="#2e6ea6", width=2),
                       fill="tozeroy", fillcolor="rgba(46,110,166,0.08)"),
            row=1, col=1
        )
        _fig_trend.add_trace(
            go.Scatter(x=_trend_df["date"], y=_trend_df["units"],
                       mode="lines", name="Units",
                       line=dict(color="#375623", width=2),
                       fill="tozeroy", fillcolor="rgba(55,86,35,0.08)"),
            row=2, col=1
        )
        _fig_trend.add_trace(
            go.Scatter(x=_trend_df["date"], y=_trend_df["asp"],
                       mode="lines", name="ASP",
                       line=dict(color="#b45309", width=2)),
            row=3, col=1
        )

        _fig_trend.update_layout(
            height=480,
            margin=dict(t=40, b=20, l=10, r=10),
            plot_bgcolor="white",
            showlegend=False,
        )
        _fig_trend.update_yaxes(gridcolor="#f0f0f0")
        _fig_trend.update_yaxes(tickprefix="$", row=3, col=1)
        _fig_trend.update_xaxes(showgrid=False)

        st.plotly_chart(_fig_trend, use_container_width=True)
    else:
        st.info("No daily trend data available for the selected window.")

    st.markdown("---")

    # ── Row 1: Revenue bar + donut ────────────────────────────────────────────────
    r1a, r1b = st.columns([3, 2])

    with r1a:
        rev_dim = st.radio(
            "Group revenue by",
            ["GPU", "Tier", "CPU"],
            horizontal=True,
            key="rev_dim",
            label_visibility="collapsed",
        )
        st.markdown(f"#### Estimated Revenue by {rev_dim} — {CW_LABEL} vs {PW_LABEL}")
        _rev_col = rev_dim if rev_dim in dff.columns else "GPU"
        g = dff.groupby(_rev_col)[["Rev_CW","Rev_PW"]].sum().reset_index()
        g = g[g["Rev_CW"] > 0].sort_values("Rev_CW", ascending=False)
        fig = go.Figure()
        fig.add_bar(name=f"CW ({CW_LABEL})", x=g[_rev_col], y=g["Rev_CW"],
                    marker_color="#1a4e8a",
                    text=g["Rev_CW"].apply(lambda v: f"${v/1000:.0f}k"),
                    textposition="outside")
        fig.add_bar(name=f"PW ({PW_LABEL})", x=g[_rev_col], y=g["Rev_PW"],
                    marker_color="#9dc3e6",
                    text=g["Rev_PW"].apply(lambda v: f"${v/1000:.0f}k"),
                    textposition="outside")
        fig.update_layout(barmode="group", height=340,
                          margin=dict(t=30, b=60, l=10, r=10),
                          plot_bgcolor="white",
                          yaxis=dict(gridcolor="#f0f0f0", tickprefix="$"),
                          xaxis=dict(tickangle=-35, type="category"),
                          legend=dict(orientation="h", y=-0.35))
        st.plotly_chart(fig, use_container_width=True)

    with r1b:
        st.markdown(f"#### CW Revenue Share by {rev_dim}")
        pie = g[g["Rev_CW"] > 0].copy()
        fig2 = px.pie(pie, names=_rev_col, values="Rev_CW", hole=0.42,
                      color_discrete_sequence=px.colors.qualitative.Bold)
        fig2.update_traces(textposition="inside", textinfo="percent+label")
        fig2.update_layout(height=340, margin=dict(t=30, b=10, l=10, r=10), showlegend=False)
        st.plotly_chart(fig2, use_container_width=True)

    # ── Row 2: Units + Organic/Paid ───────────────────────────────────────────────
    r2a, r2b = st.columns(2)

    with r2a:
        units_dim = st.radio(
            "Group units by",
            ["GPU", "Tier", "CPU"],
            horizontal=True,
            key="units_dim",
            label_visibility="collapsed",
        )
        st.markdown(f"#### Units Sold by {units_dim} — CW vs PW")
        _units_col = units_dim if units_dim in dff.columns else "GPU"
        g2 = dff.groupby(_units_col)[[C["cw_units"], C["pw_units"]]].sum().reset_index()
        g2 = g2[g2[C["cw_units"]] > 0].sort_values(C["cw_units"], ascending=False)
        fig3 = go.Figure()
        fig3.add_bar(name="CW", x=g2[_units_col], y=g2[C["cw_units"]],
                     marker_color="#375623",
                     text=g2[C["cw_units"]].apply(lambda v: f"{v:.0f}"),
                     textposition="outside")
        fig3.add_bar(name="PW", x=g2[_units_col], y=g2[C["pw_units"]],
                     marker_color="#a9d18e",
                     text=g2[C["pw_units"]].apply(lambda v: f"{v:.0f}"),
                     textposition="outside")
        fig3.update_layout(barmode="group", height=320,
                           margin=dict(t=30, b=60, l=10, r=10),
                           plot_bgcolor="white",
                           xaxis=dict(tickangle=-35, type="category"),
                           yaxis=dict(gridcolor="#f0f0f0"),
                           legend=dict(orientation="h", y=-0.35))
        st.plotly_chart(fig3, use_container_width=True)

    with r2b:
        clicks_dim = st.radio(
            "Group clicks by",
            ["GPU", "Tier", "CPU"],
            horizontal=True,
            key="clicks_dim",
            label_visibility="collapsed",
        )
        st.markdown(f"#### Organic vs Paid Clicks — {CW_LABEL} vs {PW_LABEL} by {clicks_dim}")
        _clicks_col = clicks_dim if clicks_dim in dff.columns else "GPU"

        # Paid = ad clicks (from Ads product sheets); Organic = total traffic − paid
        _dff_clicks = dff.copy()
        _dff_clicks["_paid_cw"]  = _dff_clicks.get("Ad Clicks CW", 0)
        _dff_clicks["_paid_pw"]  = _dff_clicks.get("Ad Clicks PW", 0)
        _dff_clicks["_total_cw"] = _dff_clicks[C["cw_traf"]].fillna(0)
        _dff_clicks["_total_pw"] = _dff_clicks[C["pw_traf"]].fillna(0)
        _dff_clicks["_org_cw"]   = (_dff_clicks["_total_cw"] - _dff_clicks["_paid_cw"]).clip(lower=0)
        _dff_clicks["_org_pw"]   = (_dff_clicks["_total_pw"] - _dff_clicks["_paid_pw"]).clip(lower=0)

        g3 = _dff_clicks.groupby(_clicks_col)[
            ["_org_cw","_paid_cw","_org_pw","_paid_pw"]
        ].sum().reset_index()
        g3 = g3[(g3["_org_cw"] + g3["_paid_cw"]) > 0]
        g3 = g3.sort_values("_org_cw", ascending=False)

        fig4 = go.Figure()
        fig4.add_bar(name="CW Organic", x=g3[_clicks_col], y=g3["_org_cw"],
                     marker_color="#2e6ea6", offsetgroup="CW", legendgroup="CW")
        fig4.add_bar(name="CW Paid",    x=g3[_clicks_col], y=g3["_paid_cw"],
                     marker_color="#f4b942", offsetgroup="CW", legendgroup="CW",
                     base=g3["_org_cw"])
        fig4.add_bar(name="PW Organic", x=g3[_clicks_col], y=g3["_org_pw"],
                     marker_color="#7ab3d4", offsetgroup="PW", legendgroup="PW")
        fig4.add_bar(name="PW Paid",    x=g3[_clicks_col], y=g3["_paid_pw"],
                     marker_color="#f9d48b", offsetgroup="PW", legendgroup="PW",
                     base=g3["_org_pw"])

        fig4.update_layout(barmode="group", height=340,
                           margin=dict(t=30, b=60, l=10, r=10),
                           plot_bgcolor="white",
                           xaxis=dict(tickangle=-35, type="category"),
                           yaxis=dict(gridcolor="#f0f0f0"),
                           legend=dict(orientation="h", y=-0.35))
        st.plotly_chart(fig4, use_container_width=True)

    # ── WoW Drop by GPU ───────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("#### WoW Change by GPU")

    drop_metric = st.radio(
        "Metric",
        ["Clicks (Traffic)", "Revenue", "Units Sold"],
        horizontal=True,
        key="drop_metric",
        label_visibility="collapsed",
    )

    gpu_grp = dff.groupby("GPU").agg(
        cw_traf  =(C["cw_traf"],  "sum"),
        pw_traf  =(C["pw_traf"],  "sum"),
        cw_units =(C["cw_units"], "sum"),
        pw_units =(C["pw_units"], "sum"),
        rev_cw   =("Rev_CW",      "sum"),
        rev_pw   =("Rev_PW",      "sum"),
    ).reset_index()

    if drop_metric == "Clicks (Traffic)":
        gpu_grp["wow_pct"] = (gpu_grp["cw_traf"]  - gpu_grp["pw_traf"])  / gpu_grp["pw_traf"].replace(0, pd.NA)  * 100
        x_label = "Traffic WoW %"
    elif drop_metric == "Revenue":
        gpu_grp["wow_pct"] = (gpu_grp["rev_cw"]   - gpu_grp["rev_pw"])   / gpu_grp["rev_pw"].replace(0, pd.NA)   * 100
        x_label = "Revenue WoW %"
    else:
        gpu_grp["wow_pct"] = (gpu_grp["cw_units"] - gpu_grp["pw_units"]) / gpu_grp["pw_units"].replace(0, pd.NA) * 100
        x_label = "Units Sold WoW %"

    # Require meaningful PW baseline to avoid extreme outliers
    pw_col_map = {"Clicks (Traffic)": "pw_traf", "Revenue": "rev_pw", "Units Sold": "pw_units"}
    min_pw = {"Clicks (Traffic)": 50, "Revenue": 500, "Units Sold": 2}
    gpu_grp = gpu_grp[gpu_grp[pw_col_map[drop_metric]] >= min_pw[drop_metric]]
    gpu_grp = gpu_grp.dropna(subset=["wow_pct"])
    # Clip to ±150% so one outlier can't destroy the scale
    gpu_grp["wow_pct"] = gpu_grp["wow_pct"].clip(-150, 150)
    gpu_grp = gpu_grp.sort_values("wow_pct")

    bar_col = gpu_grp["wow_pct"].apply(lambda v: "#c62828" if v < 0 else "#2e7d32")

    fig_drop = go.Figure(go.Bar(
        x=gpu_grp["wow_pct"],
        y=gpu_grp["GPU"],
        orientation="h",
        marker_color=bar_col,
        text=gpu_grp["wow_pct"].apply(lambda v: f"{v:+.1f}%"),
        textposition="auto",
        insidetextanchor="end",
        textfont=dict(size=12, color="white"),
        cliponaxis=False,
    ))
    fig_drop.add_vline(x=0, line_dash="solid", line_color="#555", line_width=1)
    x_bound = max(abs(gpu_grp["wow_pct"].min()), abs(gpu_grp["wow_pct"].max()), 10) * 1.25
    fig_drop.update_layout(
        height=max(320, len(gpu_grp) * 32),
        margin=dict(t=10, b=20, l=10, r=110),
        plot_bgcolor="white",
        xaxis=dict(gridcolor="#f0f0f0", ticksuffix="%", zeroline=False,
                   range=[-x_bound, x_bound]),
        yaxis=dict(autorange="reversed", type="category"),
        uniformtext=dict(mode="hide", minsize=9),
    )
    st.plotly_chart(fig_drop, use_container_width=True)

    # ── Row 3: Top SKU table ──────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("#### Top 20 SKUs by CW Revenue")

    # All available columns with display names mapped to source data
    ALL_COLS = {
        "SKU":              ("SKU",                    None),
        "GPU":              ("GPU",                    None),
        "CPU":              ("CPU",                    None),
        "Memory":           ("RAM",                    None),
        "ASIN":             ("ASIN",                   None),
        "MSRP":             ("MSRP",                   "${:,.0f}"),
        "Margin DS":        ("Margin DS",              "{:.1f}%"),
        "Netsuite Margin":  ("Margin NS",              "{:.1f}%"),
        "Rev CW (est)":     ("Rev_CW",                 "${:,.0f}"),
        "Rev PW (est)":     ("Rev_PW",                 "${:,.0f}"),
        "Units CW":         (C["cw_units"],             "{:,.0f}"),
        "Units PW":         (C["pw_units"],             "{:,.0f}"),
        "Units Organic CW": (C["cw_uorg"],              "{:,.0f}"),
        "Units Paid CW":    (C["cw_upaid"],             "{:,.0f}"),
        "Traffic CW":       (C["cw_traf"],              "{:,.0f}"),
        "Traffic PW":       (C["pw_traf"],              "{:,.0f}"),
        "Units WoW %":      ("WoW Units Total %",       "{:+.1f}%"),
        "Traffic WoW %":    ("WoW Traffic Total %",     "{:+.1f}%"),
        "Units Org WoW %":  ("WoW Units Organic %",     "{:+.1f}%"),
        "Traffic Org WoW %":("WoW Traffic Organic %",   "{:+.1f}%"),
        "Ad Clicks CW":     ("Ad Clicks CW",            "{:,.0f}"),
        "Ad Clicks PW":     ("Ad Clicks PW",            "{:,.0f}"),
        "Ad Spend CW":      ("Ad Spend CW",             "${:,.2f}"),
        "Ad Sales CW":      ("Ad Sales CW",             "${:,.2f}"),
    }

    DEFAULT_SHOWN = ["SKU", "GPU", "MSRP", "Rev CW (est)", "Rev PW (est)",
                     "Units CW", "Units PW", "Units WoW %", "Traffic WoW %", "Margin DS"]

    with st.expander("⚙️ Choose columns", expanded=False):
        st.caption("Add or remove columns. To reorder: deselect a column then re-add it at the end.")
        chosen_cols = st.multiselect(
            "Columns",
            options=list(ALL_COLS.keys()),
            default=DEFAULT_SHOWN,
            key="table_cols",
            label_visibility="collapsed",
        )
        chosen_cols = chosen_cols if chosen_cols else ["SKU"]

    # Build table from chosen columns
    src_cols   = [ALL_COLS[c][0] for c in chosen_cols if c in ALL_COLS and ALL_COLS[c][0] in dff.columns]
    label_map  = {ALL_COLS[c][0]: c for c in chosen_cols if c in ALL_COLS}
    fmt_map    = {c: ALL_COLS[c][1] for c in chosen_cols if ALL_COLS[c][1]}

    top = dff.nlargest(20, "Rev_CW")[src_cols].copy().reset_index(drop=True)
    top.rename(columns=label_map, inplace=True)

    wow_cols = [c for c in top.columns if "WoW %" in c]

    def wow_style(val):
        try:
            v = float(val)
            if v > 0:   return "background-color:#c6efce;color:#006100;font-weight:bold"
            elif v < 0: return "background-color:#ffc7ce;color:#9c0006;font-weight:bold"
        except:
            pass
        return ""

    fmt_final = {c: fmt_map[c] for c in fmt_map if c in top.columns}
    styled = top.style.format(fmt_final, na_rep="-")
    if wow_cols:
        styled = styled.map(wow_style, subset=wow_cols)

    st.dataframe(styled, use_container_width=False, width=10000, height=520, hide_index=True)

    st.markdown(f"""
    <div style="text-align:center;color:#aaa;font-size:11px;margin-top:16px;">
      KPIs from <b>Weekly dashboard</b> (exact) &nbsp;·&nbsp;
      Breakdown from <b>WoW Dashboard</b> (verified) &nbsp;·&nbsp;
      Revenue = Units × MSRP (±0.6%) &nbsp;·&nbsp;
      Week {CW_LABEL} vs {PW_LABEL} &nbsp;·&nbsp; Auto-refreshes every 5 min
    </div>
    """, unsafe_allow_html=True)

    # ═══════════════════════════════════════════════════════════════════════════════
    # INVENTORY TAB
    # ═══════════════════════════════════════════════════════════════════════════════
with tab_inv:
    st.markdown("""
    <div style="background:linear-gradient(90deg,#1a3a1a,#2e6e2e);padding:16px 28px;border-radius:10px;margin-bottom:12px;">
      <h2 style="color:white;margin:0;font-size:24px;">📦 Inventory Overview</h2>
    </div>
    """, unsafe_allow_html=True)

    # Mirror all sidebar filters (GPU + MSRP + Tier) via the filtered SKU set
    _inv_skus = set(dff["SKU"].str.strip())
    inv_dff = inv_df[inv_df["SKU"].isin(_inv_skus)].copy() if "SKU" in inv_df.columns else inv_df.copy()

    # ── Health KPIs ──────────────────────────────────────────────────────────
    aging_units    = inv_health.get("Aging",     {}).get("units", 0)
    nonaging_units = inv_health.get("Non-aging", {}).get("units", 0)
    total_units    = inv_health.get("Total",     {}).get("units", 0)
    rr7d           = inv_health.get("Total",     {}).get("runrate_7d", 0)
    aging_val      = inv_health.get("Aging",     {}).get("value", "$0")
    aging_pct      = aging_units / total_units * 100 if total_units else 0
    weeks_inv      = total_units / rr7d if rr7d else 0

    sellable_total = inv_dff["AMZ sellable"].dropna().sum()
    aging_total    = inv_dff["AMZ aging"].dropna().sum()
    open_po_total  = inv_dff["AMZ Open PO"].dropna().sum()
    skytech_avail  = inv_dff["Skytech Available"].dropna().sum()

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("🟢 AMZ Sellable",     f"{sellable_total:,.0f} units")
    k2.metric("🔴 AMZ Aging",        f"{aging_total:,.0f} units",  f"{aging_pct:.1f}% of total")
    k3.metric("🏭 Skytech Warehouse", f"{skytech_avail:,.0f} units")
    k4.metric("📋 Open PO at AMZ",   f"{open_po_total:,.0f} units")
    k5.metric("⏱️ Weeks of Inv",      f"{weeks_inv:.1f} wks",       f"@ {rr7d:.0f} units/wk run rate")

    st.markdown("---")

    # ── Row 1: Sellable by GPU + Aging by GPU ────────────────────────────────
    inv_gpu = inv_dff.groupby("GPU")[["AMZ sellable","AMZ aging","AMZ Open PO","Skytech Available"]].sum().reset_index()
    inv_gpu = inv_gpu[inv_gpu["AMZ sellable"] > 0].sort_values("AMZ sellable", ascending=False)

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("#### Sellable Units by GPU")
        fig = px.bar(inv_gpu, x="GPU", y="AMZ sellable",
                     text="AMZ sellable", color="AMZ sellable",
                     color_continuous_scale="Blues",
                     labels={"AMZ sellable": "Sellable Units"})
        fig.update_traces(texttemplate="%{text:.0f}", textposition="outside")
        fig.update_layout(height=320, margin=dict(t=10,b=40,l=10,r=10),
                          plot_bgcolor="white", showlegend=False,
                          coloraxis_showscale=False,
                          xaxis=dict(type="category"),
                          yaxis=dict(gridcolor="#f0f0f0"))
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        st.markdown("#### Aging Units by GPU")
        aging_gpu = inv_gpu[inv_gpu["AMZ aging"] > 0].sort_values("AMZ aging", ascending=False)
        if aging_gpu.empty:
            st.info("No aging inventory.")
        else:
            fig2 = px.bar(aging_gpu, x="GPU", y="AMZ aging",
                          text="AMZ aging", color="AMZ aging",
                          color_continuous_scale="Reds",
                          labels={"AMZ aging": "Aging Units"})
            fig2.update_traces(texttemplate="%{text:.0f}", textposition="outside")
            fig2.update_layout(height=320, margin=dict(t=10,b=40,l=10,r=10),
                               plot_bgcolor="white", showlegend=False,
                               coloraxis_showscale=False,
                               xaxis=dict(type="category"),
                               yaxis=dict(gridcolor="#f0f0f0"))
            st.plotly_chart(fig2, use_container_width=True)

    # ── Row 2: Sellable vs Aging stacked + Inventory weeks ───────────────────
    c3, c4 = st.columns(2)
    with c3:
        st.markdown("#### Sellable vs Aging vs Open PO by GPU")
        fig3 = go.Figure()
        fig3.add_bar(name="Sellable",  x=inv_gpu["GPU"], y=inv_gpu["AMZ sellable"],  marker_color="#2e7d32")
        fig3.add_bar(name="Aging",     x=inv_gpu["GPU"], y=inv_gpu["AMZ aging"],     marker_color="#c62828")
        fig3.add_bar(name="Open PO",   x=inv_gpu["GPU"], y=inv_gpu["AMZ Open PO"],   marker_color="#1565c0")
        fig3.update_layout(barmode="group", height=320,
                           margin=dict(t=10,b=40,l=10,r=10),
                           plot_bgcolor="white",
                           xaxis=dict(type="category"),
                           yaxis=dict(gridcolor="#f0f0f0"),
                           legend=dict(orientation="h", y=-0.25))
        st.plotly_chart(fig3, use_container_width=True)

    with c4:
        st.markdown("#### Weeks of Inventory by GPU")
        inv_wks = inv_dff.groupby("GPU").agg(
            sellable=("AMZ sellable","sum"),
            runrate=("L7D sell-thru","sum"),
        ).reset_index()
        inv_wks["weeks"] = inv_wks["sellable"] / inv_wks["runrate"].replace(0, pd.NA)
        inv_wks = inv_wks.dropna(subset=["weeks"]).sort_values("weeks", ascending=False)
        fig4 = px.bar(inv_wks, x="GPU", y="weeks", text="weeks",
                      color="weeks",
                      color_continuous_scale=["#2e7d32","#f9a825","#c62828"],
                      labels={"weeks": "Weeks"})
        fig4.update_traces(texttemplate="%{text:.1f}w", textposition="outside")
        fig4.update_layout(height=320, margin=dict(t=10,b=40,l=10,r=10),
                           plot_bgcolor="white", showlegend=False,
                           coloraxis_showscale=False,
                           xaxis=dict(type="category"),
                           yaxis=dict(gridcolor="#f0f0f0"))
        st.plotly_chart(fig4, use_container_width=True)

    st.markdown("---")

    # ── SKU-level inventory table ─────────────────────────────────────────────
    st.markdown("#### SKU Inventory Detail")
    inv_tbl = inv_dff[["SKU","GPU","CPU","AMZ sellable","AMZ aging","AMZ Open PO",
                        "Skytech Available","L7D sell-thru","Inv weeks"]].copy()
    inv_tbl = inv_tbl.sort_values("AMZ aging", ascending=False)

    def aging_style(val):
        try:
            if float(val) > 0: return "background-color:#ffc7ce;color:#9c0006;font-weight:bold"
        except: pass
        return ""

    inv_styled = (
        inv_tbl.reset_index(drop=True).style
        .map(aging_style, subset=["AMZ aging"])
        .format({
            "AMZ sellable": "{:,.0f}", "AMZ aging": "{:,.0f}",
            "AMZ Open PO": "{:,.0f}",  "Skytech Available": "{:,.0f}",
            "L7D sell-thru": "{:,.0f}", "Inv weeks": "{:.1f}",
        }, na_rep="-")
    )
    st.dataframe(inv_styled, use_container_width=True, height=400, hide_index=True)

    st.markdown("---")

    # ── Upcoming POs ──────────────────────────────────────────────────────────
    st.markdown("#### Upcoming POs (Remaining Qty > 0)")

    if po_df.empty:
        st.info("No open PO lines found.")
    else:
        # Attach GPU via SKU lookup and apply all sidebar filters via filtered SKU set
        sku_gpu_map = df.set_index("SKU")[["GPU","MSRP"]].drop_duplicates()
        po_enriched = po_df.copy()
        if "Merchant SKU" in po_enriched.columns:
            po_enriched = po_enriched.merge(
                sku_gpu_map.rename_axis("Merchant SKU").reset_index(),
                on="Merchant SKU", how="left"
            )
            po_enriched = po_enriched[po_enriched["Merchant SKU"].isin(_inv_skus)]

        # Status filter
        status_opts = sorted(po_enriched["Status"].dropna().unique().tolist()) if "Status" in po_enriched.columns else []
        sel_status  = st.multiselect("PO Status", status_opts, default=status_opts, key="po_status")
        if sel_status and "Status" in po_enriched.columns:
            po_enriched = po_enriched[po_enriched["Status"].isin(sel_status)]

        # ── SKU-level summary — one row per SKU, most recent PO date first ──
        agg_dict = {
            "Requested quantity": "sum",
            "Accepted quantity":  "sum",
            "Remaining quantity": "sum",
        }
        if "Order date"         in po_enriched.columns: agg_dict["Order date"]         = "max"
        if "Window start"       in po_enriched.columns: agg_dict["Window start"]       = "min"
        if "Window end"         in po_enriched.columns: agg_dict["Window end"]         = "max"
        if "Total accepted cost" in po_enriched.columns: agg_dict["Total accepted cost"] = "sum"
        if "Status"             in po_enriched.columns:
            agg_dict["Status"] = lambda s: ", ".join(sorted(s.dropna().unique()))

        group_cols = ["Merchant SKU"]
        if "GPU" in po_enriched.columns:
            group_cols.append("GPU")

        po_sku = (
            po_enriched.groupby(group_cols, dropna=False)
            .agg({k: v for k, v in agg_dict.items() if k in po_enriched.columns})
            .reset_index()
            .sort_values("Order date", ascending=False)   # most recent PO date first
        )

        # Reorder columns: SKU → GPU → Order date → Status → window → quantities → cost
        front = ["Merchant SKU", "GPU", "Order date", "Status",
                 "Window start", "Window end",
                 "Requested quantity", "Accepted quantity", "Remaining quantity",
                 "Total accepted cost"]
        po_sku = po_sku[[c for c in front if c in po_sku.columns]]

        fmt = {c: "{:,.0f}" for c in ["Requested quantity","Accepted quantity","Remaining quantity"]}
        if "Total accepted cost" in po_sku.columns:
            fmt["Total accepted cost"] = "${:,.0f}"

        st.dataframe(
            po_sku.reset_index(drop=True).style.format(fmt, na_rep="-"),
            use_container_width=True, height=420, hide_index=True
        )

        # ── Bar chart: requested quantity by SKU, most recent PO date first ──
        top_po = po_sku.head(20)
        if not top_po.empty:
            fig_po = px.bar(
                top_po, x="Merchant SKU", y="Requested quantity",
                color="GPU" if "GPU" in top_po.columns else None,
                text="Requested quantity",
                labels={"Requested quantity": "Requested Units"},
            )
            fig_po.update_traces(texttemplate="%{text:,.0f}", textposition="outside")
            fig_po.update_layout(
                height=340, margin=dict(t=10, b=80, l=10, r=10),
                plot_bgcolor="white",
                xaxis=dict(tickangle=-40, type="category"),
                yaxis=dict(gridcolor="#f0f0f0"),
                legend=dict(orientation="h", y=-0.45),
                showlegend="GPU" in top_po.columns,
            )
            st.plotly_chart(fig_po, use_container_width=True)
