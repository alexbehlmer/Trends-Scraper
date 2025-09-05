import os, time
import pandas as pd
from pytrends.request import TrendReq
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

# === CONFIG ===
SHEET_ID = "1JwwoOYn7Pq36atNiD8ssibPm3El2xEohEzeO_lKlReI"  # NEW SHEET ID
RAW_TAB = "trends_raw"            # <-- write here
GEO = "US"
TIMEFRAME = "today 3-m"
USE_COMPANY_NAMES_FOR_SEARCH = True  # use names for Google Trends, but still write kpi=ticker

def open_sheet():
    creds_json = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
    with open("/tmp/sa.json", "w") as f:
        f.write(creds_json)
    scope = ["https://www.googleapis.com/auth/spreadsheets",
             "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("/tmp/sa.json", scope)
    client = gspread.authorize(creds)
    sh = client.open_by_key(SHEET_ID)
    return sh

def read_kpis_with_names(sh):
    """
    Returns:
      tickers: list[str]
      search_terms: list[str] aligned 1:1 with tickers
    """
    ws = sh.worksheet("kpis")
    colA = ws.col_values(1)  # tickers
    colB = None
    try:
        colB = ws.col_values(2)  # company names (optional)
    except Exception:
        pass

    tickers = [v.strip() for v in colA[1:] if v and v.strip()]
    if USE_COMPANY_NAMES_FOR_SEARCH and colB and len(colB) > 1:
        names = [v.strip() if v else "" for v in colB[1:]]
        terms = []
        for i in range(len(tickers)):
            term = names[i] if i < len(names) and names[i] else tickers[i]
            terms.append(term)
        return tickers, terms
    else:
        return tickers, tickers

def ensure_raw_sheet(sh, tab_name=RAW_TAB):
    try:
        ws = sh.worksheet(tab_name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(tab_name, rows=2, cols=4)
        ws.update("A1:D1", [["date","kpi","value","notes"]])
    return ws

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def fetch_timeseries(search_terms):
    pytrends = TrendReq(hl="en-US", tz=360)
    frames = []
    for batch in chunks(search_terms, 2):  # small batches = fewer 429s
        retries, wait = 3, 30
        while retries > 0:
            try:
                pytrends.build_payload(batch, timeframe=TIMEFRAME, geo=GEO)
                df = pytrends.interest_over_time()
                if df.empty:
                    print(f"No data for batch: {batch}")
                    time.sleep(10)
                    break
                if "isPartial" in df.columns:
                    df = df.drop(columns=["isPartial"])
                df = df.reset_index().rename(columns={"date": "date"})
                m = df.melt(id_vars=["date"], var_name="search_term", value_name="interest_value")
                frames.append(m)
                time.sleep(10)
                break
            except Exception as e:
                if "429" in str(e):
                    print(f"429 for {batch}, waiting {wait}s...")
                    time.sleep(wait); wait *= 2; retries -= 1
                else:
                    raise
    if not frames:
        return pd.DataFrame(columns=["date","search_term","interest_value"])
    out = pd.concat(frames, ignore_index=True)
    out["date"] = pd.to_datetime(out["date"]).dt.date.astype(str)
    return out[["date","search_term","interest_value"]]

def write_raw_dedup(ws, df_raw):
    """
    df_raw must have: date | kpi | value | notes
    Dedup key: date|kpi
    """
    if df_raw.empty:
        return 0
    existing = ws.get_all_values()
    if existing and len(existing) > 1:
        data = existing[1:]
        existing_keys = {f"{r[0]}|{r[1]}" for r in data if len(r) >= 2}
    else:
        ws.update("A1:D1", [["date","kpi","value","notes"]])
        existing_keys = set()
    new = df_raw[~(df_raw["date"] + "|" + df_raw["kpi"]).isin(existing_keys)]
    if new.empty:
        return 0
    ws.append_rows(new.values.tolist(), value_input_option="RAW")
    return len(new)

if __name__ == "__main__":
    sh = open_sheet()
    tickers, search_terms = read_kpis_with_names(sh)

    # Fetch Google Trends by search terms (names or tickers)
    df = fetch_timeseries(search_terms)

    # Map search_term -> ticker (aligned 1:1)
    # Build a dict from both lists (same ordering)
    mapping = {st: tk for st, tk in zip(search_terms, tickers)}

    # Transform to raw schema expected by trends_raw
    if not df.empty:
        df["kpi"] = df["search_term"].map(mapping).fillna(df["search_term"])
        df["value"] = df["interest_value"]
        df["notes"] = f"geo={GEO}; timeframe={TIMEFRAME}"
        df_raw = df[["date","kpi","value","notes"]].copy()
    else:
        df_raw = pd.DataFrame(columns=["date","kpi","value","notes"])

    ws_raw = ensure_raw_sheet(sh, RAW_TAB)
    n = write_raw_dedup(ws_raw, df_raw)
    print(f"Wrote {n} new rows to {RAW_TAB} at {datetime.utcnow().isoformat()}Z")
