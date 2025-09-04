import os, time
import pandas as pd
from pytrends.request import TrendReq
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

# === CONFIG ===
SHEET_ID = "1JSgU5ZtvRJGfZfgqFCUFz5LxxcNkcHoj55Y-wjT2FfA"
TAB_NAME = "trends_daily"
GEO = "US"
TIMEFRAME = "today 3-m"

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

def read_kpis(sh):
    ws = sh.worksheet("kpis")
    vals = ws.col_values(1)  # column A
    return [v for v in vals[1:] if v.strip()]

def ensure_trends_sheet(sh):
    try:
        ws = sh.worksheet(TAB_NAME)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(TAB_NAME, rows=2, cols=5)
        ws.update("A1:E1", [["date","keyword","geo","timeframe","interest_value"]])
    return ws

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def fetch_timeseries(terms):
    pytrends = TrendReq(hl="en-US", tz=360)
    frames = []
    for batch in chunks(terms, 2):  # smaller batch size to reduce 429s
        retries = 3
        wait = 30  # start with 30s wait if 429
        while retries > 0:
            try:
                pytrends.build_payload(batch, timeframe=TIMEFRAME, geo=GEO)
                df = pytrends.interest_over_time()
                if df.empty:
                    print(f"No data for batch: {batch}")
                    time.sleep(10)
                    break
                df = df.drop(columns=[c for c in df.columns if c == "isPartial"])
                df = df.reset_index().rename(columns={"date": "date"})
                m = df.melt(id_vars=["date"], var_name="keyword", value_name="interest_value")
                m["geo"] = GEO
                m["timeframe"] = TIMEFRAME
                frames.append(m)
                time.sleep(10)  # wait before next batch
                break  # success → break retry loop
            except Exception as e:
                if "429" in str(e):
                    print(f"429 Too Many Requests for {batch}, waiting {wait}s before retry...")
                    time.sleep(wait)
                    wait *= 2
                    retries -= 1
                else:
                    raise e
    if frames:
        out = pd.concat(frames, ignore_index=True)
        out["date"] = pd.to_datetime(out["date"]).dt.date.astype(str)
        return out[["date", "keyword", "geo", "timeframe", "interest_value"]]
    return pd.DataFrame(columns=["date", "keyword", "geo", "timeframe", "interest_value"])

def write_dedup(ws, df):
    if df.empty:
        return 0
    existing = ws.get_all_values()
    if existing:
        data = existing[1:]
        existing_keys = {f"{r[0]}|{r[1]}" for r in data if len(r) >= 2}
    else:
        ws.update("A1:E1", [["date","keyword","geo","timeframe","interest_value"]])
        existing_keys = set()
    new = df[~(df["date"] + "|" + df["keyword"]).isin(existing_keys)]
    if new.empty:
        return 0
    ws.append_rows(new.values.tolist(), value_input_option="RAW")
    return len(new)

if __name__ == "__main__":
    sh = open_sheet()
    terms = read_kpis(sh)
    ws = ensure_trends_sheet(sh)
    df = fetch_timeseries(terms)
    n = write_dedup(ws, df)
    print(f"Wrote {n} new rows to {TAB_NAME} at {datetime.utcnow().isoformat()}Z")
