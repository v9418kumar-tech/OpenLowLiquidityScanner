from flask import Flask, render_template_string
import requests
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import io
import time

app = Flask(__name__)

# =========================================================
# OPEN-LOW LIQUIDITY SCANNER
# =========================================================

MIN_AVG_TURNOVER_CR = 10.0
MAX_OPEN_LOW_GAP = 0.50

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Linux; Android 10) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/130.0 Mobile Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
}

session = requests.Session()
session.headers.update(HEADERS)


# ---------------------------------------------------------
# NSE SESSION
# ---------------------------------------------------------
def nse_session():
    try:
        session.get(
            "https://www.nseindia.com/",
            timeout=15
        )
    except Exception:
        pass


# ---------------------------------------------------------
# GET LIVE NSE DATA
# NIFTY TOTAL MARKET = broad NSE equity universe
# ---------------------------------------------------------
def get_live_data():

    nse_session()

    url = (
        "https://www.nseindia.com/api/"
        "equity-stockIndices?index=NIFTY%20TOTAL%20MARKET"
    )

    r = session.get(url, timeout=30)
    r.raise_for_status()

    data = r.json()

    rows = []

    for x in data.get("data", []):

        symbol = str(x.get("symbol", "")).strip()

        if not symbol:
            continue

        # ETFs / non-equity instruments are not wanted
        if symbol.upper().endswith("-ETF"):
            continue

        rows.append({
            "symbol": symbol,
            "open": x.get("open"),
            "high": x.get("dayHigh"),
            "low": x.get("dayLow"),
            "prev_close": x.get("previousClose"),
            "ltp": x.get("lastPrice"),
            "change_pct": x.get("pChange"),
            "volume": x.get("totalTradedVolume"),
            "live_value_lakh": x.get("totalTradedValue"),
        })

    return pd.DataFrame(rows)


# ---------------------------------------------------------
# GET LAST 20 VALID NSE TRADING DAYS
# ---------------------------------------------------------
def get_trading_dates():

    dates = []

    d = datetime.now().date() - timedelta(days=1)

    while len(dates) < 20:

        # Monday-Friday
        if d.weekday() < 5:
            dates.append(d)

        d -= timedelta(days=1)

    return dates


# ---------------------------------------------------------
# DOWNLOAD NSE BHAVCOPY
# ---------------------------------------------------------
def get_bhavcopy(dt):

    date_string = dt.strftime("%d%m%Y")

    urls = [
        f"https://archives.nseindia.com/products/content/sec_bhavdata_full_{date_string}.csv",
        f"https://nsearchives.nseindia.com/content/cm/sec_bhavdata_full_{date_string}.csv"
    ]

    for url in urls:

        try:
            r = requests.get(
                url,
                headers=HEADERS,
                timeout=20
            )

            if r.status_code == 200 and len(r.content) > 1000:

                df = pd.read_csv(io.BytesIO(r.content))

                df.columns = [
                    str(c).strip().upper()
                    for c in df.columns
                ]

                # Only normal NSE Equity shares
                if "SERIES" in df.columns:
                    df = df[
                        df["SERIES"]
                        .astype(str)
                        .str.strip()
                        .str.upper()
                        == "EQ"
                    ]

                if "SYMBOL" in df.columns and "TURNOVER_LACS" in df.columns:

                    df["SYMBOL"] = (
                        df["SYMBOL"]
                        .astype(str)
                        .str.strip()
                        .str.upper()
                    )

                    df["TURNOVER_LACS"] = pd.to_numeric(
                        df["TURNOVER_LACS"],
                        errors="coerce"
                    )

                    # ₹ lakh -> ₹ crore
                    df["TURNOVER_CR"] = (
                        df["TURNOVER_LACS"] / 100.0
                    )

                    return df[
                        ["SYMBOL", "TURNOVER_CR"]
                    ]

        except Exception:
            continue

    return pd.DataFrame(columns=["SYMBOL", "TURNOVER_CR"])


# ---------------------------------------------------------
# CALCULATE 20-DAY AVERAGE REAL TURNOVER
# ---------------------------------------------------------
def get_average_turnover():

    dates = get_trading_dates()

    all_days = []

    # Download several bhavcopies in parallel
    with ThreadPoolExecutor(max_workers=5) as executor:

        futures = {
            executor.submit(get_bhavcopy, d): d
            for d in dates
        }

        for future in as_completed(futures):

            try:
                df = future.result()

                if not df.empty:
                    all_days.append(df)

            except Exception:
                pass

    if not all_days:
        return pd.DataFrame(
            columns=["symbol", "avg_turnover_cr"]
        )

    combined = pd.concat(
        all_days,
        ignore_index=True
    )

    # ETF / BE / SME are already excluded by EQ series,
    # but keep defensive symbol filtering here.
    combined = combined[
        ~combined["SYMBOL"].str.contains(
            "ETF",
            case=False,
            na=False
        )
    ]

    avg = (
        combined
        .groupby("SYMBOL")["TURNOVER_CR"]
        .mean()
        .reset_index()
    )

    avg.columns = [
        "symbol",
        "avg_turnover_cr"
    ]

    # 20 valid days means preferably 20 observations.
    counts = (
        combined
        .groupby("SYMBOL")
        .size()
        .reset_index(name="days")
    )

    avg = avg.merge(
        counts,
        on="symbol",
        how="left"
    )

    # Require 20 valid trading-day observations
    avg = avg[avg["days"] >= 20]

    # ₹10 crore minimum liquidity
    avg = avg[
        avg["avg_turnover_cr"] >= MIN_AVG_TURNOVER_CR
    ]

    return avg[
        ["symbol", "avg_turnover_cr"]
    ]


# ---------------------------------------------------------
# SCANNER
# ---------------------------------------------------------
def run_scanner():

    live = get_live_data()

    if live.empty:
        return []

    # Only valid numerical rows
    numeric_cols = [
        "open",
        "low",
        "ltp",
        "prev_close",
        "live_value_lakh"
    ]

    for c in numeric_cols:
        live[c] = pd.to_numeric(
            live[c],
            errors="coerce"
        )

    live = live.dropna(
        subset=["open", "low", "ltp"]
    )

    # -----------------------------------------------------
    # Open-Low gap
    #
    # Example:
    # Open = 100
    # Low  = 99.70
    #
    # Gap = (100 - 99.70) / 100 * 100
    #      = 0.30%
    # -----------------------------------------------------
    live["open_low_gap"] = (
        (live["open"] - live["low"])
        / live["open"]
    ) * 100

    # Current / present turnover
    # NSE value is in ₹ lakh
    live["live_turnover_cr"] = (
        live["live_value_lakh"] / 100.0
    )

    # -----------------------------------------------------
    # Conditions
    # -----------------------------------------------------

    # 1. LTP must be above Open
    live = live[
        live["ltp"] > live["open"]
    ]

    # 2. Open-Low gap <= 0.50%
    live = live[
        live["open_low_gap"] <= MAX_OPEN_LOW_GAP
    ]

    # 3. Open should not be zero
    live = live[
        live["open"] > 0
    ]

    # Historical liquidity
    avg_turnover = get_average_turnover()

    if avg_turnover.empty:
        return []

    result = live.merge(
        avg_turnover,
        on="symbol",
        how="inner"
    )

    # -----------------------------------------------------
    # SORT:
    # Smallest Open-Low gap first
    # -----------------------------------------------------
    result = result.sort_values(
        by="open_low_gap",
        ascending=True
    )

    result = result.reset_index(drop=True)

    output = []

    for _, r in result.iterrows():

        output.append({
            "symbol": r["symbol"],
            "prev_close": r["prev_close"],
            "open": r["open"],
            "low": r["low"],
            "ltp": r["ltp"],
            "open_low_gap": r["open_low_gap"],
            "avg_turnover_cr": r["avg_turnover_cr"],
            "live_turnover_cr": r["live_turnover_cr"],
            "change_pct": r["change_pct"]
        })

    return output


# ---------------------------------------------------------
# WEB PAGE
# ---------------------------------------------------------
HTML = """
<!DOCTYPE html>
<html>
<head>

<meta name="viewport"
      content="width=device-width, initial-scale=1">

<title>Open Low Liquidity Scanner</title>

<style>

body {
    background:#111;
    color:#eee;
    font-family:Arial,sans-serif;
    margin:0;
    padding:12px;
}

h2 {
    margin:5px 0 8px 0;
}

.info {
    background:#1d1d1d;
    padding:10px;
    border-radius:8px;
    margin-bottom:12px;
    font-size:14px;
    line-height:1.6;
}

button {
    background:#198754;
    color:white;
    border:0;
    padding:12px 18px;
    border-radius:7px;
    font-size:16px;
    margin-bottom:12px;
}

.table-wrap {
    overflow-x:auto;
}

table {
    width:100%;
    border-collapse:collapse;
    font-size:13px;
    white-space:nowrap;
}

th {
    background:#292929;
    padding:8px;
    position:sticky;
    top:0;
}

td {
    padding:8px;
    border-bottom:1px solid #333;
    text-align:right;
}

td:first-child {
    text-align:left;
    font-weight:bold;
}

tr:hover {
    background:#222;
}

.good {
    font-weight:bold;
}

.small {
    color:#aaa;
    font-size:12px;
}

</style>

</head>

<body>

<h2>Open-Low Liquidity Scanner</h2>

<div class="info">

<b>Conditions:</b><br>

20-day Average Real Turnover ≥ ₹10 Cr<br>
Open-Low Gap ≤ 0.50%<br>
LTP &gt; Open<br>
NSE Equity only<br>
ETF / BE / SME excluded<br>
No fixed 1/3/5-minute timeframe<br>
Results sorted by smallest Open-Low Gap

</div>

<form method="get">
<button type="submit">🔄 Scan Now</button>
</form>

{% if scanned %}

<div class="small">
Scan completed: {{ time }}
&nbsp; | &nbsp;
Stocks found: {{ rows|length }}
</div>

<br>

<div class="table-wrap">

<table>

<tr>
<th>Share</th>
<th>Prev Close</th>
<th>Open</th>
<th>Low</th>
<th>LTP</th>
<th>Open-Low Gap %</th>
<th>20D Avg Turnover ₹Cr</th>
<th>Today Present Turnover ₹Cr</th>
<th>Change %</th>
</tr>

{% for r in rows %}

<tr>

<td>{{ r.symbol }}</td>

<td>{{ "%.2f"|format(r.prev_close or 0) }}</td>

<td>{{ "%.2f"|format(r.open or 0) }}</td>

<td>{{ "%.2f"|format(r.low or 0) }}</td>

<td>{{ "%.2f"|format(r.ltp or 0) }}</td>

<td class="good">
{{ "%.2f"|format(r.open_low_gap or 0) }}%
</td>

<td>
{{ "%.2f"|format(r.avg_turnover_cr or 0) }}
</td>

<td>
{{ "%.2f"|format(r.live_turnover_cr or 0) }}
</td>

<td>
{{ "%.2f"|format(r.change_pct or 0) }}%
</td>

</tr>

{% endfor %}

</table>

</div>

{% else %}

<p>
ऊपर <b>Scan Now</b> दबाकर scanner चलाइए।
</p>

{% endif %}

</body>
</html>
"""


# ---------------------------------------------------------
# HOME
# ---------------------------------------------------------
@app.route("/")
def home():

    try:
        rows = run_scanner()

        return render_template_string(
            HTML,
            rows=rows,
            scanned=True,
            time=datetime.now().strftime(
                "%d-%m-%Y %H:%M:%S"
            )
        )

    except Exception as e:

        return f"""
        <html>
        <body style="
            background:#111;
            color:white;
            font-family:Arial;
            padding:20px;">
        <h3>Scanner Error</h3>
        <p>{str(e)}</p>
        <p>कृपया कुछ सेकंड बाद फिर Scan Now दबाएँ।</p>
        </body>
        </html>
        """


# ---------------------------------------------------------
# RENDER START
# ---------------------------------------------------------
if __name__ == "__main__":

    import os

    port = int(
        os.environ.get("PORT", 10000)
    )

    app.run(
        host="0.0.0.0",
        port=port
    )
