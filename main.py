from flask import Flask, render_template_string
import pandas as pd
import requests
from datetime import datetime, timedelta
import io
import time
import os

app = Flask(__name__)

# ============================================================
# SCANNER SETTINGS
# ============================================================

MIN_AVG_TURNOVER_CR = 10.0
MAX_OPEN_LOW_GAP = 0.50

# ============================================================
# NSE SESSION
# ============================================================

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/139.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
    "Connection": "keep-alive",
}

session = requests.Session()
session.headers.update(HEADERS)


# ============================================================
# NUMBER CONVERSION
# ============================================================

def to_float(value):
    try:
        if value is None:
            return None

        if isinstance(value, str):
            value = value.replace(",", "").strip()

        return float(value)

    except Exception:
        return None


# ============================================================
# NSE SESSION WARM-UP
# ============================================================

def warm_nse_session():

    try:
        session.get(
            "https://www.nseindia.com/",
            timeout=15
        )

        time.sleep(1)

    except Exception:
        pass


# ============================================================
# LIVE NSE DATA
# ============================================================

def get_live_data():

    warm_nse_session()

    # Primary NSE all-stock endpoint
    urls = [
        "https://www.nseindia.com/api/equity-stock?index=allstocks",
        "https://www.nseindia.com/api/equity-stock?index=ALLSTOCKS",
    ]

    last_error = None

    for url in urls:

        try:

            response = session.get(
                url,
                timeout=30
            )

            response.raise_for_status()

            data = response.json()

            rows = []

            if isinstance(data, dict):

                if isinstance(data.get("data"), list):
                    rows = data["data"]

                elif isinstance(data.get("stocks"), list):
                    rows = data["stocks"]

                elif isinstance(data.get("records"), list):
                    rows = data["records"]

                elif isinstance(data.get("records"), dict):

                    records = data["records"]

                    if isinstance(
                        records.get("data"),
                        list
                    ):
                        rows = records["data"]

            elif isinstance(data, list):

                rows = data

            if rows:

                return rows

        except Exception as e:

            last_error = e

            continue

    raise RuntimeError(
        "NSE live equity data unavailable. "
        + str(last_error)
    )


# ============================================================
# HISTORICAL NSE BHAVCOPY
# ============================================================

def get_bhavcopy(date_obj):

    date_str = date_obj.strftime("%d%m%Y")

    urls = [

        f"https://archives.nseindia.com/products/"
        f"content/sec_bhavdata_full_{date_str}.csv",

        f"https://nsearchives.nseindia.com/content/cm/"
        f"sec_bhavdata_full_{date_str}.csv"
    ]

    for url in urls:

        try:

            response = session.get(
                url,
                timeout=25
            )

            if (
                response.status_code == 200
                and len(response.content) > 1000
            ):

                df = pd.read_csv(
                    io.BytesIO(response.content)
                )

                df.columns = [
                    str(c).strip().upper()
                    for c in df.columns
                ]

                return df

        except Exception:

            continue

    return None


# ============================================================
# 20 VALID TRADING DAYS LIQUIDITY
# ============================================================

def get_historical_liquidity():

    turnover_data = {}

    today = datetime.now().date()

    valid_days = 0

    # Check up to 60 calendar days so that weekends,
    # holidays and missing files can be skipped.
    for days_back in range(1, 61):

        if valid_days >= 20:
            break

        date_obj = (
            today - timedelta(days=days_back)
        )

        df = get_bhavcopy(date_obj)

        if df is None:
            continue

        required_columns = {
            "SYMBOL",
            "SERIES",
            "TURNOVER_LACS"
        }

        if not required_columns.issubset(
            set(df.columns)
        ):
            continue

        # Clean columns
        df["SYMBOL"] = (
            df["SYMBOL"]
            .astype(str)
            .str.strip()
            .str.upper()
        )

        df["SERIES"] = (
            df["SERIES"]
            .astype(str)
            .str.strip()
            .str.upper()
        )

        # ====================================================
        # ONLY NSE EQ
        # ====================================================

        df = df[
            df["SERIES"] == "EQ"
        ].copy()

        if df.empty:
            continue

        df["TURNOVER_LACS"] = pd.to_numeric(
            df["TURNOVER_LACS"],
            errors="coerce"
        )

        df = df.dropna(
            subset=[
                "SYMBOL",
                "TURNOVER_LACS"
            ]
        )

        # NSE bhavcopy turnover is ₹ lakh.
        # Convert lakh to crore.
        df["TURNOVER_CR"] = (
            df["TURNOVER_LACS"] / 100.0
        )

        # Store turnover for each symbol
        for _, row in df.iterrows():

            symbol = row["SYMBOL"]

            turnover = row["TURNOVER_CR"]

            if turnover < 0:
                continue

            if symbol not in turnover_data:
                turnover_data[symbol] = []

            turnover_data[symbol].append(
                turnover
            )

        valid_days += 1

    # ========================================================
    # EXACTLY PREVIOUS 20 VALID TRADING DAYS
    # ========================================================

    averages = {}

    for symbol, values in turnover_data.items():

        if len(values) >= 20:

            last_20 = values[:20]

            average_turnover = (
                sum(last_20) / 20.0
            )

            averages[symbol] = average_turnover

    return averages


# ============================================================
# LIVE ROW NORMALIZATION
# ============================================================

def normalize_live_row(row):

    symbol = (
        row.get("symbol")
        or row.get("SYMBOL")
        or row.get("Symbol")
    )

    if not symbol:
        return None

    symbol = (
        str(symbol)
        .strip()
        .upper()
    )

    open_price = (
        row.get("open")
        if row.get("open") is not None
        else row.get("OPEN")
    )

    low_price = (
        row.get("dayLow")
        if row.get("dayLow") is not None
        else row.get("low")
    )

    ltp = (
        row.get("lastPrice")
        if row.get("lastPrice") is not None
        else row.get("ltp")
    )

    previous_close = (
        row.get("previousClose")
        if row.get("previousClose") is not None
        else row.get("prevClose")
    )

    change_percent = (
        row.get("pChange")
        if row.get("pChange") is not None
        else row.get("changePercent")
    )

    traded_value = (
        row.get("totalTradedValue")
        if row.get("totalTradedValue") is not None
        else row.get("totalTradedValueInLakhs")
    )

    series = (
        row.get("series")
        or row.get("SERIES")
        or "EQ"
    )

    return {
        "symbol": symbol,

        "series": (
            str(series)
            .strip()
            .upper()
        ),

        "open": to_float(
            open_price
        ),

        "low": to_float(
            low_price
        ),

        "ltp": to_float(
            ltp
        ),

        "previous_close": to_float(
            previous_close
        ),

        "change_percent": to_float(
            change_percent
        ),

        "traded_value": to_float(
            traded_value
        ),
    }


# ============================================================
# ETF EXCLUSION
# ============================================================

def looks_like_etf(symbol):

    symbol = symbol.upper()

    etf_patterns = [

        "ETF",
        "BEES",
        "LIQUIDBEES",
        "GOLDBEES",
        "SILVERBEES",
        "JUNIORBEES",
        "BANKBEES",
        "ITBEES",
        "PHARMABEES",
        "PSUBANKBEES",
        "MON100",
        "MID150BEES",
        "MOM100",
        "ALPHAETF",
        "LOWVOL",
        "NIFTYETF",
        "MIDCAPETF",
        "SMALLCAPETF",
        "NEXT50",
    ]

    for pattern in etf_patterns:

        if pattern in symbol:
            return True

    return False


# ============================================================
# MAIN SCANNER
# ============================================================

def run_scanner():

    # --------------------------------------------------------
    # STEP 1
    # Previous 20 valid NSE trading days average turnover
    # --------------------------------------------------------

    historical_liquidity = (
        get_historical_liquidity()
    )

    if not historical_liquidity:

        raise RuntimeError(
            "NSE historical 20-day turnover data "
            "could not be loaded."
        )

    # --------------------------------------------------------
    # STEP 2
    # Current NSE live data
    # --------------------------------------------------------

    live_rows = get_live_data()

    if not live_rows:

        raise RuntimeError(
            "NSE live stock data is empty."
        )

    results = []

    # --------------------------------------------------------
    # STEP 3
    # Apply all conditions
    # --------------------------------------------------------

    for raw_row in live_rows:

        row = normalize_live_row(
            raw_row
        )

        if row is None:
            continue

        symbol = row["symbol"]

        # ====================================================
        # CONDITION 1
        # NSE EQ ONLY
        # ====================================================

        if row["series"] != "EQ":
            continue

        # ====================================================
        # CONDITION 2
        # ETF EXCLUSION
        # ====================================================

        if looks_like_etf(symbol):
            continue

        # ====================================================
        # CONDITION 3
        # 20-DAY AVERAGE REAL TURNOVER >= ₹10 CRORE
        # ====================================================

        if symbol not in historical_liquidity:
            continue

        avg_turnover = (
            historical_liquidity[symbol]
        )

        if avg_turnover < MIN_AVG_TURNOVER_CR:
            continue

        # ====================================================
        # LIVE PRICE DATA
        # ====================================================

        open_price = row["open"]

        low_price = row["low"]

        ltp = row["ltp"]

        if open_price is None:
            continue

        if low_price is None:
            continue

        if ltp is None:
            continue

        if open_price <= 0:
            continue

        # ====================================================
        # CONDITION 4
        # OPEN-LOW GAP <= 0.50%
        #
        # Formula:
        #
        # (Open - Low) / Open × 100
        # ====================================================

        open_low_gap = (
            (open_price - low_price)
            / open_price
        ) * 100.0

        if open_low_gap > MAX_OPEN_LOW_GAP:
            continue

        # ====================================================
        # CONDITION 5
        # CURRENT PRICE / LTP > OPEN
        # ====================================================

        if ltp <= open_price:
            continue

        # ====================================================
        # TODAY'S PRESENT/LIVE TURNOVER
        # ====================================================

        traded_value_lakh = (
            row["traded_value"]
        )

        if traded_value_lakh is not None:

            live_turnover_cr = (
                traded_value_lakh / 100.0
            )

        else:

            live_turnover_cr = None

        # ====================================================
        # ADD RESULT
        # ====================================================

        results.append({

            "symbol": symbol,

            "previous_close":
                row["previous_close"],

            "open":
                open_price,

            "low":
                low_price,

            "ltp":
                ltp,

            "open_low_gap":
                open_low_gap,

            "avg_turnover":
                avg_turnover,

            "live_turnover":
                live_turnover_cr,

            "change_percent":
                row["change_percent"],
        })

    # ========================================================
    # RANKING
    #
    # LOWEST OPEN-LOW GAP FIRST
    # ========================================================

    results.sort(
        key=lambda x:
            x["open_low_gap"]
    )

    return results


# ============================================================
# HTML PAGE
# ============================================================

HTML_PAGE = """
<!DOCTYPE html>

<html>

<head>

<meta name="viewport"
      content="width=device-width, initial-scale=1">

<title>
Open Low Liquidity Scanner
</title>

<style>

body {
    font-family: Arial, sans-serif;
    margin: 10px;
    background: #f5f5f5;
}

h2 {
    margin: 5px 0 10px 0;
}

.info {
    background: white;
    padding: 12px;
    border-radius: 8px;
    margin-bottom: 10px;
    line-height: 1.6;
}

button {
    font-size: 18px;
    padding: 10px 20px;
    border: none;
    border-radius: 7px;
    background: #1976d2;
    color: white;
    cursor: pointer;
}

button:active {
    opacity: 0.7;
}

.table-wrap {
    overflow-x: auto;
    background: white;
    border-radius: 8px;
}

table {
    border-collapse: collapse;
    width: 100%;
    min-width: 1000px;
}

th,
td {
    border: 1px solid #ddd;
    padding: 6px 8px;
    text-align: right;
    white-space: nowrap;
}

th {
    background: #eeeeee;
}

th:first-child,
td:first-child {
    text-align: left;
}

.gap {
    font-weight: bold;
}

.error {
    background: white;
    color: red;
    padding: 15px;
    border-radius: 8px;
    font-weight: bold;
    overflow-wrap: anywhere;
}

.no-result {
    background: white;
    padding: 15px;
    border-radius: 8px;
}

.small {
    font-size: 13px;
    color: #555;
}

</style>

</head>

<body>

<h2>
Open-Low Liquidity Scanner
</h2>

<div class="info">

<b>Scanner Conditions</b>

<br>

Open-Low Gap ≤ <b>0.50%</b>

<br>

LTP > Open

<br>

Previous 20 Valid Trading Days Average
Real Turnover ≥ <b>₹10 Crore</b>

<br>

NSE <b>EQ</b> only

<br>

ETF / BE / SME excluded

<br>

Today's Present / Live Turnover

<br>

<b>Ranking:</b>
Smallest Open-Low Gap first

<br><br>

<button onclick="location.reload()">
Scan Now
</button>

<br><br>

<span class="small">
हर बार Scan Now दबाने पर वर्तमान NSE data
के आधार पर नया scan होगा।
</span>

</div>


{% if error %}

<div class="error">

Scanner Error

<br><br>

{{ error }}

<br><br>

Scan Now दबाकर दोबारा कोशिश करें।

</div>


{% elif rows %}

<div class="table-wrap">

<table>

<thead>

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

</thead>


<tbody>

{% for r in rows %}

<tr>

<td>
<b>{{ r.symbol }}</b>
</td>

<td>
{{ "%.2f"|format(r.previous_close)
   if r.previous_close is not none
   else "-" }}
</td>

<td>
{{ "%.2f"|format(r.open) }}
</td>

<td>
{{ "%.2f"|format(r.low) }}
</td>

<td>
{{ "%.2f"|format(r.ltp) }}
</td>

<td class="gap">
{{ "%.2f"|format(r.open_low_gap) }}%
</td>

<td>
{{ "%.2f"|format(r.avg_turnover) }}
</td>

<td>
{{ "%.2f"|format(r.live_turnover)
   if r.live_turnover is not none
   else "-" }}
</td>

<td>
{{ "%.2f"|format(r.change_percent)
   if r.change_percent is not none
   else "-" }}%
</td>

</tr>

{% endfor %}

</tbody>

</table>

</div>


{% else %}

<div class="no-result">

<b>
अभी कोई share सभी conditions को पूरा नहीं कर रहा है।
</b>

</div>

{% endif %}

</body>

</html>
"""


# ============================================================
# HOME ROUTE
# ============================================================

@app.route("/")
def home():

    try:

        rows = run_scanner()

        return render_template_string(
            HTML_PAGE,
            rows=rows,
            error=None
        )

    except Exception as e:

        return render_template_string(
            HTML_PAGE,
            rows=[],
            error=str(e)
        )


# ============================================================
# RENDER START
# ============================================================

if __name__ == "__main__":

    port = int(
        os.environ.get(
            "PORT",
            "10000"
        )
    )

    app.run(
        host="0.0.0.0",
        port=port
    )
