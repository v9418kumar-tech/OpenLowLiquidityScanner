from flask import Flask, redirect, render_template_string
import pandas as pd
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import io
import time
import os
import math

app = Flask(__name__)

# ============================================================
# SETTINGS
# ============================================================

MIN_AVG_TURNOVER_CR = 10.0
MAX_OPEN_LOW_GAP = 0.50
HISTORICAL_DAYS_REQUIRED = 20
MAX_WORKERS = 4

NSE_HOME = "https://www.nseindia.com"
EQUITY_LIST_URL = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
ETF_LIST_URL = "https://nsearchives.nseindia.com/content/equities/eq_etfseclist.csv"

BHAVCOPY_URLS = [
    "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{date}.csv",
    "https://archives.nseindia.com/products/content/sec_bhavdata_full_{date}.csv",
]

QUOTE_URL = "https://www.nseindia.com/api/quote-equity?symbol={symbol}"
TRADE_INFO_URL = (
    "https://www.nseindia.com/api/quote-equity"
    "?symbol={symbol}&section=trade_info"
)

# ============================================================
# GLOBAL STATE
# ============================================================

state_lock = threading.Lock()

scan_state = {
    "status": "ready",
    "message": "Scanner तैयार है। Scan Now दबाकर scan शुरू करें।",
    "progress": 0,
    "total": 0,
    "done": 0,
    "results": [],
    "started": "",
    "finished": "",
    "error": ""
}

historical_cache = {}
equity_symbols_cache = None
etf_symbols_cache = None

# ============================================================
# NSE SESSION
# ============================================================

_thread_local = threading.local()


def get_nse_session():
    if not hasattr(_thread_local, "session"):
        session = requests.Session()

        session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Linux; Android 10) "
                "AppleWebKit/537.36 "
                "(KHTML, like Gecko) "
                "Chrome/152.0.0.0 Mobile Safari/537.36"
            ),
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.nseindia.com/",
            "Connection": "keep-alive",
        })

        _thread_local.session = session

        try:
            session.get(
                NSE_HOME,
                timeout=15
            )
        except Exception:
            pass

    return _thread_local.session


# ============================================================
# HELPERS
# ============================================================

def clean_number(value):
    try:
        if value is None:
            return None

        if isinstance(value, (int, float)):
            if math.isnan(value):
                return None
            return float(value)

        text = str(value).strip()
        if text == "" or text.lower() in ("nan", "none", "-"):
            return None

        text = text.replace(",", "")
        return float(text)

    except Exception:
        return None


def format_number(value, decimals=2):
    if value is None:
        return "-"

    try:
        return f"{float(value):,.{decimals}f}"
    except Exception:
        return "-"


def format_cr(value):
    if value is None:
        return "-"

    try:
        return f"₹{float(value):,.2f} Cr"
    except Exception:
        return "-"


def set_state(**kwargs):
    with state_lock:
        scan_state.update(kwargs)


# ============================================================
# EQUITY / ETF LIST
# ============================================================

def get_equity_symbols():
    global equity_symbols_cache

    if equity_symbols_cache is not None:
        return equity_symbols_cache

    session = get_nse_session()

    response = session.get(
        EQUITY_LIST_URL,
        timeout=30
    )
    response.raise_for_status()

    df = pd.read_csv(io.BytesIO(response.content))

    # NSE EQUITY_L.csv normally contains SYMBOL and SERIES.
    df.columns = [str(c).strip().upper() for c in df.columns]

    if "SYMBOL" not in df.columns:
        raise RuntimeError("NSE Equity list में SYMBOL column नहीं मिला।")

    if "SERIES" in df.columns:
        df["SERIES"] = df["SERIES"].astype(str).str.strip().str.upper()
        df = df[df["SERIES"] == "EQ"]

    symbols = (
        df["SYMBOL"]
        .astype(str)
        .str.strip()
        .str.upper()
        .dropna()
        .unique()
        .tolist()
    )

    equity_symbols_cache = set(symbols)

    return equity_symbols_cache


def get_etf_symbols():
    global etf_symbols_cache

    if etf_symbols_cache is not None:
        return etf_symbols_cache

    session = get_nse_session()

    try:
        response = session.get(
            ETF_LIST_URL,
            timeout=30
        )
        response.raise_for_status()

        df = pd.read_csv(io.BytesIO(response.content))
        df.columns = [str(c).strip().upper() for c in df.columns]

        symbol_column = None

        for col in ["SYMBOL", "SYMBOL_NAME"]:
            if col in df.columns:
                symbol_column = col
                break

        if symbol_column is None:
            etf_symbols_cache = set()
            return etf_symbols_cache

        symbols = (
            df[symbol_column]
            .astype(str)
            .str.strip()
            .str.upper()
            .dropna()
            .unique()
            .tolist()
        )

        etf_symbols_cache = set(symbols)

    except Exception:
        # ETF exclusion is additionally protected by EQ-series filter.
        etf_symbols_cache = set()

    return etf_symbols_cache


# ============================================================
# BHAVCOPY
# ============================================================

def get_bhavcopy(date_obj):
    date_text = date_obj.strftime("%d%m%Y")

    session = get_nse_session()

    for template in BHAVCOPY_URLS:
        url = template.format(date=date_text)

        try:
            response = session.get(
                url,
                timeout=30
            )

            if response.status_code != 200:
                continue

            if not response.content:
                continue

            # Sometimes NSE may return HTML instead of CSV.
            sample = response.content[:100].lower()

            if b"<html" in sample or b"<!doctype" in sample:
                continue

            df = pd.read_csv(io.BytesIO(response.content))

            df.columns = [
                str(c).strip().upper()
                for c in df.columns
            ]

            required = {
                "SYMBOL",
                "SERIES",
                "OPEN_PRICE",
                "HIGH_PRICE",
                "LOW_PRICE",
                "CLOSE_PRICE",
                "TURNOVER_LACS"
            }

            if not required.issubset(set(df.columns)):
                continue

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

            df = df[df["SERIES"] == "EQ"].copy()

            if df.empty:
                continue

            return df

        except Exception:
            continue

    return None


# ============================================================
# HISTORICAL LIQUIDITY
# ============================================================

def get_historical_liquidity():
    """
    Previous 20 valid NSE EQ trading days.
    Average real turnover = sum TURNOVER_LACS / 100 / 20.
    """

    global historical_cache

    if historical_cache:
        return historical_cache

    valid_days = []

    today = datetime.now().date()

    # Start from yesterday because today's bhavcopy is not required.
    current = today - timedelta(days=1)

    attempts = 0

    set_state(
        message="Previous 20 valid trading days का turnover तैयार किया जा रहा है...",
        progress=2
    )

    while len(valid_days) < HISTORICAL_DAYS_REQUIRED and attempts < 60:
        df = get_bhavcopy(current)

        if df is not None and not df.empty:
            valid_days.append((current, df))

            set_state(
                message=(
                    f"Historical liquidity: "
                    f"{len(valid_days)}/{HISTORICAL_DAYS_REQUIRED} "
                    f"valid trading days मिले।"
                ),
                progress=min(
                    30,
                    2 + int(
                        len(valid_days)
                        / HISTORICAL_DAYS_REQUIRED
                        * 28
                    )
                )
            )

        current -= timedelta(days=1)
        attempts += 1

    if len(valid_days) < HISTORICAL_DAYS_REQUIRED:
        raise RuntimeError(
            "NSE से previous 20 valid trading days का bhavcopy data पूरा नहीं मिला।"
        )

    totals = {}

    for date_obj, df in valid_days:
        for _, row in df.iterrows():

            symbol = str(row["SYMBOL"]).strip().upper()

            turnover_lacs = clean_number(
                row["TURNOVER_LACS"]
            )

            if turnover_lacs is None:
                continue

            # NSE bhavcopy TURNOVER_LACS -> ₹ Crore
            turnover_cr = turnover_lacs / 100.0

            if symbol not in totals:
                totals[symbol] = []

            totals[symbol].append(turnover_cr)

    averages = {}

    for symbol, values in totals.items():

        if len(values) == HISTORICAL_DAYS_REQUIRED:
            averages[symbol] = sum(values) / len(values)

    historical_cache = averages

    return historical_cache


# ============================================================
# LIVE QUOTE
# ============================================================

def get_live_quote(symbol):

    session = get_nse_session()

    url = QUOTE_URL.format(symbol=symbol)

    response = session.get(
        url,
        timeout=15
    )

    if response.status_code != 200:
        return None

    data = response.json()

    price_info = data.get("priceInfo", {})
    metadata = data.get("metadata", {})

    open_price = clean_number(
        price_info.get("open")
    )

    last_price = clean_number(
        price_info.get("lastPrice")
    )

    intra_day = price_info.get(
        "intraDayHighLow",
        {}
    )

    low_price = clean_number(
        intra_day.get("min")
    )

    series = str(
        metadata.get("series", "")
    ).strip().upper()

    return {
        "open": open_price,
        "low": low_price,
        "ltp": last_price,
        "series": series
    }


# ============================================================
# LIVE TURNOVER
# ============================================================

def get_live_turnover(symbol):

    session = get_nse_session()

    url = TRADE_INFO_URL.format(symbol=symbol)

    response = session.get(
        url,
        timeout=15
    )

    if response.status_code != 200:
        return None

    data = response.json()

    order_book = data.get(
        "marketDeptOrderBook",
        {}
    )

    trade_info = order_book.get(
        "tradeInfo",
        {}
    )

    total_traded_value = clean_number(
        trade_info.get("totalTradedValue")
    )

    if total_traded_value is None:
        return None

    # NSE endpoint examples provide totalTradedValue in ₹ lakh.
    # ₹ lakh / 100 = ₹ crore.
    return total_traded_value / 100.0


# ============================================================
# PROCESS ONE SYMBOL
# ============================================================

def process_symbol(
    symbol,
    historical,
    etf_symbols
):

    try:

        # ----------------------------------------------------
        # Historical liquidity filter
        # ----------------------------------------------------

        avg_turnover = historical.get(symbol)

        if avg_turnover is None:
            return None

        if avg_turnover < MIN_AVG_TURNOVER_CR:
            return None

        # ----------------------------------------------------
        # ETF exclusion
        # ----------------------------------------------------

        if symbol in etf_symbols:
            return None

        # ----------------------------------------------------
        # Live quote
        # ----------------------------------------------------

        quote = get_live_quote(symbol)

        if not quote:
            return None

        # Only NSE EQ.
        if quote.get("series") != "EQ":
            return None

        open_price = quote.get("open")
        low_price = quote.get("low")
        ltp = quote.get("ltp")

        if (
            open_price is None
            or low_price is None
            or ltp is None
            or open_price <= 0
        ):
            return None

        # ----------------------------------------------------
        # Open-Low Gap
        # ----------------------------------------------------

        gap_percent = (
            (open_price - low_price)
            / open_price
        ) * 100.0

        # We only want 0% to 0.50%.
        if gap_percent < 0:
            return None

        if gap_percent > MAX_OPEN_LOW_GAP:
            return None

        # ----------------------------------------------------
        # Current LTP must be above today's open
        # ----------------------------------------------------

        if ltp <= open_price:
            return None

        # ----------------------------------------------------
        # Current live turnover
        # ----------------------------------------------------

        live_turnover = get_live_turnover(symbol)

        if live_turnover is None:
            live_turnover = 0.0

        return {
            "symbol": symbol,
            "open": open_price,
            "low": low_price,
            "ltp": ltp,
            "gap": gap_percent,
            "avg_turnover": avg_turnover,
            "live_turnover": live_turnover
        }

    except Exception:
        return None


# ============================================================
# COMPLETE SCAN
# ============================================================

def run_scan():

    try:

        set_state(
            status="scanning",
            message="NSE data तैयार किया जा रहा है...",
            progress=1,
            done=0,
            total=0,
            results=[],
            error="",
            started=datetime.now().strftime(
                "%d-%m-%Y %H:%M:%S"
            ),
            finished=""
        )

        # ----------------------------------------------------
        # Equity list
        # ----------------------------------------------------

        equity_symbols = get_equity_symbols()

        set_state(
            message=(
                f"NSE EQ list तैयार है: "
                f"{len(equity_symbols)} symbols।"
            ),
            progress=5
        )

        # ----------------------------------------------------
        # ETF list
        # ----------------------------------------------------

        etf_symbols = get_etf_symbols()

        # ----------------------------------------------------
        # Historical liquidity
        # ----------------------------------------------------

        historical = get_historical_liquidity()

        # ----------------------------------------------------
        # Only liquid symbols need live API calls.
        # ----------------------------------------------------

        candidates = [
            symbol
            for symbol in equity_symbols
            if symbol in historical
            and historical[symbol] >= MIN_AVG_TURNOVER_CR
            and symbol not in etf_symbols
        ]

        total = len(candidates)

        set_state(
            total=total,
            done=0,
            progress=32,
            message=(
                f"{total} liquid NSE EQ shares मिलीं। "
                f"अब today's Open, Low, LTP और live turnover check हो रहा है..."
            )
        )

        results = []

        if total == 0:
            set_state(
                status="done",
                message="कोई liquid NSE EQ candidate नहीं मिला।",
                progress=100,
                done=0,
                total=0,
                results=[],
                finished=datetime.now().strftime(
                    "%d-%m-%Y %H:%M:%S"
                )
            )
            return

        # ----------------------------------------------------
        # Live scan
        # ----------------------------------------------------

        done = 0

        with ThreadPoolExecutor(
            max_workers=MAX_WORKERS
        ) as executor:

            futures = {
                executor.submit(
                    process_symbol,
                    symbol,
                    historical,
                    etf_symbols
                ): symbol
                for symbol in candidates
            }

            for future in as_completed(futures):

                try:
                    result = future.result()

                    if result is not None:
                        results.append(result)

                except Exception:
                    pass

                done += 1

                progress = 32 + int(
                    (done / total) * 65
                )

                set_state(
                    done=done,
                    total=total,
                    progress=min(progress, 97),
                    message=(
                        f"Live scan चल रहा है... "
                        f"{done}/{total} shares check हो चुकी हैं। "
                        f"Valid results: {len(results)}"
                    )
                )

        # ----------------------------------------------------
        # Smallest Open-Low Gap first
        # ----------------------------------------------------

        results.sort(
            key=lambda x: (
                x["gap"],
                -x["live_turnover"]
            )
        )

        set_state(
            status="done",
            message=(
                f"Scan पूरा हो गया। "
                f"{len(results)} shares मिलीं।"
            ),
            progress=100,
            done=total,
            total=total,
            results=results,
            finished=datetime.now().strftime(
                "%d-%m-%Y %H:%M:%S"
            )
        )

    except Exception as e:

        set_state(
            status="error",
            message="Scanner में error आया।",
            progress=100,
            error=str(e),
            finished=datetime.now().strftime(
                "%d-%m-%Y %H:%M:%S"
            )
        )


# ============================================================
# START SCAN
# ============================================================

def start_scan():

    with state_lock:

        if scan_state["status"] == "scanning":
            return

        scan_state["status"] = "scanning"
        scan_state["message"] = (
            "Scan शुरू हो गया है। कृपया कुछ समय प्रतीक्षा करें..."
        )
        scan_state["progress"] = 1
        scan_state["done"] = 0
        scan_state["total"] = 0
        scan_state["results"] = []
        scan_state["error"] = ""
        scan_state["started"] = datetime.now().strftime(
            "%d-%m-%Y %H:%M:%S"
        )
        scan_state["finished"] = ""

    # Background thread.
    thread = threading.Thread(
        target=run_scan,
        daemon=True
    )

    thread.start()


# ============================================================
# HTML PAGE
# ============================================================

PAGE = """
<!DOCTYPE html>
<html>
<head>

<meta charset="UTF-8">

<meta
    name="viewport"
    content="width=device-width, initial-scale=1.0"
>

{% if scanning %}
<meta
    http-equiv="refresh"
    content="5"
>
{% endif %}

<title>Open-Low Liquidity Scanner</title>

<style>

body {
    font-family: Arial, sans-serif;
    background: #f5f7fb;
    margin: 0;
    padding: 16px;
    color: #111827;
}

.container {
    max-width: 1100px;
    margin: auto;
}

.card {
    background: white;
    border-radius: 14px;
    padding: 18px;
    box-shadow: 0 2px 10px rgba(0,0,0,0.08);
    margin-bottom: 16px;
}

h1 {
    font-size: 24px;
    margin-top: 0;
}

.conditions {
    line-height: 1.8;
    font-size: 15px;
}

.scan-button {
    display: block;
    width: 100%;
    box-sizing: border-box;
    text-align: center;
    text-decoration: none;
    background: #1769e0;
    color: white;
    padding: 15px;
    border-radius: 10px;
    font-size: 18px;
    font-weight: bold;
    margin-top: 18px;
}

.scan-button:active {
    background: #0f56bd;
}

.status {
    margin-top: 16px;
    padding: 14px;
    border-radius: 10px;
    background: #eef2f7;
    font-size: 15px;
}

.progress-bg {
    width: 100%;
    height: 16px;
    background: #e5e7eb;
    border-radius: 20px;
    overflow: hidden;
    margin-top: 12px;
}

.progress {
    height: 100%;
    background: #1769e0;
    width: {{ progress }}%;
}

.small {
    color: #6b7280;
    font-size: 13px;
    margin-top: 8px;
}

.error {
    color: #b91c1c;
    background: #fee2e2;
    padding: 12px;
    border-radius: 8px;
    margin-top: 12px;
}

.table-wrap {
    overflow-x: auto;
    margin-top: 12px;
}

table {
    width: 100%;
    border-collapse: collapse;
    min-width: 850px;
}

th {
    background: #111827;
    color: white;
    padding: 10px;
    text-align: left;
    font-size: 13px;
}

td {
    padding: 10px;
    border-bottom: 1px solid #e5e7eb;
    font-size: 13px;
}

tr:nth-child(even) {
    background: #f9fafb;
}

.no-results {
    padding: 15px;
    background: #f9fafb;
    border-radius: 8px;
}

</style>

</head>

<body>

<div class="container">

<div class="card">

<h1>Open-Low Liquidity Scanner</h1>

<div class="conditions">

<b>Scanner Conditions:</b><br>

1. Open-Low Gap ≤ 0.50%<br>

2. Current LTP &gt; Today's Open<br>

3. Previous 20 Valid Trading Days Average Real Turnover ≥ ₹10 Crore<br>

4. NSE EQ only<br>

5. ETF / BE / BZ / SME excluded<br>

6. Today's Current / Live Turnover<br>

7. Smallest Open-Low Gap first

</div>

<a
    class="scan-button"
    href="/scan"
>
{% if scanning %}
Scan चल रहा है...
{% else %}
Scan Now
{% endif %}
</a>

<div class="status">

<b>Status:</b>
{{ message }}

<div class="progress-bg">
<div
    class="progress"
    style="width: {{ progress }}%;"
></div>
</div>

<div class="small">
Progress: {{ progress }}%
{% if total > 0 %}
&nbsp; | &nbsp;
{{ done }}/{{ total }} shares checked
{% endif %}
</div>

{% if started %}
<div class="small">
Started: {{ started }}
</div>
{% endif %}

{% if finished %}
<div class="small">
Finished: {{ finished }}
</div>
{% endif %}

</div>

{% if error %}

<div class="error">
<b>Scanner Error:</b><br>
{{ error }}
</div>

{% endif %}

</div>


{% if status == "done" %}

<div class="card">

<h2>
Results: {{ results|length }}
</h2>

{% if results %}

<div class="table-wrap">

<table>

<thead>

<tr>
<th>#</th>
<th>Share</th>
<th>Open</th>
<th>Low</th>
<th>LTP</th>
<th>Open-Low Gap</th>
<th>20D Avg Turnover</th>
<th>Today's Live Turnover</th>
</tr>

</thead>

<tbody>

{% for row in results %}

<tr>

<td>{{ loop.index }}</td>

<td><b>{{ row.symbol }}</b></td>

<td>{{ "%.2f"|format(row.open) }}</td>

<td>{{ "%.2f"|format(row.low) }}</td>

<td>{{ "%.2f"|format(row.ltp) }}</td>

<td>
<b>{{ "%.2f"|format(row.gap) }}%</b>
</td>

<td>
₹{{ "%.2f"|format(row.avg_turnover) }} Cr
</td>

<td>
₹{{ "%.2f"|format(row.live_turnover) }} Cr
</td>

</tr>

{% endfor %}

</tbody>

</table>

</div>

{% else %}

<div class="no-results">
इस समय कोई share सभी conditions को पूरा नहीं कर रहा है।
</div>

{% endif %}

</div>

{% endif %}


{% if scanning %}

<div class="card">

<b>Scanner अभी चल रहा है।</b><br>

यह page लगभग हर 5 सेकंड में अपने-आप refresh होगा।

<br><br>

Historical 20-day liquidity और आज का live NSE data दोनों check हो रहे हैं।

</div>

{% endif %}

</div>

</body>
</html>
"""


# ============================================================
# ROUTES
# ============================================================

@app.route("/")
def home():

    with state_lock:

        current = dict(scan_state)

    return render_template_string(
        PAGE,
        status=current["status"],
        message=current["message"],
        progress=current["progress"],
        done=current["done"],
        total=current["total"],
        results=current["results"],
        started=current["started"],
        finished=current["finished"],
        error=current["error"],
        scanning=current["status"] == "scanning"
    )


@app.route("/scan")
def scan():

    with state_lock:
        already_running = (
            scan_state["status"] == "scanning"
        )

    if not already_running:
        start_scan()

    return redirect("/")


@app.route("/health")
def health():

    return "OK", 200


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":

    port = int(
        os.environ.get(
            "PORT",
            10000
        )
    )

    app.run(
        host="0.0.0.0",
        port=port
    )
