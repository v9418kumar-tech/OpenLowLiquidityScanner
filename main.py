from flask import Flask, jsonify, render_template_string
import pandas as pd
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import io
import time
import os
import math
import uuid

app = Flask(__name__)

# ============================================================
# SCANNER CONDITIONS
# ============================================================

MIN_AVG_TURNOVER_CR = 10.0
MAX_OPEN_LOW_GAP = 0.50

HISTORICAL_DAYS_REQUIRED = 20
HISTORICAL_LOOKBACK_DAYS = 75

REQUEST_TIMEOUT = 15
MAX_WORKERS = 4

# ============================================================
# NSE URLS
# ============================================================

NSE_HOME = "https://www.nseindia.com"

NSE_EQUITY_LIST_URLS = [
    "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv",
    "https://archives.nseindia.com/content/equities/EQUITY_L.csv",
]

NSE_ETF_LIST_URLS = [
    "https://nsearchives.nseindia.com/content/equities/eq_etfseclist.csv",
    "https://archives.nseindia.com/content/equities/eq_etfseclist.csv",
]

NSE_BHAVCOPY_URLS = [
    "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{date}.csv",
    "https://archives.nseindia.com/products/content/sec_bhavdata_full_{date}.csv",
]

NSE_QUOTE_URL = "https://www.nseindia.com/api/quote-equity"

# ============================================================
# NSE HEADERS
# ============================================================

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/139.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9,hi;q=0.8",
    "Referer": "https://www.nseindia.com/",
    "Origin": "https://www.nseindia.com",
    "Connection": "keep-alive",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

# ============================================================
# THREAD-LOCAL NSE SESSION
# ============================================================

thread_local = threading.local()


def get_nse_session():
    if not hasattr(thread_local, "session"):
        s = requests.Session()
        s.headers.update(HEADERS)

        try:
            s.get(
                NSE_HOME,
                timeout=REQUEST_TIMEOUT
            )
            time.sleep(0.3)
        except Exception:
            pass

        thread_local.session = s

    return thread_local.session


# ============================================================
# CACHE
# ============================================================

historical_cache = {
    "date": None,
    "data": None
}

equity_cache = {
    "date": None,
    "symbols": None
}

etf_cache = {
    "date": None,
    "symbols": None
}

cache_lock = threading.Lock()

# ============================================================
# BACKGROUND SCAN STATE
# ============================================================

scan_state = {
    "job_id": None,
    "status": "idle",
    "progress": 0,
    "message": "Ready",
    "results": [],
    "error": None,
    "started": None,
    "finished": None
}

scan_lock = threading.Lock()


# ============================================================
# BASIC HELPERS
# ============================================================

def clean_number(value):
    try:
        if value is None:
            return None

        if isinstance(value, float) and math.isnan(value):
            return None

        text = str(value).strip().replace(",", "")

        if text == "":
            return None

        if text.lower() in ("nan", "none", "null", "-"):
            return None

        return float(text)

    except Exception:
        return None


def normalize_symbol(value):
    if value is None:
        return ""

    text = str(value).strip().upper()

    if text == "NAN":
        return ""

    return text


def valid_number(value):
    return (
        value is not None
        and math.isfinite(value)
    )


# ============================================================
# OFFICIAL NSE EQUITY LIST
# ============================================================

def get_equity_symbols():

    today = datetime.now().date()

    with cache_lock:
        if (
            equity_cache["date"] == today
            and equity_cache["symbols"] is not None
        ):
            return equity_cache["symbols"]

    session = get_nse_session()
    last_error = None

    for url in NSE_EQUITY_LIST_URLS:

        try:
            response = session.get(
                url,
                timeout=REQUEST_TIMEOUT
            )

            response.raise_for_status()

            df = pd.read_csv(
                io.BytesIO(response.content),
                dtype=str
            )

            df.columns = [
                str(c).strip().upper()
                for c in df.columns
            ]

            symbol_column = None

            for name in [
                "SYMBOL",
                "SYMBOLS",
                "TCKRSYMB"
            ]:
                if name in df.columns:
                    symbol_column = name
                    break

            if symbol_column is None:
                raise RuntimeError(
                    "NSE equity list में SYMBOL column नहीं मिला."
                )

            symbols = set()

            for value in df[symbol_column]:

                symbol = normalize_symbol(value)

                if symbol:
                    symbols.add(symbol)

            if not symbols:
                raise RuntimeError(
                    "NSE equity list खाली मिली."
                )

            result = sorted(symbols)

            with cache_lock:
                equity_cache["date"] = today
                equity_cache["symbols"] = result

            return result

        except Exception as exc:
            last_error = exc

    raise RuntimeError(
        "NSE Equity list नहीं मिली: "
        + str(last_error)
    )


# ============================================================
# OFFICIAL ETF LIST
# ============================================================

def get_etf_symbols():

    today = datetime.now().date()

    with cache_lock:
        if (
            etf_cache["date"] == today
            and etf_cache["symbols"] is not None
        ):
            return etf_cache["symbols"]

    session = get_nse_session()

    for url in NSE_ETF_LIST_URLS:

        try:
            response = session.get(
                url,
                timeout=REQUEST_TIMEOUT
            )

            response.raise_for_status()

            df = pd.read_csv(
                io.BytesIO(response.content),
                dtype=str
            )

            df.columns = [
                str(c).strip().upper()
                for c in df.columns
            ]

            symbol_column = None

            for name in [
                "SYMBOL",
                "SYMBOLS",
                "TCKRSYMB"
            ]:
                if name in df.columns:
                    symbol_column = name
                    break

            if symbol_column is None:
                symbol_column = df.columns[0]

            symbols = set()

            for value in df[symbol_column]:

                symbol = normalize_symbol(value)

                if symbol:
                    symbols.add(symbol)

            with cache_lock:
                etf_cache["date"] = today
                etf_cache["symbols"] = symbols

            return symbols

        except Exception:
            continue

    # Fallback ETF name patterns.
    # Official list is preferred whenever available.
    return set()


# ============================================================
# BHAVCOPY
# ============================================================

def get_bhavcopy(date_obj):

    date_string = date_obj.strftime("%d%m%Y")

    session = get_nse_session()

    for template in NSE_BHAVCOPY_URLS:

        url = template.format(
            date=date_string
        )

        try:
            response = session.get(
                url,
                timeout=REQUEST_TIMEOUT
            )

            if response.status_code != 200:
                continue

            if not response.content:
                continue

            df = pd.read_csv(
                io.BytesIO(response.content),
                dtype=str
            )

            if df.empty:
                continue

            df.columns = [
                str(c).strip().upper()
                for c in df.columns
            ]

            required = {
                "SYMBOL",
                "SERIES",
                "TURNOVER_LACS"
            }

            if not required.issubset(
                set(df.columns)
            ):
                continue

            return df

        except Exception:
            continue

    return None


# ============================================================
# HISTORICAL 20-DAY LIQUIDITY
# ============================================================

def get_historical_liquidity():

    today = datetime.now().date()

    with cache_lock:
        if (
            historical_cache["date"] == today
            and historical_cache["data"] is not None
        ):
            return historical_cache["data"]

    all_days = {}

    current_date = today - timedelta(days=1)

    checked = 0

    while (
        checked < HISTORICAL_LOOKBACK_DAYS
        and len(all_days) < HISTORICAL_DAYS_REQUIRED
    ):

        if current_date.weekday() < 5:

            df = get_bhavcopy(current_date)

            if df is not None:

                df["SERIES"] = (
                    df["SERIES"]
                    .astype(str)
                    .str.strip()
                    .str.upper()
                )

                # Only EQ.
                eq = df[
                    df["SERIES"] == "EQ"
                ].copy()

                if not eq.empty:

                    eq["SYMBOL"] = (
                        eq["SYMBOL"]
                        .astype(str)
                        .str.strip()
                        .str.upper()
                    )

                    eq["TURNOVER_LACS"] = pd.to_numeric(
                        eq["TURNOVER_LACS"],
                        errors="coerce"
                    )

                    eq = eq.dropna(
                        subset=["TURNOVER_LACS"]
                    )

                    day_map = {}

                    for _, row in eq.iterrows():

                        symbol = normalize_symbol(
                            row["SYMBOL"]
                        )

                        turnover_lacs = clean_number(
                            row["TURNOVER_LACS"]
                        )

                        if (
                            symbol
                            and valid_number(
                                turnover_lacs
                            )
                        ):
                            # lakh -> crore
                            turnover_cr = (
                                turnover_lacs / 100.0
                            )

                            day_map[symbol] = (
                                turnover_cr
                            )

                    if day_map:
                        all_days[current_date] = day_map

        current_date -= timedelta(days=1)
        checked += 1

    if len(all_days) < HISTORICAL_DAYS_REQUIRED:
        raise RuntimeError(
            "20 valid NSE trading days की "
            "bhavcopy पूरी नहीं मिली."
        )

    valid_dates = sorted(
        all_days.keys(),
        reverse=True
    )[:HISTORICAL_DAYS_REQUIRED]

    symbols = set()

    for dt in valid_dates:
        symbols.update(
            all_days[dt].keys()
        )

    liquidity = {}

    for symbol in symbols:

        values = []

        for dt in valid_dates:

            value = all_days[dt].get(
                symbol
            )

            if valid_number(value):
                values.append(value)

        if len(values) == HISTORICAL_DAYS_REQUIRED:

            average = (
                sum(values)
                / HISTORICAL_DAYS_REQUIRED
            )

            liquidity[symbol] = {
                "avg_turnover_cr": average
            }

    if not liquidity:
        raise RuntimeError(
            "Historical liquidity data नहीं मिला."
        )

    with cache_lock:
        historical_cache["date"] = today
        historical_cache["data"] = liquidity

    return liquidity


# ============================================================
# NSE LIVE QUOTE
# ============================================================

def get_live_quote(symbol):

    session = get_nse_session()

    try:

        response = session.get(
            NSE_QUOTE_URL,
            params={
                "symbol": symbol
            },
            timeout=REQUEST_TIMEOUT
        )

        if response.status_code != 200:
            return None

        text = response.text.strip()

        if not text:
            return None

        if text.lower().startswith("<!doctype"):
            return None

        if text.lower().startswith("<html"):
            return None

        data = response.json()

        price_info = data.get(
            "priceInfo",
            {}
        )

        metadata = data.get(
            "metadata",
            {}
        )

        security_info = data.get(
            "securityInfo",
            {}
        )

        open_price = clean_number(
            price_info.get("open")
        )

        ltp = clean_number(
            price_info.get("lastPrice")
        )

        intraday = price_info.get(
            "intraDayHighLow",
            {}
        )

        low_price = clean_number(
            intraday.get("min")
        )

        if low_price is None:
            low_price = clean_number(
                price_info.get("low")
            )

        series = (
            metadata.get("series")
            or security_info.get("series")
            or ""
        )

        series = str(
            series
        ).strip().upper()

        if (
            not valid_number(open_price)
            or not valid_number(low_price)
            or not valid_number(ltp)
        ):
            return None

        if open_price <= 0:
            return None

        return {
            "open": open_price,
            "low": low_price,
            "ltp": ltp,
            "series": series
        }

    except Exception:
        return None


# ============================================================
# LIVE TURNOVER
# ============================================================

def get_live_turnover(symbol):

    session = get_nse_session()

    try:

        response = session.get(
            NSE_QUOTE_URL,
            params={
                "symbol": symbol,
                "section": "trade_info"
            },
            timeout=REQUEST_TIMEOUT
        )

        if response.status_code != 200:
            return None

        text = response.text.strip()

        if not text:
            return None

        if text.lower().startswith("<!doctype"):
            return None

        if text.lower().startswith("<html"):
            return None

        data = response.json()

        book = data.get(
            "marketDeptOrderBook",
            {}
        )

        trade_info = book.get(
            "tradeInfo",
            {}
        )

        value_lacs = clean_number(
            trade_info.get(
                "totalTradedValue"
            )
        )

        if not valid_number(value_lacs):
            return None

        # NSE trade-info value is in lakh units.
        # lakh -> crore
        return value_lacs / 100.0

    except Exception:
        return None


# ============================================================
# PROCESS ONE STOCK
# ============================================================

def process_symbol(
    symbol,
    liquidity,
    etf_symbols
):

    historical = liquidity.get(
        symbol
    )

    if historical is None:
        return None

    average = historical[
        "avg_turnover_cr"
    ]

    if average < MIN_AVG_TURNOVER_CR:
        return None

    # Official ETF exclusion
    if symbol in etf_symbols:
        return None

    quote = get_live_quote(
        symbol
    )

    if quote is None:
        return None

    # EQ only.
    # If NSE gives a non-empty series,
    # it must be EQ.
    if (
        quote["series"]
        and quote["series"] != "EQ"
    ):
        return None

    open_price = quote["open"]
    low_price = quote["low"]
    ltp = quote["ltp"]

    gap = (
        (open_price - low_price)
        / open_price
    ) * 100.0

    if gap < 0:
        return None

    if gap > MAX_OPEN_LOW_GAP:
        return None

    # Current price must be above today's open.
    if ltp <= open_price:
        return None

    # Only qualifying price candidates
    # request today's live turnover.
    live_turnover = get_live_turnover(
        symbol
    )

    if live_turnover is None:
        return None

    return {
        "symbol": symbol,
        "open": open_price,
        "low": low_price,
        "ltp": ltp,
        "gap": gap,
        "avg_turnover": average,
        "live_turnover": live_turnover
    }


# ============================================================
# BACKGROUND SCAN
# ============================================================

def perform_scan(job_id):

    try:

        with scan_lock:
            scan_state["status"] = "scanning"
            scan_state["progress"] = 2
            scan_state["message"] = (
                "20-day liquidity data तैयार हो रहा है..."
            )

        liquidity = get_historical_liquidity()

        with scan_lock:
            scan_state["progress"] = 20
            scan_state["message"] = (
                "NSE Equity universe तैयार हो रहा है..."
            )

        equity_symbols = get_equity_symbols()

        with scan_lock:
            scan_state["progress"] = 25
            scan_state["message"] = (
                "ETF list check हो रही है..."
            )

        etf_symbols = get_etf_symbols()

        candidates = []

        for symbol in equity_symbols:

            if symbol not in liquidity:
                continue

            if symbol in etf_symbols:
                continue

            if (
                liquidity[symbol][
                    "avg_turnover_cr"
                ] >= MIN_AVG_TURNOVER_CR
            ):
                candidates.append(symbol)

        if not candidates:
            raise RuntimeError(
                "₹10 करोड़ average turnover वाले "
                "EQ shares नहीं मिले."
            )

        total = len(candidates)

        results = []

        with scan_lock:
            scan_state["progress"] = 30
            scan_state["message"] = (
                "NSE live Open / Low / LTP scan शुरू..."
            )

        completed = 0

        with ThreadPoolExecutor(
            max_workers=MAX_WORKERS
        ) as executor:

            futures = {
                executor.submit(
                    process_symbol,
                    symbol,
                    liquidity,
                    etf_symbols
                ): symbol
                for symbol in candidates
            }

            for future in as_completed(
                futures
            ):

                try:

                    result = future.result()

                    if result is not None:
                        results.append(result)

                except Exception:
                    pass

                completed += 1

                progress = (
                    30
                    + int(
                        completed
                        / total
                        * 65
                    )
                )

                with scan_lock:
                    scan_state["progress"] = (
                        min(progress, 95)
                    )
                    scan_state["message"] = (
                        f"NSE live scan: "
                        f"{completed}/{total}"
                    )

        # Smallest Open-Low gap first.
        results.sort(
            key=lambda x: (
                x["gap"],
                -x["live_turnover"]
            )
        )

        with scan_lock:

            scan_state["status"] = "completed"
            scan_state["progress"] = 100
            scan_state["message"] = (
                f"Scan complete. "
                f"{len(results)} shares मिले."
            )
            scan_state["results"] = results
            scan_state["finished"] = (
                datetime.now().strftime(
                    "%d-%m-%Y %H:%M:%S"
                )
            )

    except Exception as exc:

        with scan_lock:

            scan_state["status"] = "error"
            scan_state["progress"] = 100
            scan_state["message"] = "Scan failed."
            scan_state["error"] = str(exc)
            scan_state["results"] = []
            scan_state["finished"] = (
                datetime.now().strftime(
                    "%d-%m-%Y %H:%M:%S"
                )
            )


# ============================================================
# START SCAN API
# ============================================================

@app.route("/api/start")
def start_scan():

    with scan_lock:

        if scan_state["status"] == "scanning":
            return jsonify({
                "ok": True,
                "status": "scanning",
                "job_id": scan_state["job_id"]
            })

        job_id = str(
            uuid.uuid4()
        )

        scan_state["job_id"] = job_id
        scan_state["status"] = "scanning"
        scan_state["progress"] = 0
        scan_state["message"] = (
            "Scan शुरू हो रहा है..."
        )
        scan_state["results"] = []
        scan_state["error"] = None
        scan_state["started"] = (
            datetime.now().strftime(
                "%d-%m-%Y %H:%M:%S"
            )
        )
        scan_state["finished"] = None

    worker = threading.Thread(
        target=perform_scan,
        args=(job_id,),
        daemon=True
    )

    worker.start()

    return jsonify({
        "ok": True,
        "status": "scanning",
        "job_id": job_id
    })


# ============================================================
# STATUS API
# ============================================================

@app.route("/api/status")
def status():

    with scan_lock:

        return jsonify({
            "job_id": scan_state["job_id"],
            "status": scan_state["status"],
            "progress": scan_state["progress"],
            "message": scan_state["message"],
            "results": scan_state["results"],
            "error": scan_state["error"],
            "started": scan_state["started"],
            "finished": scan_state["finished"]
        })


# ============================================================
# HOME PAGE
# ============================================================

HTML = """
<!DOCTYPE html>
<html lang="hi">

<head>

<meta charset="UTF-8">

<meta name="viewport"
content="width=device-width, initial-scale=1.0">

<title>Open-Low Liquidity Scanner</title>

<style>

body {
    font-family: Arial, sans-serif;
    margin: 0;
    padding: 15px;
    background: #f5f5f5;
}

.container {
    max-width: 1100px;
    margin: auto;
    background: white;
    padding: 15px;
    border-radius: 10px;
}

h1 {
    font-size: 23px;
    margin-top: 0;
}

.conditions {
    background: #f0f7ff;
    padding: 12px;
    border-radius: 8px;
    line-height: 1.7;
    margin-bottom: 15px;
}

button {
    width: 100%;
    padding: 13px;
    font-size: 17px;
    border: none;
    border-radius: 7px;
    background: #1976d2;
    color: white;
    cursor: pointer;
}

button:disabled {
    background: #777;
}

.status {
    margin-top: 15px;
    padding: 12px;
    background: #eeeeee;
    border-radius: 8px;
}

.error {
    margin-top: 15px;
    padding: 12px;
    background: #ffe5e5;
    color: #b00020;
    border-radius: 8px;
}

.success {
    margin-top: 15px;
    padding: 12px;
    background: #e8f5e9;
    color: #1b5e20;
    border-radius: 8px;
}

.progress-box {
    margin-top: 10px;
    background: #ddd;
    border-radius: 10px;
    overflow: hidden;
}

.progress {
    height: 14px;
    width: 0%;
    background: #1976d2;
}

.table-wrap {
    overflow-x: auto;
    margin-top: 15px;
}

table {
    width: 100%;
    border-collapse: collapse;
    min-width: 850px;
}

th,
td {
    border: 1px solid #ddd;
    padding: 8px;
    white-space: nowrap;
    text-align: right;
}

th {
    background: #eeeeee;
    text-align: center;
}

td:first-child,
td:nth-child(2) {
    text-align: left;
}

.small {
    color: #666;
    font-size: 13px;
}

</style>

</head>

<body>

<div class="container">

<h1>Open-Low Liquidity Scanner</h1>

<div class="conditions">

<b>Scanner Conditions</b><br>

1. Open-Low Gap ≤ 0.50%<br>
2. Current LTP &gt; Today's Open<br>
3. Previous 20 Valid Trading Days Average Real Turnover ≥ ₹10 Crore<br>
4. NSE EQ only<br>
5. ETF / BE / BZ / SME excluded<br>
6. Today's Current / Live Turnover<br>
7. Smallest Open-Low Gap first

</div>

<button id="scanButton"
onclick="startScan()">

Scan Now

</button>

<div id="statusBox"
class="status">

Scanner तैयार है।
<br>
<strong>Scan Now</strong> दबाकर scan शुरू करें।

</div>

<div class="progress-box">

<div id="progress"
class="progress">
</div>

</div>

<div id="resultArea"></div>

</div>

<script>

let timer = null;

function startScan() {

    const button =
        document.getElementById("scanButton");

    const status =
        document.getElementById("statusBox");

    const resultArea =
        document.getElementById("resultArea");

    const progress =
        document.getElementById("progress");

    button.disabled = true;

    button.innerText =
        "Scanning...";

    resultArea.innerHTML = "";

    progress.style.width = "0%";

    status.innerHTML =
        "Scan शुरू हो रहा है...";

    fetch("/api/start")

    .then(response => response.json())

    .then(data => {

        if (!data.ok) {
            throw new Error(
                "Scan शुरू नहीं हो पाया."
            );
        }

        checkStatus();

    })

    .catch(error => {

        button.disabled = false;

        button.innerText =
            "Scan Now";

        status.innerHTML =
            "Error: " + error.message;

    });

}


function checkStatus() {

    fetch("/api/status")

    .then(response => response.json())

    .then(data => {

        const status =
            document.getElementById("statusBox");

        const progress =
            document.getElementById("progress");

        const button =
            document.getElementById("scanButton");

        progress.style.width =
            data.progress + "%";

        status.innerHTML =
            "<b>" + data.message + "</b>"
            + "<br>Progress: "
            + data.progress + "%";

        if (data.status === "scanning") {

            timer = setTimeout(
                checkStatus,
                2000
            );

            return;
        }

        if (data.status === "completed") {

            button.disabled = false;

            button.innerText =
                "Scan Now";

            showResults(
                data.results
            );

            return;
        }

        if (data.status === "error") {

            button.disabled = false;

            button.innerText =
                "Scan Now";

            status.className =
                "status error";

            status.innerHTML =
                "<b>Scanner Error</b><br><br>"
                + data.error;

            return;
        }

    })

    .catch(error => {

        timer = setTimeout(
            checkStatus,
            3000
        );

    });

}


function showResults(results) {

    const area =
        document.getElementById(
            "resultArea"
        );

    if (!results || results.length === 0) {

        area.innerHTML =
            '<div class="success">'
            + '<b>Scan completed.</b><br><br>'
            + 'कोई share सभी conditions को पूरा नहीं कर रहा है।'
            + '</div>';

        return;
    }

    let html = "";

    html +=
        '<div class="success">'
        + '<b>Scan completed.</b><br>'
        + 'Qualifying shares: '
        + results.length
        + '</div>';

    html +=
        '<div class="table-wrap">';

    html += '<table>';

    html += '<thead><tr>';

    html += '<th>#</th>';
    html += '<th>Symbol</th>';
    html += '<th>Open ₹</th>';
    html += '<th>Low ₹</th>';
    html += '<th>LTP ₹</th>';
    html += '<th>Open-Low Gap %</th>';
    html += '<th>20D Avg Turnover ₹ Cr</th>';
    html += '<th>Today's Live Turnover ₹ Cr</th>';

    html += '</tr></thead>';

    html += '<tbody>';

    results.forEach(
        (row, index) => {

        html += '<tr>';

        html +=
            '<td style="text-align:center">'
            + (index + 1)
            + '</td>';

        html +=
            '<td><b>'
            + row.symbol
            + '</b></td>';

        html +=
            '<td>'
            + Number(row.open).toFixed(2)
            + '</td>';

        html +=
            '<td>'
            + Number(row.low).toFixed(2)
            + '</td>';

        html +=
            '<td>'
            + Number(row.ltp).toFixed(2)
            + '</td>';

        html +=
            '<td>'
            + Number(row.gap).toFixed(2)
            + '%</td>';

        html +=
            '<td>'
            + Number(row.avg_turnover).toFixed(2)
            + '</td>';

        html +=
            '<td>'
            + Number(row.live_turnover).toFixed(2)
            + '</td>';

        html += '</tr>';

    });

    html += '</tbody>';

    html += '</table>';

    html += '</div>';

    area.innerHTML = html;
}

</script>

</body>

</html>
"""


@app.route("/")
def home():

    return render_template_string(
        HTML
    )


# ============================================================
# HEALTH CHECK
# ============================================================

@app.route("/health")
def health():

    return "OK", 200


# ============================================================
# MAIN
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
