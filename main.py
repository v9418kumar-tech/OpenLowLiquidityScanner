from flask import Flask, render_template_string
import pandas as pd
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import io
import time
import os
import math
import random

app = Flask(__name__)

# ============================================================
# SCANNER SETTINGS
# ============================================================

MIN_AVG_TURNOVER_CR = 10.0
MAX_OPEN_LOW_GAP = 0.50

# Maximum number of previous valid trading sessions required
HISTORICAL_DAYS_REQUIRED = 20

# How many calendar days we are willing to look back
HISTORICAL_LOOKBACK_DAYS = 75

# NSE request settings
REQUEST_TIMEOUT = 15
MAX_WORKERS = 4

# ============================================================
# NSE URLS
# ============================================================

NSE_HOME = "https://www.nseindia.com"

NSE_EQUITY_LIST_URL = (
    "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
)

NSE_ETF_LIST_URL = (
    "https://nsearchives.nseindia.com/content/equities/eq_etfseclist.csv"
)

NSE_BHAVCOPY_URLS = [
    "https://nsearchives.nseindia.com/products/content/"
    "sec_bhavdata_full_{date}.csv",

    "https://archives.nseindia.com/products/content/"
    "sec_bhavdata_full_{date}.csv",
]

NSE_QUOTE_URL = "https://www.nseindia.com/api/quote-equity"

NSE_TRADE_INFO_URL = "https://www.nseindia.com/api/quote-equity"

# ============================================================
# BROWSER-LIKE HEADERS
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

session = requests.Session()
session.headers.update(HEADERS)

# ============================================================
# CACHE
# ============================================================

HISTORICAL_CACHE = {
    "date": None,
    "data": None,
}

EQUITY_CACHE = {
    "date": None,
    "symbols": None,
}

ETF_CACHE = {
    "date": None,
    "symbols": None,
}

# ============================================================
# HELPERS
# ============================================================

def clean_number(value):
    """
    Convert NSE numeric value safely to float.
    """
    try:
        if value is None:
            return None

        if isinstance(value, float) and math.isnan(value):
            return None

        text = str(value).strip().replace(",", "")

        if text == "" or text.lower() in {
            "nan",
            "none",
            "null",
            "-",
        }:
            return None

        return float(text)

    except Exception:
        return None


def normalize_symbol(value):
    """
    Normalize NSE symbol.
    """
    if value is None:
        return ""

    text = str(value).strip().upper()

    if text == "NAN":
        return ""

    return text


def is_valid_number(value):
    return value is not None and math.isfinite(value)


# ============================================================
# NSE SESSION
# ============================================================

def warm_nse_session():
    """
    Visit NSE homepage first to obtain cookies.
    """
    try:
        response = session.get(
            NSE_HOME,
            timeout=REQUEST_TIMEOUT,
        )

        if response.status_code in (200, 403):
            # Even a 403 may establish useful cookies.
            pass

        time.sleep(0.5)

    except Exception:
        pass


# ============================================================
# OFFICIAL NSE EQUITY LIST
# ============================================================

def get_equity_symbols():
    """
    Download NSE official equity master list.

    This is used as the main universe.
    ETF is separately removed.
    """

    today = datetime.now().date()

    if (
        EQUITY_CACHE["date"] == today
        and EQUITY_CACHE["symbols"] is not None
    ):
        return EQUITY_CACHE["symbols"]

    urls = [
        NSE_EQUITY_LIST_URL,
        "https://archives.nseindia.com/content/equities/EQUITY_L.csv",
    ]

    last_error = None

    for url in urls:
        try:
            response = session.get(
                url,
                timeout=REQUEST_TIMEOUT,
            )

            response.raise_for_status()

            df = pd.read_csv(
                io.BytesIO(response.content),
                dtype=str,
            )

            df.columns = [
                str(c).strip().upper()
                for c in df.columns
            ]

            symbol_col = None

            for candidate in [
                "SYMBOL",
                "SYMBOLS",
                "TCKRSYMB",
            ]:
                if candidate in df.columns:
                    symbol_col = candidate
                    break

            if symbol_col is None:
                raise RuntimeError(
                    "NSE equity list में SYMBOL column नहीं मिला."
                )

            symbols = set()

            for value in df[symbol_col].tolist():
                symbol = normalize_symbol(value)

                if symbol:
                    symbols.add(symbol)

            if not symbols:
                raise RuntimeError(
                    "NSE equity list खाली मिली."
                )

            result = sorted(symbols)

            EQUITY_CACHE["date"] = today
            EQUITY_CACHE["symbols"] = result

            return result

        except Exception as exc:
            last_error = exc

    raise RuntimeError(
        "NSE official equity list उपलब्ध नहीं हुई: "
        + str(last_error)
    )


# ============================================================
# OFFICIAL NSE ETF LIST
# ============================================================

def get_etf_symbols():
    """
    Download official NSE ETF list.

    ETF symbols are removed even if they otherwise appear
    in the broad equity universe.
    """

    today = datetime.now().date()

    if (
        ETF_CACHE["date"] == today
        and ETF_CACHE["symbols"] is not None
    ):
        return ETF_CACHE["symbols"]

    urls = [
        NSE_ETF_LIST_URL,
        "https://archives.nseindia.com/content/equities/eq_etfseclist.csv",
    ]

    last_error = None

    for url in urls:
        try:
            response = session.get(
                url,
                timeout=REQUEST_TIMEOUT,
            )

            response.raise_for_status()

            df = pd.read_csv(
                io.BytesIO(response.content),
                dtype=str,
            )

            df.columns = [
                str(c).strip().upper()
                for c in df.columns
            ]

            symbol_col = None

            for candidate in [
                "SYMBOL",
                "SYMBOLS",
                "TCKRSYMB",
            ]:
                if candidate in df.columns:
                    symbol_col = candidate
                    break

            if symbol_col is None:
                # Some versions may have first column containing symbol.
                symbol_col = df.columns[0]

            symbols = set()

            for value in df[symbol_col].tolist():
                symbol = normalize_symbol(value)

                if symbol:
                    symbols.add(symbol)

            ETF_CACHE["date"] = today
            ETF_CACHE["symbols"] = symbols

            return symbols

        except Exception as exc:
            last_error = exc

    # ETF list failure should not make the entire scanner unusable.
    # Return empty set, while EQ/BE/SME filtering still remains active.
    ETF_CACHE["date"] = today
    ETF_CACHE["symbols"] = set()

    return set()


# ============================================================
# NSE BHAVCOPY
# ============================================================

def get_bhavcopy(date_obj):
    """
    Get NSE daily bhavcopy for a particular date.
    """

    date_string = date_obj.strftime("%d%m%Y")

    for template in NSE_BHAVCOPY_URLS:

        url = template.format(date=date_string)

        try:
            response = session.get(
                url,
                timeout=REQUEST_TIMEOUT,
            )

            if response.status_code != 200:
                continue

            if not response.content:
                continue

            df = pd.read_csv(
                io.BytesIO(response.content),
                dtype=str,
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
                "TURNOVER_LACS",
            }

            if not required.issubset(set(df.columns)):
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

    # Use same day's cached result
    if (
        HISTORICAL_CACHE["date"] == today
        and HISTORICAL_CACHE["data"] is not None
    ):
        return HISTORICAL_CACHE["data"]

    warm_nse_session()

    all_days = {}

    current_date = today - timedelta(days=1)

    checked_days = 0

    while (
        checked_days < HISTORICAL_LOOKBACK_DAYS
        and len(all_days) < HISTORICAL_DAYS_REQUIRED
    ):

        # Monday etc. naturally skips weekends
        if current_date.weekday() < 5:

            df = get_bhavcopy(current_date)

            if df is not None:

                # Only EQ series.
                # This automatically excludes BE/BZ/SM/ST/SZ
                # from the historical liquidity universe.
                df["SERIES"] = (
                    df["SERIES"]
                    .astype(str)
                    .str.strip()
                    .str.upper()
                )

                eq_df = df[
                    df["SERIES"] == "EQ"
                ].copy()

                if not eq_df.empty:

                    eq_df["SYMBOL"] = (
                        eq_df["SYMBOL"]
                        .astype(str)
                        .str.strip()
                        .str.upper()
                    )

                    eq_df["TURNOVER_LACS"] = pd.to_numeric(
                        eq_df["TURNOVER_LACS"],
                        errors="coerce",
                    )

                    eq_df = eq_df.dropna(
                        subset=["TURNOVER_LACS"]
                    )

                    if not eq_df.empty:

                        turnover_map = {}

                        for _, row in eq_df.iterrows():

                            symbol = normalize_symbol(
                                row["SYMBOL"]
                            )

                            turnover_lacs = clean_number(
                                row["TURNOVER_LACS"]
                            )

                            if (
                                symbol
                                and is_valid_number(
                                    turnover_lacs
                                )
                            ):
                                # NSE TURNOVER_LACS
                                # lakh -> crore = /100
                                turnover_cr = (
                                    turnover_lacs / 100.0
                                )

                                turnover_map[symbol] = (
                                    turnover_cr
                                )

                        if turnover_map:
                            all_days[current_date] = (
                                turnover_map
                            )

        current_date -= timedelta(days=1)
        checked_days += 1

    if len(all_days) < HISTORICAL_DAYS_REQUIRED:
        raise RuntimeError(
            "पिछले 20 valid NSE trading days की "
            "भरोसेमंद bhavcopy नहीं मिल पाई।"
        )

    # Most recent 20 valid trading sessions
    valid_dates = sorted(
        all_days.keys(),
        reverse=True,
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

            value = all_days[dt].get(symbol)

            if is_valid_number(value):
                values.append(value)

        # IMPORTANT:
        # Average only when all 20 valid sessions are available.
        if len(values) == HISTORICAL_DAYS_REQUIRED:

            avg_turnover = (
                sum(values)
                / HISTORICAL_DAYS_REQUIRED
            )

            liquidity[symbol] = {
                "avg_turnover_cr": avg_turnover,
                "days_used": len(values),
            }

    if not liquidity:
        raise RuntimeError(
            "20-day historical liquidity data खाली है."
        )

    HISTORICAL_CACHE["date"] = today
    HISTORICAL_CACHE["data"] = liquidity

    return liquidity


# ============================================================
# NSE EQUITY QUOTE
# ============================================================

def get_quote(symbol):
    """
    Get current NSE equity quote.

    Returns:
        open
        low
        ltp
        series
    """

    params = {
        "symbol": symbol,
    }

    try:

        response = session.get(
            NSE_QUOTE_URL,
            params=params,
            timeout=REQUEST_TIMEOUT,
        )

        if response.status_code != 200:
            return None

        content_type = (
            response.headers.get(
                "Content-Type",
                "",
            ).lower()
        )

        text = response.text.strip()

        if not text:
            return None

        # NSE may return HTML when blocked.
        if (
            "text/html" in content_type
            or text.lower().startswith("<!doctype")
            or text.lower().startswith("<html")
        ):
            return None

        data = response.json()

        price_info = data.get(
            "priceInfo",
            {},
        )

        metadata = data.get(
            "metadata",
            {},
        )

        info = data.get(
            "info",
            {},
        )

        security_info = data.get(
            "securityInfo",
            {},
        )

        open_price = clean_number(
            price_info.get("open")
        )

        ltp = clean_number(
            price_info.get("lastPrice")
        )

        day_low = None

        intraday = price_info.get(
            "intraDayHighLow",
            {},
        )

        if isinstance(intraday, dict):
            day_low = clean_number(
                intraday.get("min")
            )

        if day_low is None:
            day_low = clean_number(
                price_info.get("low")
            )

        series = (
            metadata.get("series")
            or security_info.get("series")
            or info.get("series")
            or ""
        )

        series = str(series).strip().upper()

        if (
            not is_valid_number(open_price)
            or not is_valid_number(day_low)
            or not is_valid_number(ltp)
        ):
            return None

        if open_price <= 0:
            return None

        return {
            "symbol": symbol,
            "open": open_price,
            "low": day_low,
            "ltp": ltp,
            "series": series,
        }

    except Exception:
        return None


# ============================================================
# NSE TRADE INFO
# ============================================================

def get_live_turnover(symbol):
    """
    Get today's current NSE traded value.

    NSE trade-info returns totalTradedValue in ₹ lakh.
    We convert lakh -> crore by /100.
    """

    params = {
        "symbol": symbol,
        "section": "trade_info",
    }

    try:

        response = session.get(
            NSE_TRADE_INFO_URL,
            params=params,
            timeout=REQUEST_TIMEOUT,
        )

        if response.status_code != 200:
            return None

        text = response.text.strip()

        if not text:
            return None

        content_type = (
            response.headers.get(
                "Content-Type",
                "",
            ).lower()
        )

        if (
            "text/html" in content_type
            or text.lower().startswith("<!doctype")
            or text.lower().startswith("<html")
        ):
            return None

        data = response.json()

        market_book = data.get(
            "marketDeptOrderBook",
            {},
        )

        trade_info = market_book.get(
            "tradeInfo",
            {},
        )

        traded_value_lacs = clean_number(
            trade_info.get(
                "totalTradedValue"
            )
        )

        if not is_valid_number(
            traded_value_lacs
        ):
            return None

        # NSE value is in ₹ lakh
        traded_value_cr = (
            traded_value_lacs / 100.0
        )

        return traded_value_cr

    except Exception:
        return None


# ============================================================
# PROCESS ONE SYMBOL
# ============================================================

def process_symbol(
    symbol,
    liquidity,
    etf_symbols,
):
    """
    Process one symbol.

    Order:
      1. Historical liquidity
      2. ETF exclusion
      3. NSE live quote
      4. EQ check
      5. Open-Low gap
      6. LTP > Open
      7. Live turnover
    """

    # Historical liquidity filter
    historical = liquidity.get(symbol)

    if historical is None:
        return None

    avg_turnover = historical[
        "avg_turnover_cr"
    ]

    if avg_turnover < MIN_AVG_TURNOVER_CR:
        return None

    # Official ETF exclusion
    if symbol in etf_symbols:
        return None

    # Get current NSE quote
    quote = get_quote(symbol)

    if quote is None:
        return None

    # EQ only
    series = quote.get(
        "series",
        "",
    ).upper()

    if series and series != "EQ":
        return None

    open_price = quote["open"]
    low_price = quote["low"]
    ltp = quote["ltp"]

    if open_price <= 0:
        return None

    # Open-Low gap
    gap_percent = (
        (open_price - low_price)
        / open_price
    ) * 100.0

    # Protection against impossible negative values
    if gap_percent < 0:
        return None

    if gap_percent > MAX_OPEN_LOW_GAP:
        return None

    # Current price must be above today's open
    if ltp <= open_price:
        return None

    # Only after price conditions pass,
    # request today's live traded value.
    live_turnover = get_live_turnover(
        symbol
    )

    if live_turnover is None:
        # We still know the stock passed the
        # price/liquidity conditions, but today's
        # turnover could not be obtained.
        # Do not show an incorrect value.
        return None

    return {
        "symbol": symbol,
        "open": open_price,
        "low": low_price,
        "ltp": ltp,
        "gap": gap_percent,
        "avg_turnover": avg_turnover,
        "live_turnover": live_turnover,
    }


# ============================================================
# RUN SCANNER
# ============================================================

def run_scanner():

    warm_nse_session()

    # --------------------------------------------------------
    # 1. Historical liquidity
    # --------------------------------------------------------

    liquidity = get_historical_liquidity()

    # --------------------------------------------------------
    # 2. Official ETF list
    # --------------------------------------------------------

    etf_symbols = get_etf_symbols()

    # --------------------------------------------------------
    # 3. NSE EQ universe
    # --------------------------------------------------------

    equity_symbols = get_equity_symbols()

    # --------------------------------------------------------
    # 4. Final historical candidates
    # --------------------------------------------------------

    candidates = []

    for symbol in equity_symbols:

        if symbol not in liquidity:
            continue

        if symbol in etf_symbols:
            continue

        if (
            liquidity[symbol][
                "avg_turnover_cr"
            ]
            >= MIN_AVG_TURNOVER_CR
        ):
            candidates.append(symbol)

    if not candidates:
        raise RuntimeError(
            "₹10 करोड़ average turnover वाले "
            "NSE EQ candidates नहीं मिले."
        )

    # --------------------------------------------------------
    # 5. Process NSE live quotes
    # --------------------------------------------------------

    results = []

    # Small random delay before starting
    time.sleep(
        random.uniform(0.2, 0.6)
    )

    with ThreadPoolExecutor(
        max_workers=MAX_WORKERS
    ) as executor:

        futures = {
            executor.submit(
                process_symbol,
                symbol,
                liquidity,
                etf_symbols,
            ): symbol
            for symbol in candidates
        }

        for future in as_completed(futures):

            try:

                result = future.result()

                if result is not None:
                    results.append(result)

            except Exception:
                continue

    # --------------------------------------------------------
    # 6. Sort by smallest Open-Low gap
    # --------------------------------------------------------

    results.sort(
        key=lambda x: (
            x["gap"],
            -x["live_turnover"],
        )
    )

    return results


# ============================================================
# HTML
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
    margin-bottom: 15px;
}

button:hover {
    background: #125ca8;
}

.error {
    background: #ffe5e5;
    color: #b00020;
    padding: 12px;
    border-radius: 7px;
    margin-bottom: 15px;
    word-break: break-word;
}

.success {
    background: #e8f5e9;
    color: #1b5e20;
    padding: 12px;
    border-radius: 7px;
    margin-bottom: 15px;
}

.table-wrap {
    overflow-x: auto;
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
    text-align: right;
    white-space: nowrap;
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

3. Previous 20 Valid Trading Days Average
Real Turnover ≥ ₹10 Crore<br>

4. NSE EQ only<br>

5. ETF / BE / BZ / SME excluded<br>

6. Today's Current / Live Turnover<br>

7. Ranking: Smallest Open-Low Gap first

</div>

<form method="get">

<button type="submit">
Scan Now
</button>

</form>

{% if error %}

<div class="error">

<b>Scanner Error</b><br><br>

{{ error }}

</div>

{% endif %}

{% if scanned %}

<div class="success">

<b>Scan completed.</b><br>

Qualifying shares:
{{ results|length }}

<br>

<span class="small">
Open-Low Gap के अनुसार smallest से largest क्रम में.
</span>

</div>

{% endif %}

{% if results %}

<div class="table-wrap">

<table>

<thead>

<tr>

<th>#</th>
<th>Symbol</th>
<th>Open ₹</th>
<th>Low ₹</th>
<th>LTP ₹</th>
<th>Open-Low Gap %</th>
<th>20D Avg Turnover ₹ Cr</th>
<th>Today's Live Turnover ₹ Cr</th>

</tr>

</thead>

<tbody>

{% for row in results %}

<tr>

<td style="text-align:center;">
{{ loop.index }}
</td>

<td>
<b>{{ row.symbol }}</b>
</td>

<td>
{{ "%.2f"|format(row.open) }}
</td>

<td>
{{ "%.2f"|format(row.low) }}
</td>

<td>
{{ "%.2f"|format(row.ltp) }}
</td>

<td>
{{ "%.2f"|format(row.gap) }}%
</td>

<td>
{{ "%.2f"|format(row.avg_turnover) }}
</td>

<td>
{{ "%.2f"|format(row.live_turnover) }}
</td>

</tr>

{% endfor %}

</tbody>

</table>

</div>

{% elif scanned and not error %}

<div class="success">

कोई share सभी conditions को पूरा नहीं कर रहा है।

</div>

{% endif %}

</div>

</body>
</html>
"""


# ============================================================
# FLASK ROUTE
# ============================================================

@app.route("/")
def home():

    results = []
    error = None
    scanned = False

    # Scan whenever the page is opened
    # or Scan Now is pressed.
    try:

        scanned = True

        results = run_scanner()

    except Exception as exc:

        error = str(exc)

    return render_template_string(
        HTML,
        results=results,
        error=error,
        scanned=scanned,
    )


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    port = int(
        os.environ.get(
            "PORT",
            "10000",
        )
    )

    app.run(
        host="0.0.0.0",
        port=port,
    )
