import os
import gzip
import json
import time
import logging
import threading

from datetime import datetime, timezone, timedelta
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from flask import Flask, jsonify, send_from_directory


# ============================================================
# CONFIGURATION
# ============================================================

PORT = int(os.environ.get("PORT", "10000"))

BASE = "https://api.upstox.com"

INSTR_URL = (
    "https://assets.upstox.com/"
    "market-quote/instruments/exchange/"
    "complete.json.gz"
)

IST = timezone(timedelta(hours=5, minutes=30))

# Scanner rules
MIN_PRICE = 20.0
MAX_GAP = 0.50
MIN_AVG_TURNOVER = 10_00_00_000.0   # ₹10 crore
LIQUIDITY_DAYS = 20

# Historical API workers
MAX_WORKERS = 5

# Upstox Full Market Quotes V3 limit
BATCH_SIZE = 500

# Cache file
CACHE_FILE = "liquidity_cache.json"


# ============================================================
# APP
# ============================================================

app = Flask(
    __name__,
    static_folder=".",
    static_url_path=""
)


# ============================================================
# GLOBAL STATE
# ============================================================

INSTRUMENTS = []
BY_KEY = {}

INSTRUMENTS_LOADED_AT = 0

LIVE_RESULTS = []
LAST_SCAN_TIME = None
LAST_SCAN_ERROR = ""

SCAN_LOCK = threading.Lock()
SCAN_RUNNING = False


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)


def log(message):
    logging.info(message)


# ============================================================
# TOKEN
# ============================================================

def get_token():
    """
    Read token at runtime.

    This is intentional so the application can clearly detect
    whether UPSTOX_ACCESS_TOKEN exists in Render Environment Variables.
    """
    return os.environ.get(
        "UPSTOX_ACCESS_TOKEN",
        ""
    ).strip()


def get_headers():
    token = get_token()

    if not token:
        raise RuntimeError(
            "UPSTOX_ACCESS_TOKEN is missing in "
            "Render Environment Variables."
        )

    return {
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
    }


# ============================================================
# HTTP SESSION
# ============================================================

http = requests.Session()

http.headers.update({
    "User-Agent": "OpenLowLiquidityScanner/1.0"
})


# ============================================================
# INSTRUMENT FILTER
# ============================================================

def is_real_equity(item):
    """
    Keep only genuine NSE equity shares.

    Exclude:
    - ETFs
    - Mutual Funds
    - Index Funds
    - BE/BZ series
    - obvious SME instruments
    """

    if item.get("segment") != "NSE_EQ":
        return False

    if item.get("instrument_type") != "EQ":
        return False

    if item.get("security_type") not in (
        None,
        "",
        "NORMAL"
    ):
        return False

    instrument_key = item.get("instrument_key")

    if not instrument_key:
        return False

    symbol = str(
        item.get("trading_symbol") or ""
    ).upper().strip()

    name = str(
        item.get("name") or ""
    ).upper().strip()

    short_name = str(
        item.get("short_name") or ""
    ).upper().strip()

    combined = (
        f"{symbol} {name} {short_name}"
    )

    # ETF / fund exclusion
    if "ETF" in combined:
        return False

    if "EXCHANGE TRADED FUND" in combined:
        return False

    if "MUTUAL FUND" in combined:
        return False

    if "INDEX FUND" in combined:
        return False

    # Common ETF suffix
    if symbol.endswith("BEES"):
        return False

    # Exclude BE/BZ series if symbol itself carries it.
    if symbol.endswith("BE"):
        return False

    if symbol.endswith("BZ"):
        return False

    # Exclude obvious SME naming.
    if "SME" in combined:
        return False

    return True


# ============================================================
# LOAD NSE INSTRUMENTS
# ============================================================

def load_instruments(force=False):
    global INSTRUMENTS
    global BY_KEY
    global INSTRUMENTS_LOADED_AT

    # Reuse for 6 hours.
    if (
        not force
        and INSTRUMENTS
        and time.time() - INSTRUMENTS_LOADED_AT < 21600
    ):
        return

    log("Downloading Upstox NSE instrument file...")

    response = http.get(
        INSTR_URL,
        timeout=30
    )

    response.raise_for_status()

    raw = gzip.decompress(
        response.content
    )

    data = json.loads(
        raw.decode("utf-8")
    )

    selected = []

    for item in data:

        try:
            if is_real_equity(item):
                selected.append(item)
        except Exception:
            continue

    INSTRUMENTS = selected

    BY_KEY = {
        item["instrument_key"]: item
        for item in INSTRUMENTS
    }

    INSTRUMENTS_LOADED_AT = time.time()

    log(
        f"Loaded {len(INSTRUMENTS)} "
        "NSE EQ stocks (ETF/SME excluded)."
    )


# ============================================================
# CHUNKS
# ============================================================

def chunks(items, size):
    for i in range(
        0,
        len(items),
        size
    ):
        yield items[i:i + size]


# ============================================================
# LIVE FULL MARKET QUOTES
# ============================================================

def fetch_live_quotes():
    """
    Get today's live quote for the complete NSE EQ universe.

    Uses Upstox Full Market Quotes V3.

    Maximum 500 instrument keys per request.
    """

    load_instruments()

    token = get_token()

    if not token:
        raise RuntimeError(
            "UPSTOX_ACCESS_TOKEN is missing in "
            "Render Environment Variables."
        )

    all_quotes = []

    total = len(INSTRUMENTS)

    log(
        f"Requesting Full Market Quotes for "
        f"{total} NSE EQ stocks..."
    )

    successful = 0
    failed = 0
    batch_no = 0

    for batch in chunks(
        INSTRUMENTS,
        BATCH_SIZE
    ):

        batch_no += 1

        keys = ",".join(
            item["instrument_key"]
            for item in batch
        )

        try:

            response = http.get(
                BASE
                + "/v3/market-quote/quotes",

                headers=get_headers(),

                params={
                    "instrument_key": keys
                },

                timeout=30
            )

            if response.status_code != 200:

                failed += 1

                log(
                    f"Live batch {batch_no} "
                    f"HTTP {response.status_code}: "
                    f"{response.text[:500]}"
                )

                continue

            payload = response.json()

            data = payload.get(
                "data",
                {}
            )

            if not isinstance(
                data,
                dict
            ):

                failed += 1

                log(
                    f"Live batch {batch_no}: "
                    "invalid data returned by Upstox."
                )

                continue

            received = 0

            for response_key, quote_data in data.items():

                if not isinstance(
                    quote_data,
                    dict
                ):
                    continue

                # Usually the response dictionary key itself
                # is the instrument key.
                instrument_key = (
                    quote_data.get(
                        "instrument_token"
                    )
                    or quote_data.get(
                        "instrument_key"
                    )
                    or response_key
                )

                if not instrument_key:
                    continue

                quote_data["_instrument_key"] = (
                    instrument_key
                )

                all_quotes.append(
                    quote_data
                )

                received += 1

            successful += 1

            log(
                f"Live batch {batch_no}: "
                f"{received} quotes received."
            )

        except Exception as e:

            failed += 1

            log(
                f"Live batch {batch_no} "
                f"ERROR: {repr(e)}"
            )

    log(
        f"Live quote batches successful: "
        f"{successful}"
    )

    log(
        f"Live quote batches failed: "
        f"{failed}"
    )

    log(
        f"Total live quotes received: "
        f"{len(all_quotes)}"
    )

    if successful == 0:

        raise RuntimeError(
            "Upstox live market quote API "
            "failed for every batch. "
            "Check the HTTP errors in Render logs."
        )

    return all_quotes


# ============================================================
# LIQUIDITY CACHE
# ============================================================

def load_liquidity_cache():

    if not os.path.exists(
        CACHE_FILE
    ):
        return {}

    try:

        with open(
            CACHE_FILE,
            "r",
            encoding="utf-8"
        ) as f:

            data = json.load(f)

        if isinstance(
            data,
            dict
        ):
            return data

    except Exception as e:

        log(
            f"Liquidity cache read error: "
            f"{repr(e)}"
        )

    return {}


def save_liquidity_cache(cache):

    temp_file = (
        CACHE_FILE
        + ".tmp"
    )

    try:

        with open(
            temp_file,
            "w",
            encoding="utf-8"
        ) as f:

            json.dump(
                cache,
                f
            )

        os.replace(
            temp_file,
            CACHE_FILE
        )

    except Exception as e:

        log(
            f"Liquidity cache save error: "
            f"{repr(e)}"
        )


# ============================================================
# HISTORICAL LIQUIDITY
# ============================================================

def historical_20day_turnover(
    instrument_key,
    today
):
    """
    Previous 20 valid trading days.

    Real turnover =
        Daily Close × Daily Volume

    Returns average turnover in rupees.
    """

    yesterday = (
        today
        - timedelta(days=1)
    )

    start_date = (
        today
        - timedelta(days=45)
    )

    url = (
        BASE
        + "/v3/historical-candle/"
        + quote(
            instrument_key,
            safe="|"
        )
        + "/days/1/"
        + yesterday.isoformat()
        + "/"
        + start_date.isoformat()
    )

    response = http.get(
        url,
        headers=get_headers(),
        timeout=20
    )

    response.raise_for_status()

    candles = (
        response
        .json()
        .get("data", {})
        .get("candles", [])
    )

    valid = []

    for candle in candles:

        if len(candle) < 6:
            continue

        try:

            timestamp = candle[0]

            close = float(
                candle[4]
            )

            volume = float(
                candle[5]
            )

            if close <= 0:
                continue

            if volume <= 0:
                continue

            turnover = (
                close
                * volume
            )

            valid.append(
                (
                    timestamp,
                    turnover
                )
            )

        except Exception:
            continue

    # Newest first
    valid.sort(
        key=lambda x: x[0],
        reverse=True
    )

    if len(valid) < LIQUIDITY_DAYS:
        return None

    latest_20 = [
        x[1]
        for x in valid[:LIQUIDITY_DAYS]
    ]

    return (
        sum(latest_20)
        / len(latest_20)
    )


# ============================================================
# GET LIQUIDITY WITH CACHE
# ============================================================

def get_liquidity(
    instrument_key,
    today,
    cache
):

    today_key = today.isoformat()

    cached = cache.get(
        instrument_key
    )

    if (
        isinstance(cached, dict)
        and cached.get("date") == today_key
        and cached.get("avg_turnover") is not None
    ):

        return float(
            cached["avg_turnover"]
        )

    value = historical_20day_turnover(
        instrument_key,
        today
    )

    if value is not None:

        cache[instrument_key] = {
            "date": today_key,
            "avg_turnover": value
        }

    return value


# ============================================================
# LIVE SNAPSHOT
# ============================================================

def build_live_candidates():

    quotes = fetch_live_quotes()

    today = datetime.now(
        IST
    ).date()

    candidates = []

    rejected_price = 0
    rejected_open_low = 0
    rejected_ltp = 0
    rejected_gap = 0
    processing_errors = 0

    for q in quotes:

        try:

            key = q.get(
                "_instrument_key"
            )

            if not key:
                continue

            meta = BY_KEY.get(
                key,
                {}
            )

            symbol = (
                meta.get(
                    "trading_symbol"
                )
                or q.get("symbol")
                or ""
            )

            price = float(
                q.get("last_price")
                or 0
            )

            # ------------------------------------------------
            # PRICE > ₹20
            # ------------------------------------------------

            if price <= MIN_PRICE:

                rejected_price += 1

                continue

            # ------------------------------------------------
            # TODAY'S OHLC
            # ------------------------------------------------

            ohlc = (
                q.get("ohlc")
                or {}
            )

            op = float(
                ohlc.get("open")
                or 0
            )

            low = float(
                ohlc.get("low")
                or 0
            )

            volume = float(
                q.get("volume")
                or ohlc.get("volume")
                or 0
            )

            average_price = float(
                q.get("average_price")
                or 0
            )

            # ------------------------------------------------
            # OPEN / LOW MUST EXIST
            # ------------------------------------------------

            if op <= 0 or low <= 0:

                rejected_open_low += 1

                continue

            # ------------------------------------------------
            # LTP > OPEN
            # ------------------------------------------------

            if price <= op:

                rejected_ltp += 1

                continue

            # ------------------------------------------------
            # OPEN-LOW GAP
            # ------------------------------------------------

            gap = (
                (op - low)
                / op
            ) * 100.0

            if gap > MAX_GAP:

                rejected_gap += 1

                continue

            # ------------------------------------------------
            # LIVE TURNOVER
            # ------------------------------------------------

            live_price_for_turnover = (
                average_price
                if average_price > 0
                else price
            )

            live_turnover = (
                volume
                * live_price_for_turnover
            )

            candidates.append({

                "key": key,

                "symbol": symbol,

                "price": price,

                "open": op,

                "low": low,

                "gap": gap,

                "volume": volume,

                "live_turnover": live_turnover,

                "today": today.isoformat(),

            })

        except Exception as e:

            processing_errors += 1

            log(
                f"Live quote processing error: "
                f"{repr(e)}"
            )

    # Smallest gap first
    candidates.sort(
        key=lambda x: x["gap"]
    )

    log(
        f"Live quotes: {len(quotes)}"
    )

    log(
        f"Live candidates after price/open/LTP/gap filters: "
        f"{len(candidates)}"
    )

    log(
        f"Rejected price <= ₹{MIN_PRICE:g}: "
        f"{rejected_price}"
    )

    log(
        f"Rejected missing Open/Low: "
        f"{rejected_open_low}"
    )

    log(
        f"Rejected LTP <= Open: "
        f"{rejected_ltp}"
    )

    log(
        f"Rejected Gap > {MAX_GAP:.2f}%: "
        f"{rejected_gap}"
    )

    log(
        f"Processing errors: "
        f"{processing_errors}"
    )

    return candidates


# ============================================================
# HISTORICAL LIQUIDITY FILTER
# ============================================================

def add_liquidity_to_candidates(
    candidates
):

    cache = load_liquidity_cache()

    today = datetime.now(
        IST
    ).date()

    qualified = []

    cache_hits = 0
    history_requests = 0
    history_errors = 0

    # --------------------------------------------------------
    # First separate cached and uncached candidates.
    # --------------------------------------------------------

    pending = []

    for candidate in candidates:

        key = candidate["key"]

        cached = cache.get(
            key
        )

        if (
            isinstance(cached, dict)
            and cached.get("date")
            == today.isoformat()
            and cached.get("avg_turnover")
            is not None
        ):

            avg_turnover = float(
                cached["avg_turnover"]
            )

            cache_hits += 1

            candidate[
                "avg_turnover"
            ] = avg_turnover

            if (
                avg_turnover
                >= MIN_AVG_TURNOVER
            ):

                qualified.append(
                    candidate
                )

        else:

            pending.append(
                candidate
            )

    log(
        f"Liquidity cache hits: "
        f"{cache_hits}"
    )

    log(
        f"Historical liquidity requests needed: "
        f"{len(pending)}"
    )

    # --------------------------------------------------------
    # Historical requests in parallel.
    # --------------------------------------------------------

    if pending:

        def worker(candidate):

            key = candidate["key"]

            try:

                value = historical_20day_turnover(
                    key,
                    today
                )

                return (
                    key,
                    value,
                    None
                )

            except Exception as e:

                return (
                    key,
                    None,
                    repr(e)
                )

        with ThreadPoolExecutor(
            max_workers=MAX_WORKERS
        ) as executor:

            futures = [
                executor.submit(
                    worker,
                    candidate
                )
                for candidate in pending
            ]

            for future in as_completed(
                futures
            ):

                key, value, error = (
                    future.result()
                )

                history_requests += 1

                if error:

                    history_errors += 1

                    log(
                        f"Historical liquidity "
                        f"error {key}: "
                        f"{error}"
                    )

                    continue

                if value is None:

                    continue

                cache[key] = {

                    "date":
                        today.isoformat(),

                    "avg_turnover":
                        value

                }

                # Find original candidate
                # and add liquidity.
                for candidate in pending:

                    if candidate["key"] == key:

                        candidate[
                            "avg_turnover"
                        ] = value

                        if (
                            value
                            >= MIN_AVG_TURNOVER
                        ):

                            qualified.append(
                                candidate
                            )

                        break

        save_liquidity_cache(
            cache
        )

    # --------------------------------------------------------
    # Sort by smallest gap.
    # --------------------------------------------------------

    qualified.sort(
        key=lambda x: x["gap"]
    )

    log(
        f"Historical requests completed: "
        f"{history_requests}"
    )

    log(
        f"Historical errors: "
        f"{history_errors}"
    )

    log(
        f"Final qualifying stocks: "
        f"{len(qualified)}"
    )

    return qualified


# ============================================================
# FORMAT RESULT
# ============================================================

def format_result(item):

    avg_turnover = float(
        item.get(
            "avg_turnover",
            0
        )
    )

    live_turnover = float(
        item.get(
            "live_turnover",
            0
        )
    )

    return {

        "symbol":
            item["symbol"],

        "price":
            round(
                item["price"],
                2
            ),

        "open":
            round(
                item["open"],
                2
            ),

        "low":
            round(
                item["low"],
                2
            ),

        "gap":
            round(
                item["gap"],
                3
            ),

        "volume":
            int(
                item.get(
                    "volume",
                    0
                )
            ),

        "live_turnover":
            live_turnover,

        "live_turnover_cr":
            round(
                live_turnover
                / 1_00_00_000.0,
                2
            ),

        "avg_turnover":
            avg_turnover,

        "avg_turnover_cr":
            round(
                avg_turnover
                / 1_00_00_000.0,
                2
            ),

        "today":
            item.get(
                "today",
                ""
            ),

    }


# ============================================================
# SCAN ENGINE
# ============================================================

def perform_scan():

    global LAST_SCAN_TIME
    global LAST_SCAN_ERROR

    LAST_SCAN_ERROR = ""

    log(
        "=========================================="
    )

    log(
        "STARTING OPEN-LOW LIQUIDITY SCAN"
    )

    log(
        "=========================================="
    )

    # Check token first.
    token = get_token()

    if not token:

        raise RuntimeError(
            "UPSTOX_ACCESS_TOKEN is missing in "
            "Render Environment Variables."
        )

    load_instruments()

    live_candidates = (
        build_live_candidates()
    )

    if not live_candidates:

        log(
            "No live candidates. "
            "Historical check not required."
        )

        return []

    final_candidates = (
        add_liquidity_to_candidates(
            live_candidates
        )
    )

    results = [
        format_result(x)
        for x in final_candidates
    ]

    LAST_SCAN_TIME = datetime.now(
        IST
    ).strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    log(
        f"SCAN COMPLETE: "
        f"{len(results)} qualifying stocks."
    )

    return results


# ============================================================
# BACKGROUND SCAN
# ============================================================

def start_scan():

    global SCAN_RUNNING
    global LIVE_RESULTS
    global LAST_SCAN_ERROR

    with SCAN_LOCK:

        if SCAN_RUNNING:

            log(
                "Scan already running. "
                "Not starting another scan."
            )

            return False

        SCAN_RUNNING = True

    def runner():

        global SCAN_RUNNING
        global LIVE_RESULTS
        global LAST_SCAN_ERROR

        try:

            results = perform_scan()

            LIVE_RESULTS = results

        except Exception as e:

            LAST_SCAN_ERROR = (
                repr(e)
            )

            log(
                f"SCAN ERROR: "
                f"{repr(e)}"
            )

        finally:

            with SCAN_LOCK:
                SCAN_RUNNING = False

    thread = threading.Thread(
        target=runner,
        daemon=True
    )

    thread.start()

    return True


# ============================================================
# HOME
# ============================================================

@app.get("/")
def home():

    return send_from_directory(
        ".",
        "index.html"
    )


# ============================================================
# HEALTH
# ============================================================

@app.get("/api/health")
def health():

    token_exists = bool(
        get_token()
    )

    return jsonify({

        "ok": True,

        "token_configured":
            token_exists,

        "nse_eq_stocks":
            len(INSTRUMENTS),

        "scan_running":
            SCAN_RUNNING,

        "last_scan":
            LAST_SCAN_TIME,

        "last_error":
            LAST_SCAN_ERROR,

        "updated_at":
            datetime.now(
                IST
            ).strftime(
                "%Y-%m-%d %H:%M:%S"
            )

    })


# ============================================================
# SCAN
# ============================================================

@app.get("/api/scan")
def scan():

    global LAST_SCAN_ERROR

    token = get_token()

    if not token:

        return jsonify({

            "connected":
                False,

            "scanned":
                len(INSTRUMENTS),

            "running":
                False,

            "results":
                [],

            "message":
                "UPSTOX_ACCESS_TOKEN अभी Render "
                "Environment Variables में नहीं मिला।"

        }), 500

    # If there is no result yet, start scan.
    if not LIVE_RESULTS:

        start_scan()

    # If scan is already running, return warmup state.
    if SCAN_RUNNING:

        return jsonify({

            "connected":
                True,

            "running":
                True,

            "scanned":
                len(INSTRUMENTS),

            "updated_at":
                datetime.now(
                    IST
                ).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),

            "message":
                "Scanner चल रहा है। "
                "Live Open-Low candidates और "
                "20-day liquidity check हो रहा है।",

            "results":
                LIVE_RESULTS

        })

    return jsonify({

        "connected":
            True,

        "running":
            False,

        "scanned":
            len(INSTRUMENTS),

        "updated_at":
            LAST_SCAN_TIME
            or datetime.now(
                IST
            ).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),

        "message":
            LAST_SCAN_ERROR
            or "Live Open-Low Liquidity Scanner",

        "results":
            LIVE_RESULTS

    })


# ============================================================
# SCAN NOW
# ============================================================

@app.get("/api/scan-now")
def scan_now():

    global LIVE_RESULTS
    global LAST_SCAN_ERROR

    token = get_token()

    if not token:

        return jsonify({

            "connected":
                False,

            "running":
                False,

            "results":
                [],

            "message":
                "UPSTOX_ACCESS_TOKEN अभी Render "
                "Environment Variables में नहीं मिला।"

        }), 500

    # If already scanning, don't start duplicate scan.
    if SCAN_RUNNING:

        return jsonify({

            "connected":
                True,

            "running":
                True,

            "results":
                LIVE_RESULTS,

            "message":
                "एक scan पहले से चल रहा है।"

        })

    # Clear old result so UI knows fresh scan is running.
    LIVE_RESULTS = []

    LAST_SCAN_ERROR = ""

    start_scan()

    return jsonify({

        "connected":
            True,

        "running":
            True,

        "results":
            [],

        "message":
            "Fresh scan शुरू हो गया है..."

    })


# ============================================================
# STARTUP
# ============================================================

if __name__ == "__main__":

    try:

        load_instruments()

    except Exception as e:

        LAST_SCAN_ERROR = (
            f"Instrument file error: {repr(e)}"
        )

        log(
            LAST_SCAN_ERROR
        )

    app.run(
        host="0.0.0.0",
        port=PORT
    )
