import os
import gzip
import json
import time
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote
from zoneinfo import ZoneInfo

import requests
from flask import Flask, jsonify, send_from_directory


# =========================================================
# OPEN-LOW LIQUIDITY FAST SCANNER
# =========================================================

PORT = int(os.environ.get("PORT", "10000"))
TOKEN = os.environ.get("UPSTOX_ACCESS_TOKEN", "").strip()

BASE = "https://api.upstox.com"
INSTR_URL = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"

IST = ZoneInfo("Asia/Kolkata")

# ---------------- SETTINGS ----------------

MIN_PRICE = 20.0
MAX_GAP = 0.50

MIN_AVG_TURNOVER = 10_00_00_000.0   # ₹10 Crore
LIQUIDITY_DAYS = 20

MAX_WORKERS = 5
CACHE_FILE = "liquidity_cache.json"

# ----------------------------------------------------------

app = Flask(__name__, static_folder=".", static_url_path="")

http = requests.Session()

INSTRUMENTS = []
BY_KEY = {}
LOADED_AT = 0

LIVE = {}
RESULTS = []
CANDIDATES = []

LAST_SCAN = ""
SCAN_RUNNING = False
SCAN_ERROR = ""

LIQUIDITY_DONE = 0
LIQUIDITY_TOTAL = 0

LOCK = threading.RLock()


# =========================================================
# UPSTOX HEADERS
# =========================================================

def headers():
    return {
        "Accept": "application/json",
        "Authorization": f"Bearer {TOKEN}",
    }


# =========================================================
# REAL NSE EQUITY FILTER
# =========================================================

def is_real_equity(x):

    if x.get("segment") != "NSE_EQ":
        return False

    if x.get("instrument_type") != "EQ":
        return False

    if x.get("security_type") not in (None, "", "NORMAL"):
        return False

    key = x.get("instrument_key")

    symbol = str(
        x.get("trading_symbol") or ""
    ).upper().strip()

    if not key or not symbol:
        return False

    text = " ".join([
        symbol,
        str(x.get("name") or "").upper(),
        str(x.get("short_name") or "").upper(),
    ])

    # ETF / FUND protection
    if "ETF" in text:
        return False

    if symbol.endswith("BEES"):
        return False

    if "EXCHANGE TRADED FUND" in text:
        return False

    if "MUTUAL FUND" in text:
        return False

    if "INDEX FUND" in text:
        return False

    # SME protection
    if " SME" in text or "SME " in text:
        return False

    return True


# =========================================================
# LOAD NSE EQUITY INSTRUMENTS
# =========================================================

def load_instruments():

    global INSTRUMENTS
    global BY_KEY
    global LOADED_AT

    with LOCK:

        if INSTRUMENTS and time.time() - LOADED_AT < 21600:
            return

    r = http.get(
        INSTR_URL,
        timeout=30
    )

    r.raise_for_status()

    data = json.loads(
        gzip.decompress(r.content)
    )

    arr = [
        x for x in data
        if is_real_equity(x)
    ]

    with LOCK:

        INSTRUMENTS = arr

        BY_KEY = {
            x["instrument_key"]: x
            for x in arr
        }

        LOADED_AT = time.time()

    print(
        f"Loaded {len(INSTRUMENTS)} NSE EQ stocks"
    )


# =========================================================
# BATCH HELPER
# =========================================================

def chunks(items, size):

    for i in range(0, len(items), size):
        yield items[i:i + size]


# =========================================================
# LIVE DAILY OHLC
# =========================================================

def fetch_live_quotes():

    load_instruments()

    out = []

    for batch in chunks(INSTRUMENTS, 500):

        keys = ",".join(
            x["instrument_key"]
            for x in batch
        )

        r = http.get(
            BASE + "/v3/market-quote/ohlc",
            headers=headers(),
            params={
                "instrument_key": keys,
                "interval": "1d"
            },
            timeout=30
        )

        r.raise_for_status()

        data = r.json().get(
            "data",
            {}
        )

        # Keep the instrument key because
        # Upstox returns it as the dictionary key.
        for instrument_key, quote_data in data.items():

            quote_data["_instrument_key"] = instrument_key

            out.append(
                quote_data
            )

    return out


# =========================================================
# CACHE
# =========================================================

def load_cache():

    try:

        with open(
            CACHE_FILE,
            "r",
            encoding="utf-8"
        ) as f:

            data = json.load(f)

        if isinstance(data, dict):
            return data

    except Exception:
        pass

    return {}


def save_cache(cache):

    temp_file = CACHE_FILE + ".tmp"

    with open(
        temp_file,
        "w",
        encoding="utf-8"
    ) as f:

        json.dump(
            cache,
            f,
            separators=(",", ":")
        )

    os.replace(
        temp_file,
        CACHE_FILE
    )


LIQUIDITY_CACHE = load_cache()


# =========================================================
# PREVIOUS 20 VALID TRADING DAYS
# =========================================================

def historical_20day_turnover(
    instrument_key,
    today
):

    yesterday = today - timedelta(days=1)

    start_date = today - timedelta(days=45)

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

    r = http.get(
        url,
        headers=headers(),
        timeout=20
    )

    r.raise_for_status()

    candles = (
        r.json()
        .get("data", {})
        .get("candles", [])
    )

    valid = []

    for candle in candles:

        if len(candle) < 6:
            continue

        try:

            timestamp = candle[0]

            close = float(candle[4])
            volume = float(candle[5])

            if close <= 0 or volume <= 0:
                continue

            turnover = close * volume

            valid.append(
                (
                    timestamp,
                    turnover
                )
            )

        except Exception:
            continue

    # Make sure newest trading days are selected correctly.
    valid.sort(
        key=lambda x: x[0],
        reverse=True
    )

    if len(valid) < LIQUIDITY_DAYS:
        return None

    turnovers = [
        x[1]
        for x in valid[:LIQUIDITY_DAYS]
    ]

    return sum(turnovers) / len(turnovers)


# =========================================================
# ADD RESULT
# =========================================================

def add_result(row):

    global RESULTS

    with LOCK:

        RESULTS = [
            x
            for x in RESULTS
            if x["symbol"] != row["symbol"]
        ]

        RESULTS.append(row)

        RESULTS.sort(
            key=lambda x: (
                x["gap"],
                -x["avg_turnover"]
            )
        )


# =========================================================
# BUILD TODAY'S FAST SNAPSHOT
# =========================================================

def build_live_snapshot():

    quotes = fetch_live_quotes()

    today = datetime.now(
        IST
    ).date()

    live = []

    for q in quotes:

        try:

            instrument_key = q.get(
                "_instrument_key"
            )

            meta = BY_KEY.get(
                instrument_key,
                {}
            )

            symbol = (
                meta.get("trading_symbol")
                or q.get("symbol")
                or ""
            )

            price = float(
                q.get("last_price") or 0
            )

            ohlc = q.get(
                "ohlc"
            ) or {}

            op = float(
                ohlc.get("open") or 0
            )

            low = float(
                ohlc.get("low") or 0
            )

            volume = float(
                q.get("volume")
                or ohlc.get("volume")
                or 0
            )

            avg_price = float(
                q.get("average_price") or 0
            )

            # -------------------------
            # PRICE > ₹20
            # -------------------------

            if price <= MIN_PRICE:
                continue

            # -------------------------
            # Valid OHLC
            # -------------------------

            if op <= 0 or low <= 0:
                continue

            # -------------------------
            # LTP > TODAY OPEN
            # -------------------------

            if price <= op:
                continue

            # -------------------------
            # OPEN-LOW GAP
            # -------------------------

            gap = (
                (op - low)
                / op
            ) * 100.0

            if gap > MAX_GAP:
                continue

            # -------------------------
            # LIVE TURNOVER
            # -------------------------

            if avg_price > 0:
                live_turnover = (
                    volume * avg_price
                )
            else:
                live_turnover = (
                    volume * price
                )

            live.append({

                "key": instrument_key,

                "symbol": symbol,

                "price": price,

                "open": op,

                "low": low,

                "gap": gap,

                "volume": volume,

                "live_turnover": live_turnover,

                "today": today.isoformat()
            })

        except Exception:

            continue

    # Smallest Open-Low gap first

    live.sort(
        key=lambda x: x["gap"]
    )

    with LOCK:

        LIVE.clear()

        LIVE.update({
            x["symbol"]: x
            for x in live
        })

    return live


# =========================================================
# HISTORICAL LIQUIDITY WORKER
# =========================================================

def liquidity_worker(
    candidates,
    today
):

    global SCAN_RUNNING
    global SCAN_ERROR
    global LIQUIDITY_DONE
    global LIQUIDITY_TOTAL

    with LOCK:

        LIQUIDITY_DONE = 0

        LIQUIDITY_TOTAL = len(
            candidates
        )

        SCAN_ERROR = ""

    futures = {}

    with ThreadPoolExecutor(
        max_workers=MAX_WORKERS
    ) as pool:

        for row in candidates:

            cache_key = row["symbol"]

            cached = (
                LIQUIDITY_CACHE.get(
                    cache_key
                )
            )

            # -------------------------
            # USE TODAY'S CACHE
            # -------------------------

            if (
                cached
                and cached.get("date")
                == today.isoformat()
            ):

                try:

                    avg = float(
                        cached[
                            "avg_turnover"
                        ]
                    )

                    if avg >= MIN_AVG_TURNOVER:

                        add_result({

                            **row,

                            "avg_turnover": avg,

                            "liquidity": "PASS"
                        })

                except Exception:
                    pass

                with LOCK:
                    LIQUIDITY_DONE += 1

            else:

                future = pool.submit(
                    historical_20day_turnover,
                    row["key"],
                    today
                )

                futures[future] = row

        # -------------------------
        # PROGRESSIVE RESULTS
        # -------------------------

        for future in as_completed(
            futures
        ):

            row = futures[future]

            try:

                avg = future.result()

                if avg is not None:

                    LIQUIDITY_CACHE[
                        row["symbol"]
                    ] = {

                        "date":
                        today.isoformat(),

                        "avg_turnover":
                        avg
                    }

                    if avg >= MIN_AVG_TURNOVER:

                        add_result({

                            **row,

                            "avg_turnover":
                            avg,

                            "liquidity":
                            "PASS"
                        })

            except Exception as e:

                with LOCK:

                    SCAN_ERROR = str(e)

            finally:

                with LOCK:

                    LIQUIDITY_DONE += 1

    try:

        save_cache(
            LIQUIDITY_CACHE
        )

    except Exception as e:

        with LOCK:

            SCAN_ERROR = str(e)

    with LOCK:

        SCAN_RUNNING = False


# =========================================================
# START SCAN
# =========================================================

def start_scan(
    force=False
):

    global SCAN_RUNNING
    global RESULTS
    global CANDIDATES
    global LAST_SCAN
    global SCAN_ERROR

    with LOCK:

        if (
            SCAN_RUNNING
            and not force
        ):
            return

        SCAN_RUNNING = True

        RESULTS = []

        CANDIDATES = []

        SCAN_ERROR = ""

        LAST_SCAN = (
            datetime.now(IST)
            .strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        )

    def runner():

        global SCAN_RUNNING
        global CANDIDATES
        global SCAN_ERROR

        try:

            # First do today's very fast scan.

            live = build_live_snapshot()

            with LOCK:

                CANDIDATES = live

            # Then check 20-day liquidity
            # only for today's candidates.

            today = datetime.now(
                IST
            ).date()

            liquidity_worker(
                live,
                today
            )

        except Exception as e:

            with LOCK:

                SCAN_ERROR = repr(e)

                SCAN_RUNNING = False

    threading.Thread(
        target=runner,
        daemon=True
    ).start()


# =========================================================
# HOME PAGE
# =========================================================

@app.get("/")
def home():

    return send_from_directory(
        ".",
        "index.html"
    )


# =========================================================
# SCAN API
# =========================================================

@app.get("/api/scan")
def api_scan():

    # IMPORTANT:
    # Opening another browser tab does NOT
    # restart the historical scan.

    with LOCK:

        if (
            not LIVE
            and not SCAN_RUNNING
        ):

            start_scan()

        total = len(
            INSTRUMENTS
        )

        candidates = len(
            CANDIDATES
        )

        done = LIQUIDITY_DONE

        results = list(
            RESULTS
        )

        running = SCAN_RUNNING

        last_scan = LAST_SCAN

        error = SCAN_ERROR

    return jsonify({

        "running":
        running,

        "scanned_instruments":
        total,

        "candidates":
        candidates,

        "liquidity_done":
        done,

        "liquidity_total":
        candidates,

        "results":
        results[:50],

        "last_scan":
        last_scan,

        "error":
        error,

        "rules": {

            "price":
            "> ₹20",

            "open_low_gap":
            "<= 0.50%",

            "ltp":
            "> today's open",

            "avg_turnover":
            ">= ₹10 crore over previous 20 valid trading days",

            "segment":
            "NSE EQ only; ETF/BE/BZ/SME excluded",

            "sort":
            "smallest Open-Low gap first"
        }
    })


# =========================================================
# MANUAL SCAN BUTTON
# =========================================================

@app.post("/api/scan-now")
def api_scan_now():

    start_scan(
        force=True
    )

    return jsonify({

        "ok":
        True,

        "message":
        "Fast scan started"
    })


# =========================================================
# START SERVER
# =========================================================

if __name__ == "__main__":

    if not TOKEN:

        print(
            "WARNING: "
            "UPSTOX_ACCESS_TOKEN is missing"
        )

    try:

        load_instruments()

    except Exception as e:

        print(
            "Instrument load error:",
            repr(e)
        )

    app.run(
        host="0.0.0.0",
        port=PORT,
        threaded=True
    )
