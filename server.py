import os
import json
import gzip
import threading
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from flask import Flask, jsonify, send_from_directory


# =========================================================
# CONFIGURATION
# =========================================================

BASE = "https://api.upstox.com"

IST = timezone(timedelta(hours=5, minutes=30))

MIN_PRICE = 20.0
MAX_GAP = 0.50

# ₹10 Crore
MIN_AVG_TURNOVER = 10_00_00_000.0

LIQUIDITY_DAYS = 20

# Historical API workers
MAX_WORKERS = 5

CACHE_FILE = "liquidity_cache.json"

INSTRUMENTS_URL = (
    "https://assets.upstox.com/"
    "market-quote/instruments/exchange/"
    "complete.json.gz"
)


# =========================================================
# FLASK
# =========================================================

app = Flask(__name__, static_folder=".")


# =========================================================
# GLOBAL STATE
# =========================================================

INSTRUMENTS = []
BY_KEY = {}

LIVE = {}

SCAN_STATE = {
    "running": False,
    "finished": False,
    "started_at": None,
    "finished_at": None,
    "live_candidates": 0,
    "checked": 0,
    "total": 0,
    "results": [],
    "error": None,
}

LOCK = threading.Lock()

SCAN_THREAD = None


# =========================================================
# HTTP SESSION
# =========================================================

http = requests.Session()

http.headers.update({
    "Accept": "application/json"
})


# =========================================================
# HELPERS
# =========================================================

def now_ist():
    return datetime.now(IST)


def chunks(items, size):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def headers():
    token = os.getenv("UPSTOX_ACCESS_TOKEN", "").strip()

    if not token:
        raise RuntimeError(
            "UPSTOX_ACCESS_TOKEN environment variable is missing."
        )

    return {
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
    }


# =========================================================
# INSTRUMENT FILTER
# =========================================================

def is_real_equity(x):
    """
    Keep only genuine NSE equity shares.

    Exclude:
    - ETF
    - SME
    - BE
    - BZ
    - other non-equity instruments
    """

    try:
        segment = str(
            x.get("segment", "")
        ).upper().strip()

        instrument_type = str(
            x.get("instrument_type", "")
        ).upper().strip()

        security_type = str(
            x.get("security_type", "")
        ).upper().strip()

        exchange = str(
            x.get("exchange", "")
        ).upper().strip()

        trading_symbol = str(
            x.get("trading_symbol", "")
        ).upper().strip()

        instrument_key = str(
            x.get("instrument_key", "")
        ).upper().strip()

        # Must be NSE
        if exchange and exchange != "NSE":
            return False

        # Must be NSE_EQ
        if segment and segment != "NSE_EQ":
            return False

        if instrument_key and not instrument_key.startswith(
            "NSE_EQ|"
        ):
            return False

        # Must be equity
        if instrument_type and instrument_type != "EQ":
            return False

        # Security type, when supplied, should be equity
        if security_type:
            allowed_security = {
                "EQUITY",
                "EQ",
                "STOCK"
            }

            if security_type not in allowed_security:
                return False

        # Exclude obvious non-equity symbols
        bad_words = [
            "ETF",
            "LIQUIDBEES",
            "GOLDBEES",
            "SILVERBEES",
            "MON100",
            "JUNIORBEES",
            "BANKBEES",
            "ITBEES",
            "PHARMABEES",
            "AUTOBEES",
            "PSUBANK",
            "CPSEETF",
            "SETFNIF",
            "NIFTYBEES",
        ]

        for word in bad_words:
            if word in trading_symbol:
                return False

        # Exclude SME-style symbols if present
        if trading_symbol.endswith("-SM"):
            return False

        if trading_symbol.endswith("-BE"):
            return False

        if trading_symbol.endswith("-BZ"):
            return False

        if trading_symbol.endswith("BE"):
            # Only use this when symbol clearly carries BE suffix.
            if trading_symbol.endswith("-BE"):
                return False

        return True

    except Exception:
        return False


# =========================================================
# LOAD UPSTOX INSTRUMENTS
# =========================================================

def load_instruments():
    global INSTRUMENTS
    global BY_KEY

    with LOCK:
        if INSTRUMENTS:
            return

    print("Loading Upstox complete instruments...")

    r = http.get(
        INSTRUMENTS_URL,
        timeout=60
    )

    r.raise_for_status()

    raw = gzip.decompress(
        r.content
    )

    data = json.loads(
        raw.decode("utf-8")
    )

    instruments = []

    for x in data:

        if not isinstance(x, dict):
            continue

        if not is_real_equity(x):
            continue

        key = x.get(
            "instrument_key"
        )

        symbol = x.get(
            "trading_symbol"
        )

        if not key or not symbol:
            continue

        instruments.append({
            "instrument_key": key,
            "trading_symbol": symbol,
            "name": x.get("name", ""),
            "isin": x.get("isin", ""),
            "exchange": x.get("exchange", ""),
            "segment": x.get("segment", ""),
            "instrument_type": x.get(
                "instrument_type", ""
            ),
            "security_type": x.get(
                "security_type", ""
            ),
        })

    by_key = {
        x["instrument_key"]: x
        for x in instruments
    }

    with LOCK:
        INSTRUMENTS = instruments
        BY_KEY = by_key

    print(
        f"Loaded {len(INSTRUMENTS)} NSE EQ stocks."
    )


# =========================================================
# LIQUIDITY CACHE
# =========================================================

def load_cache():

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

            if isinstance(data, dict):
                return data

    except Exception as e:

        print(
            "Cache load error:",
            repr(e)
        )

    return {}


def save_cache(cache):

    try:

        temp_file = CACHE_FILE + ".tmp"

        with open(
            temp_file,
            "w",
            encoding="utf-8"
        ) as f:

            json.dump(
                cache,
                f,
                ensure_ascii=False
            )

        os.replace(
            temp_file,
            CACHE_FILE
        )

    except Exception as e:

        print(
            "Cache save error:",
            repr(e)
        )


LIQUIDITY_CACHE = load_cache()


# =========================================================
# UPSTOX LIVE QUOTES
# =========================================================

def fetch_live_quotes():

    load_instruments()

    out = []

    print(
        f"Requesting live OHLC for "
        f"{len(INSTRUMENTS)} NSE EQ stocks..."
    )

    batch_number = 0

    for batch in chunks(
        INSTRUMENTS,
        500
    ):

        batch_number += 1

        keys = ",".join(
            x["instrument_key"]
            for x in batch
        )

        try:

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

            if not isinstance(
                data,
                dict
            ):
                continue

            print(
                f"Live batch {batch_number}: "
                f"{len(data)} quotes received."
            )

            # IMPORTANT:
            # Upstox dictionary key may be symbol-like.
            # Actual instrument key is inside instrument_token.
            for _, quote_data in data.items():

                if not isinstance(
                    quote_data,
                    dict
                ):
                    continue

                instrument_key = (
                    quote_data.get(
                        "instrument_token"
                    )
                    or quote_data.get(
                        "instrument_key"
                    )
                )

                if not instrument_key:
                    continue

                quote_data[
                    "_instrument_key"
                ] = instrument_key

                out.append(
                    quote_data
                )

        except Exception as e:

            print(
                f"Live batch {batch_number} error:",
                repr(e)
            )

    print(
        f"Total live quotes received: {len(out)}"
    )

    return out


# =========================================================
# BUILD LIVE SNAPSHOT
# =========================================================

def build_live_snapshot():

    quotes = fetch_live_quotes()

    today = now_ist().date()

    live = []

    rejected_price = 0
    rejected_open = 0
    rejected_direction = 0
    rejected_gap = 0
    rejected_error = 0

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
                meta.get(
                    "trading_symbol"
                )
                or q.get(
                    "symbol"
                )
                or ""
            )

            if not symbol:
                rejected_error += 1
                continue

            price = float(
                q.get(
                    "last_price"
                )
                or 0
            )

            # -------------------------------------------------
            # IMPORTANT:
            # Upstox V3 current daily OHLC is in live_ohlc
            # -------------------------------------------------

            live_ohlc = q.get(
                "live_ohlc"
            ) or {}

            op = float(
                live_ohlc.get(
                    "open"
                )
                or 0
            )

            low = float(
                live_ohlc.get(
                    "low"
                )
                or 0
            )

            volume = float(
                live_ohlc.get(
                    "volume"
                )
                or q.get(
                    "volume"
                )
                or 0
            )

            avg_price = float(
                q.get(
                    "average_price"
                )
                or live_ohlc.get(
                    "average_price"
                )
                or 0
            )

            # -------------------------------------------------
            # PRICE > ₹20
            # -------------------------------------------------

            if price <= MIN_PRICE:

                rejected_price += 1
                continue

            # -------------------------------------------------
            # OPEN / LOW MUST EXIST
            # -------------------------------------------------

            if op <= 0 or low <= 0:

                rejected_open += 1
                continue

            # -------------------------------------------------
            # LTP > TODAY OPEN
            # -------------------------------------------------

            if price <= op:

                rejected_direction += 1
                continue

            # -------------------------------------------------
            # OPEN-LOW GAP
            # -------------------------------------------------

            gap = (
                (op - low)
                / op
            ) * 100.0

            if gap > MAX_GAP:

                rejected_gap += 1
                continue

            # -------------------------------------------------
            # LIVE TURNOVER
            # -------------------------------------------------

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

        except Exception as e:

            rejected_error += 1

            print(
                "Quote processing error:",
                repr(e)
            )

            continue

    # Smallest Open-Low Gap first
    live.sort(
        key=lambda x: x["gap"]
    )

    with LOCK:

        LIVE.clear()

        LIVE.update({
            x["symbol"]: x
            for x in live
        })

    print(
        "----------------------------------------"
    )

    print(
        f"Live quotes: {len(quotes)}"
    )

    print(
        f"Live candidates: {len(live)}"
    )

    print(
        f"Rejected price <= ₹20: "
        f"{rejected_price}"
    )

    print(
        f"Rejected missing Open/Low: "
        f"{rejected_open}"
    )

    print(
        f"Rejected LTP <= Open: "
        f"{rejected_direction}"
    )

    print(
        f"Rejected Gap > {MAX_GAP}%: "
        f"{rejected_gap}"
    )

    print(
        f"Quote errors: {rejected_error}"
    )

    print(
        "----------------------------------------"
    )

    return live


# =========================================================
# HISTORICAL 20-DAY REAL TURNOVER
# =========================================================

def historical_20day_turnover(
    instrument_key,
    today
):

    yesterday = (
        today
        - timedelta(days=1)
    )

    # 45 calendar days gives enough room
    # for weekends/holidays.
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

            # Real turnover
            turnover = (
                close * volume
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

    turnovers = [
        x[1]
        for x in valid[
            :LIQUIDITY_DAYS
        ]
    ]

    average_turnover = (
        sum(turnovers)
        / len(turnovers)
    )

    return average_turnover


# =========================================================
# HISTORICAL LIQUIDITY CHECK
# =========================================================

def get_liquidity(
    instrument_key,
    today
):

    cache_key = (
        f"{instrument_key}|"
        f"{today.isoformat()}|"
        f"{LIQUIDITY_DAYS}"
    )

    # -----------------------------------------------------
    # CACHE
    # -----------------------------------------------------

    if cache_key in LIQUIDITY_CACHE:

        try:

            cached = (
                LIQUIDITY_CACHE[
                    cache_key
                ]
            )

            if cached is None:
                return None

            return float(
                cached
            )

        except Exception:
            pass

    # -----------------------------------------------------
    # API
    # -----------------------------------------------------

    try:

        value = historical_20day_turnover(
            instrument_key,
            today
        )

        LIQUIDITY_CACHE[
            cache_key
        ] = value

        return value

    except Exception as e:

        print(
            f"Historical error "
            f"{instrument_key}:",
            repr(e)
        )

        return None


# =========================================================
# RUN HISTORICAL SCAN
# =========================================================

def run_historical_scan(
    candidates
):

    global SCAN_STATE

    today = now_ist().date()

    total = len(candidates)

    with LOCK:

        SCAN_STATE[
            "total"
        ] = total

        SCAN_STATE[
            "checked"
        ] = 0

        SCAN_STATE[
            "results"
        ] = []

        SCAN_STATE[
            "error"
        ] = None

    if total == 0:

        with LOCK:

            SCAN_STATE[
                "finished"
            ] = True

            SCAN_STATE[
                "running"
            ] = False

            SCAN_STATE[
                "finished_at"
            ] = now_ist().isoformat()

        print(
            "No live candidates. "
            "Historical scan not required."
        )

        return

    results = []

    print(
        f"Starting historical liquidity "
        f"check for {total} candidates..."
    )

    # -----------------------------------------------------
    # Parallel historical checks
    # -----------------------------------------------------

    with ThreadPoolExecutor(
        max_workers=MAX_WORKERS
    ) as executor:

        future_map = {
            executor.submit(
                get_liquidity,
                item["key"],
                today
            ): item

            for item in candidates
        }

        for future in as_completed(
            future_map
        ):

            item = future_map[
                future
            ]

            try:

                average_turnover = (
                    future.result()
                )

                if (
                    average_turnover
                    is not None
                    and average_turnover
                    >= MIN_AVG_TURNOVER
                ):

                    row = dict(
                        item
                    )

                    row[
                        "avg_turnover_20d"
                    ] = average_turnover

                    row[
                        "avg_turnover_cr"
                    ] = (
                        average_turnover
                        / 1_00_00_000
                    )

                    row[
                        "live_turnover_cr"
                    ] = (
                        item[
                            "live_turnover"
                        ]
                        / 1_00_00_000
                    )

                    results.append(
                        row
                    )

            except Exception as e:

                print(
                    "Historical future error:",
                    repr(e)
                )

            with LOCK:

                SCAN_STATE[
                    "checked"
                ] += 1

                # Progressive results
                current_results = list(
                    results
                )

                current_results.sort(
                    key=lambda x: x[
                        "gap"
                    ]
                )

                SCAN_STATE[
                    "results"
                ] = current_results

    # Save cache after scan
    save_cache(
        LIQUIDITY_CACHE
    )

    # Final sort
    results.sort(
        key=lambda x: x[
            "gap"
        ]
    )

    with LOCK:

        SCAN_STATE[
            "results"
        ] = results

        SCAN_STATE[
            "finished"
        ] = True

        SCAN_STATE[
            "running"
        ] = False

        SCAN_STATE[
            "finished_at"
        ] = now_ist().isoformat()

    print(
        "========================================"
    )

    print(
        f"Historical scan finished."
    )

    print(
        f"Final qualifying stocks: "
        f"{len(results)}"
    )

    print(
        "========================================"
    )


# =========================================================
# FULL SCAN STARTER
# =========================================================

def start_scan(
    force=False
):

    global SCAN_THREAD
    global SCAN_STATE

    with LOCK:

        if (
            SCAN_STATE["running"]
            and not force
        ):

            print(
                "Scan already running. "
                "Not starting another scan."
            )

            return

        # If already finished and not forced,
        # simply reuse existing result.
        if (
            SCAN_STATE["finished"]
            and not force
        ):

            print(
                "Existing scan result reused."
            )

            return

        SCAN_STATE[
            "running"
        ] = True

        SCAN_STATE[
            "finished"
        ] = False

        SCAN_STATE[
            "started_at"
        ] = now_ist().isoformat()

        SCAN_STATE[
            "finished_at"
        ] = None

        SCAN_STATE[
            "live_candidates"
        ] = 0

        SCAN_STATE[
            "checked"
        ] = 0

        SCAN_STATE[
            "total"
        ] = 0

        SCAN_STATE[
            "results"
        ] = []

        SCAN_STATE[
            "error"
        ] = None

    def worker():

        global SCAN_STATE

        try:

            print(
                "========================================"
            )

            print(
                "STARTING OPEN-LOW LIQUIDITY SCAN"
            )

            print(
                f"Minimum price: ₹{MIN_PRICE}"
            )

            print(
                f"Maximum Open-Low gap: {MAX_GAP}%"
            )

            print(
                f"Minimum 20-day average turnover: "
                f"₹{MIN_AVG_TURNOVER / 1_00_00_000:.2f} Cr"
            )

            print(
                "========================================"
            )

            # ---------------------------------------------
            # Fast live stage
            # ---------------------------------------------

            candidates = (
                build_live_snapshot()
            )

            with LOCK:

                SCAN_STATE[
                    "live_candidates"
                ] = len(candidates)

                SCAN_STATE[
                    "total"
                ] = len(candidates)

            # ---------------------------------------------
            # Historical stage
            # ---------------------------------------------

            run_historical_scan(
                candidates
            )

        except Exception as e:

            print(
                "SCAN ERROR:",
                repr(e)
            )

            with LOCK:

                SCAN_STATE[
                    "error"
                ] = str(e)

                SCAN_STATE[
                    "running"
                ] = False

                SCAN_STATE[
                    "finished"
                ] = True

                SCAN_STATE[
                    "finished_at"
                ] = now_ist().isoformat()

    SCAN_THREAD = threading.Thread(
        target=worker,
        daemon=True
    )

    SCAN_THREAD.start()


# =========================================================
# API: SCAN
# =========================================================

@app.route(
    "/api/scan",
    methods=["GET"]
)
def api_scan():

    # -----------------------------------------------------
    # IMPORTANT:
    # Normal /api/scan does NOT restart a running scan.
    # Opening another browser tab will reuse the same state.
    # -----------------------------------------------------

    with LOCK:

        running = SCAN_STATE[
            "running"
        ]

        finished = SCAN_STATE[
            "finished"
        ]

    if not running and not finished:

        start_scan(
            force=False
        )

    with LOCK:

        state = dict(
            SCAN_STATE
        )

        state[
            "results"
        ] = list(
            SCAN_STATE[
                "results"
            ]
        )

    return jsonify(
        state
    )


# =========================================================
# API: SCAN NOW
# =========================================================

@app.route(
    "/api/scan-now",
    methods=["GET"]
)
def api_scan_now():

    print(
        "Manual Scan Now requested."
    )

    start_scan(
        force=True
    )

    with LOCK:

        state = dict(
            SCAN_STATE
        )

        state[
            "results"
        ] = list(
            SCAN_STATE[
                "results"
            ]
        )

    return jsonify(
        state
    )


# =========================================================
# API: HEALTH
# =========================================================

@app.route(
    "/api/health",
    methods=["GET"]
)
def api_health():

    with LOCK:

        return jsonify({

            "status": "ok",

            "stocks_loaded": len(
                INSTRUMENTS
            ),

            "live_candidates": len(
                LIVE
            ),

            "scan_running":
                SCAN_STATE[
                    "running"
                ],

            "scan_finished":
                SCAN_STATE[
                    "finished"
                ]

        })


# =========================================================
# API: CONFIG
# =========================================================

@app.route(
    "/api/config",
    methods=["GET"]
)
def api_config():

    return jsonify({

        "min_price": MIN_PRICE,

        "max_gap_percent": MAX_GAP,

        "min_average_turnover":
            MIN_AVG_TURNOVER,

        "min_average_turnover_cr":
            MIN_AVG_TURNOVER
            / 1_00_00_000,

        "liquidity_days":
            LIQUIDITY_DAYS,

        "exchange":
            "NSE",

        "segment":
            "NSE_EQ"

    })


# =========================================================
# HOME PAGE
# =========================================================

@app.route(
    "/",
    methods=["GET"]
)
def index():

    return send_from_directory(
        ".",
        "index.html"
    )


# =========================================================
# STARTUP
# =========================================================

def startup():

    print(
        "========================================"
    )

    print(
        "Open-Low Liquidity Scanner"
    )

    print(
        "Server starting..."
    )

    print(
        "========================================"
    )

    try:

        load_instruments()

    except Exception as e:

        print(
            "Initial instrument loading failed:",
            repr(e)
        )

        # Do not stop Flask.
        # The scanner will try again when requested.

    # Do NOT start a historical scan here.
    #
    # This is intentional.
    # Scan begins when /api/scan is requested.


# =========================================================
# MAIN
# =========================================================

startup()


if __name__ == "__main__":

    port = int(
        os.getenv(
            "PORT",
            "10000"
        )
    )

    app.run(
        host="0.0.0.0",
        port=port,
        debug=False
    )
