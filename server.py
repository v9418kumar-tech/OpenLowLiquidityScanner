import os
import json
import gzip
import threading
from datetime import datetime, timedelta, timezone
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from flask import Flask, jsonify, send_from_directory


# =========================================================
# SETTINGS
# =========================================================

BASE = "https://api.upstox.com"

IST = timezone(
    timedelta(hours=5, minutes=30)
)

MIN_PRICE = 20.0

MAX_GAP = 0.50

MIN_AVG_TURNOVER = 10_00_00_000.0

LIQUIDITY_DAYS = 20

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

app = Flask(
    __name__,
    static_folder="."
)


# =========================================================
# GLOBAL STATE
# =========================================================

INSTRUMENTS = []

BY_KEY = {}

LIVE = {}

LIQUIDITY_CACHE = {}

LOCK = threading.Lock()

SCAN_THREAD = None

SCAN_STATE = {
    "running": False,
    "finished": False,
    "started_at": None,
    "finished_at": None,
    "live_candidates": 0,
    "checked": 0,
    "total": 0,
    "results": [],
    "error": None
}


# =========================================================
# HTTP SESSION
# =========================================================

http = requests.Session()

http.headers.update({
    "Accept": "application/json"
})


# =========================================================
# BASIC HELPERS
# =========================================================

def log(message):

    print(
        message,
        flush=True
    )


def now_ist():

    return datetime.now(IST)


def chunks(items, size):

    for i in range(
        0,
        len(items),
        size
    ):

        yield items[
            i:i + size
        ]


def get_headers():

    token = os.getenv(
        "UPSTOX_ACCESS_TOKEN",
        ""
    ).strip()

    if not token:

        raise RuntimeError(
            "UPSTOX_ACCESS_TOKEN is missing in Render Environment Variables."
        )

    return {
        "Accept": "application/json",
        "Authorization": (
            f"Bearer {token}"
        )
    }


# =========================================================
# REAL NSE EQUITY FILTER
# =========================================================

def is_real_equity(x):

    try:

        exchange = str(
            x.get(
                "exchange",
                ""
            )
        ).upper().strip()

        segment = str(
            x.get(
                "segment",
                ""
            )
        ).upper().strip()

        instrument_type = str(
            x.get(
                "instrument_type",
                ""
            )
        ).upper().strip()

        symbol = str(
            x.get(
                "trading_symbol",
                ""
            )
        ).upper().strip()

        key = str(
            x.get(
                "instrument_key",
                ""
            )
        ).upper().strip()

        # NSE only
        if exchange and exchange != "NSE":
            return False

        # NSE_EQ only
        if segment and segment != "NSE_EQ":
            return False

        if key and not key.startswith(
            "NSE_EQ|"
        ):
            return False

        # Equity only
        if instrument_type and instrument_type != "EQ":
            return False

        # Exclude obvious ETF names
        bad_symbols = [
            "ETF",
            "BEES",
            "MON100",
            "JUNIORBEES",
            "BANKBEES",
            "ITBEES",
            "GOLDBEES",
            "SILVERBEES",
            "PHARMABEES",
            "AUTOBEES",
            "PSUBANK",
            "CPSEETF",
            "NIFTYBEES",
            "SETFNIF"
        ]

        for bad in bad_symbols:

            if bad in symbol:
                return False

        # Exclude SME style symbols
        if symbol.endswith(
            "-SM"
        ):
            return False

        if symbol.endswith(
            "-BE"
        ):
            return False

        if symbol.endswith(
            "-BZ"
        ):
            return False

        return True

    except Exception:

        return False


# =========================================================
# LOAD INSTRUMENTS
# =========================================================

def load_instruments():

    global INSTRUMENTS
    global BY_KEY

    with LOCK:

        if INSTRUMENTS:

            return

    log(
        "Loading Upstox complete instruments..."
    )

    r = http.get(
        INSTRUMENTS_URL,
        timeout=60
    )

    r.raise_for_status()

    raw = gzip.decompress(
        r.content
    )

    data = json.loads(
        raw.decode(
            "utf-8"
        )
    )

    instruments = []

    for x in data:

        if not isinstance(
            x,
            dict
        ):
            continue

        if not is_real_equity(x):
            continue

        instrument_key = x.get(
            "instrument_key"
        )

        trading_symbol = x.get(
            "trading_symbol"
        )

        if not instrument_key:
            continue

        if not trading_symbol:
            continue

        instruments.append({

            "instrument_key":
                instrument_key,

            "trading_symbol":
                trading_symbol,

            "name":
                x.get(
                    "name",
                    ""
                ),

            "isin":
                x.get(
                    "isin",
                    ""
                ),

            "exchange":
                x.get(
                    "exchange",
                    ""
                ),

            "segment":
                x.get(
                    "segment",
                    ""
                ),

            "instrument_type":
                x.get(
                    "instrument_type",
                    ""
                )

        })

    by_key = {

        x[
            "instrument_key"
        ]: x

        for x in instruments
    }

    with LOCK:

        INSTRUMENTS = instruments

        BY_KEY = by_key

    log(
        f"Loaded {len(INSTRUMENTS)} NSE EQ stocks."
    )


# =========================================================
# CACHE
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

            if isinstance(
                data,
                dict
            ):

                return data

    except Exception as e:

        log(
            f"Cache load error: {e}"
        )

    return {}


def save_cache():

    try:

        temp_file = (
            CACHE_FILE
            + ".tmp"
        )

        with open(
            temp_file,
            "w",
            encoding="utf-8"
        ) as f:

            json.dump(
                LIQUIDITY_CACHE,
                f,
                ensure_ascii=False
            )

        os.replace(
            temp_file,
            CACHE_FILE
        )

    except Exception as e:

        log(
            f"Cache save error: {e}"
        )


LIQUIDITY_CACHE = load_cache()


# =========================================================
# LIVE UPSTOX QUOTES
# =========================================================

def fetch_live_quotes():

    load_instruments()

    all_quotes = []

    total_stocks = len(
        INSTRUMENTS
    )

    log(
        f"Requesting live OHLC for {total_stocks} NSE EQ stocks..."
    )

    batch_no = 0

    for batch in chunks(
        INSTRUMENTS,
        500
    ):

        batch_no += 1

        keys = ",".join(

            item[
                "instrument_key"
            ]

            for item in batch
        )

        try:

            response = http.get(

                BASE
                + "/v3/market-quote/ohlc",

                headers=get_headers(),

                params={

                    "instrument_key":
                        keys,

                    "interval":
                        "1d"
                },

                timeout=30
            )

            response.raise_for_status()

            payload = response.json()

            data = payload.get(
                "data",
                {}
            )

            if not isinstance(
                data,
                dict
            ):

                log(
                    f"Batch {batch_no}: invalid response data."
                )

                continue

            received = 0

            for _, quote_data in data.items():

                if not isinstance(
                    quote_data,
                    dict
                ):
                    continue

                # IMPORTANT:
                # V3 actual instrument key
                # is available as instrument_token
                instrument_key = (

                    quote_data.get(
                        "instrument_token"
                    )

                    or

                    quote_data.get(
                        "instrument_key"
                    )
                )

                if not instrument_key:

                    continue

                quote_data[
                    "_instrument_key"
                ] = instrument_key

                all_quotes.append(
                    quote_data
                )

                received += 1

            log(
                f"Live batch {batch_no}: {received} quotes received."
            )

        except Exception as e:

            log(
                f"Live batch {batch_no} ERROR: {repr(e)}"
            )

    log(
        f"Total live quotes received: {len(all_quotes)}"
    )

    return all_quotes


# =========================================================
# BUILD LIVE CANDIDATES
# =========================================================

def build_live_snapshot():

    quotes = fetch_live_quotes()

    today = now_ist().date()

    candidates = []

    rejected_price = 0
    rejected_open = 0
    rejected_direction = 0
    rejected_gap = 0
    processing_errors = 0

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

                processing_errors += 1

                continue

            # ------------------------------------------------
            # CURRENT LTP
            # ------------------------------------------------

            price = float(

                q.get(
                    "last_price"
                )
                or 0
            )

            # ------------------------------------------------
            # TODAY'S LIVE OHLC
            # ------------------------------------------------

            live_ohlc = (

                q.get(
                    "live_ohlc"
                )
                or {}
            )

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
                or 0
            )

            # ------------------------------------------------
            # AVERAGE TRADED PRICE
            # ------------------------------------------------

            avg_price = float(

                q.get(
                    "average_price"
                )

                or live_ohlc.get(
                    "average_price"
                )

                or 0
            )

            # ------------------------------------------------
            # CONDITION 1
            # PRICE > ₹20
            # ------------------------------------------------

            if price <= MIN_PRICE:

                rejected_price += 1

                continue

            # ------------------------------------------------
            # OPEN AND LOW AVAILABLE
            # ------------------------------------------------

            if op <= 0 or low <= 0:

                rejected_open += 1

                continue

            # ------------------------------------------------
            # CONDITION 2
            # LTP > OPEN
            # ------------------------------------------------

            if price <= op:

                rejected_direction += 1

                continue

            # ------------------------------------------------
            # CONDITION 3
            # OPEN-LOW GAP <= 0.50%
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

            turnover_price = (

                avg_price

                if avg_price > 0

                else price
            )

            live_turnover = (

                volume
                * turnover_price
            )

            candidates.append({

                "key":
                    instrument_key,

                "symbol":
                    symbol,

                "price":
                    price,

                "open":
                    op,

                "low":
                    low,

                "gap":
                    gap,

                "volume":
                    volume,

                "live_turnover":
                    live_turnover,

                "live_turnover_cr":
                    live_turnover
                    / 1_00_00_000,

                "today":
                    today.isoformat()

            })

        except Exception as e:

            processing_errors += 1

            log(
                f"Quote processing error: {repr(e)}"
            )

    # Lowest gap first
    candidates.sort(
        key=lambda x: x[
            "gap"
        ]
    )

    with LOCK:

        LIVE.clear()

        for item in candidates:

            LIVE[
                item["symbol"]
            ] = item

    log(
        "----------------------------------------"
    )

    log(
        f"Live quotes: {len(quotes)}"
    )

    log(
        f"Live candidates: {len(candidates)}"
    )

    log(
        f"Rejected price <= ₹20: {rejected_price}"
    )

    log(
        f"Rejected missing Open/Low: {rejected_open}"
    )

    log(
        f"Rejected LTP <= Open: {rejected_direction}"
    )

    log(
        f"Rejected Gap > {MAX_GAP}%: {rejected_gap}"
    )

    log(
        f"Processing errors: {processing_errors}"
    )

    log(
        "----------------------------------------"
    )

    return candidates


# =========================================================
# HISTORICAL 20 DAY TURNOVER
# =========================================================

def historical_20day_turnover(
    instrument_key,
    today
):

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

    data = response.json()

    candles = (

        data
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

    # Newest valid trading days first
    valid.sort(
        key=lambda x: x[0],
        reverse=True
    )

    if len(valid) < LIQUIDITY_DAYS:

        return None

    last_20 = [

        item[1]

        for item in valid[
            :LIQUIDITY_DAYS
        ]
    ]

    average_turnover = (

        sum(last_20)
        / len(last_20)
    )

    return average_turnover


# =========================================================
# GET CACHED / HISTORICAL LIQUIDITY
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

    # ------------------------------------------------
    # CACHE CHECK
    # ------------------------------------------------

    if cache_key in LIQUIDITY_CACHE:

        cached = (
            LIQUIDITY_CACHE[
                cache_key
            ]
        )

        if cached is None:

            return None

        try:

            return float(
                cached
            )

        except Exception:

            pass

    # ------------------------------------------------
    # HISTORICAL API
    # ------------------------------------------------

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

        log(
            f"Historical error {instrument_key}: {repr(e)}"
        )

        return None


# =========================================================
# HISTORICAL SCAN
# =========================================================

def run_historical_scan(
    candidates
):

    today = now_ist().date()

    total = len(
        candidates
    )

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

    if total == 0:

        log(
            "No live candidates. Historical check not required."
        )

        with LOCK:

            SCAN_STATE[
                "running"
            ] = False

            SCAN_STATE[
                "finished"
            ] = True

            SCAN_STATE[
                "finished_at"
            ] = now_ist().isoformat()

        return

    log(
        f"Starting historical liquidity check for {total} candidates..."
    )

    results = []

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

                    and

                    average_turnover
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

                    results.append(
                        row
                    )

            except Exception as e:

                log(
                    f"Historical future error: {repr(e)}"
                )

            with LOCK:

                SCAN_STATE[
                    "checked"
                ] += 1

                temporary = list(
                    results
                )

                temporary.sort(
                    key=lambda x: x[
                        "gap"
                    ]
                )

                SCAN_STATE[
                    "results"
                ] = temporary

                checked = (
                    SCAN_STATE[
                        "checked"
                    ]
                )

            log(
                f"Historical liquidity check: "
                f"{checked}/{total}"
            )

    save_cache()

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
            "running"
        ] = False

        SCAN_STATE[
            "finished"
        ] = True

        SCAN_STATE[
            "finished_at"
        ] = now_ist().isoformat()

    log(
        "========================================"
    )

    log(
        f"Historical scan finished. "
        f"Final qualifying stocks: {len(results)}"
    )

    log(
        "========================================"
    )


# =========================================================
# SCAN WORKER
# =========================================================

def scan_worker():

    try:

        log(
            "========================================"
        )

        log(
            "STARTING OPEN-LOW LIQUIDITY SCAN"
        )

        log(
            f"Minimum price: ₹{MIN_PRICE}"
        )

        log(
            f"Maximum Open-Low gap: {MAX_GAP}%"
        )

        log(
            "Minimum 20-day average real turnover: ₹10 Cr"
        )

        log(
            "Universe: NSE EQ only"
        )

        log(
            "========================================"
        )

        # ---------------------------------------------
        # FAST LIVE STAGE
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
        # HISTORICAL STAGE
        # ---------------------------------------------

        run_historical_scan(
            candidates
        )

    except Exception as e:

        log(
            f"SCAN ERROR: {repr(e)}"
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


# =========================================================
# START SCAN
# =========================================================

def start_scan(
    force=False
):

    global SCAN_THREAD

    with LOCK:

        # Existing scan is running
        if (

            SCAN_STATE[
                "running"
            ]

            and

            not force

        ):

            log(
                "Scan already running. Reusing existing scan."
            )

            return False

        # Existing completed result
        if (

            SCAN_STATE[
                "finished"
            ]

            and

            not force

        ):

            log(
                "Existing completed scan reused."
            )

            return False

        # Reset state
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

    SCAN_THREAD = threading.Thread(

        target=scan_worker,

        daemon=True
    )

    SCAN_THREAD.start()

    log(
        "Scan worker started."
    )

    return True


# =========================================================
# API / SCAN
# =========================================================

@app.route(
    "/api/scan",
    methods=["GET"]
)
def api_scan():

    # First request starts scan.
    # Further requests only read status.

    with LOCK:

        running = (
            SCAN_STATE[
                "running"
            ]
        )

        finished = (
            SCAN_STATE[
                "finished"
            ]
        )

    if not running and not finished:

        start_scan(
            force=False
        )

    with LOCK:

        response = dict(
            SCAN_STATE
        )

        response[
            "results"
        ] = list(
            SCAN_STATE[
                "results"
            ]
        )

    return jsonify(
        response
    )


# =========================================================
# API / SCAN-NOW
# =========================================================

@app.route(
    "/api/scan-now",
    methods=["GET"]
)
def api_scan_now():

    log(
        "========================================"
    )

    log(
        "SCAN NOW BUTTON PRESSED"
    )

    log(
        "========================================"
    )

    start_scan(
        force=True
    )

    with LOCK:

        response = dict(
            SCAN_STATE
        )

        response[
            "results"
        ] = list(
            SCAN_STATE[
                "results"
            ]
        )

    return jsonify(
        response
    )


# =========================================================
# API / HEALTH
# =========================================================

@app.route(
    "/api/health",
    methods=["GET"]
)
def api_health():

    with LOCK:

        return jsonify({

            "status":
                "ok",

            "stocks_loaded":
                len(INSTRUMENTS),

            "live_candidates":
                len(LIVE),

            "scan_running":
                SCAN_STATE[
                    "running"
                ],

            "scan_finished":
                SCAN_STATE[
                    "finished"
                ],

            "error":
                SCAN_STATE[
                    "error"
                ]

        })


# =========================================================
# API / CONFIG
# =========================================================

@app.route(
    "/api/config",
    methods=["GET"]
)
def api_config():

    return jsonify({

        "min_price":
            MIN_PRICE,

        "max_gap_percent":
            MAX_GAP,

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

    log(
        "========================================"
    )

    log(
        "Open-Low Liquidity Scanner"
    )

    log(
        "Server starting..."
    )

    log(
        "========================================"
    )

    try:

        load_instruments()

    except Exception as e:

        log(
            f"Initial instrument loading failed: {repr(e)}"
        )

        # Server should remain alive.
        # Scanner will retry when requested.


# =========================================================
# RUN
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
