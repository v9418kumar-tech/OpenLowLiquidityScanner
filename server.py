import os
import gzip
import json
import time
import logging
import threading
from datetime import datetime, timedelta, timezone
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor, as_completed, wait

import requests
import upstox_client
from flask import Flask, jsonify, send_from_directory


# ============================================================
# OPEN-LOW LIQUIDITY SCANNER
# FINAL VERSION
# ============================================================

PORT = int(os.environ.get("PORT", "10000"))

BASE = "https://api.upstox.com"

INSTR_URL = (
    "https://assets.upstox.com/market-quote/"
    "instruments/exchange/complete.json.gz"
)

IST = timezone(timedelta(hours=5, minutes=30))


# ============================================================
# SCANNER SETTINGS
# ============================================================

MIN_PRICE = 20.0

MAX_GAP = 0.50

MIN_AVG_TURNOVER = 10_00_00_000.0

LIQUIDITY_DAYS = 20

BATCH_SIZE = 500

# Live quote requests
LIVE_WORKERS = 6

# Historical requests are deliberately controlled
HIST_WORKERS = 4

# Maximum 5 historical requests per second
HIST_MIN_INTERVAL = 0.20

# Retry count for 429 / temporary errors
HIST_RETRIES = 5

CACHE_FILE = "liquidity_cache.json"

# Live scanner refresh interval
SCAN_INTERVAL_SECONDS = 30


# ============================================================
# FLASK
# ============================================================

app = Flask(
    __name__,
    static_folder=".",
    static_url_path=""
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

http = requests.Session()

http.headers.update({
    "User-Agent": "OpenLowStrengthScanner/Final"
})


# ============================================================
# GLOBAL STATE
# ============================================================

INSTRUMENTS = []

BY_KEY = {}

INSTRUMENTS_LOADED_AT = 0.0

LIVE_RESULTS = []

QUALIFIED_TODAY = {}

LAST_SCAN_TIME = None

LAST_SCAN_ERROR = ""

LAST_SCAN_DATE = None

LAST_SCAN_EPOCH = 0.0

LAST_SCAN_ATTEMPT_DATE = None

SCAN_RUNNING = False

SCAN_LOCK = threading.Lock()

QUALIFIED_LOCK = threading.Lock()


# ============================================================
# PRE-OPEN STATE
# ============================================================

PREOPEN = {}

PREOPEN_CONNECTED = False

PREOPEN_ERROR = ""

PREOPEN_LAST_UPDATE = None

PREOPEN_LOCK = threading.Lock()

PREOPEN_STREAMER = None


# ============================================================
# HISTORICAL RATE LIMITER
# ============================================================

HIST_RATE_LOCK = threading.Lock()

NEXT_HIST_REQUEST = 0.0


# ============================================================
# BASIC HELPERS
# ============================================================

def log(message):
    logging.info(message)


def get_token():
    return os.environ.get(
        "UPSTOX_ACCESS_TOKEN",
        ""
    ).strip()


def headers():

    token = get_token()

    if not token:
        raise RuntimeError(
            "UPSTOX_ACCESS_TOKEN is missing in Render Environment Variables."
        )

    return {
        "Accept": "application/json",
        "Authorization": f"Bearer {token}"
    }


def ist_now():
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


# ============================================================
# NSE EQUITY FILTER
# ============================================================

def is_real_equity(item):

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

    key = item.get("instrument_key")

    if not key:
        return False

    symbol = str(
        item.get("trading_symbol") or ""
    ).upper().strip()

    name = str(
        item.get("name") or ""
    ).upper().strip()

    short = str(
        item.get("short_name") or ""
    ).upper().strip()

    combined = (
        symbol
        + " "
        + name
        + " "
        + short
    )

    for bad in (
        "ETF",
        "EXCHANGE TRADED FUND",
        "MUTUAL FUND",
        "INDEX FUND",
        "SME"
    ):

        if bad in combined:
            return False

    if symbol.endswith("BEES"):
        return False

    if symbol.endswith("BE"):
        return False

    if symbol.endswith("BZ"):
        return False

    return True


# ============================================================
# LOAD NSE INSTRUMENTS
# ============================================================

def load_instruments(force=False):

    global INSTRUMENTS
    global BY_KEY
    global INSTRUMENTS_LOADED_AT

    if (
        INSTRUMENTS
        and not force
        and (
            time.time()
            - INSTRUMENTS_LOADED_AT
        ) < 21600
    ):

        return

    log(
        "Downloading Upstox NSE instrument file..."
    )

    response = http.get(
        INSTR_URL,
        timeout=30
    )

    response.raise_for_status()

    data = json.loads(
        gzip.decompress(
            response.content
        ).decode("utf-8")
    )

    selected = []

    for item in data:

        try:

            if is_real_equity(item):
                selected.append(item)

        except Exception:

            pass

    INSTRUMENTS = selected

    BY_KEY = {
        x["instrument_key"]: x
        for x in selected
    }

    INSTRUMENTS_LOADED_AT = time.time()

    log(
        f"Loaded {len(INSTRUMENTS)} NSE EQ stocks."
    )


# ============================================================
# PRE-OPEN FEED
# ============================================================

def extract_ltpc(feed):

    if not isinstance(
        feed,
        dict
    ):
        return {}

    ltpc = feed.get("ltpc")

    if isinstance(
        ltpc,
        dict
    ):
        return ltpc

    for parent_name in (
        "ff",
        "fullFeed",
        "firstLevelWithGreeks"
    ):

        parent = feed.get(
            parent_name
        )

        if not isinstance(
            parent,
            dict
        ):
            continue

        x = parent.get(
            "ltpc"
        )

        if isinstance(
            x,
            dict
        ):
            return x

        x = parent.get(
            "marketFF"
        )

        if (
            isinstance(x, dict)
            and isinstance(
                x.get("ltpc"),
                dict
            )
        ):
            return x["ltpc"]

    return {}


def update_preopen(message):

    global PREOPEN_LAST_UPDATE

    if not isinstance(
        message,
        dict
    ):
        return

    feeds = message.get(
        "feeds",
        {}
    )

    if not isinstance(
        feeds,
        dict
    ):
        return

    now = time.time()

    with PREOPEN_LOCK:

        for key, feed in feeds.items():

            meta = BY_KEY.get(
                key
            )

            if not meta:
                continue

            ltpc = extract_ltpc(
                feed
            )

            if not ltpc:
                continue

            iep = ltpc.get(
                "iep"
            )

            cp = ltpc.get(
                "cp"
            )

            ltp = ltpc.get(
                "ltp"
            )

            try:

                if iep is None:
                    continue

                iep = float(
                    iep
                )

                if iep <= MIN_PRICE:
                    continue

                cp = float(
                    cp or 0
                )

                ltp = float(
                    ltp or 0
                )

            except Exception:

                continue

            if cp <= 0:
                continue

            symbol = str(
                meta.get(
                    "trading_symbol"
                )
                or ""
            ).upper()

            change = (
                (
                    iep - cp
                )
                / cp
            ) * 100.0

            PREOPEN[key] = {

                "key":
                    key,

                "symbol":
                    symbol,

                "iep":
                    iep,

                "cp":
                    cp,

                "ltp":
                    ltp,

                "change":
                    change,

                "seen":
                    now
            }

        PREOPEN_LAST_UPDATE = now


def feed_thread():

    global PREOPEN_CONNECTED
    global PREOPEN_ERROR
    global PREOPEN_STREAMER

    if not get_token():

        PREOPEN_ERROR = (
            "UPSTOX_ACCESS_TOKEN "
            "environment variable is missing."
        )

        return

    try:

        load_instruments()

        configuration = (
            upstox_client.Configuration()
        )

        configuration.access_token = (
            get_token()
        )

        streamer = (
            upstox_client.MarketDataStreamerV3(
                upstox_client.ApiClient(
                    configuration
                )
            )
        )

        PREOPEN_STREAMER = streamer

        def on_open():

            global PREOPEN_CONNECTED
            global PREOPEN_ERROR

            PREOPEN_CONNECTED = True

            PREOPEN_ERROR = ""

            keys = [
                x["instrument_key"]
                for x in INSTRUMENTS
            ]

            for batch in chunks(
                keys,
                4500
            ):

                streamer.subscribe(
                    batch,
                    "ltpc"
                )

                time.sleep(
                    0.5
                )

            log(
                "Pre-open feed subscribed to "
                f"{len(keys)} NSE EQ instruments."
            )

        def on_message(message):

            update_preopen(
                message
            )

        def on_close(*args):

            global PREOPEN_CONNECTED

            PREOPEN_CONNECTED = False

            log(
                f"Upstox pre-open stream closed: {args}"
            )

        def on_error(err):

            global PREOPEN_ERROR

            PREOPEN_ERROR = str(
                err
            )

            log(
                f"Upstox pre-open feed error: {err}"
            )

        streamer.on(
            "open",
            on_open
        )

        streamer.on(
            "message",
            on_message
        )

        streamer.on(
            "close",
            on_close
        )

        streamer.on(
            "error",
            on_error
        )

        streamer.auto_reconnect(
            True,
            10,
            100
        )

        streamer.connect()

    except Exception as e:

        PREOPEN_CONNECTED = False

        PREOPEN_ERROR = repr(
            e
        )

        log(
            f"Pre-open startup error: {repr(e)}"
        )


def start_preopen_feed():

    threading.Thread(
        target=feed_thread,
        daemon=True,
        name="preopen-feed"
    ).start()


# ============================================================
# SESSION MODE
# ============================================================

def session_mode():

    now = ist_now()

    t = now.time()

    if t < datetime.strptime(
        "09:15",
        "%H:%M"
    ).time():

        return "preopen"

    return "live"


# ============================================================
# CACHE
# ============================================================

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

            data = json.load(
                f
            )

        if isinstance(
            data,
            dict
        ):
            return data

    except Exception:

        pass

    return {}


def save_cache(cache):

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
                cache,
                f
            )

        os.replace(
            temp_file,
            CACHE_FILE
        )

    except Exception as e:

        log(
            f"Cache save error: {repr(e)}"
        )


# ============================================================
# PRE-OPEN RESULTS
# ============================================================

def preopen_results(limit=100):

    today = (
        ist_now()
        .date()
        .isoformat()
    )

    rows = []

    with PREOPEN_LOCK:

        items = list(
            PREOPEN.values()
        )

    cache = load_cache()

    for x in items:

        try:

            if (
                x["iep"] <= MIN_PRICE
                or x["change"] <= 0
            ):
                continue

            avg = 0.0

            key = x.get(
                "key"
            )

            cached = (
                cache.get(key)
                if key
                else None
            )

            if (
                isinstance(
                    cached,
                    dict
                )
                and cached.get(
                    "date"
                ) == today
            ):

                avg = float(
                    cached.get(
                        "avg_turnover"
                    )
                    or 0
                )

            rows.append({

                "symbol":
                    x["symbol"],

                "price":
                    x["iep"],

                "iep":
                    x["iep"],

                "prev_close":
                    x["cp"],

                "preopen_gain":
                    x["change"],

                "avg_turnover_cr":
                    avg / 1_00_00_000.0
            })

        except Exception:

            continue

    if not rows:
        return []

    gains = [
        x["preopen_gain"]
        for x in rows
    ]

    avgs = [
        x["avg_turnover_cr"]
        for x in rows
    ]

    def pct(
        values,
        value
    ):

        if len(values) <= 1:
            return 100.0

        return (
            100.0
            * (
                sum(
                    1
                    for v in values
                    if v <= value
                )
                - 1
            )
            / (
                len(values) - 1
            )
        )

    for x in rows:

        gain_score = min(
            100.0,
            max(
                0.0,
                x["preopen_gain"]
                * 10.0
            )
        )

        relative_gain = pct(
            gains,
            x["preopen_gain"]
        )

        liquidity_score = (
            pct(
                avgs,
                x["avg_turnover_cr"]
            )
            if any(avgs)
            else 50.0
        )

        x["strength"] = (
            gain_score * 0.45
            + relative_gain * 0.45
            + liquidity_score * 0.10
        )

    rows.sort(
        key=lambda x: (
            -x["strength"],
            -x["preopen_gain"],
            -x["avg_turnover_cr"]
        )
    )

    output = []

    for x in rows[:limit]:

        output.append({

            "symbol":
                x["symbol"],

            "price":
                round(
                    x["iep"],
                    2
                ),

            "iep":
                round(
                    x["iep"],
                    2
                ),

            "prev_close":
                round(
                    x["prev_close"],
                    2
                ),

            "preopen_gain":
                round(
                    x["preopen_gain"],
                    2
                ),

            "strength":
                round(
                    x["strength"],
                    1
                ),

            "avg_turnover_cr":
                round(
                    x["avg_turnover_cr"],
                    2
                )
        })

    return output


# ============================================================
# LIVE QUOTES
# ============================================================

def fetch_quote_batch(
    batch_no,
    batch
):

    keys = ",".join(
        x["instrument_key"]
        for x in batch
    )

    log(
        f"Live batch {batch_no} START: "
        f"{len(batch)} symbols"
    )

    try:

        response = requests.get(
            BASE
            + "/v3/market-quote/quotes",
            headers=headers(),
            params={
                "instrument_key":
                    keys
            },
            timeout=(3, 8)
        )

        if response.status_code != 200:

            return (
                batch_no,
                [],
                (
                    f"HTTP "
                    f"{response.status_code}: "
                    f"{response.text[:250]}"
                )
            )

        data = response.json().get(
            "data",
            {}
        )

        if not isinstance(
            data,
            dict
        ):

            return (
                batch_no,
                [],
                "Invalid data object"
            )

        output = []

        for response_key, quote_data in data.items():

            if not isinstance(
                quote_data,
                dict
            ):
                continue

            key = (
                quote_data.get(
                    "instrument_token"
                )
                or response_key
            )

            quote_data[
                "_instrument_key"
            ] = key

            output.append(
                quote_data
            )

        return (
            batch_no,
            output,
            None
        )

    except Exception as e:

        return (
            batch_no,
            [],
            repr(e)
        )


def fetch_live_quotes():

    load_instruments()

    batches = list(
        chunks(
            INSTRUMENTS,
            BATCH_SIZE
        )
    )

    log(
        "LIVE QUOTE START: "
        f"{len(INSTRUMENTS)} NSE EQ stocks, "
        f"{len(batches)} batches"
    )

    if not batches:

        raise RuntimeError(
            "No NSE EQ instruments are loaded."
        )

    all_quotes = []

    workers = min(
        LIVE_WORKERS,
        len(batches)
    )

    executor = ThreadPoolExecutor(
        max_workers=workers
    )

    futures = [
        executor.submit(
            fetch_quote_batch,
            i,
            batch
        )
        for i, batch in enumerate(
            batches,
            1
        )
    ]

    done, not_done = wait(
        futures,
        timeout=15
    )

    log(
        "LIVE QUOTE WAIT DONE: "
        f"{len(done)}/{len(futures)} batches finished"
    )

    for future in done:

        try:

            batch_no, quotes, error = (
                future.result()
            )

        except Exception as e:

            log(
                f"Live batch worker ERROR: {repr(e)}"
            )

            continue

        if error:

            log(
                f"Live batch {batch_no} ERROR: "
                f"{error}"
            )

            continue

        all_quotes.extend(
            quotes
        )

        log(
            f"Live batch {batch_no}: "
            f"{len(quotes)} quotes received."
        )

    if not_done:

        log(
            "LIVE QUOTE ABANDONED: "
            f"{len(not_done)} batch request(s) "
            "exceeded timeout"
        )

        for future in not_done:
            future.cancel()

    executor.shutdown(
        wait=False,
        cancel_futures=True
    )

    log(
        f"LIVE QUOTE DONE: "
        f"{len(all_quotes)} quotes received"
    )

    if not all_quotes:

        raise RuntimeError(
            "Upstox live market quote returned "
            "no usable data. Check token/API access."
        )

    return all_quotes


# ============================================================
# HISTORICAL RATE LIMITER
# ============================================================

def wait_for_historical_slot():

    global NEXT_HIST_REQUEST

    with HIST_RATE_LOCK:

        now = time.monotonic()

        wait_time = max(
            0.0,
            NEXT_HIST_REQUEST - now
        )

        NEXT_HIST_REQUEST = (
            max(
                now,
                NEXT_HIST_REQUEST
            )
            + HIST_MIN_INTERVAL
        )

    if wait_time > 0:

        time.sleep(
            wait_time
        )


# ============================================================
# HISTORICAL 20-DAY TURNOVER
# ============================================================

def historical_20day_turnover(
    key,
    today
):

    yesterday = (
        today
        - timedelta(days=1)
    )

    start = (
        today
        - timedelta(days=45)
    )

    url = (
        BASE
        + "/v3/historical-candle/"
        + quote(
            key,
            safe="|"
        )
        + "/days/1/"
        + yesterday.isoformat()
        + "/"
        + start.isoformat()
    )

    last_error = ""

    for attempt in range(
        HIST_RETRIES
    ):

        wait_for_historical_slot()

        try:

            response = http.get(
                url,
                headers=headers(),
                timeout=(5, 15)
            )

            if response.status_code == 429:

                last_error = (
                    "HTTP 429 Too Many Requests"
                )

                delay = min(
                    15,
                    2 ** attempt
                )

                log(
                    f"429 for {key}; "
                    f"retrying after {delay}s"
                )

                time.sleep(
                    delay
                )

                continue

            if response.status_code in (
                500,
                502,
                503,
                504
            ):

                last_error = (
                    f"HTTP {response.status_code}"
                )

                delay = min(
                    10,
                    2 ** attempt
                )

                time.sleep(
                    delay
                )

                continue

            response.raise_for_status()

            candles = (
                response
                .json()
                .get(
                    "data",
                    {}
                )
                .get(
                    "candles",
                    []
                )
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

                    if (
                        close > 0
                        and volume > 0
                    ):

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

                    pass

            valid.sort(
                key=lambda x: x[0],
                reverse=True
            )

            if len(valid) < LIQUIDITY_DAYS:

                return None

            latest_20 = valid[
                :LIQUIDITY_DAYS
            ]

            return (
                sum(
                    x[1]
                    for x in latest_20
                )
                / LIQUIDITY_DAYS
            )

        except Exception as e:

            last_error = repr(e)

            if (
                attempt
                < HIST_RETRIES - 1
            ):

                delay = min(
                    10,
                    2 ** attempt
                )

                time.sleep(
                    delay
                )

    raise RuntimeError(
        last_error
        or "Historical request failed"
    )


# ============================================================
# LIVE CANDIDATES
# ============================================================

def live_candidates():

    quotes = fetch_live_quotes()

    today = ist_now().date()

    output = []

    for q in quotes:

        try:

            key = q.get(
                "_instrument_key"
            )

            meta = BY_KEY.get(
                key,
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

            ltp = float(
                q.get(
                    "last_price"
                )
                or 0
            )

            prev_close = float(
                q.get(
                    "prev_close_price"
                )
                or 0
            )

            ohlc = (
                q.get(
                    "ohlc"
                )
                or {}
            )

            opening_price = float(
                ohlc.get(
                    "open"
                )
                or 0
            )

            low_price = float(
                ohlc.get(
                    "low"
                )
                or 0
            )

            volume = float(
                q.get(
                    "volume"
                )
                or ohlc.get(
                    "volume"
                )
                or 0
            )

            average_price = float(
                q.get(
                    "average_price"
                )
                or 0
            )

            # PRICE > ₹20
            if ltp <= MIN_PRICE:
                continue

            if (
                opening_price <= 0
                or low_price <= 0
            ):
                continue

            # LTP > OPEN
            if ltp <= opening_price:
                continue

            # OPEN-LOW GAP <= 0.50%
            gap = (
                (
                    opening_price
                    - low_price
                )
                / opening_price
            ) * 100.0

            if gap > MAX_GAP:
                continue

            live_price = (
                average_price
                if average_price > 0
                else ltp
            )

            live_turnover = (
                volume
                * live_price
            )

            recovery = max(
                0.0,
                (
                    (
                        ltp
                        - low_price
                    )
                    / low_price
                ) * 100.0
            )

            gain = 0.0

            if prev_close > 0:

                gain = (
                    (
                        ltp
                        - prev_close
                    )
                    / prev_close
                ) * 100.0

            output.append({

                "key":
                    key,

                "symbol":
                    symbol,

                "price":
                    ltp,

                "open":
                    opening_price,

                "low":
                    low_price,

                "gap":
                    gap,

                "volume":
                    volume,

                "live_turnover":
                    live_turnover,

                "recovery":
                    recovery,

                "gain":
                    gain,

                "today":
                    today.isoformat()
            })

        except Exception:

            continue

    output.sort(
        key=lambda x: (
            x["gap"],
            -x["gain"],
            -x["recovery"]
        )
    )

    return output


# ============================================================
# PERCENTILE
# ============================================================

def percentile_score(
    values,
    value
):

    if len(values) <= 1:
        return 100.0

    count = sum(
        1
        for v in values
        if v <= value
    )

    return (
        100.0
        * (count - 1)
        / (len(values) - 1)
    )


# ============================================================
# STRENGTH SCORE
# ============================================================

def apply_strength_score(items):

    if not items:
        return items

    for item in items:

        item["gap_score"] = max(
            0.0,
            100.0
            * (
                1.0
                - item["gap"]
                / MAX_GAP
            )
        )

    recovery_values = [
        x["recovery"]
        for x in items
    ]

    gain_values = [
        x["gain"]
        for x in items
    ]

    live_values = [
        x["live_turnover"]
        for x in items
    ]

    avg_values = [
        x.get(
            "avg_turnover",
            0.0
        )
        for x in items
    ]

    for item in items:

        item["recovery_score"] = (
            percentile_score(
                recovery_values,
                item["recovery"]
            )
        )

        item["gain_score"] = (
            percentile_score(
                gain_values,
                item["gain"]
            )
        )

        item["live_score"] = (
            percentile_score(
                live_values,
                item["live_turnover"]
            )
        )

        item["avg_score"] = (
            percentile_score(
                avg_values,
                item.get(
                    "avg_turnover",
                    0.0
                )
            )
        )

        item["strength"] = (

            item["gap_score"]
            * 0.30

            + item["recovery_score"]
            * 0.25

            + item["gain_score"]
            * 0.15

            + item["live_score"]
            * 0.20

            + item["avg_score"]
            * 0.10
        )

    items.sort(
        key=lambda x: (
            -x["strength"],
            x["gap"],
            -x["live_turnover"],
            -x.get(
                "avg_turnover",
                0
            )
        )
    )

    return items


# ============================================================
# FORMAT RESULT
# ============================================================

def format_result(item):

    avg = float(
        item.get(
            "avg_turnover",
            0
        )
    )

    live = float(
        item.get(
            "live_turnover",
            0
        )
    )

    return {

        "symbol":
            item["symbol"],

        # CURRENT PRICE / LTP
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
                4
            ),

        "recovery":
            round(
                item["recovery"],
                2
            ),

        "gain":
            round(
                item["gain"],
                2
            ),

        "strength":
            round(
                item["strength"],
                1
            ),

        # PREVIOUS 20 VALID TRADING DAYS
        "avg_turnover_cr":
            round(
                avg
                / 1_00_00_000.0,
                2
            ),

        # TODAY LIVE TURNOVER
        "live_turnover_cr":
            round(
                live
                / 1_00_00_000.0,
                2
            ),

        "volume":
            int(
                item.get(
                    "volume",
                    0
                )
            ),

        "today":
            item.get(
                "today",
                ""
            )
    }


# ============================================================
# ADD LIQUIDITY
# ============================================================

def add_liquidity(
    candidates
):

    global LIVE_RESULTS
    global QUALIFIED_TODAY

    cache = load_cache()

    today = ist_now().date()

    today_key = today.isoformat()

    qualified = []

    pending = []

    # ----------------------------------------
    # CACHE CHECK
    # ----------------------------------------

    for candidate in candidates:

        cached = cache.get(
            candidate["key"]
        )

        if (
            isinstance(
                cached,
                dict
            )
            and cached.get(
                "date"
            ) == today_key
            and cached.get(
                "avg_turnover"
            ) is not None
        ):

            avg = float(
                cached[
                    "avg_turnover"
                ]
            )

            candidate[
                "avg_turnover"
            ] = avg

            if (
                avg
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
        "Liquidity cache hits: "
        f"{len(candidates) - len(pending)}"
    )

    log(
        "Historical liquidity "
        "requests needed: "
        f"{len(pending)}"
    )

    # ----------------------------------------
    # PUBLISH PARTIAL RESULTS
    # ----------------------------------------

    def publish_partial():

        global LIVE_RESULTS

        try:

            with QUALIFIED_LOCK:

                for item in qualified:

                    QUALIFIED_TODAY[
                        item["key"]
                    ] = dict(
                        item
                    )

                all_today = list(
                    QUALIFIED_TODAY.values()
                )

            apply_strength_score(
                all_today
            )

            LIVE_RESULTS = [
                format_result(x)
                for x in all_today
            ]

        except Exception as e:

            log(
                "Partial result publish error: "
                f"{repr(e)}"
            )

    # Cached results appear immediately
    publish_partial()

    # ----------------------------------------
    # HISTORICAL WORKER
    # ----------------------------------------

    def worker(candidate):

        try:

            value = (
                historical_20day_turnover(
                    candidate["key"],
                    today
                )
            )

            return (
                candidate["key"],
                value,
                None
            )

        except Exception as e:

            return (
                candidate["key"],
                None,
                repr(e)
            )

    # ----------------------------------------
    # CONTROLLED HISTORICAL SCAN
    # ----------------------------------------

    if pending:

        pending_map = {
            c["key"]: c
            for c in pending
        }

        with ThreadPoolExecutor(
            max_workers=HIST_WORKERS
        ) as executor:

            futures = [
                executor.submit(
                    worker,
                    c
                )
                for c in pending
            ]

            completed = 0

            for future in as_completed(
                futures
            ):

                completed += 1

                key, value, error = (
                    future.result()
                )

                if (
                    error
                    or value is None
                ):

                    log(
                        "Liquidity request failed "
                        f"[{completed}/{len(pending)}] "
                        f"{key}: {error}"
                    )

                    continue

                # SAVE CACHE
                cache[key] = {

                    "date":
                        today_key,

                    "avg_turnover":
                        value
                }

                candidate = (
                    pending_map.get(
                        key
                    )
                )

                if candidate is not None:

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

                # SHOW NEW RESULT IMMEDIATELY
                publish_partial()

                if (
                    completed % 10 == 0
                    or completed == len(pending)
                ):

                    log(
                        "Liquidity progress: "
                        f"{completed}/{len(pending)} "
                        f"| qualifying={len(qualified)}"
                    )

    save_cache(
        cache
    )

    publish_partial()

    return qualified


# ============================================================
# COMPLETE SCAN
# ============================================================

def perform_scan():

    global LAST_SCAN_TIME
    global LAST_SCAN_ERROR
    global LAST_SCAN_DATE
    global LAST_SCAN_EPOCH
    global LIVE_RESULTS
    global QUALIFIED_TODAY

    LAST_SCAN_ERROR = ""

    today_key = (
        ist_now()
        .date()
        .isoformat()
    )

    # New trading day
    if LAST_SCAN_DATE != today_key:

        with QUALIFIED_LOCK:

            QUALIFIED_TODAY = {}

        LIVE_RESULTS = []

        log(
            "NEW TRADING DAY: "
            "clearing yesterday's qualifying list"
        )

    log(
        "STARTING OPEN-LOW STRENGTH SCAN"
    )

    if not get_token():

        raise RuntimeError(
            "UPSTOX_ACCESS_TOKEN is missing "
            "in Render Environment Variables."
        )

    load_instruments()

    candidates = live_candidates()

    log(
        "Live candidates after "
        "price/open/gap filters: "
        f"{len(candidates)}"
    )

    if not candidates:

        LAST_SCAN_TIME = (
            ist_now().strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        )

        LAST_SCAN_EPOCH = time.time()

        LAST_SCAN_DATE = today_key

        return list(
            QUALIFIED_TODAY.values()
        )

    qualified = add_liquidity(
        candidates
    )

    apply_strength_score(
        qualified
    )

    results = [
        format_result(x)
        for x in qualified
    ]

    LAST_SCAN_TIME = (
        ist_now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    )

    LAST_SCAN_EPOCH = time.time()

    LAST_SCAN_DATE = today_key

    log(
        "TODAY QUALIFYING STOCKS CAPTURED: "
        f"{len(results)}"
    )

    return results


# ============================================================
# START SCAN
# ============================================================

def start_scan(
    force=False
):

    global SCAN_RUNNING
    global LAST_SCAN_ATTEMPT_DATE

    with SCAN_LOCK:

        if SCAN_RUNNING:

            return False

        SCAN_RUNNING = True

        LAST_SCAN_ATTEMPT_DATE = (
            ist_now()
            .date()
            .isoformat()
        )

    def runner():

        global SCAN_RUNNING
        global LIVE_RESULTS
        global LAST_SCAN_ERROR
        global LAST_SCAN_DATE

        try:

            LIVE_RESULTS = (
                perform_scan()
            )

        except Exception as e:

            LAST_SCAN_ERROR = repr(
                e
            )

            LAST_SCAN_DATE = (
                ist_now()
                .date()
                .isoformat()
            )

            log(
                f"SCAN ERROR: {repr(e)}"
            )

        finally:

            with SCAN_LOCK:

                SCAN_RUNNING = False

    threading.Thread(
        target=runner,
        daemon=True
    ).start()

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

    return jsonify({

        "ok":
            True,

        "token_configured":
            bool(
                get_token()
            ),

        "nse_eq_stocks":
            len(
                INSTRUMENTS
            ),

        "scan_running":
            SCAN_RUNNING,

        "last_scan":
            LAST_SCAN_TIME,

        "last_error":
            LAST_SCAN_ERROR,

        "preopen_connected":
            PREOPEN_CONNECTED,

        "preopen_last_update":
            (
                datetime.fromtimestamp(
                    PREOPEN_LAST_UPDATE,
                    IST
                ).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                if PREOPEN_LAST_UPDATE
                else None
            ),

        "mode":
            session_mode(),

        "updated_at":
            ist_now().strftime(
                "%Y-%m-%d %H:%M:%S"
            )
    })


# ============================================================
# PRE-OPEN API
# ============================================================

@app.get("/api/preopen")
def preopen_api():

    mode = session_mode()

    if mode != "preopen":

        return jsonify({

            "mode":
                "live",

            "connected":
                PREOPEN_CONNECTED,

            "results":
                [],

            "message":
                "Pre-Open Mode समाप्त हो चुका है। "
                "9:15 के बाद Live Open-Low Scanner चलेगा।"
        })

    results = preopen_results(
        100
    )

    if not get_token():

        return jsonify({

            "mode":
                "preopen",

            "connected":
                False,

            "results":
                [],

            "message":
                "UPSTOX_ACCESS_TOKEN Render "
                "Environment Variables में नहीं मिला।"
        })

    if not PREOPEN_CONNECTED:

        return jsonify({

            "mode":
                "preopen",

            "connected":
                False,

            "results":
                results,

            "message":
                PREOPEN_ERROR
                or
                "Upstox Pre-Open feed connect हो रहा है..."
        })

    return jsonify({

        "mode":
            "preopen",

        "connected":
            True,

        "results":
            results,

        "message":
            "Pre-Open IEP data live है। "
            "Order नहीं लगाया जा रहा है। "
            "9:15 पर यह mode अपने-आप "
            "Live Open-Low Scanner में बदल जाएगा।",

        "updated_at":
            (
                datetime.fromtimestamp(
                    PREOPEN_LAST_UPDATE,
                    IST
                ).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                if PREOPEN_LAST_UPDATE
                else ist_now().strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            )
    })


# ============================================================
# NORMAL SCAN API
# ============================================================

@app.get("/api/scan")
def scan():

    if session_mode() == "preopen":

        return jsonify({

            "mode":
                "preopen",

            "connected":
                PREOPEN_CONNECTED,

            "running":
                False,

            "finished":
                False,

            "results":
                preopen_results(
                    100
                ),

            "message":
                "Pre-Open Mode सक्रिय है। "
                "9:15 पर Live Scanner अपने-आप शुरू होगा।",

            "updated_at":
                ist_now().strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
        })

    if not get_token():

        return jsonify({

            "mode":
                "live",

            "connected":
                False,

            "running":
                False,

            "finished":
                True,

            "results":
                [],

            "message":
                "UPSTOX_ACCESS_TOKEN Render "
                "Environment Variables में नहीं मिला।"
        }), 500

    today_key = (
        ist_now()
        .date()
        .isoformat()
    )

    # --------------------------------------------------------
    # IMPORTANT:
    # Live scan can refresh every 30 seconds.
    # Historical data comes from today's cache after
    # the first successful historical request.
    # Therefore opening another browser tab does NOT
    # restart 20-day historical downloads.
    # --------------------------------------------------------

    due = (
        LAST_SCAN_EPOCH == 0
        or LAST_SCAN_TIME is None
        or LAST_SCAN_DATE != today_key
        or (
            time.time()
            - LAST_SCAN_EPOCH
            >= SCAN_INTERVAL_SECONDS
        )
    )

    if (
        due
        and not SCAN_RUNNING
    ):

        start_scan()

    return jsonify({

        "mode":
            "live",

        "connected":
            True,

        "running":
            SCAN_RUNNING,

        "finished":
            (
                not SCAN_RUNNING
                and LAST_SCAN_TIME is not None
            ),

        "scanned":
            len(
                INSTRUMENTS
            ),

        "results":
            LIVE_RESULTS,

        "message":
            LAST_SCAN_ERROR
            or (
                "Strength scan चल रहा है..."
                if SCAN_RUNNING
                else
                "Scanner तैयार है।"
            ),

        "updated_at":
            LAST_SCAN_TIME
            or
            ist_now().strftime(
                "%Y-%m-%d %H:%M:%S"
            )
    })


# ============================================================
# MANUAL SCAN NOW
# ============================================================

@app.get("/api/scan-now")
def scan_now():

    global LAST_SCAN_ERROR
    global LAST_SCAN_ATTEMPT_DATE

    if session_mode() == "preopen":

        return jsonify({

            "mode":
                "preopen",

            "connected":
                PREOPEN_CONNECTED,

            "running":
                False,

            "finished":
                False,

            "results":
                preopen_results(
                    100
                ),

            "message":
                "अभी Pre-Open Mode है। "
                "Live Open-Low Scan 9:15 के बाद चलेगा।"
        })

    if not get_token():

        return jsonify({

            "mode":
                "live",

            "connected":
                False,

            "running":
                False,

            "finished":
                True,

            "results":
                [],

            "message":
                "UPSTOX_ACCESS_TOKEN Render "
                "Environment Variables में नहीं मिला।"
        }), 500

    if SCAN_RUNNING:

        return jsonify({

            "mode":
                "live",

            "connected":
                True,

            "running":
                True,

            "finished":
                False,

            "results":
                LIVE_RESULTS,

            "message":
                "एक scan पहले से चल रहा है।"
        })

    LAST_SCAN_ERROR = ""

    LAST_SCAN_ATTEMPT_DATE = None

    # Do NOT delete today's qualified results.
    # Fresh scan adds newly qualifying stocks.

    start_scan(
        force=True
    )

    return jsonify({

        "mode":
            "live",

        "connected":
            True,

        "running":
            True,

        "finished":
            False,

        "results":
            LIVE_RESULTS,

        "message":
            "Fresh Strength scan शुरू हो गया है..."
    })


# ============================================================
# START PRE-OPEN FEED
# ============================================================

try:

    start_preopen_feed()

except Exception as e:

    PREOPEN_ERROR = repr(
        e
    )

    log(
        f"Pre-open thread could not start: {repr(e)}"
    )


# ============================================================
# LOCAL RUN
# ============================================================

if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=PORT
    )
