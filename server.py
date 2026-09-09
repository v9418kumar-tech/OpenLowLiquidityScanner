import os
import gzip
import json
import time
import logging
import threading
from datetime import datetime, timedelta, timezone
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from flask import Flask, jsonify, send_from_directory

PORT = int(os.environ.get("PORT", "10000"))
BASE = "https://api.upstox.com"
INSTR_URL = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"
IST = timezone(timedelta(hours=5, minutes=30))

MIN_PRICE = 20.0
MAX_GAP = 0.50
MIN_AVG_TURNOVER = 10_00_00_000.0
LIQUIDITY_DAYS = 20
MAX_WORKERS = 8
BATCH_SIZE = 500
CACHE_FILE = "liquidity_cache.json"

app = Flask(__name__, static_folder=".", static_url_path="")

INSTRUMENTS = []
BY_KEY = {}
INSTRUMENTS_LOADED_AT = 0

LIVE_RESULTS = []
LAST_SCAN_TIME = None
LAST_SCAN_ERROR = ""
SCAN_RUNNING = False
SCAN_LOCK = threading.Lock()

logging.basicConfig(level=logging.INFO)


def get_token():
    return os.environ.get("UPSTOX_ACCESS_TOKEN", "").strip()


def headers():
    token = get_token()
    if not token:
        raise RuntimeError("UPSTOX_ACCESS_TOKEN is missing.")
    return {
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
    }


http = requests.Session()
http.headers.update({"User-Agent": "OpenLowStrengthScanner/2.0"})


def is_real_equity(item):
    if item.get("segment") != "NSE_EQ":
        return False
    if item.get("instrument_type") != "EQ":
        return False
    if item.get("security_type") not in (None, "", "NORMAL"):
        return False

    key = item.get("instrument_key")
    if not key:
        return False

    symbol = str(item.get("trading_symbol") or "").upper()
    name = str(item.get("name") or "").upper()
    combined = f"{symbol} {name}"

    for bad in (
        "ETF",
        "EXCHANGE TRADED FUND",
        "MUTUAL FUND",
        "INDEX FUND",
        "SME",
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


def load_instruments(force=False):
    global INSTRUMENTS, BY_KEY, INSTRUMENTS_LOADED_AT

    if (
        INSTRUMENTS
        and not force
        and time.time() - INSTRUMENTS_LOADED_AT < 21600
    ):
        return

    print("Downloading Upstox NSE instrument file...")

    r = http.get(INSTR_URL, timeout=30)
    r.raise_for_status()

    data = json.loads(
        gzip.decompress(r.content).decode("utf-8")
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

    print(f"Loaded {len(INSTRUMENTS)} NSE EQ stocks.")


def chunks(items, size):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def fetch_live_quotes():
    load_instruments()

    all_quotes = []
    successful_batches = 0

    batches = list(chunks(INSTRUMENTS, BATCH_SIZE))

    print(
        f"Requesting Full Market Quotes for "
        f"{len(INSTRUMENTS)} NSE EQ stocks..."
    )

    for number, batch in enumerate(batches, 1):

        keys = ",".join(
            x["instrument_key"]
            for x in batch
        )

        try:
            r = http.get(
                BASE + "/v3/market-quote/quotes",
                headers=headers(),
                params={
                    "instrument_key": keys
                },
                timeout=30,
            )

            if r.status_code != 200:
                print(
                    f"Live batch {number} HTTP "
                    f"{r.status_code}: {r.text[:300]}"
                )
                continue

            data = r.json().get("data", {})

            if not isinstance(data, dict):
                continue

            received = 0

            for response_key, q in data.items():

                if not isinstance(q, dict):
                    continue

                key = (
                    q.get("instrument_token")
                    or response_key
                )

                q["_instrument_key"] = key

                all_quotes.append(q)
                received += 1

            successful_batches += 1

            print(
                f"Live batch {number}: "
                f"{received} quotes received."
            )

        except Exception as e:
            print(
                f"Live batch {number} ERROR: {repr(e)}"
            )

    if successful_batches == 0:
        raise RuntimeError(
            "Upstox live market quote API failed "
            "for every batch."
        )

    print(
        f"Total live quotes received: "
        f"{len(all_quotes)}"
    )

    return all_quotes


def load_cache():

    if not os.path.exists(CACHE_FILE):
        return {}

    try:
        with open(
            CACHE_FILE,
            "r",
            encoding="utf-8"
        ) as f:
            data = json.load(f)

        return data if isinstance(data, dict) else {}

    except Exception:
        return {}


def save_cache(cache):

    try:
        tmp = CACHE_FILE + ".tmp"

        with open(
            tmp,
            "w",
            encoding="utf-8"
        ) as f:
            json.dump(cache, f)

        os.replace(tmp, CACHE_FILE)

    except Exception as e:
        print(
            f"Cache save error: {repr(e)}"
        )


def historical_20day_turnover(key, today):

    yesterday = today - timedelta(days=1)
    start = today - timedelta(days=45)

    url = (
        BASE
        + "/v3/historical-candle/"
        + quote(key, safe="|")
        + "/days/1/"
        + yesterday.isoformat()
        + "/"
        + start.isoformat()
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

            if close > 0 and volume > 0:
                turnover = close * volume
                valid.append(
                    (timestamp, turnover)
                )

        except Exception:
            pass

    valid.sort(
        key=lambda x: x[0],
        reverse=True
    )

    if len(valid) < LIQUIDITY_DAYS:
        return None

    return (
        sum(
            x[1]
            for x in valid[:LIQUIDITY_DAYS]
        )
        / LIQUIDITY_DAYS
    )


def get_live_candidates():

    quotes = fetch_live_quotes()

    today = datetime.now(IST).date()

    candidates = []

    for q in quotes:

        try:
            key = q.get("_instrument_key")

            meta = BY_KEY.get(key, {})

            symbol = (
                meta.get("trading_symbol")
                or q.get("symbol")
                or ""
            )

            ltp = float(
                q.get("last_price") or 0
            )

            previous_close = float(
                q.get("prev_close_price") or 0
            )

            ohlc = q.get("ohlc") or {}

            opening = float(
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

            average_price = float(
                q.get("average_price") or 0
            )

            if ltp <= MIN_PRICE:
                continue

            if opening <= 0 or low <= 0:
                continue

            if ltp <= opening:
                continue

            gap = (
                (opening - low)
                / opening
                * 100
            )

            if gap > MAX_GAP:
                continue

            live_price = (
                average_price
                if average_price > 0
                else ltp
            )

            live_turnover = (
                volume * live_price
            )

            recovery = (
                (ltp - low)
                / low
                * 100
            )

            if recovery < 0:
                recovery = 0

            gain = 0

            if previous_close > 0:
                gain = (
                    (ltp - previous_close)
                    / previous_close
                    * 100
                )

            candidates.append({
                "key": key,
                "symbol": symbol,
                "price": ltp,
                "open": opening,
                "low": low,
                "gap": gap,
                "recovery": recovery,
                "gain": gain,
                "volume": volume,
                "live_turnover": live_turnover,
                "today": today.isoformat(),
            })

        except Exception:
            continue

    candidates.sort(
        key=lambda x: (
            -x["gain"],
            -x["recovery"],
            x["gap"]
        )
    )

    return candidates


def add_liquidity(candidates):

    cache = load_cache()

    today = datetime.now(IST).date()
    today_key = today.isoformat()

    qualified = []
    pending = []

    for c in candidates:

        cached = cache.get(c["key"])

        if (
            isinstance(cached, dict)
            and cached.get("date")
            == today_key
            and cached.get("avg_turnover")
            is not None
        ):

            avg = float(
                cached["avg_turnover"]
            )

            c["avg_turnover"] = avg

            if avg >= MIN_AVG_TURNOVER:
                qualified.append(c)

        else:
            pending.append(c)

    print(
        f"Liquidity cache hits: "
        f"{len(candidates) - len(pending)}"
    )

    print(
        f"Historical requests needed: "
        f"{len(pending)}"
    )

    def worker(candidate):

        try:
            value = historical_20day_turnover(
                candidate["key"],
                today
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

    with ThreadPoolExecutor(
        max_workers=MAX_WORKERS
    ) as executor:

        futures = [
            executor.submit(
                worker,
                c
            )
            for c in pending
        ]

        pending_map = {
            c["key"]: c
            for c in pending
        }

        for future in as_completed(futures):

            key, value, error = future.result()

            if error:
                print(
                    f"Historical error "
                    f"{key}: {error}"
                )
                continue

            if value is None:
                continue

            cache[key] = {
                "date": today_key,
                "avg_turnover": value
            }

            candidate = pending_map.get(key)

            if candidate is not None:

                candidate["avg_turnover"] = value

                if value >= MIN_AVG_TURNOVER:
                    qualified.append(candidate)

    save_cache(cache)

    return qualified


def percentile(values, value):

    if not values:
        return 0.0

    if len(values) == 1:
        return 100.0

    count = sum(
        1
        for x in values
        if x <= value
    )

    return (
        100.0
        * (count - 1)
        / (len(values) - 1)
    )


def apply_strength_score(items):

    if not items:
        return items

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
        x.get("avg_turnover", 0)
        for x in items
    ]

    for x in items:

        # Smaller Open-Low Gap = better
        gap_score = max(
            0,
            100
            * (
                1
                - x["gap"]
                / MAX_GAP
            )
        )

        recovery_score = percentile(
            recovery_values,
            x["recovery"]
        )

        gain_score = percentile(
            gain_values,
            x["gain"]
        )

        live_score = percentile(
            live_values,
            x["live_turnover"]
        )

        avg_score = percentile(
            avg_values,
            x.get("avg_turnover", 0)
        )

        x["strength"] = (
            gap_score * 0.30
            + recovery_score * 0.25
            + gain_score * 0.15
            + live_score * 0.20
            + avg_score * 0.10
        )

    # Strongest share first
    items.sort(
        key=lambda x: (
            -x["strength"],
            x["gap"],
            -x["live_turnover"],
            -x.get("avg_turnover", 0)
        )
    )

    return items


def format_result(x):

    return {
        "symbol": x["symbol"],
        "price": round(x["price"], 2),
        "open": round(x["open"], 2),
        "low": round(x["low"], 2),

        # 4 decimal places so 0.00% confusion is avoided
        "gap": round(x["gap"], 4),

        "recovery": round(
            x["recovery"],
            2
        ),

        "gain": round(
            x["gain"],
            2
        ),

        "strength": round(
            x["strength"],
            1
        ),

        "avg_turnover_cr": round(
            x.get("avg_turnover", 0)
            / 1_00_00_000,
            2
        ),

        "live_turnover_cr": round(
            x.get("live_turnover", 0)
            / 1_00_00_000,
            2
        ),

        "volume": int(
            x.get("volume", 0)
        ),

        "today": x.get(
            "today",
            ""
        ),
    }


def perform_scan():

    global LAST_SCAN_TIME
    global LAST_SCAN_ERROR

    LAST_SCAN_ERROR = ""

    print(
        "=========================================="
    )

    print(
        "STARTING OPEN-LOW STRENGTH SCAN"
    )

    print(
        "=========================================="
    )

    if not get_token():
        raise RuntimeError(
            "UPSTOX_ACCESS_TOKEN is missing."
        )

    load_instruments()

    candidates = get_live_candidates()

    print(
        "Live candidates after "
        "price/open/gap filters:",
        len(candidates)
    )

    if not candidates:

        LAST_SCAN_TIME = (
            datetime.now(IST)
            .strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        )

        return []

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
        datetime.now(IST)
        .strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    )

    print(
        "FINAL QUALIFYING STOCKS:",
        len(results)
    )

    return results


def start_scan():

    global SCAN_RUNNING
    global LIVE_RESULTS
    global LAST_SCAN_ERROR

    with SCAN_LOCK:

        if SCAN_RUNNING:
            return False

        SCAN_RUNNING = True

    def runner():

        global SCAN_RUNNING
        global LIVE_RESULTS
        global LAST_SCAN_ERROR

        try:

            LIVE_RESULTS = perform_scan()

        except Exception as e:

            LAST_SCAN_ERROR = repr(e)

            print(
                "SCAN ERROR:",
                repr(e)
            )

        finally:

            with SCAN_LOCK:
                SCAN_RUNNING = False

    threading.Thread(
        target=runner,
        daemon=True
    ).start()

    return True


@app.get("/")
def home():
    return send_from_directory(
        ".",
        "index.html"
    )


@app.get("/api/health")
def health():

    return jsonify({
        "ok": True,
        "token_configured": bool(
            get_token()
        ),
        "nse_eq_stocks": len(
            INSTRUMENTS
        ),
        "scan_running": SCAN_RUNNING,
        "last_scan": LAST_SCAN_TIME,
        "last_error": LAST_SCAN_ERROR,
        "updated_at":
            datetime.now(IST).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
    })


@app.get("/api/scan")
def scan():

    if not get_token():

        return jsonify({
            "connected": False,
            "running": False,
            "finished": True,
            "results": [],
            "message":
                "UPSTOX_ACCESS_TOKEN "
                "Render Environment "
                "Variables में नहीं मिला।",
        }), 500

    if (
        not LIVE_RESULTS
        and not SCAN_RUNNING
    ):
        start_scan()

    return jsonify({
        "connected": True,
        "running": SCAN_RUNNING,
        "finished":
            (
                not SCAN_RUNNING
                and LAST_SCAN_TIME is not None
            ),
        "scanned": len(
            INSTRUMENTS
        ),
        "results": LIVE_RESULTS,

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
            or datetime.now(IST).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
    })


@app.get("/api/scan-now")
def scan_now():

    global LIVE_RESULTS
    global LAST_SCAN_ERROR

    if not get_token():

        return jsonify({
            "connected": False,
            "running": False,
            "finished": True,
            "results": [],
            "message":
                "UPSTOX_ACCESS_TOKEN "
                "Render Environment "
                "Variables में नहीं मिला।",
        }), 500

    if SCAN_RUNNING:

        return jsonify({
            "connected": True,
            "running": True,
            "finished": False,
            "results": LIVE_RESULTS,
            "message":
                "एक scan पहले से चल रहा है।",
        })

    LIVE_RESULTS = []
    LAST_SCAN_ERROR = ""

    start_scan()

    return jsonify({
        "connected": True,
        "running": True,
        "finished": False,
        "results": [],
        "message":
            "Fresh Strength scan शुरू हो गया है...",
    })


if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=PORT
    )
