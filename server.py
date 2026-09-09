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
import upstox_client
from flask import Flask, jsonify, send_from_directory

PORT = int(os.environ.get("PORT", "10000"))
BASE = "https://api.upstox.com"
INSTR_URL = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"
IST = timezone(timedelta(hours=5, minutes=30))

MIN_PRICE = 20.0
MAX_GAP = 0.50
MIN_AVG_TURNOVER = 10_00_00_000.0
LIQUIDITY_DAYS = 20
MAX_WORKERS = 20
BATCH_SIZE = 500
CACHE_FILE = "liquidity_cache.json"

app = Flask(__name__, static_folder=".", static_url_path="")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

INSTRUMENTS = []
BY_KEY = {}
INSTRUMENTS_LOADED_AT = 0

LIVE_RESULTS = []
LAST_SCAN_TIME = None
LAST_SCAN_ERROR = ""
LAST_SCAN_DATE = None
SCAN_RUNNING = False
SCAN_LOCK = threading.Lock()

# ------------------------------------------------------------
# PRE-OPEN LIVE FEED
# Upstox LTPC provides IEP during the pre-open session.
# ------------------------------------------------------------
PREOPEN = {}
PREOPEN_CONNECTED = False
PREOPEN_ERROR = ""
PREOPEN_LAST_UPDATE = None
PREOPEN_LOCK = threading.Lock()
PREOPEN_STREAMER = None


def log(msg):
    logging.info(msg)


def get_token():
    return os.environ.get("UPSTOX_ACCESS_TOKEN", "").strip()


def headers():
    token = get_token()
    if not token:
        raise RuntimeError("UPSTOX_ACCESS_TOKEN is missing in Render Environment Variables.")
    return {"Accept": "application/json", "Authorization": f"Bearer {token}"}


http = requests.Session()
http.headers.update({"User-Agent": "OpenLowStrengthScanner/3.0"})


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

    symbol = str(item.get("trading_symbol") or "").upper().strip()
    name = str(item.get("name") or "").upper().strip()
    short = str(item.get("short_name") or "").upper().strip()
    combined = f"{symbol} {name} {short}"

    for bad in ("ETF", "EXCHANGE TRADED FUND", "MUTUAL FUND", "INDEX FUND", "SME"):
        if bad in combined:
            return False
    if symbol.endswith("BEES") or symbol.endswith("BE") or symbol.endswith("BZ"):
        return False
    return True


def load_instruments(force=False):
    global INSTRUMENTS, BY_KEY, INSTRUMENTS_LOADED_AT

    if INSTRUMENTS and not force and time.time() - INSTRUMENTS_LOADED_AT < 21600:
        return

    log("Downloading Upstox NSE instrument file...")
    r = http.get(INSTR_URL, timeout=30)
    r.raise_for_status()
    data = json.loads(gzip.decompress(r.content).decode("utf-8"))

    selected = []
    for item in data:
        try:
            if is_real_equity(item):
                selected.append(item)
        except Exception:
            pass

    INSTRUMENTS = selected
    BY_KEY = {x["instrument_key"]: x for x in selected}
    INSTRUMENTS_LOADED_AT = time.time()
    log(f"Loaded {len(INSTRUMENTS)} NSE EQ stocks.")


def chunks(items, size):
    for i in range(0, len(items), size):
        yield items[i:i + size]


# ------------------------------------------------------------
# PRE-OPEN FEED
# ------------------------------------------------------------

def extract_ltpc(feed):
    if not isinstance(feed, dict):
        return {}
    ltpc = feed.get("ltpc")
    if isinstance(ltpc, dict):
        return ltpc

    # Some SDK/feed structures can wrap LTPC under ff/firstLevelWithGreeks.
    for parent_name in ("ff", "fullFeed", "firstLevelWithGreeks"):
        parent = feed.get(parent_name)
        if isinstance(parent, dict):
            x = parent.get("ltpc")
            if isinstance(x, dict):
                return x
            x = parent.get("marketFF")
            if isinstance(x, dict) and isinstance(x.get("ltpc"), dict):
                return x["ltpc"]
    return {}


def update_preopen(message):
    global PREOPEN_LAST_UPDATE
    if not isinstance(message, dict):
        return

    feeds = message.get("feeds", {})
    if not isinstance(feeds, dict):
        return

    now = time.time()
    with PREOPEN_LOCK:
        for key, feed in feeds.items():
            meta = BY_KEY.get(key)
            if not meta:
                continue

            ltpc = extract_ltpc(feed)
            if not ltpc:
                continue

            iep = ltpc.get("iep")
            cp = ltpc.get("cp")
            ltp = ltpc.get("ltp")

            try:
                if iep is None:
                    continue
                iep = float(iep)
                if iep <= MIN_PRICE:
                    continue
                cp = float(cp or 0)
                ltp = float(ltp or 0)
            except Exception:
                continue

            if cp <= 0:
                continue

            symbol = str(meta.get("trading_symbol") or "").upper()
            change = ((iep - cp) / cp) * 100.0

            # Small deterministic boost for higher priced/meaningful IEP,
            # while the main ranking remains IEP change.
            with PREOPEN_LOCK:
                PREOPEN[key] = {
                    "key": key,
                    "symbol": symbol,
                    "iep": iep,
                    "cp": cp,
                    "ltp": ltp,
                    "change": change,
                    "seen": now,
                }

        PREOPEN_LAST_UPDATE = now


def feed_thread():
    global PREOPEN_CONNECTED, PREOPEN_ERROR, PREOPEN_STREAMER

    if not get_token():
        PREOPEN_ERROR = "UPSTOX_ACCESS_TOKEN environment variable is missing."
        return

    try:
        load_instruments()

        configuration = upstox_client.Configuration()
        configuration.access_token = get_token()

        streamer = upstox_client.MarketDataStreamerV3(
            upstox_client.ApiClient(configuration)
        )
        PREOPEN_STREAMER = streamer

        def on_open():
            global PREOPEN_CONNECTED, PREOPEN_ERROR
            PREOPEN_CONNECTED = True
            PREOPEN_ERROR = ""
            keys = [x["instrument_key"] for x in INSTRUMENTS]

            # LTPC individual limit is 5000 keys.
            for batch in chunks(keys, 4500):
                streamer.subscribe(batch, "ltpc")
                time.sleep(0.5)

            log(f"Pre-open feed subscribed to {len(keys)} NSE EQ instruments.")

        def on_message(message):
            update_preopen(message)

        def on_close(*args):
            global PREOPEN_CONNECTED
            PREOPEN_CONNECTED = False
            log(f"Upstox pre-open stream closed: {args}")

        def on_error(err):
            global PREOPEN_ERROR
            PREOPEN_ERROR = str(err)
            log(f"Upstox pre-open feed error: {err}")

        streamer.on("open", on_open)
        streamer.on("message", on_message)
        streamer.on("close", on_close)
        streamer.on("error", on_error)
        streamer.auto_reconnect(True, 10, 100)
        streamer.connect()

    except Exception as e:
        PREOPEN_CONNECTED = False
        PREOPEN_ERROR = repr(e)
        log(f"Pre-open feed startup error: {repr(e)}")


def start_preopen_feed():
    threading.Thread(target=feed_thread, daemon=True, name="preopen-feed").start()


def ist_now():
    return datetime.now(IST)


def session_mode():
    now = ist_now()
    t = now.time()

    # Before normal market: pre-open mode.
    if t < datetime.strptime("09:15", "%H:%M").time():
        return "preopen"

    return "live"


def preopen_results(limit=100):
    today = ist_now().date().isoformat()
    rows = []

    with PREOPEN_LOCK:
        items = list(PREOPEN.values())

    cache = load_cache()

    for x in items:
        try:
            if x["iep"] <= MIN_PRICE or x["change"] <= 0:
                continue

            # Historical liquidity is used only when today's cache already exists.
            # Missing cache never blocks the pre-open ranking.
            avg = 0.0
            key = x.get("key")
            cached = cache.get(key) if key else None
            if isinstance(cached, dict) and cached.get("date") == today:
                avg = float(cached.get("avg_turnover") or 0)

            rows.append({
                "symbol": x["symbol"],
                "price": x["iep"],
                "iep": x["iep"],
                "prev_close": x["cp"],
                "preopen_gain": x["change"],
                "avg_turnover_cr": avg / 1_00_00_000.0,
            })
        except Exception:
            continue

    if not rows:
        return []

    gains = [x["preopen_gain"] for x in rows]
    avgs = [x["avg_turnover_cr"] for x in rows]

    def pct(values, value):
        if len(values) <= 1:
            return 100.0
        return 100.0 * (sum(1 for v in values if v <= value) - 1) / (len(values) - 1)

    for x in rows:
        gain_score = min(100.0, max(0.0, x["preopen_gain"] * 10.0))
        relative_gain = pct(gains, x["preopen_gain"])
        liquidity_score = pct(avgs, x["avg_turnover_cr"]) if any(avgs) else 50.0

        x["strength"] = (
            gain_score * 0.45
            + relative_gain * 0.45
            + liquidity_score * 0.10
        )

    rows.sort(key=lambda x: (-x["strength"], -x["preopen_gain"], -x["avg_turnover_cr"]))

    out = []
    for x in rows[:limit]:
        out.append({
            "symbol": x["symbol"],
            "price": round(x["iep"], 2),
            "iep": round(x["iep"], 2),
            "prev_close": round(x["prev_close"], 2),
            "preopen_gain": round(x["preopen_gain"], 2),
            "strength": round(x["strength"], 1),
            "avg_turnover_cr": round(x["avg_turnover_cr"], 2),
        })
    return out


# ------------------------------------------------------------
# EXISTING OPEN-LOW STRENGTH SCANNER
# ------------------------------------------------------------

def _fetch_quote_batch(batch_no, batch):
    keys = ",".join(x["instrument_key"] for x in batch)
    try:
        # One independent request per worker keeps the live scan parallel and
        # avoids waiting for one slow 500-symbol batch before starting the next.
        r = requests.get(
            BASE + "/v3/market-quote/quotes",
            headers=headers(),
            params={"instrument_key": keys},
            timeout=15,
    )
        if r.status_code != 200:
            return batch_no, [], f"HTTP {r.status_code}: {r.text[:250]}"

        data = r.json().get("data", {})
        if not isinstance(data, dict):
            return batch_no, [], "Invalid data object"

        out = []
        for response_key, q in data.items():
            if not isinstance(q, dict):
                continue
            key = q.get("instrument_token") or response_key
            q["_instrument_key"] = key
            out.append(q)
        return batch_no, out, None
    except Exception as e:
        return batch_no, [], repr(e)


def fetch_live_quotes():
    load_instruments()
    batches = list(chunks(INSTRUMENTS, BATCH_SIZE))
    all_quotes = []
    ok_batches = 0

    # NSE EQ is normally only a handful of 500-symbol batches. Fetch them
    # concurrently so the first live scan is much faster.
    workers = min(8, max(1, len(batches)))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(_fetch_quote_batch, i, batch)
                   for i, batch in enumerate(batches, 1)]
        for f in as_completed(futures):
            n, quotes, error = f.result()
            if error:
                log(f"Live batch {n} ERROR: {error}")
                continue
            all_quotes.extend(quotes)
            ok_batches += 1
            log(f"Live batch {n}: {len(quotes)} quotes received.")

    if ok_batches == 0:
        raise RuntimeError("Upstox live market quote API failed for every batch. Check Render logs.")

    return all_quotes


def load_cache():
    if not os.path.exists(CACHE_FILE):
        return {}
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            x = json.load(f)
        return x if isinstance(x, dict) else {}
    except Exception:
        return {}


def save_cache(cache):
    try:
        tmp = CACHE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(cache, f)
        os.replace(tmp, CACHE_FILE)
    except Exception as e:
        log(f"Cache save error: {repr(e)}")


def historical_20day_turnover(key, today):
    yesterday = today - timedelta(days=1)
    start = today - timedelta(days=45)

    url = (
        BASE + "/v3/historical-candle/"
        + quote(key, safe="|")
        + "/days/1/"
        + yesterday.isoformat()
        + "/"
        + start.isoformat()
    )

    r = http.get(url, headers=headers(), timeout=8)
    r.raise_for_status()
    candles = r.json().get("data", {}).get("candles", [])

    valid = []
    for c in candles:
        if len(c) < 6:
            continue
        try:
            ts = c[0]
            close = float(c[4])
            volume = float(c[5])
            if close > 0 and volume > 0:
                valid.append((ts, close * volume))
        except Exception:
            pass

    valid.sort(key=lambda x: x[0], reverse=True)
    if len(valid) < LIQUIDITY_DAYS:
        return None

    return sum(x[1] for x in valid[:LIQUIDITY_DAYS]) / LIQUIDITY_DAYS


def live_candidates():
    quotes = fetch_live_quotes()
    today = ist_now().date()
    out = []

    for q in quotes:
        try:
            key = q.get("_instrument_key")
            meta = BY_KEY.get(key, {})
            symbol = meta.get("trading_symbol") or q.get("symbol") or ""

            ltp = float(q.get("last_price") or 0)
            prev_close = float(q.get("prev_close_price") or 0)
            ohlc = q.get("ohlc") or {}
            op = float(ohlc.get("open") or 0)
            low = float(ohlc.get("low") or 0)
            volume = float(q.get("volume") or ohlc.get("volume") or 0)
            avg_price = float(q.get("average_price") or 0)

            if ltp <= MIN_PRICE or op <= 0 or low <= 0:
                continue
            if ltp <= op:
                continue

            gap = ((op - low) / op) * 100.0
            if gap > MAX_GAP:
                continue

            live_price = avg_price if avg_price > 0 else ltp
            live_turnover = volume * live_price
            recovery = max(0.0, ((ltp - low) / low) * 100.0)

            gain = 0.0
            if prev_close > 0:
                gain = ((ltp - prev_close) / prev_close) * 100.0

            out.append({
                "key": key,
                "symbol": symbol,
                "price": ltp,
                "open": op,
                "low": low,
                "gap": gap,
                "volume": volume,
                "live_turnover": live_turnover,
                "recovery": recovery,
                "gain": gain,
                "today": today.isoformat(),
            })
        except Exception:
            continue

    out.sort(key=lambda x: (-x["gain"], -x["recovery"], x["gap"]))
    return out


def add_liquidity(candidates):
    """Check 20D liquidity in parallel and publish qualifying rows incrementally.

    The old version waited for every historical request before returning anything.
    This version updates LIVE_RESULTS after each completed historical request, so the
    browser starts receiving the final qualifying list while the remaining symbols
    are still being checked.
    """
    global LIVE_RESULTS

    cache = load_cache()
    today = ist_now().date()
    today_key = today.isoformat()

    qualified = []
    pending = []

    for c in candidates:
        cached = cache.get(c["key"])
        if (isinstance(cached, dict)
                and cached.get("date") == today_key
                and cached.get("avg_turnover") is not None):
            avg = float(cached["avg_turnover"])
            c["avg_turnover"] = avg
            if avg >= MIN_AVG_TURNOVER:
                qualified.append(c)
        else:
            pending.append(c)

    log(f"Liquidity cache hits: {len(candidates) - len(pending)}")
    log(f"Historical liquidity requests needed: {len(pending)}")

    def publish_partial():
        """Refresh the browser-visible results without waiting for all requests."""
        nonlocal qualified
        global LIVE_RESULTS
        try:
            apply_strength_score(qualified)
            LIVE_RESULTS = [format_result(x) for x in qualified]
        except Exception as e:
            log(f"Partial result publish error: {repr(e)}")

    # Cached qualifying candidates can be shown immediately.
    publish_partial()

    def worker(c):
        try:
            return c["key"], historical_20day_turnover(c["key"], today), None
        except Exception as e:
            return c["key"], None, repr(e)

    if pending:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = [ex.submit(worker, c) for c in pending]
            pending_map = {c["key"]: c for c in pending}

            completed = 0
            for f in as_completed(futures):
                completed += 1
                key, value, error = f.result()
                if error or value is None:
                    log(f"Liquidity request failed [{completed}/{len(pending)}] {key}: {error}")
                    continue

                cache[key] = {"date": today_key, "avg_turnover": value}
                c = pending_map.get(key)
                if c is not None:
                    c["avg_turnover"] = value
                    if value >= MIN_AVG_TURNOVER:
                        qualified.append(c)

                # IMPORTANT: publish immediately. The UI sees new qualifying shares
                # every time one historical request finishes.
                publish_partial()
                if completed % 10 == 0 or completed == len(pending):
                    log(f"Liquidity progress: {completed}/{len(pending)} | qualifying={len(qualified)}")

    save_cache(cache)
    publish_partial()
    return qualified


def apply_strength_score(items):
    if not items:
        return items

    for x in items:
        x["gap_score"] = max(0.0, 100.0 * (1.0 - x["gap"] / MAX_GAP))

    recovery_values = [x["recovery"] for x in items]
    gain_values = [x["gain"] for x in items]
    live_values = [x["live_turnover"] for x in items]
    avg_values = [x.get("avg_turnover", 0.0) for x in items]

    for x in items:
        x["recovery_score"] = percentile_score(recovery_values, x["recovery"])
        x["gain_score"] = percentile_score(gain_values, x["gain"])
        x["live_score"] = percentile_score(live_values, x["live_turnover"])
        x["avg_score"] = percentile_score(avg_values, x.get("avg_turnover", 0.0))

        x["strength"] = (
            x["gap_score"] * 0.30
            + x["recovery_score"] * 0.25
            + x["gain_score"] * 0.15
            + x["live_score"] * 0.20
            + x["avg_score"] * 0.10
        )

    items.sort(
        key=lambda x: (
            -x["strength"],
            x["gap"],
            -x["live_turnover"],
            -x.get("avg_turnover", 0),
        )
    )
    return items


def format_result(x):
    avg = float(x.get("avg_turnover", 0))
    live = float(x.get("live_turnover", 0))

    return {
        "symbol": x["symbol"],
        "price": round(x["price"], 2),
        "open": round(x["open"], 2),
        "low": round(x["low"], 2),
        "gap": round(x["gap"], 4),
        "recovery": round(x["recovery"], 2),
        "gain": round(x["gain"], 2),
        "strength": round(x["strength"], 1),
        "avg_turnover_cr": round(avg / 1_00_00_000.0, 2),
        "live_turnover_cr": round(live / 1_00_00_000.0, 2),
        "volume": int(x.get("volume", 0)),
        "today": x.get("today", ""),
    }


def perform_scan():
    global LAST_SCAN_TIME, LAST_SCAN_ERROR, LAST_SCAN_DATE, LIVE_RESULTS
    LAST_SCAN_ERROR = ""

    LIVE_RESULTS = []
    log("STARTING OPEN-LOW STRENGTH SCAN")

    if not get_token():
        raise RuntimeError("UPSTOX_ACCESS_TOKEN is missing in Render Environment Variables.")

    load_instruments()
    candidates = live_candidates()
    log(f"Live candidates after price/open/gap filters: {len(candidates)}")

    if not candidates:
        LAST_SCAN_TIME = ist_now().strftime("%Y-%m-%d %H:%M:%S")
        LAST_SCAN_DATE = ist_now().date().isoformat()
        return []

    qualified = add_liquidity(candidates)
    apply_strength_score(qualified)

    results = [format_result(x) for x in qualified]
    LAST_SCAN_TIME = ist_now().strftime("%Y-%m-%d %H:%M:%S")
    LAST_SCAN_DATE = ist_now().date().isoformat()
    log(f"FINAL QUALIFYING STOCKS: {len(results)}")
    return results


def start_scan():
    global SCAN_RUNNING, LIVE_RESULTS, LAST_SCAN_ERROR

    with SCAN_LOCK:
        if SCAN_RUNNING:
            return False
        SCAN_RUNNING = True

    def runner():
        global SCAN_RUNNING, LIVE_RESULTS, LAST_SCAN_ERROR
        try:
            LIVE_RESULTS = perform_scan()
        except Exception as e:
            LAST_SCAN_ERROR = repr(e)
            log(f"SCAN ERROR: {repr(e)}")
        finally:
            with SCAN_LOCK:
                SCAN_RUNNING = False

    threading.Thread(target=runner, daemon=True).start()
    return True


# ------------------------------------------------------------
# API
# ------------------------------------------------------------

@app.get("/")
def home():
    return send_from_directory(".", "index.html")


@app.get("/api/health")
def health():
    return jsonify({
        "ok": True,
        "token_configured": bool(get_token()),
        "nse_eq_stocks": len(INSTRUMENTS),
        "scan_running": SCAN_RUNNING,
        "last_scan": LAST_SCAN_TIME,
        "last_error": LAST_SCAN_ERROR,
        "preopen_connected": PREOPEN_CONNECTED,
        "preopen_last_update": (
            datetime.fromtimestamp(PREOPEN_LAST_UPDATE, IST).strftime("%Y-%m-%d %H:%M:%S")
            if PREOPEN_LAST_UPDATE else None
        ),
        "mode": session_mode(),
        "updated_at": ist_now().strftime("%Y-%m-%d %H:%M:%S"),
    })


@app.get("/api/preopen")
def preopen_api():
    mode = session_mode()

    if mode != "preopen":
        return jsonify({
            "mode": "live",
            "connected": PREOPEN_CONNECTED,
            "results": [],
            "message": "Pre-Open Mode समाप्त हो चुका है। 9:15 के बाद Live Open-Low Scanner चलेगा।",
        })

    results = preopen_results(100)

    if not get_token():
        return jsonify({
            "mode": "preopen",
            "connected": False,
            "results": [],
            "message": "UPSTOX_ACCESS_TOKEN Render Environment Variables में नहीं मिला।",
        })

    if not PREOPEN_CONNECTED:
        return jsonify({
            "mode": "preopen",
            "connected": False,
            "results": results,
            "message": PREOPEN_ERROR or "Upstox Pre-Open feed connect हो रहा है...",
        })

    return jsonify({
        "mode": "preopen",
        "connected": True,
        "results": results,
        "message": (
            "Pre-Open IEP data live है। Order नहीं लगाया जा रहा है। "
            "9:15 पर यह mode अपने-आप Live Open-Low Scanner में बदल जाएगा।"
        ),
        "updated_at": (
            datetime.fromtimestamp(PREOPEN_LAST_UPDATE, IST).strftime("%Y-%m-%d %H:%M:%S")
            if PREOPEN_LAST_UPDATE else ist_now().strftime("%Y-%m-%d %H:%M:%S")
        ),
    })


@app.get("/api/scan")
def scan():
    global LIVE_RESULTS

    if session_mode() == "preopen":
        return jsonify({
            "mode": "preopen",
            "connected": PREOPEN_CONNECTED,
            "running": False,
            "finished": False,
            "results": preopen_results(100),
            "message": "Pre-Open Mode सक्रिय है। 9:15 पर Live Scanner अपने-आप शुरू होगा।",
            "updated_at": ist_now().strftime("%Y-%m-%d %H:%M:%S"),
        })

    if not get_token():
        return jsonify({
            "mode": "live",
            "connected": False,
            "running": False,
            "finished": True,
            "results": [],
            "message": "UPSTOX_ACCESS_TOKEN Render Environment Variables में नहीं मिला।",
        }), 500

    # At 9:15+ the existing live scanner starts automatically on the first
    # poll of the new trading day. Yesterday's results never block today's scan.
    today_key = ist_now().date().isoformat()
    if LAST_SCAN_DATE != today_key and not SCAN_RUNNING:
        LIVE_RESULTS = []
        start_scan()

    return jsonify({
        "mode": "live",
        "connected": True,
        "running": SCAN_RUNNING,
        "finished": (not SCAN_RUNNING and LAST_SCAN_TIME is not None),
        "scanned": len(INSTRUMENTS),
        "results": LIVE_RESULTS,
        "message": LAST_SCAN_ERROR or (
            "Strength scan चल रहा है..." if SCAN_RUNNING else "Scanner तैयार है।"
        ),
        "updated_at": LAST_SCAN_TIME or ist_now().strftime("%Y-%m-%d %H:%M:%S"),
    })


@app.get("/api/scan-now")
def scan_now():
    global LIVE_RESULTS, LAST_SCAN_ERROR

    if session_mode() == "preopen":
        return jsonify({
            "mode": "preopen",
            "connected": PREOPEN_CONNECTED,
            "running": False,
            "finished": False,
            "results": preopen_results(100),
            "message": "अभी Pre-Open Mode है। Live Open-Low Scan 9:15 के बाद चलेगा।",
        })

    if not get_token():
        return jsonify({
            "mode": "live",
            "connected": False,
            "running": False,
            "finished": True,
            "results": [],
            "message": "UPSTOX_ACCESS_TOKEN Render Environment Variables में नहीं मिला।",
        }), 500

    if SCAN_RUNNING:
        return jsonify({
            "mode": "live",
            "connected": True,
            "running": True,
            "finished": False,
            "results": LIVE_RESULTS,
            "message": "एक scan पहले से चल रहा है।",
        })

    LIVE_RESULTS = []
    LAST_SCAN_ERROR = ""
    start_scan()

    return jsonify({
        "mode": "live",
        "connected": True,
        "running": True,
        "finished": False,
        "results": [],
        "message": "Fresh Strength scan शुरू हो गया है...",
    })


# Start the WebSocket feed under Gunicorn too.
try:
    start_preopen_feed()
except Exception as e:
    PREOPEN_ERROR = repr(e)
    log(f"Pre-open thread could not start: {repr(e)}")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
