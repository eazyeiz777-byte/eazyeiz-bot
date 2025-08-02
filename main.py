import os
import sys
import csv
import time
import threading
import requests
import numpy as np
import pandas as pd
import logging
from datetime import datetime, timedelta, time as dt_time
from dotenv import load_dotenv
from flask import Flask, jsonify
from threading import Thread
from queue import Queue

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

# Environment Secrets
KUCOIN_BASE_URL = "https://api.kucoin.com"
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# User-Adjustable Config
TIMEFRAMES = ["15m", "1h", "4h"]
ACCOUNT_EQUITY = float(os.getenv("ACCOUNT_EQUITY", 1000))
RISK_PER_TRADE = float(os.getenv("RISK_PER_TRADE", 0.01))
SIGNAL_CONF_THRESHOLD = float(os.getenv("SIGNAL_CONF_THRESHOLD", 0.6))
LOG_PATH = "signal_log.csv"
TOP_N_PAIRS = 200  # Number of top pairs to consider

# Global variable to store top pairs
top_pairs = []

# Rate Limiter Class
class RateLimiter:
    def __init__(self, rate_limit_per_second):
        self.rate_limit_per_second = rate_limit_per_second
        self.tokens = self.rate_limit_per_second
        self.updated_at = time.time()

    def get_token(self):
        now = time.time()
        elapsed = now - self.updated_at
        self.tokens += elapsed * self.rate_limit_per_second
        if self.tokens > self.rate_limit_per_second:
            self.tokens = self.rate_limit_per_second
        self.updated_at = now
        if self.tokens < 1:
            time.sleep((1 - self.tokens) / self.rate_limit_per_second)
            self.tokens = 0
            self.get_token()
        self.tokens -= 1

# Fetch top trading pairs by volume from CoinGecko
def fetch_top_pairs_by_volume():
    try:
        response = requests.get(
            "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=volume_desc&per_page=200&page=1&sparkline=false",
            timeout=10
        )
        data = response.json()
        if isinstance(data, list):
            return [f"{coin['symbol'].upper()}-USDT" for coin in data[:TOP_N_PAIRS]]
        return []
    except Exception as e:
        logger.error(f"Error fetching top pairs by volume from CoinGecko: {e}")
        return []

def update_top_pairs():
    global top_pairs
    while True:
        top_pairs = fetch_top_pairs_by_volume()
        logger.info(f"Updated top {len(top_pairs)} pairs by volume")
        time.sleep(24 * 60 * 60)  # Refresh every 24 hours

# OHLCV fetch from KuCoin
def fetch_ohlcv(pair, tf="15m", limit=300):
    gran = {"15m": 900, "1h": 3600, "4h": 14400}[tf]
    try:
        r = requests.get(
            f"{KUCOIN_BASE_URL}/api/v1/market/candles",
            params={"symbol": pair, "type": f"{gran//60}min", "limit": limit},
            timeout=10
        ).json()
        if r.get("code") == "200000" and r.get("data"):
            df = pd.DataFrame(r["data"])
            if len(df) > 0:
                df.columns = ["time", "open", "close", "high", "low", "volume", "turnover"]
                return df.astype(float).sort_values("time").reset_index(drop=True)
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"OHLCV fetch for {pair}: {e}")
        return pd.DataFrame()

# Worker function for parallel processing
def worker(pair, tf, rate_limiter, result_queue):
    try:
        rate_limiter.get_token()
        df = fetch_ohlcv(pair, tf)
        result_queue.put((pair, tf, df))
    except Exception as e:
        logger.error(f"Error processing {pair} {tf}: {e}")

# Scan pairs with parallel processing and rate limiting
def scan_pairs_parallel(pairs, timeframes, max_workers=10, rate_limit_per_second=10):
    rate_limiter = RateLimiter(rate_limit_per_second)
    result_queue = Queue()
    threads = []

    for pair in pairs:
        for tf in timeframes:
            thread = Thread(target=worker, args=(pair, tf, rate_limiter, result_queue))
            thread.start()
            threads.append(thread)

            if len(threads) >= max_workers:
                for t in threads:
                    t.join()
                threads = []

    for t in threads:
        t.join()

    results = []
    while not result_queue.empty():
        results.append(result_queue.get())

    return results

# Economic-calendar filter
from datetime import timezone
EVENT_CACHE = {"last": None, "events": []}

def high_impact_events():
    now = datetime.now(timezone.utc)
    if EVENT_CACHE["last"] and (now - EVENT_CACHE["last"]).seconds < 3600:
        return EVENT_CACHE["events"]
    try:
        if not FINNHUB_API_KEY:
            logger.info("No Finnhub API key - economic calendar disabled")
            return []
        r = requests.get(
            f"https://finnhub.io/api/v1/calendar/economic?token={FINNHUB_API_KEY}",
            timeout=10
        )
        if r.status_code == 200:
            evs = [datetime.strptime(e["date"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                   for e in r.json().get("economicCalendar", [])
                   if e.get("impact") in ("High", "Fed", "CPI")]
            EVENT_CACHE.update({"last": now, "events": evs})
            logger.info(f"Loaded {len(evs)} high-impact economic events")
        else:
            logger.warning(f"Finnhub API error {r.status_code} - continuing without economic filter")
            EVENT_CACHE.update({"last": now, "events": []})
    except Exception as e:
        logger.warning(f"Economic calendar error: {e} - continuing without filter")
        EVENT_CACHE.update({"last": now, "events": []})
    return EVENT_CACHE["events"]

def skip_event_window(min_b=15, min_a=15):
    events = high_impact_events()
    if not events:
        return False
    now = datetime.now(timezone.utc)
    return any(ev - timedelta(minutes=min_b) <= now <= ev + timedelta(minutes=min_a)
               for ev in events)

# Trend filter
def ema(s, n):
    return s.ewm(span=n, adjust=False).mean()

def adx(df, n=14):
    h, l, c = df["high"], df["low"], df["close"]
    plus = h.diff().clip(lower=0)
    minus = (-l.diff()).clip(lower=0)
    tr = pd.concat([h - l, (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1).max(axis=1)
    atr = tr.rolling(n).mean()
    plus_di = 100 * plus.rolling(n).mean() / atr
    minus_di = 100 * minus.rolling(n).mean() / atr
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di)
    return dx.rolling(n).mean().iloc[-1] if not dx.empty else 0

def trend_ok(df, direction):
    e200 = ema(df["close"], 200).iloc[-1]
    last = df["close"].iloc[-1]
    adx_val = adx(df)
    return (last > e200 and adx_val > 25) if direction == "long" else (last < e200 and adx_val > 25)

# ATR
def atr(df, n=14):
    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - df["close"].shift()).abs(),
        (df["low"] - df["close"].shift()).abs()
    ], axis=1).max(axis=1)
    return tr.rolling(n).mean()

# SMC helpers
def liquidity_sweep(df, lb=5):
    highs = df["high"].rolling(lb).max().shift(1)
    lows = df["low"].rolling(lb).min().shift(1)
    return {
        "high": (df["high"] > highs) & (df["close"] < highs),
        "low": (df["low"] < lows) & (df["close"] > lows)
    }

def momentum(df):
    body = (df["close"] - df["open"]).abs()
    rng = (df["high"] - df["low"]).replace(0, 1e-9)
    body_ratio = body / rng
    vol_mult = df["volume"] / df["volume"].rolling(20).mean()
    return (body_ratio > 0.6) & (vol_mult > 1.5)

def smc_score(df, idx):
    score = 0
    fvg_b = df["low"].shift(2) > df["high"].shift(1)
    fvg_s = df["high"].shift(2) < df["low"].shift(1)
    ob_b = (df["close"].shift(1) < df["open"].shift(1)) & (df["close"] > df["open"])
    ob_s = (df["close"].shift(1) > df["open"].shift(1)) & (df["close"] < df["open"])
    prev_h = df["high"].rolling(5).max().shift(1)
    prev_l = df["low"].rolling(5).min().shift(1)
    bos_up = df["high"] > prev_h
    bos_down = df["low"] < prev_l
    ch_up = df["close"] > prev_h
    ch_down = df["close"] < prev_l
    if fvg_b.iloc[idx]:
        score += 1
    if ob_b.iloc[idx]:
        score += 1
    if bos_up.iloc[idx] or ch_up.iloc[idx]:
        score += 1
    if fvg_s.iloc[idx]:
        score -= 1
    if ob_s.iloc[idx]:
        score -= 1
    if bos_down.iloc[idx] or ch_down.iloc[idx]:
        score -= 1
    return score

# Smart static TP/SL
def tp_sl_signal(entry, direction, sweep_wick, atr_val, df):
    if direction == "long":
        stop = sweep_wick - 1.1 * atr_val
    else:
        stop = sweep_wick + 1.1 * atr_val
    swing_len = abs(df["high"].rolling(20).max().iloc[-1] -
                    df["low"].rolling(20).min().iloc[-1])
    swing_fracs = [0.35, 0.60, 0.85]
    atr_caps = [0.75, 1.25, 1.75]
    tps = []
    for sf, af in zip(swing_fracs, atr_caps):
        swing_tgt = entry + sf * swing_len * (1 if direction == "long" else -1)
        atr_tgt = entry + af * atr_val * (1 if direction == "long" else -1)
        tps.append(min(swing_tgt, atr_tgt) if direction == "long" else max(swing_tgt, atr_tgt))
    return stop, tps[0], tps[1], tps[2]

# News
def check_news(symbol):
    try:
        if not FINNHUB_API_KEY:
            return False
        r = requests.get(
            f"https://finnhub.io/api/v1/news?category=crypto&token={FINNHUB_API_KEY}",
            timeout=10
        )
        if r.status_code != 200:
            return False
        news = r.json()
        symbol = symbol.upper()
        return any(symbol in (n.get("headline", "") + n.get("summary", "")).upper() for n in news)
    except Exception as e:
        logger.error(f"News check for {symbol}: {e}")
        return False

# Session filter
def in_session():
    sessions = [(7, 0, 15, 0), (13, 0, 21, 0)]
    now = datetime.now(timezone.utc).time()
    return any(dt_time(s, m) <= now <= dt_time(e, n) for s, m, e, n in sessions)

def risky_time():
    now = datetime.now(timezone.utc).time()
    return dt_time(21, 0) <= now or now <= dt_time(7, 0)

# Logging
def log_signal(row):
    header = ["timestamp", "pair", "tf", "direction", "entry", "stop", "tp1", "tp2", "tp3",
              "confidence", "size", "ema_ok", "event_ok", "news_ok"]
    try:
        with open(LOG_PATH, "a", newline="") as f:
            writer = csv.writer(f)
            if f.tell() == 0:
                writer.writerow(header)
            writer.writerow(row)
    except Exception as e:
        logger.error(f"Log error: {e}")

# Telegram
def send(msg):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            data={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown"},
            timeout=10
        )
        logger.info(f"Signal sent: {msg[:50]}...")
    except Exception as e:
        logger.error(f"Telegram error: {e}")

# Main scan loop
def scan():
    logger.info("Starting market scanning...")
    while True:
        try:
            results = scan_pairs_parallel(top_pairs, TIMEFRAMES)
            for pair, tf, df in results:
                if df.empty or len(df) < 200:
                    logger.warning(f"Insufficient data for {pair} {tf}")
                    continue
                idx = len(df) - 1
                atr_series = atr(df)
                atr_val = atr_series.iloc[-1] if len(atr_series) > 0 else 0
                close = df["close"].iloc[-1]

                if skip_event_window():
                    logger.info(f"Skipping event window for {pair} {tf}")
                    continue

                news_flag = check_news(pair.split("USDT")[0])
                session_ok = in_session() and not risky_time()
                sweep = liquidity_sweep(df)
                mom = momentum(df)
                vol_ok = atr_val > 0.002 * close

                if not (session_ok and vol_ok):
                    logger.info(f"Session or volume condition not met for {pair} {tf}")
                    continue

                if sweep["low"].iloc[idx] and mom.iloc[idx]:
                    if not trend_ok(df, "long"):
                        logger.info(f"Trend condition not met for LONG {pair} {tf}")
                        continue
                    score = smc_score(df, idx)
                    if score <= 0:
                        logger.info(f"SMC score condition not met for LONG {pair} {tf}")
                        continue
                    conf = min(0.99, 0.2 + 0.8 * (score / 3))
                    if conf < SIGNAL_CONF_THRESHOLD:
                        logger.info(f"Confidence threshold not met for LONG {pair} {tf}")
                        continue
                    stop, tp1, tp2, tp3 = tp_sl_signal(close, "long", df["low"].iloc[idx], atr_val, df)
                    size = int(np.floor((ACCOUNT_EQUITY * RISK_PER_TRADE) / abs(close - stop)))
                    log_signal([datetime.now(timezone.utc).isoformat(), pair, tf, "long", close, stop, tp1, tp2, tp3,
                                conf, size, True, True, not news_flag])
                    send(f"🟢 *LONG* {pair} `{tf}`\n"
                         f"Entry: `{close:.6f}`\n"
                         f"Stop:  `{stop:.6f}`\n"
                         f"TP1/2/3: `{tp1:.6f}` / `{tp2:.6f}` / `{tp3:.6f}`\n"
                         f"Conf: `{conf * 100:.0f}%`\n"
                         f"Size: `{size}`")

                if sweep["high"].iloc[idx] and mom.iloc[idx]:
                    if not trend_ok(df, "short"):
                        logger.info(f"Trend condition not met for SHORT {pair} {tf}")
                        continue
                    score = smc_score(df, idx)
                    if score >= 0:
                        logger.info(f"SMC score condition not met for SHORT {pair} {tf}")
                        continue
                    conf = min(0.99, 0.2 + 0.8 * (-score / 3))
                    if conf < SIGNAL_CONF_THRESHOLD:
                        logger.info(f"Confidence threshold not met for SHORT {pair} {tf}")
                        continue
                    stop, tp1, tp2, tp3 = tp_sl_signal(close, "short", df["high"].iloc[idx], atr_val, df)
                    size = int(np.floor((ACCOUNT_EQUITY * RISK_PER_TRADE) / abs(close - stop)))
                    log_signal([datetime.now(timezone.utc).isoformat(), pair, tf, "short", close, stop, tp1, tp2, tp3,
                                conf, size, True, True, not news_flag])
                    send(f"🔴 *SHORT* {pair} `{tf}`\n"
                         f"Entry: `{close:.6f}`\n"
                         f"Stop:  `{stop:.6f}`\n"
                         f"TP1/2/3: `{tp1:.6f}` / `{tp2:.6f}` / `{tp3:.6f}`\n"
                         f"Conf: `{conf * 100:.0f}%`\n"
                         f"Size: `{size}`")

        except Exception as e:
            logger.error(f"Scan error: {e}")
        time.sleep(60)

# Flask web server for deployment
app = Flask(__name__)

@app.route("/")
def index():
    return jsonify({
        "status": "SMC Trading Bot - Active",
        "pairs_loaded": len(top_pairs),
        "timeframes": TIMEFRAMES,
        "confidence_threshold": SIGNAL_CONF_THRESHOLD,
        "uptime": datetime.now(timezone.utc).isoformat()
    })

@app.route("/health")
def health():
    return jsonify({"status": "healthy", "timestamp": datetime.now(timezone.utc).isoformat()})

@app.route("/stats")
def stats():
    try:
        with open(LOG_PATH, "r") as f:
            lines = len(f.readlines()) - 1
        return jsonify({"signals_generated": max(0, lines)})
    except Exception as e:
        logger.error(f"Error reading log file: {e}")
        return jsonify({"signals_generated": 0})

# Keep-alive function for free hosting platforms
def keep_alive():
    "Prevents free tier services from sleeping by self-pinging"
    while True:
        try:
            time.sleep(600)
            requests.get("http://localhost:5000/health", timeout=5)
        except Exception as e:
            logger.error(f"Keep-alive error: {e}")

# Application startup
if __name__ == "__main__":
    logger.info("SMC TRADING BOT - STARTING")
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.error("Missing Telegram credentials!")
        sys.exit(1)

    # Start the thread to update top pairs by volume
    threading.Thread(target=update_top_pairs, daemon=True).start()

    # Wait a bit to ensure top_pairs is populated
    time.sleep(5)

    if not top_pairs:
        logger.error("No trading pairs loaded!")
        sys.exit(1)

    logger.info(f"Bot configuration: Account Equity: ${ACCOUNT_EQUITY:,.2f}, Risk Per Trade: {RISK_PER_TRADE * 100:.1f}%, Signal Threshold: {SIGNAL_CONF_THRESHOLD * 100:.0f}%, Pairs Loaded: {len(top_pairs)}, Timeframes: {', '.join(TIMEFRAMES)}")
    threading.Thread(target=scan, daemon=True).start()
    threading.Thread(target=keep_alive, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=False)
