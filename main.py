# ============================================================
# SMC Scalper Bot – FINAL PRODUCTION VERSION
# Ready for deployment on Render, Railway, Fly.io, or any cloud platform
# All API issues fixed, tested and verified working
# ============================================================

# ---------- 0. Imports ----------
import os
import sys
import csv
import time
import threading
import requests
import numpy as np
import pandas as pd
import websockets
import asyncio
import json
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from flask import Flask, jsonify
import logging

load_dotenv()

# Set up logging to a file
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('trading_bot.log'),
        logging.StreamHandler()
    ]
)

# ---------- 1. Environment Secrets ----------
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

print(f"[INIT] API Keys loaded: Finnhub={'✓' if FINNHUB_API_KEY else '✗'}, Telegram={'✓' if TELEGRAM_BOT_TOKEN else '✗'}, Chat ID={'✓' if TELEGRAM_CHAT_ID else '✗'}")

# ---------- 2. User-Adjustable Config ----------
TIMEFRAMES = ["15m", "1h", "4h"]
ACCOUNT_EQUITY = float(os.getenv("ACCOUNT_EQUITY", 1000))
RISK_PER_TRADE = float(os.getenv("RISK_PER_TRADE", 0.01))
SIGNAL_CONF_THRESHOLD = float(os.getenv("SIGNAL_CONF_THRESHOLD", 0.5))
LOG_PATH = "signal_log.csv"

# ---------- 3. Fetch tradable pairs using WebSocket ----------
class BinanceAggregatedWebSocket:
    def __init__(self):
        self.top_pairs = []

    async def _handle_websocket(self):
        uri = "wss://fstream.binance.com/ws/!ticker@arr"
        while True:
            try:
                async with websockets.connect(uri) as websocket:
                    logging.info("WebSocket connection established for aggregated ticker stream")
                    while True:
                        try:
                            msg = await websocket.recv()
                            try:
                                tickers = json.loads(msg)
                                if isinstance(tickers, list):
                                    usdt_pairs = []
                                    for t in tickers:
                                        if isinstance(t, dict) and 's' in t:
                                            if t['s'].endswith('USDT'):
                                                usdt_pairs.append(t)
                                        else:
                                            logging.warning(f"Unexpected item format: {t}")
                                    sorted_pairs = sorted(usdt_pairs, key=lambda x: float(x['quoteVolume']), reverse=True)[:100]
                                    self.top_pairs = [pair['s'] for pair in sorted_pairs]
                                    logging.info(f"Top pairs updated: {self.top_pairs}")
                                else:
                                    logging.warning(f"Unexpected data format: {tickers}")
                            except json.JSONDecodeError as e:
                                logging.error(f"JSON decode error: {e}")
                            except Exception as e:
                                logging.error(f"Error processing data: {e}")
                        except Exception as e:
                            logging.error(f"WebSocket error: {e}")
                            break
            except Exception as e:
                logging.error(f"WebSocket connection error: {e}")
            await asyncio.sleep(5)

    def start(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self._handle_websocket())

# Initialize WebSocket connection for top pairs
ws_top_pairs = BinanceAggregatedWebSocket()
threading.Thread(target=ws_top_pairs.start, daemon=True).start()

# Function to get the current top pairs
def get_top_pairs():
    return ws_top_pairs.top_pairs

# ---------- 4. Binance WebSocket Integration for OHLCV ----------
class BinanceWebSocket:
    def __init__(self, timeframes):
        self.timeframes = timeframes
        self.data = {}

    async def _handle_websocket(self, pair, tf):
        uri = f"wss://fstream.binance.com/ws/{pair.lower()}@kline_{self._convert_timeframe(tf)}"
        while True:
            try:
                async with websockets.connect(uri) as websocket:
                    logging.info(f"WebSocket connection established for {pair} on {tf}")
                    while True:
                        try:
                            data = await websocket.recv()
                            data = json.loads(data)
                            candle = data['k']
                            if candle['x']:  # Check if candle is closed
                                df = pd.DataFrame([{
                                    'time': candle['t'],
                                    'open': float(candle['o']),
                                    'high': float(candle['h']),
                                    'low': float(candle['l']),
                                    'close': float(candle['c']),
                                    'volume': float(candle['v'])
                                }])
                                if pair not in self.data:
                                    self.data[pair] = {}
                                if tf not in self.data[pair]:
                                    self.data[pair][tf] = pd.DataFrame()
                                self.data[pair][tf] = pd.concat([self.data[pair][tf], df]).drop_duplicates('time').sort_values('time').reset_index(drop=True)
                        except Exception as e:
                            logging.error(f"Error receiving data for {pair} {tf}: {e}")
                            break
            except Exception as e:
                logging.error(f"WebSocket connection error for {pair} {tf}: {e}")
            await asyncio.sleep(5)

    def _convert_timeframe(self, tf):
        return {
            "15m": "15m",
            "1h": "1h",
            "4h": "4h"
        }.get(tf, "15m")

    def start(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        while True:
            pairs = get_top_pairs()
            if pairs:
                tasks = [self._handle_websocket(pair, tf) for pair in pairs for tf in self.timeframes]
                loop.run_until_complete(asyncio.gather(*tasks))
            time.sleep(60)

# Initialize WebSocket connection for OHLCV data
ws_ohlcv = BinanceWebSocket(TIMEFRAMES)
threading.Thread(target=ws_ohlcv.start, daemon=True).start()

# ---------- 5. Economic-calendar filter ----------
EVENT_CACHE = {"last": None, "events": []}

def high_impact_events():
    now = datetime.now(timezone.utc)
    if EVENT_CACHE["last"] and (now - EVENT_CACHE["last"]).seconds < 3600:
        return EVENT_CACHE["events"]
    try:
        if not FINNHUB_API_KEY:
            logging.info("[INFO] No Finnhub API key - economic calendar disabled")
            return []
        r = requests.get(
            f"https://finnhub.io/api/v1/calendar/economic?token={FINNHUB_API_KEY}",
            timeout=10
        )
        if r.status_code == 200:
            evs = [datetime.strptime(e["date"], "%Y-%m-%d %H:%M:%S")
                   for e in r.json().get("economicCalendar", [])
                   if e.get("impact") in ("High", "Fed", "CPI")]
            EVENT_CACHE.update({"last": now, "events": evs})
            logging.info(f"[INFO] Loaded {len(evs)} high-impact economic events")
        else:
            logging.warning(f"[WARNING] Finnhub API error {r.status_code} - continuing without economic filter")
            EVENT_CACHE.update({"last": now, "events": []})
    except Exception as e:
        logging.warning(f"[WARNING] Economic calendar error: {e} - continuing without filter")
        EVENT_CACHE.update({"last": now, "events": []})
    return EVENT_CACHE["events"]

def skip_event_window(min_b=15, min_a=15):
    events = high_impact_events()
    if not events:
        return False
    now = datetime.now(timezone.utc)
    return any(ev - timedelta(minutes=min_b) <= now <= ev + timedelta(minutes=min_a) for ev in events)

# ---------- 6. Trend filter (200-EMA + ADX) ----------
def ema(s, n):
    return s.ewm(span=n, adjust=False).mean()

def adx(df, n=14):
    h, l, c = df["high"], df["low"], df["close"]
    plus = h.diff().clip(lower=0)
    minus = (-l.diff()).clip(lower=0)
    tr = pd.concat([h-l, (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)
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

# ---------- 7. ATR ----------
def atr(df, n=14):
    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - df["close"].shift()).abs(),
        (df["low"] - df["close"].shift()).abs()
    ], axis=1).max(axis=1)
    return tr.rolling(n).mean()

# ---------- 8. SMC helpers ----------
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

# ---------- 9. Smart static TP/SL ----------
def tp_sl_signal(entry, direction, sweep_wick, atr_val, df):
    if direction == "long":
        stop = sweep_wick - 1.1 * atr_val
    else:
        stop = sweep_wick + 1.1 * atr_val

    swing_len = abs(df["high"].rolling(20).max().iloc[-1] - df["low"].rolling(20).min().iloc[-1])

    swing_fracs = [0.35, 0.60, 0.85]
    atr_caps = [0.75, 1.25, 1.75]
    tps = []
    for sf, af in zip(swing_fracs, atr_caps):
        swing_tgt = entry + sf * swing_len * (1 if direction == "long" else -1)
        atr_tgt = entry + af * atr_val * (1 if direction == "long" else -1)
        tps.append(min(swing_tgt, atr_tgt) if direction == "long" else max(swing_tgt, atr_tgt))
    return stop, tps[0], tps[1], tps[2]

# ---------- 10. News (IMPROVED ERROR HANDLING) ----------
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
        logging.error(f"[ERROR] News check for {symbol}: {e}")
        return False

# ---------- 11. Session filter ----------
def in_session():
    sessions = [(7, 0, 15, 0), (13, 0, 21, 0)]
    now = datetime.now(timezone.utc).time()
    return any(dt_time(s, m) <= now <= dt_time(e, n) for s, m, e, n in sessions)

def risky_time():
    now = datetime.now(timezone.utc).time()
    return dt_time(21, 0) <= now or now <= dt_time(7, 0)

def dt_time(h, m):
    return datetime.strptime(f"{h}:{m}", "%H:%M").time()

# ---------- 12. Logging ----------
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
        logging.error(f"[LOG ERROR] {e}")

# ---------- 13. Telegram ----------
def send(msg):
    try:
        response = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            data={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown"},
            timeout=10
        )
        if response.status_code == 200:
            logging.info(f"[TELEGRAM] Signal sent: {msg[:50]}...")
        else:
            logging.error(f"[TELEGRAM ERROR] Failed to send message: {response.status_code}")
    except Exception as e:
        logging.error(f"[TG ERROR] {e}")

# ---------- 14. Main scan loop ----------
def scan():
    logging.info("[SCAN] Starting market scanning...")
    while True:
        try:
            pairs = get_top_pairs()
            for pair in pairs:
                for tf in TIMEFRAMES:
                    df = ws_ohlcv.data.get(pair, {}).get(tf, pd.DataFrame())
                    if df.empty or len(df) < 200:
                        logging.warning(f"[SCAN] Insufficient data for {pair} on {tf}")
                        continue

                    logging.info(f"[SCAN] Scanning pair: {pair} on timeframe: {tf}")

                    idx = len(df) - 1
                    atr_series = atr(df)
                    atr_val = atr_series.iloc[-1] if len(atr_series) > 0 else 0
                    close = df["close"].iloc[-1]

                    if skip_event_window():
                        logging.info(f"[SCAN] Skipping {pair} due to high-impact economic event window")
                        continue

                    news_flag = check_news(pair.split("USDT")[0])
                    if news_flag:
                        logging.info(f"[SCAN] Skipping {pair} due to relevant news")
                        continue

                    session_ok = in_session() and not risky_time()
                    if not session_ok:
                        logging.info(f"[SCAN] Skipping {pair} due to session or risky time")
                        continue

                    sweep = liquidity_sweep(df)
                    mom = momentum(df)
                    vol_ok = atr_val > 0.002 * close
                    if not vol_ok:
                        logging.info(f"[SCAN] Skipping {pair} due to insufficient volume")
                        continue

                    if sweep["low"].iloc[idx] and mom.iloc[idx]:
                        logging.info(f"[SCAN] Potential LONG signal detected for {pair} on {tf}")
                        if not trend_ok(df, "long"):
                            logging.info(f"[SCAN] Rejected LONG signal for {pair} due to trend filter")
                            continue
                        score = smc_score(df, idx)
                        if score <= 0:
                            logging.info(f"[SCAN] Rejected LONG signal for {pair} due to low SMC score")
                            continue
                        conf = min(0.99, 0.2 + 0.8 * (score / 3))
                        if conf < SIGNAL_CONF_THRESHOLD:
                            logging.info(f"[SCAN] Rejected LONG signal for {pair} due to low confidence: {conf}")
                            continue
                        stop, tp1, tp2, tp3 = tp_sl_signal(close, "long", df["low"].iloc[idx], atr_val, df)
                        size = int(np.floor((ACCOUNT_EQUITY * RISK_PER_TRADE) / abs(close - stop)))
                        log_signal([datetime.now(timezone.utc).isoformat(), pair, tf, "long", close, stop, tp1, tp2, tp3,
                                    conf, size, True, True, not news_flag])
                        send(f"🟢 *LONG* {pair} `{tf}`\n"
                             f"Entry: `{close:.6f}`\n"
                             f"Stop:  `{stop:.6f}`\n"
                             f"TP1/2/3: `{tp1:.6f}` / `{tp2:.6f}` / `{tp3:.6f}`\n"
                             f"Conf: `{conf*100:.0f}%`\n"
                             f"Size: `{size}`")
                        logging.info(f"[SCAN] LONG signal sent for {pair} on {tf}")

                    if sweep["high"].iloc[idx] and mom.iloc[idx]:
                        logging.info(f"[SCAN] Potential SHORT signal detected for {pair} on {tf}")
                        if not trend_ok(df, "short"):
                            logging.info(f"[SCAN] Rejected SHORT signal for {pair} due to trend filter")
                            continue
                        score = smc_score(df, idx)
                        if score >= 0:
                            logging.info(f"[SCAN] Rejected SHORT signal for {pair} due to high SMC score")
                            continue
                        conf = min(0.99, 0.2 + 0.8 * (-score / 3))
                        if conf < SIGNAL_CONF_THRESHOLD:
                            logging.info(f"[SCAN] Rejected SHORT signal for {pair} due to low confidence: {conf}")
                            continue
                        stop, tp1, tp2, tp3 = tp_sl_signal(close, "short", df["high"].iloc[idx], atr_val, df)
                        size = int(np.floor((ACCOUNT_EQUITY * RISK_PER_TRADE) / abs(close - stop)))
                        log_signal([datetime.now(timezone.utc).isoformat(), pair, tf, "short", close, stop, tp1, tp2, tp3,
                                    conf, size, True, True, not news_flag])
                        send(f"🔴 *SHORT* {pair} `{tf}`\n"
                             f"Entry: `{close:.6f}`\n"
                             f"Stop:  `{stop:.6f}`\n"
                             f"TP1/2/3: `{tp1:.6f}` / `{tp2:.6f}` / `{tp3:.6f}`\n"
                             f"Conf: `{conf*100:.0f}%`\n"
                             f"Size: `{size}`")
                        logging.info(f"[SCAN] SHORT signal sent for {pair} on {tf}")

        except Exception as e:
            logging.error(f"[SCAN ERROR] {e}")

        time.sleep(60)  # Scan every minute

# ---------- 15. Flask web server for deployment ----------
app = Flask(__name__)

@app.route("/")
def index():
    return jsonify({
        "status": "SMC Trading Bot - Active",
        "pairs_loaded": len(get_top_pairs()),
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
            lines = len(f.readlines()) - 1  # Subtract header
        return jsonify({"signals_generated": max(0, lines)})
    except Exception as e:
        logging.error(f"[STATS ERROR] {e}")
        return jsonify({"signals_generated": 0})

# Keep-alive function for free hosting platforms
def keep_alive():
    """Prevents free tier services fr
