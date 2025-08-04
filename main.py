# ============================================================
# SMC Scalper – Fully Corrected & SUPER-THROTTLED
# ============================================================

import os
import time
import threading
import asyncio
import json
import logging
import requests
import numpy as np
import pandas as pd
import websockets
from datetime import datetime, timezone
from flask import Flask, jsonify

# ---------- LOGGING ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler()]
)

# ---------- CONFIG ----------
TIMEFRAMES = ["15m", "1h", "4h"]
ACCOUNT_EQUITY = float(os.getenv("ACCOUNT_EQUITY", 1000))
RISK_PER_TRADE = float(os.getenv("RISK_PER_TRADE", 0.01))
SIGNAL_CONF_THRESHOLD = float(os.getenv("SIGNAL_CONF_THRESHOLD", 0.5))
MAX_DF_LENGTH = 300

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

pairs_lock = threading.Lock()
ohlcv_lock = threading.Lock()

# ---------- TOP-100 PAIRS ----------
class TopPairs:
    def __init__(self):
        self._pairs = []
        self._vol = {}
        self._last_refresh = 0

    def get_current(self):
        with pairs_lock:
            return self._pairs[:]

    async def run(self):
        uri = "wss://fstream.binance.com/ws/!ticker@arr"
        while True:
            try:
                async with websockets.connect(uri) as ws:
                    logging.info("TopPairs WS connected.")
                    async for msg in ws:
                        tickers = json.loads(msg)
                        if not isinstance(tickers, list): continue
                        for t in tickers:
                            sym = t.get("s")
                            vol = t.get("q")
                            if sym and sym.endswith("USDT") and vol is not None:
                                self._vol[sym] = float(vol)

                        now = time.time()
                        if now - self._last_refresh >= 3600 or not self._pairs:
                            with pairs_lock:
                                self._pairs = sorted(self._vol, key=self._vol.get, reverse=True)[:20]
                            self._last_refresh = now
                            logging.info(f"Top-100 pairs refreshed -> {len(self._pairs)} pairs")
            except Exception as e:
                logging.warning(f"TopPairs WS error -> {e}")
                await asyncio.sleep(15)

top_pairs = TopPairs()

# ---------- OHLCV (SUPER-THROTTLED VERSION) ----------
class OHLCV:
    def __init__(self, tfs):
        self.tfs = tfs
        self.store = {}
        # --- FIX: Drastically reduce concurrency ---
        self.semaphore = asyncio.Semaphore(5) # Allow only 5 connections at a time

    def get_df(self, pair, tf):
        with ohlcv_lock:
            if pair in self.store and tf in self.store[pair]:
                return self.store[pair][tf].copy()
        return pd.DataFrame()

    def _add_candle(self, pair, tf, k):
        new_row = {
            "time": k["t"], "open": float(k["o"]), "high": float(k["h"]),
            "low": float(k["l"]), "close": float(k["c"]), "volume": float(k["v"])
        }
        with ohlcv_lock:
            if pair not in self.store: self.store[pair] = {}
            if tf not in self.store[pair]:
                self.store[pair][tf] = pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume"])
            df = self.store[pair][tf]
            df.loc[len(df)] = new_row
            df.drop_duplicates(subset="time", keep="last", inplace=True)
            df.sort_values("time", inplace=True)
            df.reset_index(drop=True, inplace=True)
            if len(df) > MAX_DF_LENGTH:
                self.store[pair][tf] = df.iloc[-MAX_DF_LENGTH:].reset_index(drop=True)
            else:
                self.store[pair][tf] = df

    async def run(self):
        active_connections = set()
        while True:
            pairs = top_pairs.get_current()
            if not pairs:
                await asyncio.sleep(5)
                continue
            
            new_conn_ids = []
            for pair in pairs:
                for tf in self.tfs:
                    conn_id = f"{pair}-{tf}"
                    if conn_id not in active_connections:
                        new_conn_ids.append((pair, tf, conn_id))

            if new_conn_ids:
                logging.info(f"Starting {len(new_conn_ids)} new OHLCV streams (super-throttled)...")
                for pair, tf, conn_id in new_conn_ids:
                    asyncio.create_task(self._conn(pair, tf))
                    active_connections.add(conn_id)
                    # --- FIX: Add a small sleep between each connection attempt ---
                    await asyncio.sleep(0.2) # Wait 200ms before starting the next one

            await asyncio.sleep(60)

    async def _conn(self, pair, tf):
        uri = f"wss://fstream.binance.com/ws/{pair.lower()}@kline_{tf}"
        async with self.semaphore:
            while True:
                try:
                    async with websockets.connect(uri) as ws:
                        async for msg in ws:
                            data = json.loads(msg)
                            k = data.get("k")
                            if k and k.get("x"): self._add_candle(pair, tf, k)
                except Exception as e:
                    logging.warning(f"OHLCV WS {pair} {tf} -> {e}")
                    # Increase wait time before retrying a failed connection
                    await asyncio.sleep(30)

ohlcv = OHLCV(TIMEFRAMES)

# ---------- TECH (No changes) ----------
def ema(s, n): return s.ewm(span=n, adjust=False).mean()
def atr(df, n=14):
    tr = pd.concat([(df["high"]-df["low"]), (df["high"]-df["close"].shift()).abs(), (df["low"]-df["close"].shift()).abs()], axis=1).max(axis=1)
    return tr.rolling(n).mean()
def adx(df, n=14):
    if len(df) < 2 * n: return 0
    h, l, c = df["high"], df["low"], df["close"]
    plus_dm = h.diff(); minus_dm = l.diff()
    plus_di = 100 * (plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0).rolling(n).sum() / atr(df, n))
    minus_di = 100 * (minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0).rolling(n).sum() / atr(df, n))
    with np.errstate(divide='ignore', invalid='ignore'):
        dx = 100 * (abs(plus_di - minus_di) / (plus_di + minus_di))
    return dx.rolling(n).mean().iloc[-1] if not dx.empty else 0
def trend_ok(df, direction):
    if len(df) < 200: return False
    e200 = ema(df["close"], 200).iloc[-1]; last = df["close"].iloc[-1]; adx_v = adx(df)
    return (last > e200 and adx_v > 25) if direction == "long" else (last < e200 and adx_v > 25)
def liquidity_sweep(df, lb=5):
    highs = df["high"].rolling(lb, min_periods=lb).max().shift(1); lows = df["low"].rolling(lb, min_periods=lb).min().shift(1)
    return {"high": (df["high"] > highs) & (df["close"] < highs), "low": (df["low"] < lows) & (df["close"] > lows)}
def momentum(df):
    body = (df["close"] - df["open"]).abs(); rng = (df["high"] - df["low"]).replace(0, 1e-9)
    body_ratio = body / rng; vol_mult = df["volume"] / df["volume"].rolling(20).mean()
    return (body_ratio > 0.6) & (vol_mult > 1.5)
def smc_score(df, idx):
    score = 0
    if len(df) < 10: return 0
    fvg_b = df["low"].iloc[idx-2] > df["high"].iloc[idx-1] if idx >= 2 else False
    fvg_s = df["high"].iloc[idx-2] < df["low"].iloc[idx-1] if idx >= 2 else False
    ob_b  = df["close"].iloc[idx-1] < df["open"].iloc[idx-1] and df["close"].iloc[idx] > df["open"].iloc[idx] if idx >= 1 else False
    ob_s  = df["close"].iloc[idx-1] > df["open"].iloc[idx-1] and df["close"].iloc[idx] < df["open"].iloc[idx] if idx >= 1 else False
    prev_h = df["high"].iloc[idx-5:idx].max(); prev_l = df["low"].iloc[idx-5:idx].min()
    bos_up = df["high"].iloc[idx] > prev_h; bos_down = df["low"].iloc[idx] < prev_l
    if fvg_b: score += 1;
    if ob_b: score += 1;
    if bos_up: score += 1
    if fvg_s: score -= 1;
    if ob_s: score -= 1;
    if bos_down: score -= 1
    return score
def tp_sl_signal(entry, direction, sweep_wick, atr_val, df):
    stop = (sweep_wick - 1.1 * atr_val) if direction == "long" else (sweep_wick + 1.1 * atr_val)
    tps = []
    for r_r in [1, 2, 3]:
        tp_dist = abs(entry - stop) * r_r
        tp = entry + tp_dist if direction == "long" else entry - tp_dist
        tps.append(tp)
    return stop, tps[0], tps[1], tps[2]

# ---------- TELEGRAM (No changes) ----------
def send_telegram(msg):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.warning("Telegram credentials not set. Skipping message.")
        return
    try: requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage", json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown"}, timeout=10)
    except Exception as e: logging.error(f"Telegram send error -> {e}")

# ---------- SCANNER & SIGNALING (No changes) ----------
def check_and_send_signal(pair, tf, df, direction):
    idx = len(df) - 1; sweep = liquidity_sweep(df); mom = momentum(df)
    is_sweep = sweep["low"].iloc[idx] if direction == "long" else sweep["high"].iloc[idx]
    if is_sweep and mom.iloc[idx]:
        if not trend_ok(df, direction): return
        score = smc_score(df, idx)
        is_score_valid = (score > 0) if direction == "long" else (score < 0)
        if not is_score_valid: return
        conf = min(0.99, 0.2 + 0.8 * (abs(score) / 3))
        if conf < SIGNAL_CONF_THRESHOLD: return
        close = df["close"].iloc[-1]; atr_val = atr(df).iloc[-1]; sweep_wick = df["low"].iloc[idx] if direction == "long" else df["high"].iloc[idx]
        stop, tp1, tp2, tp3 = tp_sl_signal(close, direction, sweep_wick, atr_val, df)
        if abs(close - stop) < 1e-9: return
        size = int(np.floor((ACCOUNT_EQUITY * RISK_PER_TRADE) / abs(close - stop)))
        if size == 0: return
        icon = "🟢" if direction == "long" else "🔴"
        msg = (f"{icon} *{direction.upper()}* {pair} ({tf})\n\n"
               f"**Entry:** `{close:.6f}`\n"
               f"**Stop Loss:** `{stop:.6f}`\n"
               f"**Take Profit 1:** `{tp1:.6f}`\n"
               f"**Take Profit 2:** `{tp2:.6f}`\n"
               f"**Take Profit 3:** `{tp3:.6f}`\n\n"
               f"*Confidence:* `{conf:.0%}` | *Position Size:* `{size}`")
        send_telegram(msg)
        logging.info(f"Signal sent for {pair} ({tf}) - {direction.upper()}")
        with ohlcv_lock:
            if pair not in ohlcv.store: ohlcv.store[pair] = {}
            if 'last_signal_time' not in ohlcv.store[pair]: ohlcv.store[pair]['last_signal_time'] = {}
            ohlcv.store[pair]['last_signal_time'][tf] = time.time()
def scan_worker():
    logging.info("Scan loop started.")
    time.sleep(20)
    while True:
        try:
            pairs = top_pairs.get_current()
            for pair in pairs:
                for tf in TIMEFRAMES:
                    df = ohlcv.get_df(pair, tf)
                    if len(df) < 200: continue
                    last_signal_time = ohlcv.store.get(pair, {}).get('last_signal_time', {}).get(tf, 0)
                    if time.time() - last_signal_time < 3600: continue
                    if not (7 <= datetime.now(timezone.utc).hour < 21): continue
                    check_and_send_signal(pair, tf, df, "long")
                    check_and_send_signal(pair, tf, df, "short")
        except Exception as e: logging.error(f"Scan tick error -> {e}", exc_info=True)
        time.sleep(60)

# ---------- FLASK & BOOTSTRAP (No changes) ----------
app = Flask(__name__)
@app.route("/")
def root(): return "SMC Scalper is running."
@app.route("/health")
def health():
    return jsonify({
        "status": "healthy", "timestamp": datetime.now(timezone.utc).isoformat(),
        "top_pairs_count": len(top_pairs.get_current()), "ohlcv_pairs_tracked": len(ohlcv.store)
    })
def main():
    logging.info("Bot bootstrapping...")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    async_tasks = asyncio.gather(top_pairs.run(), ohlcv.run())
    threading.Thread(target=lambda: loop.run_until_complete(async_tasks), daemon=True).start()
    threading.Thread(target=scan_worker, daemon=True).start()
    port = int(os.environ.get("PORT", 8080))
    logging.info(f"Starting Flask server on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False)

if __name__ == "__main__":
    main()
