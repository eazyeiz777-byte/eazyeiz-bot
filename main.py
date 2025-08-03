# ============================================================
# SMC Scalper Bot – FINAL
# Fixed: safe quoteVolume, free-tier keep-alive
# ============================================================

import os
import csv
import time
import threading
import asyncio
import json
import logging
import requests
import numpy as np
import pandas as pd
import websockets
from datetime import datetime, timedelta, timezone
from flask import Flask, jsonify

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler()])

# ---------- CONFIG ----------
TIMEFRAMES = ["15m", "1h", "4h"]
ACCOUNT_EQUITY = float(os.getenv("ACCOUNT_EQUITY", 1000))
RISK_PER_TRADE = float(os.getenv("RISK_PER_TRADE", 0.01))
SIGNAL_CONF_THRESHOLD = float(os.getenv("SIGNAL_CONF_THRESHOLD", 0.5))
LOG_PATH = "signal_log.csv"

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# ---------- TOP-100 PAIRS (SAFE quoteVolume) ----------
class TopPairs:
    def __init__(self):
        self._pairs = []
        self._vol = {}
        self._last_sort = 0

    def current(self):
        return self._pairs[:]

    async def run(self):
        uri = "wss://fstream.binance.com/ws/!ticker@arr"
        while True:
            try:
                async with websockets.connect(uri) as ws:
                    async for msg in ws:
                        tickers = json.loads(msg)
                        if not isinstance(tickers, list):
                            continue
                        for t in tickers:
                            sym = t.get("s")
                            vol = t.get("quoteVolume")
                            if sym and sym.endswith("USDT") and vol is not None:
                                self._vol[sym] = float(vol)
                        now = time.time()
                        if now - self._last_sort >= 60:
                            self._pairs = sorted(self._vol, key=self._vol.get, reverse=True)[:100]
                            self._last_sort = now
                            logging.info("Top-100 updated: %d pairs", len(self._pairs))
            except Exception as e:
                logging.error("TopPairs WS error: %s", e)
                await asyncio.sleep(5)

top_pairs = TopPairs()

# ---------- OHLCV ----------
class OHLCV:
    def __init__(self, tfs):
        self.tfs = tfs
        self.store = {}

    def add_candle(self, pair, tf, k):
        if pair not in self.store:
            self.store[pair] = {}
        if tf not in self.store[pair]:
            self.store[pair][tf] = pd.DataFrame(columns=["time","open","high","low","close","volume"])
        row = {
            "time": k["t"],
            "open": float(k["o"]),
            "high": float(k["h"]),
            "low": float(k["l"]),
            "close": float(k["c"]),
            "volume": float(k["v"])
        }
        self.store[pair][tf] = pd.concat([self.store[pair][tf], pd.DataFrame([row])]).drop_duplicates("time").sort_values("time").reset_index(drop=True)

    async def run(self):
        while True:
            pairs = top_pairs.current()
            if not pairs:
                await asyncio.sleep(5)
                continue
            tasks = [self._conn(pair, tf) for pair in pairs for tf in self.tfs]
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _conn(self, pair, tf):
        uri = f"wss://fstream.binance.com/ws/{pair.lower()}@kline_{tf}"
        while True:
            try:
                async with websockets.connect(uri) as ws:
                    async for msg in ws:
                        data = json.loads(msg)
                        k = data.get("k")
                        if k and k.get("x"):
                            self.add_candle(pair, tf, k)
            except Exception as e:
                logging.error("OHLCV WS %s %s: %s", pair, tf, e)
                await asyncio.sleep(5)

ohlcv = OHLCV(TIMEFRAMES)

# ---------- TECH HELPERS ----------
def ema(s, n):
    return s.ewm(span=n, adjust=False).mean()

def atr(df, n=14):
    tr = pd.concat([df["high"] - df["low"],
                    (df["high"] - df["close"].shift()).abs(),
                    (df["low"] - df["close"].shift()).abs()], axis=1).max(axis=1)
    return tr.rolling(n).mean()

def adx(df, n=14):
    h, l, c = df["high"], df["low"], df["close"]
    plus = h.diff().clip(lower=0)
    minus = (-l.diff()).clip(lower=0)
    tr = pd.concat([h-l, (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)
    atr_ = tr.rolling(n).mean()
    plus_di = 100 * plus.rolling(n).mean() / atr_
    minus_di = 100 * minus.rolling(n).mean() / atr_
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di)
    return dx.rolling(n).mean().iloc[-1] if not dx.empty else 0

def trend_ok(df, direction):
    e200 = ema(df["close"], 200).iloc[-1]
    last = df["close"].iloc[-1]
    adx_val = adx(df)
    return (last > e200 and adx_val > 25) if direction == "long" else (last < e200 and adx_val > 25)

def liquidity_sweep(df, lb=5):
    highs = df["high"].rolling(lb).max().shift(1)
    lows = df["low"].rolling(lb).min().shift(1)
    return {"high": (df["high"] > highs) & (df["close"] < highs),
            "low": (df["low"] < lows) & (df["close"] > lows)}

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
    if fvg_b.iloc[idx]: score += 1
    if ob_b.iloc[idx]: score += 1
    if bos_up.iloc[idx] or ch_up.iloc[idx]: score += 1
    if fvg_s.iloc[idx]: score -= 1
    if ob_s.iloc[idx]: score -= 1
    if bos_down.iloc[idx] or ch_down.iloc[idx]: score -= 1
    return score

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

# ---------- 6. FILTERS ----------
EVENT_CACHE = {"last": None, "events": []}
def high_impact_events():
    now = datetime.now(timezone.utc)
    if EVENT_CACHE["last"] and (now - EVENT_CACHE["last"]).seconds < 3600:
        return EVENT_CACHE["events"]
    try:
        if not FINNHUB_API_KEY:
            return []
        r = requests.get(f"https://finnhub.io/api/v1/calendar/economic?token={FINNHUB_API_KEY}", timeout=10)
        evs = [datetime.strptime(e["date"], "%Y-%m-%d %H:%M:%S")
               for e in r.json().get("economicCalendar", [])
               if e.get("impact") in ("High", "Fed", "CPI")]
        EVENT_CACHE.update({"last": now, "events": evs})
    except:
        EVENT_CACHE.update({"last": now, "events": []})
    return EVENT_CACHE["events"]

def skip_event_window(min_b=15, min_a=15):
    events = high_impact_events()
    if not events:
        return False
    now = datetime.now(timezone.utc)
    return any(ev - timedelta(minutes=min_b) <= now <= ev + timedelta(minutes=min_a) for ev in events)

def check_news(symbol):
    try:
        if not FINNHUB_API_KEY:
            return False
        r = requests.get(f"https://finnhub.io/api/v1/news?category=crypto&token={FINNHUB_API_KEY}", timeout=10)
        news = r.json()
        symbol = symbol.upper()
        return any(symbol in (n.get("headline", "") + n.get("summary", "")).upper() for n in news)
    except:
        return False

def in_session():
    sessions = [(7, 0, 15, 0), (13, 0, 21, 0)]
    now = datetime.now(timezone.utc).time()
    return any(datetime.strptime(f"{s}:{m}", "%H:%M").time() <= now <= datetime.strptime(f"{e}:{n}", "%H:%M").time()
               for s, m, e, n in sessions)

# ---------- 7. LOGGING & TELEGRAM ----------
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
        logging.error("Log error: %s", e)

def send(msg):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            data={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown"},
            timeout=10
        )
    except Exception as e:
        logging.error("Telegram error: %s", e)

# ---------- 8. SCAN LOOP ----------
def scan():
    while True:
        try:
            pairs = top_pairs.current()
            for pair in pairs:
                for tf in TIMEFRAMES:
                    df = ohlcv.store.get(pair, {}).get(tf, pd.DataFrame())
                    if len(df) < 200:
                        continue

                    idx = len(df) - 1
                    atr_val = atr(df).iloc[-1]
                    close = df["close"].iloc[-1]

                    if skip_event_window():
                        continue
                    if check_news(pair.split("USDT")[0]):
                        continue
                    if not in_session():
                        continue

                    sweep = liquidity_sweep(df)
                    mom = momentum(df)

                    if sweep["low"].iloc[idx] and mom.iloc[idx]:
                        if not trend_ok(df, "long"):
                            continue
                        score = smc_score(df, idx)
                        if score <= 0:
                            continue
                        conf = min(0.99, 0.2 + 0.8 * (score / 3))
                        if conf < SIGNAL_CONF_THRESHOLD:
                            continue
                        stop, tp1, tp2, tp3 = tp_sl_signal(close, "long", df["low"].iloc[idx], atr_val, df)
                        size = int(np.floor((ACCOUNT_EQUITY * RISK_PER_TRADE) / abs(close - stop)))
                        log_signal([datetime.now(timezone.utc).isoformat(), pair, tf, "long", close, stop, tp1, tp2, tp3, conf, size, True, True, True])
                        send(f"🟢 LONG {pair} {tf}\nEntry: {close:.6f}\nStop: {stop:.6f}\nTP1/2/3: {tp1:.6f}/{tp2:.6f}/{tp3:.6f}\nConf: {conf:.0%}")

                    if sweep["high"].iloc[idx] and mom.iloc[idx]:
                        if not trend_ok(df, "short"):
                            continue
                        score = smc_score(df, idx)
                        if score >= 0:
                            continue
                        conf = min(0.99, 0.2 + 0.8 * (-score / 3))
                        if conf < SIGNAL_CONF_THRESHOLD:
                            continue
                        stop, tp1, tp2, tp3 = tp_sl_signal(close, "short", df["high"].iloc[idx], atr_val, df)
                        size = int(np.floor((ACCOUNT_EQUITY * RISK_PER_TRADE) / abs(close - stop)))
                        log_signal([datetime.now(timezone.utc).isoformat(), pair, tf, "short", close, stop, tp1, tp2, tp3, conf, size, True, True, True])
                        send(f"🔴 SHORT {pair} {tf}\nEntry: {close:.6f}\nStop: {stop:.6f}\nTP1/2/3: {tp1:.6f}/{tp2:.6f}/{tp3:.6f}\nConf: {conf:.0%}")
        except Exception as e:
            logging.error("Scan error: %s", e)
        time.sleep(60)

# ---------- 9. FLASK ----------
app = Flask(__name__)

@app.route("/")
def ok():
    return jsonify(status="running", pairs=len(top_pairs.current()))

@app.route("/health")
def health():
    return jsonify(status="healthy")

# ---------- 10. KEEP-ALIVE ----------
def keep_alive():
    """Self-ping every 5 min to prevent Render free-tier sleep"""
    while True:
        try:
            requests.get("http://localhost:5000/health", timeout=5)
        except Exception:
            pass
        time.sleep(300)

# ---------- 11. BOOT STRAP ----------
def main():
    logging.info("SMC Bot starting…")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    threading.Thread(target=lambda: loop.run_until_complete(asyncio.gather(top_pairs.run(), ohlcv.run())), daemon=True).start()
    threading.Thread(target=scan, daemon=True).start()
    threading.Thread(target=keep_alive, daemon=True).start()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)

if __name__ == "__main__":
    main()
        
