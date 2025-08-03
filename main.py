# ============================================================
# SMC Scalper Bot – Production-ready Render Edition
# - Fixed all malformed URLs
# - Controlled concurrency, no leaks
# - Bounded deque instead of exploding DataFrames
# - NaN / divide-by-zero hardening
# - Graceful SIGTERM shutdown
# ============================================================

import os
import csv
import time
import asyncio
import json
import logging
import signal
import threading
from datetime import datetime, timedelta, timezone
from collections import deque
from typing import Dict, Tuple

import aiohttp                ### FIX async http
import numpy as np
import pandas as pd
import websockets
from flask import Flask, jsonify

# ---------- LOGGING ----------
logging.basicConfig(
    level=logging.INFO,       ### less spam than DEBUG in prod
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ---------- CONFIG ----------
TIMEFRAMES = ["15m", "1h", "4h"]
ACCOUNT_EQUITY = float(os.getenv("ACCOUNT_EQUITY", 1000))
RISK_PER_TRADE = float(os.getenv("RISK_PER_TRADE", 0.01))
SIGNAL_CONF_THRESHOLD = float(os.getenv("SIGNAL_CONF_THRESHOLD", 0.5))
LOG_PATH = "signal_log.csv"

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# ---------- TOP-100 ----------
class TopPairs:
    def __init__(self):
        self._pairs: list = []
        self._vol: Dict[str, float] = {}
        self._last_sort = 0
        self._lock = threading.Lock()

    def current(self):
        with self._lock:
            return self._pairs[:]

    async def run(self):
        uri = "wss://fstream.binance.com/ws/!ticker@arr"
        while True:
            try:
                async with websockets.connect(uri) as ws:
                    async for msg in ws:
                        tickers = json.loads(msg)
                        for t in tickers:
                            sym, vol = t.get("s"), t.get("quoteVolume")
                            if sym and sym.endswith("USDT") and vol is not None:
                                self._vol[sym] = float(vol)
                        now = time.time()
                        if now - self._last_sort >= 60:
                            with self._lock:
                                self._pairs = sorted(
                                    self._vol, key=self._vol.get, reverse=True
                                )[:100]
                                self._last_sort = now
                            logging.info("Top-100 refreshed (%d pairs)", len(self._pairs))
            except Exception as e:
                logging.error("TopPairs WS error: %s", e)
                await asyncio.sleep(5)

top_pairs = TopPairs()

# ---------- OHLCV ----------
class OHLCV:
    def __init__(self, tfs):
        self.tfs = tfs
        self.store: Dict[Tuple[str, str], deque] = {}   ### FIX bounded deque
        self.tasks: Dict[Tuple[str, str], asyncio.Task] = {}  ### FIX task registry

    def _df(self, pair_tf: Tuple[str, str]) -> pd.DataFrame:
        """Return a DataFrame copy for indicators."""
        d = self.store.get(pair_tf, deque())
        return pd.DataFrame(list(d))

    def add_candle(self, pair_tf: Tuple[str, str], k):
        if pair_tf not in self.store:
            self.store[pair_tf] = deque(maxlen=210)
        self.store[pair_tf].append(
            {
                "time": int(k["t"]),
                "open": float(k["o"]),
                "high": float(k["h"]),
                "low": float(k["l"]),
                "close": float(k["c"]),
                "volume": float(k["v"]),
            }
        )

    async def backfill(self, pair):
        async with aiohttp.ClientSession() as session:
            for tf in self.tfs:
                url = (
                    "https://fapi.binance.com/fapi/v1/klines"
                    f"?symbol={pair}&interval={tf}&limit=210"
                )
                try:
                    async with session.get(url, timeout=10) as r:
                        data = await r.json()
                        for c in data:
                            k = {
                                "t": c[0],
                                "o": c[1],
                                "h": c[2],
                                "l": c[3],
                                "c": c[4],
                                "v": c[5],
                            }
                            self.add_candle((pair, tf), k)
                        logging.info(
                            "Backfilled %s %s (%d bars)",
                            pair,
                            tf,
                            len(self.store[(pair, tf)]),
                        )
                except Exception as e:
                    logging.warning("Backfill %s %s failed: %s", pair, tf, e)

    async def run(self):
        while True:
            pairs = top_pairs.current()
            desired = {(p, tf) for p in pairs for tf in self.tfs}
            running = set(self.tasks.keys())
            # Cancel removed
            for key in running - desired:
                self.tasks.pop(key).cancel()
            # Add new
            for key in desired - running:
                p, tf = key
                self.tasks[key] = asyncio.create_task(self._conn(key))
            await asyncio.sleep(5)

    async def _conn(self, pair_tf: Tuple[str, str]):
        pair, tf = pair_tf
        uri = f"wss://fstream.binance.com/ws/{pair.lower()}@kline_{tf}"
        while True:
            try:
                async with websockets.connect(uri) as ws:
                    async for msg in ws:
                        data = json.loads(msg)
                        k = data.get("k")
                        if k and k.get("x"):
                            self.add_candle(pair_tf, k)
                            asyncio.create_task(
                                process_signal(pair, tf, self._df(pair_tf))
                            )
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error("OHLCV WS %s %s: %s", pair, tf, e)
                await asyncio.sleep(1)

ohlcv = OHLCV(TIMEFRAMES)

# ---------- TECH HELPERS ----------
def ema(s, n):
    return s.ewm(span=n, adjust=False).mean()

def atr(df, n=14):
    high, low, close = df["high"], df["low"], df["close"]
    tr = pd.concat(
        [
            high - low,
            (high - close.shift()).abs(),
            (low - close.shift()).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.rolling(n).mean()

def adx(df, n=14):
    h, l, c = df["high"], df["low"], df["close"]
    plus, minus = h.diff().clip(lower=0), (-l.diff()).clip(lower=0)
    tr = pd.concat([h - l, (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1).max(
        axis=1
    )
    atr_ = tr.rolling(n).mean().replace(0, np.nan)
    plus_di = 100 * plus.rolling(n).mean() / atr_
    minus_di = 100 * minus.rolling(n).mean() / atr_
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    return dx.rolling(n).mean().iloc[-1] if not dx.empty else 0

def trend_ok(df, direction):
    e200 = ema(df["close"], 200).iloc[-1]
    last = df["close"].iloc[-1]
    adx_val = adx(df)
    if direction == "long":
        return last > e200 and adx_val > 25
    return last < e200 and adx_val > 25

def liquidity_sweep(df, lb=5):
    highs = df["high"].rolling(lb).max().shift(1)
    lows = df["low"].rolling(lb).min().shift(1)
    return {
        "high": (df["high"] > highs) & (df["close"] < highs),
        "low": (df["low"] < lows) & (df["close"] > lows),
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

def tp_sl_signal(entry, direction, sweep_wick, atr_val, df):
    if direction == "long":
        stop = sweep_wick - 1.1 * atr_val
    else:
        stop = sweep_wick + 1.1 * atr_val
    swing_len = abs(
        df["high"].rolling(20).max().iloc[-1] - df["low"].rolling(20).min().iloc[-1]
    )
    swing_fracs = [0.35, 0.60, 0.85]
    atr_caps = [0.75, 1.25, 1.75]
    tps = []
    for sf, af in zip(swing_fracs, atr_caps):
        swing_tgt = entry + sf * swing_len * (1 if direction == "long" else -1)
        atr_tgt = entry + af * atr_val * (1 if direction == "long" else -1)
        tps.append(
            min(swing_tgt, atr_tgt) if direction == "long" else max(swing_tgt, atr_tgt)
        )
    return stop, tps[0], tps[1], tps[2]

# ---------- FILTERS ----------
EVENT_CACHE = {"last": None, "events": []}

async def high_impact_events():
    now = datetime.now(timezone.utc)
    if (
        EVENT_CACHE["last"]
        and (now - EVENT_CACHE["last"]).total_seconds() < 3600
    ):
        return EVENT_CACHE["events"]
    if not FINNHUB_API_KEY:
        EVENT_CACHE["events"] = []
        return EVENT_CACHE["events"]
    try:
        async with aiohttp.ClientSession() as session:
            url = f"https://finnhub.io/api/v1/calendar/economic?token={FINNHUB_API_KEY}"
            async with session.get(url, timeout=10) as r:
                data = await r.json()
        evs = [
            datetime.strptime(e["date"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            for e in data.get("economicCalendar", [])
            if e.get("impact") in ("High", "Fed", "CPI")
        ]
        EVENT_CACHE.update({"last": now, "events": evs})
    except Exception:
        EVENT_CACHE.update({"last": now, "events": []})
    return EVENT_CACHE["events"]

async def skip_event_window(min_b=15, min_a=15):
    events = await high_impact_events()
    if not events:
        return False
    now = datetime.now(timezone.utc)
    return any(
        ev - timedelta(minutes=min_b) <= now <= ev + timedelta(minutes=min_a)
        for ev in events
    )

async def check_news(symbol):
    if not FINNHUB_API_KEY:
        return False
    try:
        async with aiohttp.ClientSession() as session:
            url = f"https://finnhub.io/api/v1/news?category=crypto&token={FINNHUB_API_KEY}"
            async with session.get(url, timeout=10) as r:
                news = await r.json()
        needle = symbol.upper().replace("USDT", "")
        return any(
            needle in (n.get("headline", "") + n.get("summary", "")).upper()
            for n in news
        )
    except Exception:
        return False

def in_session():
    now = datetime.now(timezone.utc)
    weekday = now.weekday()
    if weekday > 4:  ### FIX weekend
        return False
    hhmm = now.hour * 60 + now.minute
    sessions = [(7 * 60, 15 * 60), (13 * 60, 21 * 60)]
    return any(s <= hhmm <= e for s, e in sessions)

# ---------- LOGGING & TELEGRAM ----------
LOG_LOCK = asyncio.Lock()

async def log_signal(row):
    header = [
        "timestamp",
        "pair",
        "tf",
        "direction",
        "entry",
        "stop",
        "tp1",
        "tp2",
        "tp3",
        "confidence",
        "size",
        "ema_ok",
        "event_ok",
        "news_ok",
    ]
    async with LOG_LOCK:
        try:
            with open(LOG_PATH, "a", newline="") as f:
                writer = csv.writer(f)
                if f.tell() == 0:
                    writer.writerow(header)
                writer.writerow(row)
        except Exception as e:
            logging.error("Log error: %s", e)

async def send(msg):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        async with aiohttp.ClientSession() as session:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            await session.post(
                url,
                data={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": msg,
                    "parse_mode": "Markdown",
                },
                timeout=10,
            )
    except Exception as e:
        logging.error("Telegram error: %s", e)

# ---------- SIGNAL PROCESSOR ----------
async def process_signal(pair, tf, df):
    if len(df) < 200:
        return
    idx = len(df) - 1
    atr_val = atr(df).iloc[-1] or 1e-8  ### FIX zero
    close = df["close"].iloc[-1]

    if await skip_event_window():
        return
    if await check_news(pair):
        return
    if not in_session():
        return

    sweep = liquidity_sweep(df)
    mom = momentum(df)

    # LONG
    if sweep["low"].iloc[idx] and mom.iloc[idx]:
        if not trend_ok(df, "long"):
            return
        score = smc_score(df, idx)
        if score <= 0:
            return
        conf = min(0.99, 0.2 + 0.8 * (score / 3))
        if conf < SIGNAL_CONF_THRESHOLD:
            return
        stop, tp1, tp2, tp3 = tp_sl_signal(close, "long", df["low"].iloc[idx], atr_val, df)
        risk = abs(close - stop) or 1e-8
        size = int(np.floor((ACCOUNT_EQUITY * RISK_PER_TRADE) / risk))
        await log_signal(
            [
                datetime.now(timezone.utc).isoformat(),
                pair,
                tf,
                "long",
                close,
                stop,
                tp1,
                tp2,
                tp3,
                conf,
                size,
                True,
                True,
                True,
            ]
        )
        await send(
            f"🟢 LONG {pair} {tf}\nEntry: {close:.6f}\nStop: {stop:.6f}\n"
            f"TP1/2/3: {tp1:.6f}/{tp2:.6f}/{tp3:.6f}\nConf: {conf:.0%}"
        )

    # SHORT
    if sweep["high"].iloc[idx] and mom.iloc[idx]:
        if not trend_ok(df, "short"):
            return
        score = smc_score(df, idx)
        if score >= 0:
            return
        conf = min(0.99, 0.2 + 0.8 * (-score / 3))
        if conf < SIGNAL_CONF_THRESHOLD:
            return
        stop, tp1, tp2, tp3 = tp_sl_signal(
            close, "short", df["high"].iloc[idx], atr_val, df
        )
        risk = abs(close - stop) or 1e-8
        size = int(np.floor((ACCOUNT_EQUITY * RISK_PER_TRADE) / risk))
        await log_signal(
            [
                datetime.now(timezone.utc).isoformat(),
                pair,
                tf,
                "short",
                close,
                stop,
                tp1,
                tp2,
                tp3,
                conf,
                size,
                True,
                True,
                True,
            ]
        )
        await send(
            f"🔴 SHORT {pair} {tf}\nEntry: {close:.6f}\nStop: {stop:.6f}\n"
            f"TP1/2/3: {tp1:.6f}/{tp2:.6f}/{tp3:.6f}\nConf: {conf:.0%}"
        )

# ---------- FLASK ----------
app = Flask(__name__)

@app.route("/")
def ok():
    return jsonify(status="running", pairs=len(top_pairs.current()))

@app.route("/health")
def health():
    return jsonify(status="healthy")

# ---------- BOOT ----------
async def async_main():
    logging.info("SMC Bot – Production Edition starting…")

    # backfill majors
    await ohlcv.backfill("BTCUSDT")
    await ohlcv.backfill("ETHUSDT")

    await asyncio.gather(top_pairs.run(), ohlcv.run())

def main():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, loop.stop)
    try:
        loop.run_until_complete(async_main())
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()

if __name__ == "__main__":
    # Run Flask in a thread so the main thread owns the asyncio loop
    threading.Thread(
        target=lambda: app.run(
            host="0.0.0.0", port=int(os.getenv("PORT", 5000)), debug=False
        ),
        daemon=True,
    ).start()
    main()
        
