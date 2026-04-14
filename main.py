"""
BloFin Signal Bot — 5 Signal Types
=====================================
Signals : Crime Watch, Pump Cooloff Retest, Entry Signal, Whale Scope, Drift Scope
Exchange : BloFin via CCXT
Alerts   : Discord webhook
Hosting  : Railway ready

Environment variables needed:
  BLOFIN_API_KEY
  BLOFIN_API_SECRET
  BLOFIN_PASSPHRASE
  DISCORD_WEBHOOK_URL
"""

import os
import time
import statistics
import requests
import ccxt
from datetime import datetime, timezone
from threading import Thread
from flask import Flask

# ──────────────────────────────────────────────
#  KEYS FROM ENVIRONMENT
# ──────────────────────────────────────────────
BLOFIN_API_KEY      = os.environ.get("BLOFIN_API_KEY")
BLOFIN_API_SECRET   = os.environ.get("BLOFIN_API_SECRET")
BLOFIN_PASSPHRASE   = os.environ.get("BLOFIN_PASSPHRASE")
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")

BYBIT_BASE_URL = "https://api.bybit.com"

# ──────────────────────────────────────────────
#  SIGNAL PARAMETERS
# ──────────────────────────────────────────────
SCAN_INTERVAL       = 60
MIN_VOLUME_USD      = 500_000
FUNDING_EXTREME     = 0.10
FUNDING_MODERATE    = 0.05
LS_HIGH             = 1.5
LS_LOW              = 0.7
DEPTH_OI_THIN_PCT   = 3.0
THIN_BOOK_USD       = 50_000
COIL_DAYS           = 5
COIL_RANGE_PCT      = 5.0
VOL_SPIKE_X         = 2.5
MIN_CRIME_SCORE     = 40
PUMP_1H_PCT         = 15.0
RETEST_DROP_PCT     = 20.0
RETEST_SCANS        = 5

COOLDOWN = {
    "crime":  3600,
    "retest": 1800,
    "entry":  900,
    "whale":  900,
    "drift":  3600,
}

# ──────────────────────────────────────────────
#  STARTUP CHECK
# ──────────────────────────────────────────────
def check_config():
    missing = []
    for name, val in [
        ("BLOFIN_API_KEY",      BLOFIN_API_KEY),
        ("BLOFIN_API_SECRET",   BLOFIN_API_SECRET),
        ("BLOFIN_PASSPHRASE",   BLOFIN_PASSPHRASE),
        ("DISCORD_WEBHOOK_URL", DISCORD_WEBHOOK_URL),
    ]:
        if not val:
            missing.append(name)
    if missing:
        print("❌ Missing environment variables:")
        for m in missing:
            print(f"   → {m}")
        raise SystemExit(1)
    print("✅ All credentials loaded")
    print("✅ 5 signal types active:")
    print("   1. Crime Watch")
    print("   2. Pump Cooloff Retest")
    print("   3. Entry Signal (VWAP + OI Delta)")
    print("   4. Whale Scope (Pump Detected)")
    print("   5. Drift Scope (Graded Trade Setup)")
    print("=" * 54)

# ──────────────────────────────────────────────
#  CONNECT TO BLOFIN
# ──────────────────────────────────────────────
def connect():
    return ccxt.blofin({
        "apiKey":          BLOFIN_API_KEY,
        "secret":          BLOFIN_API_SECRET,
        "password":        BLOFIN_PASSPHRASE,
        "enableRateLimit": True,
    })

# ──────────────────────────────────────────────
#  KEEP ALIVE
# ──────────────────────────────────────────────
def start_keep_alive():
    app = Flask("")

    @app.route("/")
    def home():
        return "BloFin Signal Bot is running ✅"

    Thread(target=lambda: app.run(host="0.0.0.0", port=8080), daemon=True).start()
    print("✅ Keep-alive server on port 8080")

# ──────────────────────────────────────────────
#  DISCORD
# ──────────────────────────────────────────────
def send_discord(msg, username="BloFin Signal Bot"):
    try:
        r = requests.post(DISCORD_WEBHOOK_URL, json={"content": msg, "username": username}, timeout=10)
        if r.status_code in (200, 204):
            print("  [✓] Discord alert sent")
        else:
            print(f"  [!] Discord error: {r.status_code}")
    except Exception as e:
        print(f"  [!] Discord failed: {e}")

# ──────────────────────────────────────────────
#  UTILITIES
# ──────────────────────────────────────────────
def safe_float(value, default=0.0):
    try:
        return float(value) if value is not None else default
    except Exception:
        return default

def get_ticker_volume_usd(ticker):
    """Best-effort 24h notional volume fallback chain for BloFin/CCXT."""
    info = ticker.get("info", {}) if isinstance(ticker, dict) else {}

    candidates = [
        ticker.get("quoteVolume"),
        info.get("quoteVolume"),
        info.get("quote_volume"),
        info.get("turnover24h"),
        info.get("turnover_24h"),
        info.get("vol24h"),
        info.get("vol_24h"),
        info.get("amount24h"),
        info.get("amount_24h"),
        info.get("volume24h"),
        info.get("volume_24h"),
    ]

    for value in candidates:
        num = safe_float(value, None)
        if num is not None and num > 0:
            return num

    base_vol = safe_float(ticker.get("baseVolume"), None)
    last = safe_float(ticker.get("last"), None)
    if base_vol is not None and last is not None and base_vol > 0 and last > 0:
        return base_vol * last

    return 0.0

def latest_bars(ohlcv, n):
    return ohlcv[-n:] if ohlcv and len(ohlcv) >= n else ohlcv

def fmt_usd(v):
    try:
        v = float(v)
        if v >= 1_000_000_000: return f"${v/1_000_000_000:.2f}B"
        elif v >= 1_000_000:   return f"${v/1_000_000:.1f}M"
        elif v >= 1_000:       return f"${v/1_000:.1f}K"
        return f"${v:.6f}"
    except:
        return "N/A"

def now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def chart_url(symbol):
    base = symbol.replace("/USDT:USDT", "").replace("/USDT", "")
    return f"https://www.tradingview.com/chart/?symbol=BLOFIN:{base}USDT"

def bubblemaps_url(symbol):
    base = symbol.replace("/USDT:USDT", "").replace("/USDT", "").lower()
    return f"https://app.bubblemaps.io/bsc/token/{base}"

def score_label(score):
    if score >= 70:   return "HIGH 🔴"
    elif score >= 40: return "MODERATE 🟡"
    else:             return "LOW 🟢"

def symbol_to_bybit(symbol):
    return symbol.replace("/USDT:USDT", "USDT").replace("/USDT", "USDT").replace("/", "").replace(":USDT", "")

def get_bybit_oi_snapshot(symbol, interval="5min"):
    try:
        bybit_symbol = symbol_to_bybit(symbol)
        url = f"{BYBIT_BASE_URL}/v5/market/open-interest"
        params = {
            "category": "linear",
            "symbol": bybit_symbol,
            "intervalTime": interval,
            "limit": 2,
        }
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        items = data.get("result", {}).get("list", [])
        if not items:
            return None

        oi_base = safe_float(items[0].get("openInterest"), None)
        if oi_base is None:
            return None
        return oi_base
    except Exception as e:
        print(f"[bybit oi error] {symbol} ({interval}): {e}")
        return None

def get_oi_with_fallback(exchange, symbol, price=None):
    oi = get_oi(exchange, symbol)
    if oi is not None and oi > 0:
        return oi, "BloFin"

    oi_base = get_bybit_oi_snapshot(symbol, interval="5min")
    px = price if price and price > 0 else None
    if oi_base is not None and px is not None:
        return oi_base * px, "Bybit fallback"

    return None, "Unavailable"

def get_bybit_oi_delta_pct(symbol, price, interval="5min"):
    try:
        bybit_symbol = symbol_to_bybit(symbol)
        url = f"{BYBIT_BASE_URL}/v5/market/open-interest"
        params = {
            "category": "linear",
            "symbol": bybit_symbol,
            "intervalTime": interval,
            "limit": 2,
        }
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        items = data.get("result", {}).get("list", [])
        if len(items) < 2:
            return None

        oi_now = safe_float(items[0].get("openInterest"), None)
        oi_prev = safe_float(items[1].get("openInterest"), None)
        if oi_now is None or oi_prev is None or oi_prev <= 0:
            return None

        return round(((oi_now - oi_prev) / oi_prev) * 100, 2)
    except Exception as e:
        print(f"[bybit oi delta error] {symbol} ({interval}): {e}")
        return None

def calc_vwap(ohlcv):
    try:
        total_pv = sum(((c[2]+c[3]+c[4])/3) * c[5] for c in ohlcv)
        total_v  = sum(c[5] for c in ohlcv)
        return total_pv / total_v if total_v > 0 else 0
    except:
        return 0

def calc_rvol(ohlcv, period=20):
    try:
        vols = [c[5] for c in ohlcv]
        if len(vols) < 2:
            return 0
        cur_vol = vols[-1]
        hist = vols[:-1][-period:]
        if not hist:
            return 0
        avg_vol = statistics.mean(hist)
        return round(cur_vol / avg_vol, 2) if avg_vol > 0 else 0
    except:
        return 0

def rvol_label(rvol):
    if rvol < 0.5:   return f"Low ({rvol}x avg)"
    elif rvol < 1.5: return f"Normal ({rvol}x avg)"
    else:             return f"High ({rvol}x avg) 🔥"

# ──────────────────────────────────────────────
#  DATA FETCHERS
# ──────────────────────────────────────────────
def get_funding(exchange, symbol):
    try:
        data = exchange.fetch_funding_rate(symbol)
        raw_rate = data.get("fundingRate")
        if raw_rate is None:
            return None
        rate = raw_rate * 100
        return round(rate / 8, 4)
    except Exception as e:
        print(f"[funding error] {symbol}: {e}")
        return None

def get_oi(exchange, symbol):
    try:
        data = exchange.fetch_open_interest(symbol)
        return safe_float(data.get("openInterestValue"), 0.0)
    except Exception as e:
        print(f"[oi error] {symbol}: {e}")
        return None

def get_ls(exchange, symbol):
    try:
        data = exchange.fetch_long_short_ratio_history(symbol, "1h", limit=1)
        if data:
            return round(data[-1].get("longShortRatio", 1.0), 2)
    except:
        pass
    try:
        book    = exchange.fetch_order_book(symbol, limit=50)
        bid_vol = sum(b[1] for b in book["bids"])
        ask_vol = sum(a[1] for a in book["asks"])
        return round(bid_vol / ask_vol, 2) if ask_vol > 0 else None
    except:
        return None

def get_orderbook(exchange, symbol):
    try:
        return exchange.fetch_order_book(symbol, limit=100)
    except:
        return None

def get_ohlcv(exchange, symbol, tf, limit=50):
    try:
        return exchange.fetch_ohlcv(symbol, timeframe=tf, limit=limit)
    except:
        return []

# ──────────────────────────────────────────────
#  SIGNAL 1 — CRIME WATCH
# ──────────────────────────────────────────────
def run_crime_watch(exchange, symbol, ticker, funding_hr, ls_ratio):
    try:
        score   = 0
        reasons = []
        price   = safe_float(ticker.get("last"))

        # funding
        if funding_hr is not None:
            abs_f = abs(funding_hr)
            if abs_f >= FUNDING_EXTREME:
                score += 25
                side = "shorts" if funding_hr < 0 else "longs"
                reasons.append(f"Funding {funding_hr:+.4f}%/hr — {side} paying extreme rate, forced close pressure building")
            elif abs_f >= FUNDING_MODERATE:
                score += 12
                reasons.append(f"Funding {funding_hr:+.4f}%/hr — elevated, watch for squeeze")

        # L/S ratio
        if ls_ratio:
            if ls_ratio >= LS_HIGH:
                score += 15
                reasons.append(f"L/S ratio {ls_ratio} — longs dominant, shorts still paying")
            elif ls_ratio <= LS_LOW:
                score += 10
                reasons.append(f"L/S ratio {ls_ratio} — shorts dominant, long squeeze risk")

        # order book thinness
        ob = get_orderbook(exchange, symbol)
        oi_usd, oi_source = get_oi_with_fallback(exchange, symbol, price)
        oi_usd = oi_usd or 0
        if ob and oi_usd > 0 and price > 0:
            dr       = price * 0.01
            bid_d    = sum(b[0]*b[1] for b in ob["bids"] if b[0] >= price - dr)
            ask_d    = sum(a[0]*a[1] for a in ob["asks"] if a[0] <= price + dr)
            depth_pct = ((bid_d + ask_d) / oi_usd) * 100
            if depth_pct <= DEPTH_OI_THIN_PCT:
                score += 20
                reasons.append(f"Thin order book: {depth_pct:.1f}% depth/OI — small capital moves price significantly")

        # coiling
        klines_1d = get_ohlcv(exchange, symbol, "1d", limit=25)
        coil_days = 0
        if klines_1d and len(klines_1d) >= 3:
            for c in klines_1d[:-1]:
                rng = ((c[2] - c[3]) / c[3]) * 100 if c[3] > 0 else 0
                if rng <= COIL_RANGE_PCT:
                    coil_days += 1
                else:
                    break
            if coil_days >= COIL_DAYS:
                score += 25
                reasons.append(f"Coiling for {coil_days} days — pressure building 🔴")
            elif coil_days >= 3:
                score += 10
                reasons.append(f"Coiling {coil_days} days — compression beginning")

        # volume spike
        klines_1h = get_ohlcv(exchange, symbol, "1h", limit=25)
        if klines_1h and len(klines_1h) >= 5:
            vols = [c[5] for c in klines_1h]
            cur_vol = vols[-1]
            hist = vols[:-1]
            avg_vol = statistics.mean(hist) if hist else 0
            ratio = cur_vol / avg_vol if avg_vol > 0 else 0
            if ratio >= VOL_SPIKE_X:
                score += 15
                reasons.append(f"⚡ Volume spike {ratio:.1f}x above average — price starting to move")

        if score < MIN_CRIME_SCORE:
            return False

        vol_24h  = get_ticker_volume_usd(ticker)
        change   = safe_float(ticker.get("percentage"))
        f_str    = f"{funding_hr:+.4f}%/hr" if funding_hr else "N/A"
        ls_str   = str(ls_ratio) if ls_ratio else "N/A"
        r_text   = "\n".join(f"• {r}" for r in reasons)

        msg = f"""🔮 **CRIME WATCH — {symbol.replace('/USDT:USDT','').replace('/USDT','')}USDT**
━━━━━━━━━━━━━━━━━━━━━━━━
Crime probability: **{score}/100 ({score_label(score)})**
Price: {fmt_usd(price)}
24h volume: {fmt_usd(vol_24h)}
Open interest: {fmt_usd(oi_usd)} ({oi_source})
Funding rate: {f_str}
L/S ratio: {ls_str}
24h change: {change:+.2f}%
━━━━━━━━━━━━━━━━━━━━━━━━
**Why flagged:**
{r_text}
━━━━━━━━━━━━━━━━━━━━━━━━
⏰ {now_utc()} UTC
📊 {chart_url(symbol)}
NFA · DYOR · Size accordingly"""

        send_discord(msg, "🔮 Crime Watch")
        return True

    except Exception as e:
        print(f"  [crime error] {symbol}: {e}")
        return False

# ──────────────────────────────────────────────
#  SIGNAL 2 — PUMP COOLOFF RETEST
# ──────────────────────────────────────────────
stable_scans = {}

def run_pump_retest(exchange, symbol, ticker, funding_hr):
    try:
        price = safe_float(ticker.get("last"))
        if price == 0: return False

        klines_1d = get_ohlcv(exchange, symbol, "1d", limit=30)
        if not klines_1d: return False

        peak     = max(c[2] for c in klines_1d)
        drop_pct = ((peak - price) / peak) * 100 if peak > 0 else 0

        if drop_pct < RETEST_DROP_PCT or not funding_hr or funding_hr >= 0:
            stable_scans[symbol] = 0
            return False

        stable_scans[symbol] = stable_scans.get(symbol, 0) + 1
        if stable_scans[symbol] < RETEST_SCANS:
            return False

        scans = stable_scans[symbol]
        if scans < 8:    stage, sl, sd = 1, "STARTING", "Farming starting — watch"
        elif scans < 15: stage, sl, sd = 2, "BUILDING", "Accumulation building"
        elif scans < 25: stage, sl, sd = 3, "ACTIVE",   "Active retest phase"
        elif scans < 35: stage, sl, sd = 4, "PEAK",     "Near peak pressure"
        else:            stage, sl, sd = 5, "COOLING",  "Cooling — watch for entry"

        next_f    = int((8*3600 - (time.time() % (8*3600))) / 60)
        vol_24h   = get_ticker_volume_usd(ticker)
        change    = safe_float(ticker.get("percentage"))
        oi_usd, oi_source = get_oi_with_fallback(exchange, symbol, price)
        oi_usd = oi_usd or 0

        msg = f"""🌀 **PUMP COOLOFF RETEST — {symbol.replace('/USDT:USDT','').replace('/USDT','')}USDT**
Score: {min(scans*3, 100)}/100
Peak price: {fmt_usd(peak)}
Current price: {fmt_usd(price)} ({drop_pct:.1f}% from peak)
24h change: {change:+.2f}%
24h vol: {fmt_usd(vol_24h)}
Open interest: {fmt_usd(oi_usd)} ({oi_source})
Funding rate: {funding_hr:+.4f}%/hr per settlement
Stage {stage}/5 ({sl}) — {sd}
Next funding: ~{next_f} min
Time: {datetime.now(timezone.utc).strftime("%H:%M")} UTC
━━━━━━━━━━━━━━━━━━━━━━━━
Setup:
• Pulled back {drop_pct:.1f}% from peak, stabilising for {scans} consecutive scans
• Funding still negative: {funding_hr:+.4f}%/hr — manufactured dip
• OI held during pullback — more squeeze fuel loaded
━━━━━━━━━━━━━━━━━━━━━━━━
📊 {chart_url(symbol)}
🫧 {bubblemaps_url(symbol)}
NFA · DYOR · Size accordingly"""

        send_discord(msg, "🌀 Pump Cooloff Retest")
        return True

    except Exception as e:
        print(f"  [retest error] {symbol}: {e}")
        return False

# ──────────────────────────────────────────────
#  SIGNAL 3 — ENTRY SIGNAL
# ──────────────────────────────────────────────
def run_entry_signal(exchange, symbol, ticker):
    try:
        price = safe_float(ticker.get("last"))
        if price == 0: return False

        klines_5m = get_ohlcv(exchange, symbol, "5m", limit=50)
        klines_1d = get_ohlcv(exchange, symbol, "1d", limit=2)
        if not klines_5m: return False

        vwap_15m  = calc_vwap(latest_bars(klines_5m, 3))
        vwap_day  = calc_vwap(klines_1d) if klines_1d else 0
        above_15m = price > vwap_15m if vwap_15m > 0 else None
        above_day = price > vwap_day  if vwap_day  > 0 else None
        if above_15m is None: return False

        vwap_15m_lbl = "ABOVE ✅" if above_15m else "BELOW"
        if above_15m and above_day:   vwap_day_lbl = "ABOVE ✅"
        elif not above_15m and not above_day: vwap_day_lbl = "BELOW"
        else:                         vwap_day_lbl = "⚠️ CONFLICTED"

        # OI delta
        oi_now, oi_source = get_oi_with_fallback(exchange, symbol, price)
        oi_delta = 0
        oi_label = "OI unavailable ⚠️"
        change   = safe_float(ticker.get("percentage"))

        if oi_now is not None and oi_now > 0:
            bybit_delta = get_bybit_oi_delta_pct(symbol, price, interval="5min")
            oi_delta = bybit_delta if bybit_delta is not None else 0
            if oi_delta > 0 and change < 0:
                oi_label = "INHALE DETECTED 🟢"
            elif oi_delta > 0:
                oi_label = "EXHALE"
            else:
                oi_label = "Declining"

        rvol     = calc_rvol(klines_5m)
        rv_label = rvol_label(rvol)

        # thin book
        ob         = get_orderbook(exchange, symbol)
        thin_book  = False
        book_depth = 0
        if ob:
            dr        = price * 0.01
            bid_d     = sum(b[0]*b[1] for b in ob["bids"] if b[0] >= price - dr)
            ask_d     = sum(a[0]*a[1] for a in ob["asks"] if a[0] <= price + dr)
            book_depth = bid_d + ask_d
            thin_book  = book_depth < THIN_BOOK_USD

        # state
        if not above_15m and (above_day is False) and rvol < 1.0:
            state, direction = "WATERFALL", "SHORT bias"
        elif above_15m and above_day and rvol >= 1.5:
            state, direction = "BREAKOUT", "LONG bias"
        elif oi_delta > 1.0 and "INHALE" in oi_label:
            state, direction = "INHALE", "LONG bias"
        elif oi_delta > 0.5 and not above_15m:
            state, direction = "SQUEEZE", "LONG bias"
        else:
            return False

        emoji     = "🔴" if "SHORT" in direction else "🟢"
        thin_line = f"\n⚠️ Thin Book (depth {fmt_usd(book_depth)} < {fmt_usd(THIN_BOOK_USD)})" if thin_book else ""

        msg = f"""{emoji} **ENTRY — {symbol.replace('/USDT:USDT','').replace('/USDT','')}USDT**
────────────────────────
📡 State      {state}
📈 Direction  {direction}
────────────────────────
💰 Price      {price:.6f}
📊 15m VWAP   {vwap_15m:.6f} [{vwap_15m_lbl}]
📐 Daily VWAP {vwap_day:.6f} [{vwap_day_lbl}]
📦 OI Delta   {oi_delta:+.2f}% [{oi_label}]
🧭 OI Source   {oi_source}
⚡ RVol        {rv_label}{thin_line}
────────────────────────
⏰ {now_utc()} UTC
📊 {chart_url(symbol)}
NFA · DYOR · Size accordingly"""

        send_discord(msg, "📡 Entry Signal")
        return True

    except Exception as e:
        print(f"  [entry error] {symbol}: {e}")
        return False

# ──────────────────────────────────────────────
#  SIGNAL 4 — WHALE SCOPE
# ──────────────────────────────────────────────
def run_whale_scope(exchange, symbol, ticker, funding_hr):
    try:
        klines_1h = get_ohlcv(exchange, symbol, "1h", limit=3)
        if not klines_1h or len(klines_1h) < 2: return False

        price_now = float(klines_1h[-1][4])
        price_1h  = float(klines_1h[-2][4])
        if price_1h == 0: return False

        change_1h = ((price_now - price_1h) / price_1h) * 100
        if change_1h < PUMP_1H_PCT: return False
        if not funding_hr or funding_hr >= 0: return False

        oi_usd, oi_source = get_oi_with_fallback(exchange, symbol, price_now)
        oi_usd = oi_usd or 0

        msg = f"""👀 **PUMP DETECTED — {symbol.replace('/USDT:USDT','').replace('/USDT','')}USDT**
────────────────────────
📈 Price +{change_1h:.1f}% in 1h
💰 Funding {funding_hr:+.4f}%/hr — extreme negative
📦 OI {fmt_usd(oi_usd)} ({oi_source})

⏳ Watching for dump → support → bounce
*Not an entry signal — do not chase*
⏰ {now_utc()} UTC
📊 {chart_url(symbol)}
NFA · DYOR · Size accordingly"""

        send_discord(msg, "👀 Whale Scope")
        return True

    except Exception as e:
        print(f"  [whale error] {symbol}: {e}")
        return False

# ──────────────────────────────────────────────
#  SIGNAL 5 — DRIFT SCOPE
# ──────────────────────────────────────────────
def run_drift_scope(exchange, symbol, ticker, funding_hr):
    try:
        price = safe_float(ticker.get("last"))
        if price == 0: return False

        klines_5m = get_ohlcv(exchange, symbol, "5m", limit=50)
        klines_1h = get_ohlcv(exchange, symbol, "1h", limit=50)
        if not klines_5m or not klines_1h: return False

        vwap_15m   = calc_vwap(latest_bars(klines_5m, 3))
        above_vwap = price > vwap_15m if vwap_15m > 0 else False
        rvol       = calc_rvol(klines_1h)

        oi_now, oi_source = get_oi_with_fallback(exchange, symbol, price)
        oi_5m_delta = get_bybit_oi_delta_pct(symbol, price, interval="5min")
        oi_1h_delta = get_bybit_oi_delta_pct(symbol, price, interval="1h")

        oi_available = oi_now is not None and oi_5m_delta is not None and oi_1h_delta is not None
        oi_rising = oi_available and oi_5m_delta > 0.3

        funding_ok = funding_hr is not None and funding_hr < -FUNDING_MODERATE

        conditions = [funding_ok, above_vwap, rvol >= 1.5]
        if oi_available:
            conditions.append(oi_rising)
        met        = sum(conditions)
        max_conditions = len(conditions)
        if met == max_conditions:
            grade = "A"
        elif met == max_conditions - 1 and funding_ok and above_vwap:
            grade = "B"
        else:
            return False

        if funding_ok and oi_rising and above_vwap: setup = "Short Squeeze Setup"
        elif above_vwap and rvol >= 1.5 and oi_rising: setup = "Breakout Setup"
        else: setup = "Momentum Setup"

        entry  = price
        sl     = round(entry * 0.848, 6)
        tp1    = round(entry * 1.15, 6)
        tp2    = round(entry * 1.30, 6)
        risk   = entry - sl
        rr1    = round((tp1 - entry) / risk, 2) if risk > 0 else 0
        rr2    = round((tp2 - entry) / risk, 2) if risk > 0 else 0

        change_1h = 0
        if len(klines_1h) >= 2:
            change_1h = ((float(klines_1h[-1][4]) - float(klines_1h[-2][4])) / float(klines_1h[-2][4])) * 100

        catalyst = "Move appears mechanical" if (abs(change_1h) > 10 and rvol < 0.5) else "No catalyst found — move appears organic"
        f_str    = f"{funding_hr:+.3f}%/hr (shorts paying)" if funding_hr else "N/A"
        if oi_1h_delta is None:
            oi_1h_s = "N/A"
        else:
            oi_1h_s = f"+{oi_1h_delta:.1f}%" if oi_1h_delta >= 0 else f"{oi_1h_delta:.1f}%"

        msg = f"""🟢 **OPEN LONG — {symbol.replace('/USDT:USDT','').replace('/USDT','')}USDT**
────────────────────────
📊 Grade {grade} | {setup}
────────────────────────
📈 Price     {change_1h:+.1f}% (1h)
💸 Funding   {f_str}
📦 OI 1h     holding {oi_1h_s}
📦 OI 5m     {f"{oi_5m_delta:+.2f}% (5m)" if oi_5m_delta is not None else "N/A"}
🧭 OI Source  {oi_source}
📐 VWAP 15m  ${vwap_15m:.6f} ({'above ✅' if above_vwap else 'below ⚠️'})
────────────────────────
🎯 Entry   ${entry:.6f}
🛡 SL      ${sl:.6f} (-15.2%)
✅ TP1     ${tp1:.6f} (+15%)  R/R 1:{rr1}
✅ TP2     ${tp2:.6f} (+30%)  R/R 1:{rr2}
────────────────────────
⚡ {catalyst}
────────────────────────
⏰ {now_utc()} UTC
📊 {chart_url(symbol)}
NFA · DYOR · Size accordingly"""

        send_discord(msg, "📊 Drift Scope")
        return True

    except Exception as e:
        print(f"  [drift error] {symbol}: {e}")
        return False

# ──────────────────────────────────────────────
#  MAIN LOOP
# ──────────────────────────────────────────────
def main():
    check_config()
    start_keep_alive()

    exchange   = connect()
    last_alert = {sig: {} for sig in COOLDOWN}

    print(f"\n[BOT] Scan every {SCAN_INTERVAL}s — watching BloFin futures")

    while True:
        try:
            now_str = datetime.now(timezone.utc).strftime("%H:%M:%S")
            print(f"\n[SCAN] {now_str} UTC — loading markets…")

            markets = exchange.load_markets()
            pairs   = [
                s for s in markets
                if s.endswith("/USDT:USDT")
                and markets[s].get("active", True)
                and markets[s].get("swap", False)
            ]
            print(f"[SCAN] {len(pairs)} active USDT perpetual pairs")

            alerts_sent = 0

            for symbol in pairs:
                try:
                    ticker = exchange.fetch_ticker(symbol)
                    vol = get_ticker_volume_usd(ticker)
                    print(
                        f"[debug] {symbol} "
                        f"last={ticker.get('last')} "
                        f"quoteVolume={ticker.get('quoteVolume')} "
                        f"baseVolume={ticker.get('baseVolume')} "
                        f"turnover24h={ticker.get('info', {}).get('turnover24h')} "
                        f"vol24h={ticker.get('info', {}).get('vol24h')} "
                        f"computedVol={vol} "
                        f"percentage={ticker.get('percentage')}"
                    )
                    price = safe_float(ticker.get("last"))

                    if price == 0 or vol < MIN_VOLUME_USD:
                        continue

                    now_ts     = time.time()
                    funding_hr = get_funding(exchange, symbol)
                    ls_ratio   = get_ls(exchange, symbol)

                    print(f"  {symbol:<30} funding={funding_hr}  ls={ls_ratio}")

                    # Crime Watch
                    if now_ts - last_alert["crime"].get(symbol, 0) > COOLDOWN["crime"]:
                        sent = run_crime_watch(exchange, symbol, ticker, funding_hr, ls_ratio)
                        if sent:
                            last_alert["crime"][symbol] = now_ts
                            alerts_sent += 1
                            time.sleep(2)

                    # Pump Cooloff Retest
                    if now_ts - last_alert["retest"].get(symbol, 0) > COOLDOWN["retest"]:
                        sent = run_pump_retest(exchange, symbol, ticker, funding_hr)
                        if sent:
                            last_alert["retest"][symbol] = now_ts
                            alerts_sent += 1

                    # Entry Signal
                    if now_ts - last_alert["entry"].get(symbol, 0) > COOLDOWN["entry"]:
                        sent = run_entry_signal(exchange, symbol, ticker)
                        if sent:
                            last_alert["entry"][symbol] = now_ts
                            alerts_sent += 1
                            time.sleep(1)

                    # Whale Scope
                    if now_ts - last_alert["whale"].get(symbol, 0) > COOLDOWN["whale"]:
                        sent = run_whale_scope(exchange, symbol, ticker, funding_hr)
                        if sent:
                            last_alert["whale"][symbol] = now_ts
                            alerts_sent += 1

                    # Drift Scope
                    if now_ts - last_alert["drift"].get(symbol, 0) > COOLDOWN["drift"]:
                        sent = run_drift_scope(exchange, symbol, ticker, funding_hr)
                        if sent:
                            last_alert["drift"][symbol] = now_ts
                            alerts_sent += 1
                            time.sleep(1)

                except Exception as e:
                    print(f"  [skip] {symbol}: {e}")
                    continue

            print(f"\n[DONE] {alerts_sent} alert(s) sent this scan")

        except Exception as e:
            print(f"[ERROR] {e}")

        print(f"[WAIT] Next scan in {SCAN_INTERVAL}s…")
        time.sleep(SCAN_INTERVAL)


if __name__ == "__main__":
    main()
