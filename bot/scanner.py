"""
Donchian Breakout scanner — 15m, parámetros fijos de estrategia.
RR 2.33:1 (SL=1.5*ATR, TP=3.5*ATR). Filtros: cuerpo vela, ATR mínimo,
no-chase, horario UTC, tendencia 1H.
"""
import logging
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import pandas_ta as ta
import ccxt.async_support as ccxt_async

from bot.config import settings

logger = logging.getLogger("scalping_bot.scanner")

# ── Universo de trading ───────────────────────────────────────────────────────
TOP_N    = 50
EXCLUDED = {
    "SHIB/USDT", "MATIC/USDT", "RNDR/USDT",
    # Commodities/forex — dinámicas distintas al crypto
    "XAU/USDT", "XAG/USDT", "CL/USDT", "BZ/USDT",
}

# ── Parámetros fijos de estrategia ────────────────────────────────────────────
LOOKBACK      = 30      # Donchian channel period (15m) — canal más largo, menos falsos breakouts
VOL_MULT      = 2.0     # volumen mínimo = 2x promedio — mayor confirmación
SL_MULT       = 1.5     # SL = 1.5x ATR
TP_MULT       = 3.5     # TP = 3.5x ATR  → ratio 2.33:1
MIN_BODY_PCT  = 0.35    # cuerpo vela ≥ 35% del rango total — filtra más mechas
# Filtro horario desactivado en paper trading — activar en live con set(range(7, 23))
ACTIVE_HOURS: set = set()
# Horas UTC bloqueadas (0% WR histórico): 04-06 apertura Asia silenciosa, 11 UTC spike
BLOCKED_HOURS: set = {int(h) for h in settings.ACTIVE_HOURS_BLOCK.split(",") if h.strip()} \
    if getattr(settings, "ACTIVE_HOURS_BLOCK", "") else set()

# ── Funding Rate filter ───────────────────────────────────────────────────────
FUNDING_LONG_BLOCK  =  0.001   # >+0.1%: longs pagan caro → no LONG
FUNDING_SHORT_BLOCK = -0.0005  # <-0.05%: shorts pagan caro → no SHORT

# ── TCP — Trend-Continuity Pullback (estrategia paralela) ────────────────────
TCP_SL_MULT  = 1.2   # SL = 1.2x ATR
TCP_TP_MULT  = 2.5   # TP = 2.5x ATR  → ratio ~2:1
TCP_ZONE_PCT = 0.003 # 0.3% tolerancia para "toca EMA20"

# ── Donchian 1H v2 ────────────────────────────────────────────────────────────
D1H_LOOKBACK = 30
D1H_VOL_MULT = 2.0
D1H_BODY_PCT = 0.35
D1H_SL_MULT  = 1.5
D1H_TP_MULT  = 3.5
D1H_TOP_N    = 50
D1H_MIN_WR   = 30.0   # backtest 1H: breakeven real ~33%, usamos 30% para incluir top 50


def _build_exchange() -> ccxt_async.binance:
    return ccxt_async.binance({
        "apiKey":          settings.BINANCE_API_KEY or None,
        "secret":          settings.BINANCE_API_SECRET or None,
        "options":         {"defaultType": "future"},
        "enableRateLimit": True,
        "timeout":         10000,
        "verbose":         False,
    })


async def _get_1h_trend(exchange: ccxt_async.binance, symbol: str) -> Optional[str]:
    """'up' si EMA20>EMA50 en 1H, 'down' si EMA20<EMA50."""
    try:
        ohlcv = await exchange.fetch_ohlcv(symbol, timeframe="1h", limit=60)
        if len(ohlcv) < 52:
            return None
        closes = pd.Series([c[4] for c in ohlcv])
        ema20  = float(closes.ewm(span=20, adjust=False).mean().iloc[-1])
        ema50  = float(closes.ewm(span=50, adjust=False).mean().iloc[-1])
        return "up" if ema20 > ema50 else "down"
    except Exception:
        return None


async def _get_regime_1h(exchange: ccxt_async.binance, symbol: str) -> str:
    """
    Detecta régimen de mercado en 1H usando Choppiness Index.
    Retorna 'lateral' si CHOP >= threshold, 'trending' si CHOP < threshold, 'unknown' si falla.
    CHOP(n) = 100 * log10(sum(TR,n) / (HH-LL,n)) / log10(n)
    """
    if not settings.REGIME_FILTER_ENABLED:
        return "trending"
    try:
        import math
        n = settings.REGIME_CHOP_LEN
        ohlcv = await exchange.fetch_ohlcv(symbol, timeframe="1h", limit=n + 5)
        if len(ohlcv) < n + 2:
            return "unknown"
        highs  = pd.Series([c[2] for c in ohlcv])
        lows   = pd.Series([c[3] for c in ohlcv])
        closes = pd.Series([c[4] for c in ohlcv])
        tr     = pd.concat([
            highs - lows,
            (highs - closes.shift()).abs(),
            (lows  - closes.shift()).abs(),
        ], axis=1).max(axis=1)
        atr_sum = float(tr.iloc[-n:].sum())
        hh      = float(highs.iloc[-n:].max())
        ll      = float(lows.iloc[-n:].min())
        if hh == ll or atr_sum <= 0:
            return "unknown"
        chop = 100 * math.log10(atr_sum / (hh - ll)) / math.log10(n)
        regime = "lateral" if chop >= settings.REGIME_CHOP_THRESHOLD else "trending"
        logger.debug(f"[REGIME] {symbol} CHOP1H={chop:.1f} → {regime}")
        return regime
    except Exception:
        return "unknown"


async def _get_funding_rate(exchange: ccxt_async.binance, symbol: str) -> Optional[float]:
    """Retorna el funding rate actual (ej: 0.001 = 0.1%). None si no disponible."""
    try:
        data = await exchange.fetch_funding_rate(symbol)
        return float(data["fundingRate"])
    except Exception:
        return None


async def scan_symbol(exchange: ccxt_async.binance, config: dict) -> Optional[dict]:
    """
    Retorna señal, señal bloqueada por tendencia, o None.
    Señal bloqueada lleva 'blocked_reason'. Señal válida no lo tiene.
    """
    symbol = config["symbol"]

    # Filtro horario: allow-list (vacío = todo habilitado) y block-list explícita
    now_hour = datetime.now(timezone.utc).hour
    if ACTIVE_HOURS and now_hour not in ACTIVE_HOURS:
        return None
    if BLOCKED_HOURS and now_hour in BLOCKED_HOURS:
        return None

    try:
        atrpct_len = settings.ATRPCT_FILTER_LEN if settings.ATRPCT_FILTER_ENABLED else 0
        fetch_limit = max(LOOKBACK + 20, atrpct_len + 20)
        ohlcv = await exchange.fetch_ohlcv(symbol, timeframe="15m", limit=fetch_limit)
        if len(ohlcv) < LOOKBACK + 2:
            return None

        df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])

        # ATR(14)
        df.ta.atr(length=14, append=True)
        atr_col = [c for c in df.columns if c.startswith("ATR")]
        if not atr_col or pd.isna(df[atr_col[0]].iloc[-1]):
            return None
        atr = float(df[atr_col[0]].iloc[-1])

        # ATR Percentile filter
        if settings.ATRPCT_FILTER_ENABLED:
            n = settings.ATRPCT_FILTER_LEN
            atr_series = df[atr_col[0]]
            atr_lo = atr_series.rolling(n).min()
            atr_hi = atr_series.rolling(n).max()
            atr_pct = (atr_series - atr_lo) / (atr_hi - atr_lo + 1e-10) * 100
            cur_pct = float(atr_pct.iloc[-2])
            if pd.isna(cur_pct) or cur_pct < settings.ATRPCT_FILTER_THRESHOLD:
                return None

        # ADX(14): confirma momentum direccional — sin ADX fuerte el breakout revierte
        df.ta.adx(length=14, append=True)
        adx_col = [c for c in df.columns if c.startswith("ADX_") and not c.startswith("ADX_D")]
        adx_val = float(df[adx_col[0]].iloc[-2]) if adx_col and not pd.isna(df[adx_col[0]].iloc[-2]) else 0.0

        # Donchian con shift(1): el canal no incluye la vela actual
        df["upper"]   = df["high"].shift(1).rolling(window=LOOKBACK).max()
        df["lower"]   = df["low"].shift(1).rolling(window=LOOKBACK).min()
        df["vol_avg"] = df["volume"].rolling(window=LOOKBACK).mean()

        # Última vela CERRADA (iloc[-2])
        last = df.iloc[-2]
        candle_ts  = str(int(last["timestamp"]))
        last_close = float(last["close"])
        last_open  = float(last["open"])
        last_high  = float(last["high"])
        last_low   = float(last["low"])
        last_vol   = float(last["volume"])
        upper      = float(last["upper"])
        lower      = float(last["lower"])
        vol_avg    = float(last["vol_avg"])

        if pd.isna(upper) or pd.isna(lower) or pd.isna(vol_avg) or vol_avg == 0:
            return None

        # Filtro de cuerpo de vela: elimina velas con mecha dominante
        candle_range = last_high - last_low
        candle_body  = abs(last_close - last_open)
        body_pct     = (candle_body / candle_range) if candle_range > 0 else 0
        body_ok      = body_pct >= MIN_BODY_PCT

        # Filtro volumen
        vol_ok = last_vol > vol_avg * VOL_MULT

        logger.debug(
            f"[SCAN] {symbol} vol_ok={vol_ok} body={body_pct*100:.0f}% "
            f"high>upper={last_high>upper} low<lower={last_low<lower} "
            f"green={last_close>last_open}"
        )

        # ── LONG: high supera DC upper, vela verde con cuerpo real, ADX confirma ─
        if last_high > upper and vol_ok and body_ok and last_close > last_open and adx_val >= settings.ADX_THRESHOLD:
            sl_price = round(last_close - atr * SL_MULT, 6)
            tp_price = round(last_close + atr * TP_MULT, 6)

            regime = await _get_regime_1h(exchange, symbol)
            if regime == "lateral":
                logger.info(f"[SCANNER] {symbol} LONG bloqueado: régimen lateral 1H")
                return {
                    "symbol": symbol, "side": "long", "price": last_close,
                    "sl_price": sl_price, "tp_price": tp_price,
                    "candle_ts": candle_ts, "blocked_reason": "regime_lateral",
                }
            trend = await _get_1h_trend(exchange, symbol)
            if trend == "down":
                logger.info(f"[SCANNER] {symbol} LONG bloqueado: 1H bajista")
                return {
                    "symbol": symbol, "side": "long", "price": last_close,
                    "sl_price": sl_price, "tp_price": tp_price,
                    "candle_ts": candle_ts, "blocked_reason": "trend_1h_bearish",
                }
            funding = await _get_funding_rate(exchange, symbol)
            if funding is not None and funding > FUNDING_LONG_BLOCK:
                logger.info(f"[SCANNER] {symbol} LONG bloqueado: funding={funding:.5f} > {FUNDING_LONG_BLOCK}")
                return {
                    "symbol": symbol, "side": "long", "price": last_close,
                    "sl_price": sl_price, "tp_price": tp_price,
                    "candle_ts": candle_ts, "blocked_reason": f"funding_high|{funding:.5f}",
                }
            logger.info(f"[SCANNER] ✅ {symbol} LONG entry={last_close} sl={sl_price} tp={tp_price} funding={funding}")
            return {
                "symbol":    symbol,
                "side":      "long",
                "price":     last_close,
                "sl_price":  sl_price,
                "tp_price":  tp_price,
                "candle_ts": candle_ts,
            }

        # ── SHORT: low cae bajo DC lower, vela roja con cuerpo real, ADX confirma ─
        elif last_low < lower and vol_ok and body_ok and last_close < last_open and adx_val >= settings.ADX_THRESHOLD:
            sl_price = round(last_close + atr * SL_MULT, 6)
            tp_price = round(last_close - atr * TP_MULT, 6)

            regime = await _get_regime_1h(exchange, symbol)
            if regime == "lateral":
                logger.info(f"[SCANNER] {symbol} SHORT bloqueado: régimen lateral 1H")
                return {
                    "symbol": symbol, "side": "short", "price": last_close,
                    "sl_price": sl_price, "tp_price": tp_price,
                    "candle_ts": candle_ts, "blocked_reason": "regime_lateral",
                }
            trend = await _get_1h_trend(exchange, symbol)
            if trend == "up":
                logger.info(f"[SCANNER] {symbol} SHORT bloqueado: 1H alcista")
                return {
                    "symbol": symbol, "side": "short", "price": last_close,
                    "sl_price": sl_price, "tp_price": tp_price,
                    "candle_ts": candle_ts, "blocked_reason": "trend_1h_bullish",
                }
            funding = await _get_funding_rate(exchange, symbol)
            if funding is not None and funding < FUNDING_SHORT_BLOCK:
                logger.info(f"[SCANNER] {symbol} SHORT bloqueado: funding={funding:.5f} < {FUNDING_SHORT_BLOCK}")
                return {
                    "symbol": symbol, "side": "short", "price": last_close,
                    "sl_price": sl_price, "tp_price": tp_price,
                    "candle_ts": candle_ts, "blocked_reason": f"funding_low|{funding:.5f}",
                }
            logger.info(f"[SCANNER] ✅ {symbol} SHORT entry={last_close} sl={sl_price} tp={tp_price} funding={funding}")
            return {
                "symbol":    symbol,
                "side":      "short",
                "price":     last_close,
                "sl_price":  sl_price,
                "tp_price":  tp_price,
                "candle_ts": candle_ts,
            }

        return None

    except Exception as e:
        logger.warning(f"[SCANNER] {symbol}: {e}")
        return None


async def scan_symbol_tcp(exchange: ccxt_async.binance, config: dict) -> Optional[dict]:
    """
    TCP — Trend-Continuity Pullback en 15m.
    LONG: EMA20>EMA50, low toca EMA20, cierra sobre EMA20, RSI 40-55, vela verde, volumen > prev 2.
    SHORT: EMA20<EMA50, high toca EMA20, cierra bajo EMA20, RSI 45-60, vela roja, volumen > prev 2.
    SL=1.2xATR, TP=2.5xATR (~2:1 ratio).
    """
    symbol = config["symbol"]
    now_hour = datetime.now(timezone.utc).hour
    if ACTIVE_HOURS and now_hour not in ACTIVE_HOURS:
        return None
    if BLOCKED_HOURS and now_hour in BLOCKED_HOURS:
        return None
    try:
        atrpct_len = settings.ATRPCT_FILTER_LEN if settings.ATRPCT_FILTER_ENABLED else 0
        fetch_limit = max(65, atrpct_len + 20)
        ohlcv = await exchange.fetch_ohlcv(symbol, timeframe="15m", limit=fetch_limit)
        if len(ohlcv) < 55:
            return None

        df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])

        df.ta.atr(length=14, append=True)
        atr_col = [c for c in df.columns if c.startswith("ATR")]
        if not atr_col or pd.isna(df[atr_col[0]].iloc[-1]):
            return None
        atr = float(df[atr_col[0]].iloc[-1])

        # ATR Percentile filter
        if settings.ATRPCT_FILTER_ENABLED:
            n = settings.ATRPCT_FILTER_LEN
            atr_series = df[atr_col[0]]
            atr_lo = atr_series.rolling(n).min()
            atr_hi = atr_series.rolling(n).max()
            atr_pct = (atr_series - atr_lo) / (atr_hi - atr_lo + 1e-10) * 100
            cur_pct = float(atr_pct.iloc[-2])
            if pd.isna(cur_pct) or cur_pct < settings.ATRPCT_FILTER_THRESHOLD:
                return None

        # ADX filter
        df.ta.adx(length=14, append=True)
        adx_col = [c for c in df.columns if c.startswith("ADX_") and not c.startswith("ADX_D")]
        adx_val = float(df[adx_col[0]].iloc[-2]) if adx_col and not pd.isna(df[adx_col[0]].iloc[-2]) else 0.0

        df.ta.rsi(length=14, append=True)
        rsi_col = [c for c in df.columns if c.startswith("RSI")]
        if not rsi_col:
            return None

        closes = df["close"]
        ema20 = float(closes.ewm(span=20, adjust=False).mean().iloc[-2])
        ema50 = float(closes.ewm(span=50, adjust=False).mean().iloc[-2])
        rsi   = float(df[rsi_col[0]].iloc[-2])

        last       = df.iloc[-2]
        candle_ts  = str(int(last["timestamp"]))
        last_close = float(last["close"])
        last_open  = float(last["open"])
        last_high  = float(last["high"])
        last_low   = float(last["low"])
        last_vol   = float(last["volume"])
        prev2_vol  = float(df["volume"].iloc[-4:-2].mean())
        vol_ok     = prev2_vol > 0 and last_vol > prev2_vol

        if pd.isna(rsi) or ema20 == 0 or ema50 == 0:
            return None

        # ── TCP LONG ──────────────────────────────────────────────────────────
        if ema20 > ema50:
            touched = last_low <= ema20 * (1 + TCP_ZONE_PCT) and last_close >= ema20 * (1 - TCP_ZONE_PCT)
            rsi_ok  = 40 <= rsi <= 55
            green   = last_close > last_open
            if touched and rsi_ok and green and vol_ok and adx_val >= settings.ADX_THRESHOLD:
                sl_price = round(last_close - atr * TCP_SL_MULT, 6)
                tp_price = round(last_close + atr * TCP_TP_MULT, 6)
                regime = await _get_regime_1h(exchange, symbol)
                if regime == "lateral":
                    logger.info(f"[TCP] {symbol} LONG bloqueado: régimen lateral 1H")
                    return {"symbol": symbol, "side": "long", "price": last_close,
                            "sl_price": sl_price, "tp_price": tp_price,
                            "candle_ts": candle_ts, "blocked_reason": "regime_lateral",
                            "strategy": "tcp"}
                trend = await _get_1h_trend(exchange, symbol)
                if trend == "down":
                    logger.info(f"[TCP] {symbol} LONG bloqueado: 1H bajista")
                    return {"symbol": symbol, "side": "long", "price": last_close,
                            "sl_price": sl_price, "tp_price": tp_price,
                            "candle_ts": candle_ts, "blocked_reason": "trend_1h_bearish",
                            "strategy": "tcp"}
                funding = await _get_funding_rate(exchange, symbol)
                if funding is not None and funding > FUNDING_LONG_BLOCK:
                    logger.info(f"[TCP] {symbol} LONG bloqueado: funding={funding:.5f}")
                    return {"symbol": symbol, "side": "long", "price": last_close,
                            "sl_price": sl_price, "tp_price": tp_price,
                            "candle_ts": candle_ts, "blocked_reason": f"funding_high|{funding:.5f}",
                            "strategy": "tcp"}
                logger.info(f"[TCP] ✅ {symbol} LONG pullback entry={last_close} ema20={ema20:.4f} rsi={rsi:.1f}")
                return {"symbol": symbol, "side": "long", "price": last_close,
                        "sl_price": sl_price, "tp_price": tp_price,
                        "candle_ts": candle_ts, "strategy": "tcp"}

        # ── TCP SHORT ─────────────────────────────────────────────────────────
        elif ema20 < ema50:
            touched = last_high >= ema20 * (1 - TCP_ZONE_PCT) and last_close <= ema20 * (1 + TCP_ZONE_PCT)
            rsi_ok  = 45 <= rsi <= 60
            red     = last_close < last_open
            if touched and rsi_ok and red and vol_ok and adx_val >= settings.ADX_THRESHOLD:
                sl_price = round(last_close + atr * TCP_SL_MULT, 6)
                tp_price = round(last_close - atr * TCP_TP_MULT, 6)
                regime = await _get_regime_1h(exchange, symbol)
                if regime == "lateral":
                    logger.info(f"[TCP] {symbol} SHORT bloqueado: régimen lateral 1H")
                    return {"symbol": symbol, "side": "short", "price": last_close,
                            "sl_price": sl_price, "tp_price": tp_price,
                            "candle_ts": candle_ts, "blocked_reason": "regime_lateral",
                            "strategy": "tcp"}
                trend = await _get_1h_trend(exchange, symbol)
                if trend == "up":
                    logger.info(f"[TCP] {symbol} SHORT bloqueado: 1H alcista")
                    return {"symbol": symbol, "side": "short", "price": last_close,
                            "sl_price": sl_price, "tp_price": tp_price,
                            "candle_ts": candle_ts, "blocked_reason": "trend_1h_bullish",
                            "strategy": "tcp"}
                funding = await _get_funding_rate(exchange, symbol)
                if funding is not None and funding < FUNDING_SHORT_BLOCK:
                    logger.info(f"[TCP] {symbol} SHORT bloqueado: funding={funding:.5f}")
                    return {"symbol": symbol, "side": "short", "price": last_close,
                            "sl_price": sl_price, "tp_price": tp_price,
                            "candle_ts": candle_ts, "blocked_reason": f"funding_low|{funding:.5f}",
                            "strategy": "tcp"}
                logger.info(f"[TCP] ✅ {symbol} SHORT pullback entry={last_close} ema20={ema20:.4f} rsi={rsi:.1f}")
                return {"symbol": symbol, "side": "short", "price": last_close,
                        "sl_price": sl_price, "tp_price": tp_price,
                        "candle_ts": candle_ts, "strategy": "tcp"}

        return None
    except Exception as e:
        logger.warning(f"[TCP] {symbol}: {e}")
        return None


# Símbolos 1H v2 — top 50 por retorno en backtest Donchian 1H (datos históricos 2024-2026)
SYMBOLS_1H_V2 = [
    "ORDI/USDT", "WIF/USDT",  "SEI/USDT",  "FTM/USDT",  "PEPE/USDT",
    "LINK/USDT", "STX/USDT",  "TIA/USDT",  "FET/USDT",  "FLOW/USDT",
    "IMX/USDT",  "DOGE/USDT", "GRT/USDT",  "DOT/USDT",  "EGLD/USDT",
    "WLD/USDT",  "ETH/USDT",  "ADA/USDT",  "AVAX/USDT", "ICP/USDT",
    "TRX/USDT",  "THETA/USDT","SOL/USDT",  "OP/USDT",   "NEAR/USDT",
    "ALGO/USDT", "INJ/USDT",  "GALA/USDT", "MATIC/USDT","FIL/USDT",
    "UNI/USDT",  "ARB/USDT",  "TRB/USDT",  "BTC/USDT",  "BNB/USDT",
    "XRP/USDT",  "ATOM/USDT", "LTC/USDT",  "BCH/USDT",  "AAVE/USDT",
    "SNX/USDT",  "MKR/USDT",  "RUNE/USDT", "APT/USDT",  "SUI/USDT",
    "BONK/USDT", "JUP/USDT",  "PENDLE/USDT","TON/USDT", "KAS/USDT",
]


def load_symbols_1h() -> list[dict]:
    """Fallback sync — en startup se usa load_top50_symbols() async."""
    return [{"symbol": s} for s in SYMBOLS_1H_V2]


async def load_top50_symbols() -> list[dict]:
    """Top 50 futuros perpetuos USDT de Binance, ordenados por volumen 24h."""
    exchange = _build_exchange()
    try:
        tickers = await exchange.fetch_tickers()
        rows = []
        for sym, t in tickers.items():
            # Binance Futures usa "BTC/USDT:USDT" — aceptamos ambos formatos
            if not (sym.endswith("/USDT:USDT") or sym.endswith("/USDT")):
                continue
            # Normalizar a "BTC/USDT" para compatibilidad con fetch_ohlcv
            normalized = sym.replace(":USDT", "")
            if normalized in EXCLUDED:
                continue
            qv = t.get("quoteVolume") or 0
            rows.append({"symbol": normalized, "quoteVolume": qv})
        rows.sort(key=lambda x: x["quoteVolume"], reverse=True)
        top50 = [{"symbol": r["symbol"]} for r in rows[:TOP_N]]
        logger.info(f"[SCANNER] {len(top50)} top símbolos cargados: {[s['symbol'] for s in top50[:5]]}...")
        return top50
    except Exception as e:
        logger.error(f"[SCANNER] Error cargando top50 de Binance: {e} — usando fallback")
        return [{"symbol": s} for s in SYMBOLS_1H_V2]
    finally:
        try:
            await exchange.close()
        except Exception:
            pass


async def scan_symbol_donchian_1h(exchange: ccxt_async.binance, config: dict) -> Optional[dict]:
    """
    Donchian Breakout en 1H — mismos parametros que 15m pero sobre velas horarias.
    Sin filtro de tendencia superior (backtest 1H fue rentable sin el).
    SL=1.5xATR, TP=3.5xATR, Vol>2x, Body>35%.
    """
    symbol = config["symbol"]
    try:
        ohlcv = await exchange.fetch_ohlcv(symbol, timeframe="1h", limit=D1H_LOOKBACK + 20)
        if len(ohlcv) < D1H_LOOKBACK + 2:
            return None

        df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df.ta.atr(length=14, append=True)
        atr_col = [c for c in df.columns if c.startswith("ATR")]
        if not atr_col or pd.isna(df[atr_col[0]].iloc[-1]):
            return None
        atr = float(df[atr_col[0]].iloc[-1])

        df["upper"]   = df["high"].shift(1).rolling(window=D1H_LOOKBACK).max()
        df["lower"]   = df["low"].shift(1).rolling(window=D1H_LOOKBACK).min()
        df["vol_avg"] = df["volume"].rolling(window=D1H_LOOKBACK).mean()

        last = df.iloc[-2]   # ultima vela 1H cerrada
        candle_ts  = str(int(last["timestamp"]))
        last_close = float(last["close"])
        last_open  = float(last["open"])
        last_high  = float(last["high"])
        last_low   = float(last["low"])
        last_vol   = float(last["volume"])
        upper      = float(last["upper"])
        lower      = float(last["lower"])
        vol_avg    = float(last["vol_avg"])

        if pd.isna(upper) or pd.isna(lower) or pd.isna(vol_avg) or vol_avg == 0:
            return None

        candle_range = last_high - last_low
        candle_body  = abs(last_close - last_open)
        body_ok = (candle_body / candle_range) >= D1H_BODY_PCT if candle_range > 0 else False
        vol_ok  = last_vol > vol_avg * D1H_VOL_MULT

        # LONG
        if last_high > upper and vol_ok and body_ok and last_close > last_open:
            sl_price = round(last_close - atr * D1H_SL_MULT, 6)
            tp_price = round(last_close + atr * D1H_TP_MULT, 6)
            funding  = await _get_funding_rate(exchange, symbol)
            if funding is not None and funding > FUNDING_LONG_BLOCK:
                return {"symbol": symbol, "side": "long", "price": last_close,
                        "sl_price": sl_price, "tp_price": tp_price,
                        "candle_ts": candle_ts, "blocked_reason": f"funding_high|{funding:.5f}",
                        "strategy": "donchian_1h"}
            logger.info(f"[1H] {symbol} LONG entry={last_close} sl={sl_price} tp={tp_price}")
            return {"symbol": symbol, "side": "long", "price": last_close,
                    "sl_price": sl_price, "tp_price": tp_price,
                    "candle_ts": candle_ts, "strategy": "donchian_1h"}

        # SHORT
        elif last_low < lower and vol_ok and body_ok and last_close < last_open:
            sl_price = round(last_close + atr * D1H_SL_MULT, 6)
            tp_price = round(last_close - atr * D1H_TP_MULT, 6)
            funding  = await _get_funding_rate(exchange, symbol)
            if funding is not None and funding < FUNDING_SHORT_BLOCK:
                return {"symbol": symbol, "side": "short", "price": last_close,
                        "sl_price": sl_price, "tp_price": tp_price,
                        "candle_ts": candle_ts, "blocked_reason": f"funding_low|{funding:.5f}",
                        "strategy": "donchian_1h"}
            logger.info(f"[1H] {symbol} SHORT entry={last_close} sl={sl_price} tp={tp_price}")
            return {"symbol": symbol, "side": "short", "price": last_close,
                    "sl_price": sl_price, "tp_price": tp_price,
                    "candle_ts": candle_ts, "strategy": "donchian_1h"}

        return None
    except Exception as e:
        logger.warning(f"[1H] {symbol}: {e}")
        return None


async def scan_all(symbols: list[dict], symbols_1h: list[dict] = None) -> tuple[list[dict], list[dict]]:
    """Corre Donchian 15m + TCP + Donchian 1H v2 en paralelo. Retorna (senales_validas, bloqueadas)."""
    import asyncio
    exchange = _build_exchange()
    signals: list[dict] = []
    blocked: list[dict] = []
    # 10 concurrent: bien dentro de límite Binance (1200 req/min). ccxt enableRateLimit también regula.
    sem = asyncio.Semaphore(10)

    async def _scan_one(config: dict, scanner, strat: str) -> None:
        async with sem:
            try:
                sig = await scanner(exchange, config)
            except Exception as e:
                logger.warning(f"[SCAN_ALL] {config.get('symbol')} {strat}: {e}")
                return
            if sig is None:
                return
            sig.setdefault("strategy", strat)
            if "blocked_reason" in sig:
                blocked.append(sig)
            else:
                signals.append(sig)

    try:
        tasks = []
        for config in symbols:
            tasks.append(_scan_one(config, scan_symbol, "donchian"))
            tasks.append(_scan_one(config, scan_symbol_tcp, "tcp"))
        for config in (symbols_1h or []):
            tasks.append(_scan_one(config, scan_symbol_donchian_1h, "donchian_1h"))

        await asyncio.gather(*tasks)
    finally:
        await exchange.close()

    return signals, blocked
