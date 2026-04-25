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

# ── Filtros de universo (CSV backtest) ────────────────────────────────────────
MIN_RETURN_PCT = 0.0    # cualquier retorno positivo
MIN_WIN_RATE   = 42.0   # con 2.33:1 RR el breakeven es 30% WR
MIN_TRADES     = 50
TOP_N          = 20
EXCLUDED       = {"SHIB/USDT", "MATIC/USDT", "RNDR/USDT"}

# ── Parámetros fijos de estrategia ────────────────────────────────────────────
LOOKBACK      = 20      # Donchian channel period (15m)
VOL_MULT      = 1.5     # volumen mínimo = 1.5x promedio
SL_MULT       = 1.5     # SL = 1.5x ATR
TP_MULT       = 3.5     # TP = 3.5x ATR  → ratio 2.33:1
MIN_BODY_PCT  = 0.25    # cuerpo vela ≥ 25% del rango total
# Filtro horario desactivado en paper trading — activar en live con set(range(7, 23))
ACTIVE_HOURS: set = set()

# ── Funding Rate filter ───────────────────────────────────────────────────────
FUNDING_LONG_BLOCK  =  0.001   # >+0.1%: longs pagan caro → no LONG
FUNDING_SHORT_BLOCK = -0.0005  # <-0.05%: shorts pagan caro → no SHORT


def load_symbols() -> list[dict]:
    try:
        df = pd.read_csv(settings.SYMBOLS_CSV)
        df = df[
            (df["return_pct"] > MIN_RETURN_PCT) &
            (df["win_rate"]   > MIN_WIN_RATE)   &
            (df["trades"]     >= MIN_TRADES)    &
            (~df["symbol"].isin(EXCLUDED))
        ]
        df = df.sort_values("return_pct", ascending=False).head(TOP_N)
        records = df.to_dict("records")
        logger.info(f"[SCANNER] {len(records)} símbolos cargados del CSV")
        return records
    except Exception as e:
        logger.error(f"[SCANNER] Error cargando CSV: {e}")
        return []


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
    """
    'up'   si EMA20>EMA50 Y precio>EMA200 en 1H  → habilita LONG
    'down' si EMA20<EMA50 Y precio<EMA200 en 1H  → habilita SHORT
    Caso mixto (corto vs macro en conflicto): bloquea la dirección del corto plazo.
    """
    try:
        ohlcv = await exchange.fetch_ohlcv(symbol, timeframe="1h", limit=210)
        if len(ohlcv) < 202:
            return None
        closes    = pd.Series([c[4] for c in ohlcv])
        last_close = float(closes.iloc[-1])
        ema20  = float(closes.ewm(span=20,  adjust=False).mean().iloc[-1])
        ema50  = float(closes.ewm(span=50,  adjust=False).mean().iloc[-1])
        ema200 = float(closes.ewm(span=200, adjust=False).mean().iloc[-1])
        short_up = ema20 > ema50
        macro_up = last_close > ema200
        if short_up and macro_up:
            return "up"
        if not short_up and not macro_up:
            return "down"
        # Conflicto corto/macro: bloquear la dirección del corto plazo
        return "down" if short_up else "up"
    except Exception:
        return None


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

    # Filtro horario: solo activo si ACTIVE_HOURS no está vacío
    if ACTIVE_HOURS and datetime.now(timezone.utc).hour not in ACTIVE_HOURS:
        return None

    try:
        ohlcv = await exchange.fetch_ohlcv(symbol, timeframe="15m", limit=LOOKBACK + 10)
        if len(ohlcv) < LOOKBACK + 2:
            return None

        df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])

        # ATR(14)
        df.ta.atr(length=14, append=True)
        atr_col = [c for c in df.columns if c.startswith("ATR")]
        if not atr_col or pd.isna(df[atr_col[0]].iloc[-1]):
            return None
        atr = float(df[atr_col[0]].iloc[-1])

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

        # ── LONG: high supera DC upper, vela verde con cuerpo real ───────────
        if last_high > upper and vol_ok and body_ok and last_close > last_open:
            sl_price = round(last_close - atr * SL_MULT, 6)
            tp_price = round(last_close + atr * TP_MULT, 6)

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

        # ── SHORT: low cae bajo DC lower, vela roja con cuerpo real ──────────
        elif last_low < lower and vol_ok and body_ok and last_close < last_open:
            sl_price = round(last_close + atr * SL_MULT, 6)
            tp_price = round(last_close - atr * TP_MULT, 6)

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


async def scan_all(symbols: list[dict]) -> tuple[list[dict], list[dict]]:
    """Escanea todos los símbolos. Retorna (señales_válidas, bloqueadas_por_tendencia)."""
    exchange = _build_exchange()
    signals  = []
    blocked  = []
    try:
        for config in symbols:
            sig = await scan_symbol(exchange, config)
            if sig is None:
                continue
            if "blocked_reason" in sig:
                blocked.append(sig)
            else:
                signals.append(sig)
    finally:
        await exchange.close()
    return signals, blocked
