"""
Donchian Breakout scanner — 15m, 20 símbolos, parámetros por símbolo desde CSV.
Genera señales LONG/SHORT con SL y TP calculados desde ATR.
"""
import logging
import pandas as pd
import pandas_ta as ta
from typing import Optional
import ccxt.async_support as ccxt_async

from bot.config import settings

logger = logging.getLogger("scalping_bot.scanner")

# Filtros: solo activos con backtest positivo y win rate decente
MIN_RETURN_PCT = 5.0
MIN_WIN_RATE   = 50.0
MIN_TRADES     = 50
TOP_N          = 15

# Símbolos que no existen en Binance Futures
EXCLUDED = {"SHIB/USDT", "MATIC/USDT", "RNDR/USDT"}


def load_symbols() -> list[dict]:
    try:
        df = pd.read_csv(settings.SYMBOLS_CSV)
        df = df[
            (df["return_pct"] > MIN_RETURN_PCT) &
            (df["win_rate"] > MIN_WIN_RATE) &
            (df["trades"] >= MIN_TRADES) &
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
        "apiKey":    settings.BINANCE_API_KEY or None,
        "secret":    settings.BINANCE_API_SECRET or None,
        "options":   {"defaultType": "future"},
        "enableRateLimit": True,
        "timeout":   10000,
        "verbose":   False,
    })


async def scan_symbol(exchange: ccxt_async.binance, config: dict) -> Optional[dict]:
    """
    Retorna un dict con señal o None si no hay breakout.
    {symbol, side, price, sl_price, tp_price, candle_ts, config}
    """
    symbol = config["symbol"]
    lb     = int(config["lookback"])
    try:
        ohlcv = await exchange.fetch_ohlcv(symbol, timeframe="15m", limit=lb + 5)
        if len(ohlcv) < lb + 2:
            return None

        df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])

        # ATR con pandas_ta
        df.ta.atr(length=14, append=True)
        atr_col = [c for c in df.columns if c.startswith("ATR")]
        if not atr_col or pd.isna(df[atr_col[0]].iloc[-1]):
            return None
        atr = float(df[atr_col[0]].iloc[-1])

        # Donchian: usa shift(1) para no mirar la vela actual en formación
        df["upper"]   = df["high"].shift(1).rolling(window=lb).max()
        df["lower"]   = df["low"].shift(1).rolling(window=lb).min()
        df["vol_avg"] = df["volume"].rolling(window=lb).mean()

        # usar la última vela CERRADA (iloc[-2]), no la vela en formación
        last       = df.iloc[-2]
        candle_ts  = str(int(last["timestamp"]))
        last_close = float(last["close"])
        last_high  = float(last["high"])
        last_low   = float(last["low"])
        last_vol   = float(last["volume"])
        upper      = float(last["upper"])
        lower      = float(last["lower"])
        vol_avg    = float(last["vol_avg"])

        if pd.isna(upper) or pd.isna(lower) or pd.isna(vol_avg) or vol_avg == 0:
            return None

        vol_ok = last_vol > vol_avg * config["vol_mult"]

        if last_high > upper and vol_ok:
            return {
                "symbol":     symbol,
                "side":       "long",
                "price":      last_close,
                "sl_price":   round(last_close - atr * config["sl_mult"], 6),
                "tp_price":   round(last_close + atr * config["tp_mult"], 6),
                "candle_ts":  candle_ts,
                "config":     config,
            }
        elif last_low < lower and vol_ok:
            return {
                "symbol":     symbol,
                "side":       "short",
                "price":      last_close,
                "sl_price":   round(last_close + atr * config["sl_mult"], 6),
                "tp_price":   round(last_close - atr * config["tp_mult"], 6),
                "candle_ts":  candle_ts,
                "config":     config,
            }
        return None

    except Exception as e:
        logger.warning(f"[SCANNER] {symbol}: {e}")
        return None


async def scan_all(symbols: list[dict]) -> list[dict]:
    """Escanea todos los símbolos y retorna lista de señales encontradas."""
    exchange = _build_exchange()
    signals  = []
    try:
        for config in symbols:
            sig = await scan_symbol(exchange, config)
            if sig:
                signals.append(sig)
    finally:
        await exchange.close()
    return signals
