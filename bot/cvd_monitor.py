"""
CVD Monitor — shadow mode (solo observación, no bloquea señales).

TBR = taker_buy_base_asset_volume / volume de la última vela 15m cerrada.
LONG hubiera sido bloqueado si TBR < 0.50 (vendedores dominan el breakout).
SHORT hubiera sido bloqueado si TBR > 0.50 (compradores dominan).
"""
import logging
from typing import Optional

import ccxt.async_support as ccxt_async

logger = logging.getLogger("scalping_bot.cvd_monitor")


async def get_tbr(symbol: str, timeframe: str = "15m") -> Optional[float]:
    """
    Retorna TBR (Taker Buy Ratio) de la última vela cerrada.
    TBR = taker_buy_volume / total_volume  (0.0 a 1.0)
    Retorna None si no está disponible.
    """
    ex = ccxt_async.binance({
        "options": {"defaultType": "future"},
        "enableRateLimit": True,
        "verbose": False,
    })
    try:
        # fetch_ohlcv con parámetro extra para obtener taker_buy_volume en Binance
        ohlcv = await ex.fetch_ohlcv(symbol, timeframe, limit=2)
        # Binance Futures klines: [ts, open, high, low, close, volume, ..., taker_buy_base]
        # CCXT retorna solo 6 cols estándar — usamos endpoint raw para obtener taker col
        raw = await ex.fetch(
            "/fapi/v1/klines",
            "GET",
            {"symbol": ex.market_id(symbol), "interval": "15m", "limit": 2}
        )
        # Binance kline: index 5=volume, index 9=taker_buy_base_asset_volume
        last_closed = raw[-2]   # -1 es la vela abierta actual, -2 es la última cerrada
        volume      = float(last_closed[5])
        taker_buy   = float(last_closed[9])
        if volume <= 0:
            return None
        tbr = taker_buy / volume
        logger.debug(f"[CVD] {symbol} TBR={tbr:.3f} (taker_buy={taker_buy:.2f} / vol={volume:.2f})")
        return round(tbr, 4)
    except Exception as e:
        logger.warning(f"[CVD] Error obteniendo TBR para {symbol}: {e}")
        return None
    finally:
        await ex.close()


def would_block(side: str, tbr: Optional[float]) -> bool:
    """Retorna True si el filtro CVD hubiera bloqueado esta señal."""
    if tbr is None:
        return False
    if side == "long"  and tbr < 0.50:
        return True
    if side == "short" and tbr > 0.50:
        return True
    return False
