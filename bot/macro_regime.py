"""
Macro Regime Monitor — shadow mode (solo observación, no bloquea señales).

Calcula EMA20 vs EMA50 en velas semanales de BTCUSDT para determinar el régimen
de mercado actual. Se usa exclusivamente como indicador en el dashboard.
"""
import asyncio
import logging
from datetime import datetime, timezone

import ccxt.async_support as ccxt_async
import pandas as pd
import pandas_ta as ta

logger = logging.getLogger("scalping_bot.macro_regime")

SYMBOL   = "BTC/USDT:USDT"
EMA_FAST = 20
EMA_SLOW = 50
NEUTRAL_BAND_PCT = 0.01   # ±1% → NEUTRAL

_state = {
    "regime":    "unknown",   # "bull" | "bear" | "neutral" | "unknown"
    "ema_fast":  None,
    "ema_slow":  None,
    "updated_at": None,
}

_task: asyncio.Task | None = None


def get_regime() -> dict:
    return dict(_state)


async def _calc_regime() -> dict:
    ex = ccxt_async.binance({
        "options": {"defaultType": "future"},
        "enableRateLimit": True,
        "verbose": False,
    })
    try:
        ohlcv = await ex.fetch_ohlcv(SYMBOL, "1w", limit=EMA_SLOW + 10)
        if len(ohlcv) < EMA_SLOW + 2:
            logger.warning("[MACRO] Datos insuficientes para calcular régimen")
            return _state

        df = pd.DataFrame(ohlcv, columns=["ts", "open", "high", "low", "close", "volume"])
        df.ta.ema(length=EMA_FAST, append=True)
        df.ta.ema(length=EMA_SLOW, append=True)

        fast_col = f"EMA_{EMA_FAST}"
        slow_col = f"EMA_{EMA_SLOW}"

        ema_fast = float(df[fast_col].iloc[-1])
        ema_slow = float(df[slow_col].iloc[-1])

        diff_pct = (ema_fast - ema_slow) / ema_slow

        if abs(diff_pct) <= NEUTRAL_BAND_PCT:
            regime = "neutral"
        elif diff_pct > 0:
            regime = "bull"
        else:
            regime = "bear"

        _state["regime"]     = regime
        _state["ema_fast"]   = round(ema_fast, 2)
        _state["ema_slow"]   = round(ema_slow, 2)
        _state["updated_at"] = datetime.now(timezone.utc).isoformat()

        logger.info(f"[MACRO] Régimen: {regime.upper()} | EMA{EMA_FAST}={ema_fast:,.2f} EMA{EMA_SLOW}={ema_slow:,.2f} diff={diff_pct*100:+.2f}%")
        return dict(_state)

    except Exception as e:
        logger.error(f"[MACRO] Error calculando régimen: {e}")
        return _state
    finally:
        await ex.close()


async def _regime_loop():
    while True:
        try:
            await _calc_regime()
        except Exception as e:
            logger.error(f"[MACRO] Loop error: {e}")
        await asyncio.sleep(3600)   # actualiza cada hora (datos semanales)


async def start():
    global _task
    await _calc_regime()            # cálculo inmediato al arrancar
    _task = asyncio.create_task(_regime_loop())
    logger.info("[MACRO] Monitor de régimen iniciado (shadow mode)")


async def stop():
    global _task
    if _task:
        _task.cancel()
