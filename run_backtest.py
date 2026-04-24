"""
Descarga datos nuevos y corre el Donchian Breakout optimizer en todos los símbolos.
Genera data/optimization_results.csv con los mejores parámetros por símbolo.

Uso: python run_backtest.py
"""
import ccxt
import pandas as pd
import pandas_ta as ta
import vectorbt as vbt
import numpy as np
import os
import time
import sys
from datetime import datetime

sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from src.config import SYMBOLS, TIMEFRAME, DATA_DIR

DATA_DIR = "data/raw"
OUT_CSV  = "data/optimization_results.csv"
os.makedirs(DATA_DIR, exist_ok=True)

# ── Descarga ──────────────────────────────────────────────────────────────────

def download_symbol(symbol: str, timeframe: str = "15m") -> bool:
    fname = os.path.join(DATA_DIR, f"{symbol.replace('/', '_')}_{timeframe}.csv")
    if os.path.exists(fname):
        print(f"  [SKIP] {symbol} ya existe")
        return True
    ex = ccxt.binance({"enableRateLimit": True})
    since = ex.milliseconds() - 2 * 365 * 24 * 60 * 60 * 1000
    rows = []
    print(f"  [DL]   {symbol} ...", end="", flush=True)
    while since < ex.milliseconds():
        try:
            ohlcv = ex.fetch_ohlcv(symbol, timeframe, since, limit=1000)
            if not ohlcv:
                break
            rows += ohlcv
            since = ohlcv[-1][0] + 1
            time.sleep(ex.rateLimit / 1000)
        except Exception as e:
            print(f" ERROR: {e}")
            return False
    if not rows:
        print(" SIN DATOS")
        return False
    df = pd.DataFrame(rows, columns=["timestamp","open","high","low","close","volume"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    df.to_csv(fname, index=False)
    print(f" {len(df)} velas OK")
    return True


# ── Backtest Donchian ─────────────────────────────────────────────────────────

def backtest_symbol(symbol: str) -> dict | None:
    fname = os.path.join(DATA_DIR, f"{symbol.replace('/', '_')}_{TIMEFRAME}.csv")
    if not os.path.exists(fname):
        return None
    try:
        df = pd.read_csv(fname)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df.set_index("timestamp", inplace=True)

        close  = df["close"]
        high   = df["high"]
        low    = df["low"]
        volume = df["volume"]

        atr = ta.atr(high, low, close, length=14)

        best_ret    = -999
        best_params = {}

        for lb in [20, 50]:
            for vm in [1.5, 2.0]:
                dc      = ta.donchian(high, low, lower_length=lb, upper_length=lb)
                avg_vol = ta.sma(volume, length=lb)
                ukey    = f"DCU_{lb}_{lb}"
                lkey    = f"DCL_{lb}_{lb}"
                if ukey not in dc.columns or lkey not in dc.columns:
                    continue

                long_e  = (close >= dc[ukey]) & (volume > avg_vol * vm)
                short_e = (close <= dc[lkey]) & (volume > avg_vol * vm)

                for sl_m in [1.5, 2.0]:
                    for tp_m in [2.0, 3.0]:
                        try:
                            pf = vbt.Portfolio.from_signals(
                                close,
                                entries=long_e,
                                short_entries=short_e,
                                sl_stop=(sl_m * atr) / close,
                                tp_stop=(tp_m * atr) / close,
                                freq="15min",
                                fees=0.0006,
                            )
                            ret = float(pf.total_return())
                            wr  = float(pf.trades.win_rate())
                            n   = int(pf.trades.count())
                            if ret > best_ret and n >= 30:
                                best_ret    = ret
                                best_params = {
                                    "symbol":     symbol,
                                    "return_pct": round(ret * 100, 4),
                                    "win_rate":   round(wr * 100, 4),
                                    "trades":     n,
                                    "lookback":   lb,
                                    "vol_mult":   vm,
                                    "sl_mult":    sl_m,
                                    "tp_mult":    tp_m,
                                }
                        except Exception:
                            pass

        return best_params if best_params else None
    except Exception as e:
        print(f"  [ERR] {symbol}: {e}")
        return None


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"\n{'='*60}")
    print(f"  Backtest Donchian Breakout 15m — {len(SYMBOLS)} símbolos")
    print(f"{'='*60}\n")

    # 1. Descargar nuevos
    print("▶ Descargando datos faltantes...\n")
    for sym in SYMBOLS:
        download_symbol(sym)

    # 2. Optimizar
    print("\n▶ Corriendo backtest...\n")
    results = []
    for i, sym in enumerate(SYMBOLS, 1):
        print(f"  [{i:02d}/{len(SYMBOLS)}] {sym} ", end="", flush=True)
        r = backtest_symbol(sym)
        if r:
            results.append(r)
            print(f"→ ret={r['return_pct']:.1f}% wr={r['win_rate']:.1f}% trades={r['trades']}")
        else:
            print("→ sin resultado")

    # 3. Guardar
    df_out = pd.DataFrame(results)
    df_out = df_out.sort_values("return_pct", ascending=False)
    df_out.to_csv(OUT_CSV, index=False)
    print(f"\n✅ Guardado en {OUT_CSV}")
    print(f"\nTop 10:\n{df_out.head(10).to_string(index=False)}")
