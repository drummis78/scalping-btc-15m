import pandas as pd
import pandas_ta as ta
import vectorbt as vbt
import numpy as np
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.config import DATA_DIR, TIMEFRAME

def strategy_breakout_pro(symbol):
    file_path = os.path.join(DATA_DIR, f"{symbol.replace('/', '_')}_{TIMEFRAME}.csv")
    if not os.path.exists(file_path): return None
    
    try:
        df = pd.read_csv(file_path)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)
        
        close = df['close']
        high = df['high']
        low = df['low']
        volume = df['volume']

        # ATR para SL/TP dinámico
        atr = ta.atr(high, low, close, length=14)
        
        # Rangos de optimización
        lookback_range = [20, 50]
        vol_mult_range = [1.5, 2.0]
        sl_mult_range = [1.5, 2.0]
        tp_mult_range = [2.0, 3.0]

        best_ret = -999
        best_params = {}

        for lb in lookback_range:
            for vm in vol_mult_range:
                dc = ta.donchian(high, low, lower_length=lb, upper_length=lb)
                avg_vol = ta.sma(volume, length=lb)
                
                entries = (close >= dc[f'DCU_{lb}_{lb}']) & (volume > avg_vol * vm)
                short_entries = (close <= dc[f'DCL_{lb}_{lb}']) & (volume > avg_vol * vm)

                for sl_m in sl_mult_range:
                    for tp_m in tp_mult_range:
                        pf = vbt.Portfolio.from_signals(
                            close, entries=entries, short_entries=short_entries,
                            sl_stop=(sl_m * atr) / close,
                            tp_stop=(tp_m * atr) / close,
                            freq='15m', fees=0.0006
                        )
                        
                        ret = pf.total_return()
                        if ret > best_ret:
                            best_ret = ret
                            best_params = {
                                'symbol': symbol,
                                'return_pct': ret * 100,
                                'win_rate': pf.trades.win_rate() * 100,
                                'trades': pf.trades.count(),
                                'lookback': lb,
                                'vol_mult': vm,
                                'sl_mult': sl_m,
                                'tp_mult': tp_m
                            }
        return best_params
    except Exception as e:
        print(f"Error en {symbol}: {e}")
        return None

if __name__ == "__main__":
    all_results = []
    files = [f for f in os.listdir(DATA_DIR) if f.endswith('.csv')]
    
    print(f"Iniciando escaneo de {len(files)} activos...")
    
    for f in files:
        symbol = f.replace(f'_{TIMEFRAME}.csv', '').replace('_', '/')
        res = strategy_breakout_pro(symbol)
        if res:
            all_results.append(res)
            print(f"{symbol}: {res['return_pct']:.2f}% | WR: {res['win_rate']:.1f}%")

    final_df = pd.DataFrame(all_results)
    print("\n--- TOP 10 ACTIVOS MÁS RENTABLES ---")
    print(final_df.sort_values(by='return_pct', ascending=False).head(10))
    
    # Guardar resultados
    final_df.to_csv('data/optimization_results.csv', index=False)
