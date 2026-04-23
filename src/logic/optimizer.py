import vectorbt as vbt
import pandas as pd
import numpy as np
import os
import sys

# Añadir el path raíz para importar config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.config import DATA_DIR, TIMEFRAME

def run_optimization(symbol):
    file_path = os.path.join(DATA_DIR, f"{symbol.replace('/', '_')}_{TIMEFRAME}.csv")
    if not os.path.exists(file_path):
        print(f"No hay datos para {symbol}")
        return
    
    df = pd.read_csv(file_path)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df.set_index('timestamp', inplace=True)
    
    close = df['close']
    
    # Estrategia: Bollinger Bands Mean Reversion
    # Buy when price touches Lower Band and RSI is oversold
    # Sell when price touches Middle Band or Upper Band
    
    bb_window_range = [20, 30, 50]
    bb_std_range = [2.0, 2.5]
    rsi_window = 14
    rsi_oversold_range = [25, 30, 35]

    print(f"Optimizando Mean Reversion (Bollinger) para {symbol}...")
    
    all_results = []
    
    for bb_w in bb_window_range:
        for bb_std in bb_std_range:
            bb = vbt.BBANDS.run(close, window=bb_w, alpha=bb_std)
            for rsi_os in rsi_oversold_range:
                rsi = vbt.RSI.run(close, window=rsi_window).rsi
                
                entries = (close <= bb.lower) & (rsi <= rsi_os)
                exits = (close >= bb.middle)
                
                pf = vbt.Portfolio.from_signals(close, entries, exits, freq='15m', fees=0.001)
                
                all_results.append({
                    'bb_w': bb_w,
                    'bb_std': bb_std,
                    'rsi_os': rsi_os,
                    'return': pf.total_return(),
                    'sharpe': pf.sharpe_ratio(),
                    'trades': pf.trades.count()
                })
                
    results_df = pd.DataFrame(all_results)
    best = results_df.sort_values(by='return', ascending=False).head(10)
    
    print(f"\nTop 10 para {symbol} (Mean Reversion):")
    print(best)
    
    return results_df

if __name__ == "__main__":
    run_optimization('BTC/USDT')
