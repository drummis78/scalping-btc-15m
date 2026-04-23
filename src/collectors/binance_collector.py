import ccxt
import pandas as pd
import os
from datetime import datetime
import time
from tqdm import tqdm
import sys

# Añadir el path raíz para importar config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.config import SYMBOLS, TIMEFRAME, DATA_DIR

def download_ohlcv(symbol, timeframe, since=None):
    exchange = ccxt.binance({
        'enableRateLimit': True,
    })
    
    print(f"Descargando {symbol}...")
    all_ohlcv = []
    
    # Si since es None, descargar últimos 2 años aprox
    if since is None:
        since = exchange.milliseconds() - 2 * 365 * 24 * 60 * 60 * 1000
        
    while since < exchange.milliseconds():
        try:
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe, since)
            if not ohlcv:
                break
            since = ohlcv[-1][0] + 1
            all_ohlcv += ohlcv
            # Evitar ban por rate limit
            time.sleep(exchange.rateLimit / 1000)
        except Exception as e:
            print(f"Error en {symbol}: {e}")
            break
            
    df = pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    return df

def main():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
        
    for symbol in tqdm(SYMBOLS):
        filename = os.path.join(DATA_DIR, f"{symbol.replace('/', '_')}_{TIMEFRAME}.csv")
        
        # Si ya existe, podríamos saltarlo o actualizarlo
        if os.path.exists(filename):
            print(f"Archivo ya existe: {filename}. Saltando...")
            continue
            
        df = download_ohlcv(symbol, TIMEFRAME)
        if not df.empty:
            df.to_csv(filename, index=False)
            print(f"Guardado {filename} - {len(df)} velas.")

if __name__ == "__main__":
    main()
