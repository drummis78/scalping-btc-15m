import ccxt
import time
import pandas as pd
import os
import sys
from datetime import datetime
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.config import GROQ_API_KEY, NEWS_API_KEY
from src.logic.ai_agent import AIAgent
from src.collectors.context_collector import ContextCollector

load_dotenv()

class SimulationBot:
    def __init__(self):
        self.exchange = ccxt.binance() # Solo para lectura, no necesita API
        self.ai_agent = AIAgent()
        self.context = ContextCollector()
        self.symbols = ['AVAX/USDT', 'SOL/USDT', 'FET/USDT', 'KAS/USDT', 'AAVE/USDT']
        self.balance = 1000.0 # Capital ficticio inicial
        self.log_file = 'registro_simulacion.csv'
        
        if not os.path.exists(self.log_file):
            pd.DataFrame(columns=['timestamp', 'symbol', 'side', 'price', 'sentiment', 'status']).to_csv(self.log_file, index=False)

    def log_trade(self, symbol, side, price, sentiment):
        new_trade = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'symbol': symbol,
            'side': side,
            'price': price,
            'sentiment': sentiment,
            'status': 'SIMULATED'
        }
        df = pd.read_csv(self.log_file)
        df = pd.concat([df, pd.DataFrame([new_trade])], ignore_index=True)
        df.to_csv(self.log_file, index=False)
        print(f"📝 Registro guardado en {self.log_file}")

    def check_signals(self, symbol):
        try:
            print(f"🔍 Analizando {symbol}...")
            ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe='15m', limit=50)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            
            # Técnica Donchian 20
            lb = 20
            upper = df['high'].shift(1).rolling(window=lb).max().iloc[-1]
            vol_avg = df['volume'].rolling(window=lb).mean().iloc[-1]
            
            last_close = df['close'].iloc[-1]
            last_high = df['high'].iloc[-1]
            last_vol = df['volume'].iloc[-1]
            
            if last_high > upper and last_vol > vol_avg * 1.5:
                print(f"🎯 [TEC] Posible Breakout en {symbol}. Consultando al Cerebro...")
                
                # Contexto IA
                news_text = self.context.fetch_news(symbol)
                sentiment = self.ai_agent.analyze_sentiment(news_text)
                
                if sentiment > 0.2:
                    print(f"✅ [AI] CONFLUENCIA: {sentiment}. ¡COMPRANDO (SIMULADO)!")
                    self.log_trade(symbol, 'LONG', last_close, sentiment)
                else:
                    print(f"❌ [AI] FILTRADO: {sentiment}. Noticia no acompaña.")
                    
        except Exception as e:
            print(f"Error en {symbol}: {e}")

    def run(self):
        print(f"🚀 Bot de Simulación Iniciado con ${self.balance}")
        while True:
            for s in self.symbols:
                self.check_signals(s)
                time.sleep(1)
            print(f"--- Ciclo completo {datetime.now().strftime('%H:%M:%S')} ---")
            time.sleep(60)

if __name__ == "__main__":
    bot = SimulationBot()
    bot.run()
