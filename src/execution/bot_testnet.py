import ccxt
import time
import pandas as pd
import pandas_ta as ta
import os
import sys
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.config import SYMBOLS, TIMEFRAME, GROQ_API_KEY, NEWS_API_KEY
from src.logic.ai_agent import AIAgent
from src.collectors.context_collector import ContextCollector

load_dotenv()

class ScalpingBot:
    def __init__(self):
        # Configuración de Exchange (Testnet)
        self.exchange = ccxt.binance({
            'apiKey': os.getenv('BINANCE_TESTNET_API_KEY'),
            'secret': os.getenv('BINANCE_TESTNET_SECRET_KEY'),
            'options': {'defaultType': 'future'},
            'enableRateLimit': True
        })
        self.exchange.set_sandbox_mode(True) # Activar modo Testnet
        
        self.ai_agent = AIAgent()
        self.context = ContextCollector()
        self.symbols = ['AVAX/USDT', 'SOL/USDT', 'FET/USDT', 'KAS/USDT'] # Empezamos con los top
        
    def check_signals(self, symbol):
        try:
            # 1. Obtener Velas recientes
            ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe='15m', limit=100)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            
            # 2. Lógica Técnica (Donchian 20)
            lb = 20
            df['upper'] = df['high'].shift(1).rolling(window=lb).max()
            df['lower'] = df['low'].shift(1).rolling(window=lb).min()
            df['vol_avg'] = df['volume'].rolling(window=lb).mean()
            
            last_close = df['close'].iloc[-1]
            last_high = df['high'].iloc[-1]
            last_volume = df['volume'].iloc[-1]
            
            is_breakout = last_high > df['upper'].iloc[-1] and last_volume > df['vol_avg'].iloc[-1] * 1.5
            
            if is_breakout:
                print(f"\n[TEC] Señal técnica detectada en {symbol}. Validando con IA...")
                
                # 3. Validación con IA
                news_text = self.context.fetch_news(symbol)
                sentiment_score = self.ai_agent.analyze_sentiment(news_text)
                print(f"[AI] Score de Sentimiento: {sentiment_score}")
                
                if sentiment_score > 0.2:
                    print(f"🚀 [OK] Confluencia detectada. Abriendo LONG en {symbol}...")
                    self.execute_trade(symbol, 'buy')
                else:
                    print(f"🛑 [SKIP] IA filtró la entrada (Sentimiento insuficiente: {sentiment_score})")
                    
        except Exception as e:
            print(f"Error analizando {symbol}: {e}")

    def execute_trade(self, symbol, side):
        # Cálculo de cantidad (muy pequeño para empezar: 5 USDT de margen)
        amount = 1.0 # Cantidad mínima o calculada según capital
        try:
            order = self.exchange.create_market_order(symbol, side, amount)
            print(f"✅ Orden ejecutada: {order['id']}")
        except Exception as e:
            print(f"❌ Error ejecutando orden: {e}")

    def run(self):
        print(f"Bot iniciado en modo TESTNET. Monitoreando: {self.symbols}")
        while True:
            for symbol in self.symbols:
                self.check_signals(symbol)
                time.sleep(2) # Respetar rate limit
            print("--- Esperando siguiente ciclo ---")
            time.sleep(60) # Revisar cada minuto

if __name__ == "__main__":
    bot = ScalpingBot()
    bot.run()
