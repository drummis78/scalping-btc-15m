import os
import sys

# CONFIGURACIÓN DE PATHS AL INICIO
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

import ccxt
import time
import pandas as pd
import pandas_ta as ta
from datetime import datetime, timedelta
from dotenv import load_dotenv
from src.execution.static_dashboard import generate_static_dashboard
from src.logic.ai_agent import AIAgent
from src.collectors.context_collector import ContextCollector

load_dotenv()

# --- Configuración ---
RISK_PER_TRADE_PCT = 0.05
DECISION_LOG = 'bitacora_decisiones.csv'
VALIDATION_PERIOD_CANDLES = 15 # Cuántas velas esperar para validar el filtro

class ScalpingBotV5:
    def __init__(self):
        self.exchange = ccxt.binance({'enableRateLimit': True})
        self.ai_agent = AIAgent()
        self.context = ContextCollector()
        self.log_file = 'registro_simulacion.csv'
        self.initial_capital = 1000.0
        
        df_opt = pd.read_csv('data/optimization_results.csv')
        self.assets_config = df_opt.sort_values(by='return_pct', ascending=False).head(20).to_dict('records')
        
        # Inicializar archivos con nuevas columnas
        if not os.path.exists(DECISION_LOG):
            pd.DataFrame(columns=['timestamp', 'symbol', 'side', 'price', 'sentiment', 'verdict', 'future_price', 'efficacy']).to_csv(DECISION_LOG, index=False)

        self.refresh_balance()

    def refresh_balance(self):
        df = pd.read_csv(self.log_file) if os.path.exists(self.log_file) else pd.DataFrame()
        closed = df[df['status'] == 'CLOSED'] if not df.empty else pd.DataFrame()
        total_pnl_usd = ((closed['pnl'] / 100) * closed['usd_invested']).sum() if not closed.empty else 0
        self.current_balance = self.initial_capital + total_pnl_usd
        self.positions = df[df['status'] == 'OPEN'].to_dict('records') if not df.empty else []

    def log_decision(self, symbol, side, price, sentiment, verdict):
        new_row = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'symbol': symbol, 'side': side, 'price': price,
            'sentiment': sentiment, 'verdict': verdict,
            'future_price': 0, 'efficacy': 'WAITING'
        }
        df = pd.read_csv(DECISION_LOG)
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        df.to_csv(DECISION_LOG, index=False)

    def validate_old_decisions(self):
        """Revisa decisiones antiguas para ver qué hizo el precio después."""
        try:
            df = pd.read_csv(DECISION_LOG)
            mask = (df['efficacy'] == 'WAITING') & (df['verdict'] == 'FILTERED')
            waiting_trades = df[mask]
            
            for idx, row in waiting_trades.iterrows():
                # Si han pasado más de 15 velas (15m * 15 = 225 min)
                decision_time = datetime.strptime(row['timestamp'], '%Y-%m-%d %H:%M:%S')
                if datetime.now() > decision_time + timedelta(minutes=15 * VALIDATION_PERIOD_CANDLES):
                    ticker = self.exchange.fetch_ticker(row['symbol'])
                    future_price = ticker['last']
                    
                    # Evaluar eficacia
                    # Filtro Correcto (✅): Evitó pérdida. 
                    # - En LONG: El precio bajó.
                    # - En SHORT: El precio subió.
                    eff = "FAIL"
                    if row['side'] == 'LONG':
                        eff = "✅ CORRECT" if future_price < row['price'] else "❌ FALSE"
                    else: # SHORT
                        eff = "✅ CORRECT" if future_price > row['price'] else "❌ FALSE"
                    
                    df.at[idx, 'future_price'] = future_price
                    df.at[idx, 'efficacy'] = eff
            
            df.to_csv(DECISION_LOG, index=False)
        except Exception as e:
            print(f"Error validando decisiones: {e}")

    def check_signals(self, config):
        symbol = config['symbol']
        if any(p['symbol'] == symbol for p in self.positions): return

        try:
            ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe='15m', limit=50)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            lb = int(config['lookback'])
            df['upper'] = df['high'].shift(1).rolling(window=lb).max()
            df['lower'] = df['low'].shift(1).rolling(window=lb).min()
            df['vol_avg'] = df['volume'].rolling(window=lb).mean()
            df.ta.atr(append=True)
            
            last_close, last_high, last_low = df['close'].iloc[-1], df['high'].iloc[-1], df['low'].iloc[-1]
            last_vol, atr = df['volume'].iloc[-1], df['ATRr_14'].iloc[-1]
            
            if last_high > df['upper'].iloc[-1] and last_vol > df['vol_avg'].iloc[-1] * config['vol_mult']:
                sentiment = self.ai_agent.analyze_sentiment(self.context.fetch_news(symbol))
                if sentiment > 0.2:
                    self.save_trade(symbol, 'LONG', last_close, last_close-(atr*1.5), last_close+(atr*3), sentiment, 'OPEN', self.current_balance*0.05)
                    self.log_decision(symbol, 'LONG', last_close, sentiment, 'OPENED')
                else:
                    self.log_decision(symbol, 'LONG', last_close, sentiment, 'FILTERED')

            elif last_low < df['lower'].iloc[-1] and last_vol > df['vol_avg'].iloc[-1] * config['vol_mult']:
                sentiment = self.ai_agent.analyze_sentiment(self.context.fetch_news(symbol))
                if sentiment < -0.2:
                    self.save_trade(symbol, 'SHORT', last_close, last_close+(atr*1.5), last_close-(atr*3), sentiment, 'OPEN', self.current_balance*0.05)
                    self.log_decision(symbol, 'SHORT', last_close, sentiment, 'OPENED')
                else:
                    self.log_decision(symbol, 'SHORT', last_close, sentiment, 'FILTERED')
        except: pass

    def save_trade(self, symbol, side, price, sl, tp, sentiment, status, usd_invested):
        new_row = {'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'symbol': symbol, 'side': side, 'price': price, 'sl': sl, 'tp': tp, 'sentiment': sentiment, 'exit_price': 0, 'pnl': 0, 'status': status, 'usd_invested': usd_invested}
        pd.concat([pd.read_csv(self.log_file) if os.path.exists(self.log_file) else pd.DataFrame(), pd.DataFrame([new_row])], ignore_index=True).to_csv(self.log_file, index=False)
        self.refresh_balance()

    def run(self):
        print(f"🚀 Bot v5.5 (Efficacy Audit) activo.")
        while True:
            self.validate_old_decisions() # Validar filtros pasados
            for config in self.assets_config:
                self.check_signals(config)
                time.sleep(0.5)
            generate_static_dashboard()
            time.sleep(30)

if __name__ == "__main__":
    bot = ScalpingBotV5()
    bot.run()
