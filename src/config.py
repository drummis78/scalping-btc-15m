import os
from dotenv import load_dotenv

load_dotenv()

# Configuración de Símbolos (Top 50 aproximado por volumen en Binance)
SYMBOLS = [
    # Top market cap — ya descargados
    'BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'BNB/USDT', 'XRP/USDT',
    'ADA/USDT', 'AVAX/USDT', 'DOGE/USDT', 'DOT/USDT', 'LINK/USDT',
    'TRX/USDT', 'LTC/USDT', 'BCH/USDT', 'UNI/USDT', 'NEAR/USDT',
    'APT/USDT', 'OP/USDT', 'ARB/USDT', 'STX/USDT', 'RENDER/USDT',
    'INJ/USDT', 'TIA/USDT', 'PEPE/USDT', 'FET/USDT', 'SUI/USDT',
    'KAS/USDT', 'AAVE/USDT', 'ORDI/USDT', 'SEI/USDT', 'IMX/USDT',
    'LDO/USDT', 'FIL/USDT', 'ATOM/USDT', 'ICP/USDT', 'GALA/USDT',
    'MKR/USDT', 'ALGO/USDT', 'ETC/USDT', 'GRT/USDT', 'FTM/USDT',
    # Nuevos top 50 — necesitan descarga
    'TON/USDT', 'WIF/USDT', 'JUP/USDT', 'BONK/USDT', 'RUNE/USDT',
    'WLD/USDT', 'PENDLE/USDT', 'JTO/USDT', 'TRB/USDT', 'PYTH/USDT',
]

TIMEFRAME = '15m'
DATA_DIR = 'data/raw'
PROCESSED_DIR = 'data/processed'
MODELS_DIR = 'models'

# Configuración de Estrategia
RISK_PER_TRADE = 0.01  # 1% del capital por operación
LEVERAGE = 3           # Apalancamiento sugerido
TARGET_DAILY_PROFIT = 0.01 # 1% diario objetivo

# APIs (Se cargan desde .env)
GROQ_API_KEY = os.getenv('GROQ_API_KEY')
NEWS_API_KEY = os.getenv('NEWS_API_KEY')
