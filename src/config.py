import os
from dotenv import load_dotenv

load_dotenv()

# Configuración de Símbolos (Top 50 aproximado por volumen en Binance)
SYMBOLS = [
    'BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'BNB/USDT', 'XRP/USDT',
    'ADA/USDT', 'AVAX/USDT', 'DOGE/USDT', 'DOT/USDT', 'LINK/USDT',
    'MATIC/USDT', 'TRX/USDT', 'SHIB/USDT', 'LTC/USDT', 'BCH/USDT',
    'UNI/USDT', 'NEAR/USDT', 'APT/USDT', 'OP/USDT', 'ARB/USDT',
    'STX/USDT', 'RENDER/USDT', 'INJ/USDT', 'TIA/USDT', 'PEPE/USDT',
    'FET/USDT', 'FTM/USDT', 'SUI/USDT', 'KAS/USDT', 'AAVE/USDT',
    'ORDI/USDT', 'SEI/USDT', 'BEAM/USDT', 'IMX/USDT', 'LDO/USDT',
    'FIL/USDT', 'ATOM/USDT', 'ETC/USDT', 'ICP/USDT', 'RNDR/USDT',
    'GALA/USDT', 'GRT/USDT', 'THETA/USDT', 'MKR/USDT', 'SNX/USDT',
    'ALGO/USDT', 'VET/USDT', 'FLOW/USDT', 'EGLD/USDT', 'QNT/USDT'
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
