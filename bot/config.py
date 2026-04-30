from pydantic_settings import BaseSettings
from pydantic import field_validator


class Settings(BaseSettings):
    BINANCE_API_KEY:    str = ""
    BINANCE_API_SECRET: str = ""

    TELEGRAM_BOT_TOKEN: str = ""
    TELEGRAM_CHAT_ID:   str = ""

    WEBHOOK_SECRET:     str = ""
    TESTNET:            bool = True

    MAX_LEVERAGE:         float = 3.0
    MAX_DD_PCT:           float = 20.0
    MAX_DAILY_LOSS_PCT:   float = 3.0
    TARGET_DAILY_PCT:     float = 1.0
    PAPER_BALANCE:        float = 1000.0
    MAX_POSITIONS:        int   = 5    # límite para estrategias 15m (donchian + tcp)
    MAX_POSITIONS_1H:     int   = 5    # límite independiente para donchian_1h
    RISK_PER_TRADE:       float = 0.015   # 1.5% equity por trade

    BE_STOP_ENABLED:      bool  = True    # mover SL a entry cuando profit >= SL_dist
    ACTIVE_HOURS_BLOCK:   str   = "4,5,6,11"  # horas UTC bloqueadas: 04-06 + 11 (0% WR)

    COOLDOWN_LOSSES:      int   = 3      # N pérdidas consecutivas activan cooldown
    COOLDOWN_MINUTES:     int   = 60     # minutos de pausa tras la racha

    REGIME_FILTER_ENABLED: bool  = True   # bloquea entradas en mercado lateral
    REGIME_CHOP_THRESHOLD: float = 61.8   # CHOP >= threshold = lateral = no operar
    REGIME_CHOP_LEN:       int   = 14     # períodos CHOP en 1H

    ATRPCT_FILTER_ENABLED:    bool  = True   # solo operar cuando ATR está en percentil alto
    ATRPCT_FILTER_THRESHOLD:  float = 60.0   # percentil mínimo (0-100)
    ATRPCT_FILTER_LEN:        int   = 100    # ventana rolling para min/max ATR
    ADX_THRESHOLD:            float = 25.0   # ADX(14) mínimo — sin momentum el breakout revierte

    FUNDAMENTAL_ENABLED:       bool = True
    FUNDAMENTAL_POLL_INTERVAL: int  = 1800
    NEWSAPI_KEY:               str  = ""
    GROQ_API_KEY:              str  = ""

    DATABASE_URL:  str = ""           # Railway lo setea automático al conectar PostgreSQL
    LOG_FILE:      str = "bot/logs/trading.log"
    SCAN_INTERVAL: int = 900              # 15 minutos
    SYMBOLS_CSV:   str = "data/optimization_results.csv"

    @field_validator("MAX_LEVERAGE", "MAX_DD_PCT", "MAX_DAILY_LOSS_PCT",
                     "TARGET_DAILY_PCT", "PAPER_BALANCE", "RISK_PER_TRADE", mode="before")
    @classmethod
    def clean_float(cls, v):
        if isinstance(v, str):
            v = v.strip().lstrip("=").strip()
        return float(v)

    class Config:
        env_file = "bot/.env"
        env_file_encoding = "utf-8"


settings = Settings()
