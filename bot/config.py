from pydantic_settings import BaseSettings
from pydantic import field_validator


class Settings(BaseSettings):
    BINANCE_API_KEY:    str = ""
    BINANCE_API_SECRET: str = ""

    TELEGRAM_BOT_TOKEN: str = ""
    TELEGRAM_CHAT_ID:   str = ""

    WEBHOOK_SECRET:     str = "cambiar_antes_de_produccion"
    TESTNET:            bool = True

    MAX_LEVERAGE:         float = 3.0
    MAX_DD_PCT:           float = 20.0
    MAX_DAILY_LOSS_PCT:   float = 3.0
    TARGET_DAILY_PCT:     float = 1.0
    PAPER_BALANCE:        float = 1000.0
    MAX_POSITIONS:        int   = 5
    RISK_PER_TRADE:       float = 0.05    # 5% equity por trade

    FUNDAMENTAL_ENABLED:       bool = True
    FUNDAMENTAL_POLL_INTERVAL: int  = 1800
    NEWSAPI_KEY:               str  = ""
    GROQ_API_KEY:              str  = ""

    DATABASE_PATH: str = "bot/trading.db"
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
