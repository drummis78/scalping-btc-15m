import logging
from telegram import Bot
from bot.config import settings

logger = logging.getLogger("scalping_bot.notifier")


class TelegramNotifier:
    def __init__(self, exchange=None):
        self.exchange = exchange
        self._bot = Bot(token=settings.TELEGRAM_BOT_TOKEN) \
            if settings.TELEGRAM_BOT_TOKEN else None

    async def start(self):
        if self._bot:
            logger.info("Telegram Bot started (send-only, no polling)")

    async def stop(self):
        pass

    async def notify(self, message: str):
        if not self._bot or not settings.TELEGRAM_CHAT_ID:
            return
        try:
            await self._bot.send_message(
                chat_id=settings.TELEGRAM_CHAT_ID,
                text=message,
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.error(f"Telegram send failed: {e}")


notifier = TelegramNotifier()
