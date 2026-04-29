import logging
from telegram import Bot, Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from bot.config import settings
from bot.state import get_all_positions, get_total_pnl, get_today_stats

logger = logging.getLogger("scalping_bot.notifier")


class TelegramNotifier:
    def __init__(self, exchange=None):
        self.exchange = exchange
        self.app = ApplicationBuilder().token(settings.TELEGRAM_BOT_TOKEN).build() \
            if settings.TELEGRAM_BOT_TOKEN else None

    def _is_authorized(self, update: Update) -> bool:
        """Solo el chat configurado en TELEGRAM_CHAT_ID puede usar comandos."""
        if not settings.TELEGRAM_CHAT_ID:
            return True
        return str(update.effective_chat.id) == str(settings.TELEGRAM_CHAT_ID)

    def _setup_handlers(self):
        if not self.app:
            return
        self.app.add_handler(CommandHandler("status", self.status_handler))
        self.app.add_handler(CommandHandler("daily", self.daily_handler))
        self.app.add_handler(CommandHandler("close_all", self.close_all_handler))

    async def start(self):
        if not self.app:
            return
        self._setup_handlers()
        await self.app.initialize()
        await self.app.start()
        await self.app.updater.start_polling()
        logger.info("Telegram Bot started")

    async def stop(self):
        if not self.app:
            return
        try:
            await self.app.updater.stop()
            await self.app.stop()
            await self.app.shutdown()
        except Exception:
            pass

    async def notify(self, message: str):
        if not self.app or not settings.TELEGRAM_CHAT_ID:
            return
        try:
            await self.app.bot.send_message(
                chat_id=settings.TELEGRAM_CHAT_ID,
                text=message,
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.error(f"Telegram send failed: {e}")

    async def status_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_authorized(update):
            return
        positions = await get_all_positions()
        total_pnl = await get_total_pnl()
        daily = await get_today_stats()
        mode = "🟡 PAPER" if settings.TESTNET else "🟢 REAL"
        msg = f"📊 *Scalping 15m* — {mode}\n\n"

        balance = None
        if self.exchange:
            try:
                balance = await self.exchange.get_balance()
            except Exception:
                pass
        if balance is not None:
            msg += f"💵 Balance: `${balance:,.2f} USDT`\n\n"

        if not positions:
            msg += "⚪️ Sin posiciones abiertas\n"
        else:
            msg += f"🔵 *{len(positions)} posición(es) abierta(s):*\n"
            for p in [dict(p) for p in positions]:
                sl_str = f"`${p['sl_price']:,.4f}`" if p.get("sl_price") else "`—`"
                tp_str = f"`${p['tp_price']:,.4f}`" if p.get("tp_price") else "`—`"
                msg += f"  • {p['side'].upper()} `{p['symbol']}` @ `${p['entry_price']:,.4f}`\n"
                msg += f"    SL {sl_str} | TP {tp_str}\n"

        if daily:
            bar = "🟩" * min(10, int(daily["progress_pct"] / 10)) + "⬜" * max(0, 10 - int(daily["progress_pct"] / 10))
            reached = "✅ ALCANZADO" if daily["reached"] else "⏳ En progreso"
            msg += (f"\n📈 *Objetivo diario:* `${daily['realized_pnl']:+.2f}` / `${daily['target_usd']:.2f}` "
                    f"({daily['progress_pct']:.0f}%) — {reached}\n{bar}\n"
                    f"Trades: {daily['trade_count']} | Wins: {daily['win_count']}\n")

        msg += f"\n💰 *PnL total:* `${total_pnl:+.2f}`"
        await update.message.reply_text(msg, parse_mode="Markdown")

    async def daily_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_authorized(update):
            return
        daily = await get_today_stats()
        if not daily:
            await update.message.reply_text("Sin datos para hoy.")
            return
        bar = "🟩" * min(10, int(daily["progress_pct"] / 10)) + "⬜" * max(0, 10 - int(daily["progress_pct"] / 10))
        reached = "✅ ALCANZADO" if daily["reached"] else "⏳ En progreso"
        msg = (f"📅 *Objetivo diario — {daily['date']}*\n"
               f"PnL: `${daily['realized_pnl']:+.2f}` / Target: `${daily['target_usd']:.2f}`\n"
               f"{bar} {daily['progress_pct']:.0f}% — {reached}\n"
               f"Trades: {daily['trade_count']} | Wins: {daily['win_count']}")
        await update.message.reply_text(msg, parse_mode="Markdown")

    async def close_all_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_authorized(update):
            return
        if not self.exchange:
            await update.message.reply_text("❌ Exchange no inicializado")
            return
        await update.message.reply_text("🚨 Cerrando todas las posiciones...")
        results = await self.exchange.emergency_close_all()
        lines = [f"• {r['symbol']} {r['side']}: {'OK' if r['status'] == 'success' else r.get('message','FAIL')}"
                 for r in results]
        msg = "✅ *Close All:*\n" + "\n".join(lines) if lines else "✅ Sin posiciones abiertas"
        await update.message.reply_text(msg, parse_mode="Markdown")


notifier = TelegramNotifier()
