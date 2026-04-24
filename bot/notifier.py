import logging
from telegram import Bot
from bot.config import settings
from bot.state import get_all_positions, get_total_pnl, get_today_stats

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

    async def status_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        positions = await get_all_positions()
        total_pnl = await get_total_pnl()
        daily = await get_today_stats()
        mode = "🟡 PAPER" if settings.TESTNET else "🟢 REAL"
        msg = f"📊 *Scalping Bot* — {mode}\n\n"

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
                sl_str = f"SL `${p['sl_price']:,.4f}`" if p.get("sl_price") else "SL `—`"
                tp_str = f"TP `${p['tp_price']:,.4f}`" if p.get("tp_price") else "TP `—`"
                msg += f"  • {p['side'].upper()} `{p['symbol']}` @ `${p['entry_price']:,.4f}` | {sl_str} | {tp_str}\n"

        if daily:
            bar = "🟩" * min(10, int(daily["progress_pct"] / 10)) + "⬜" * max(0, 10 - int(daily["progress_pct"] / 10))
            msg += (f"\n📈 *Objetivo diario:* `${daily['realized_pnl']:+.2f}` / `${daily['target_usd']:.2f}` "
                    f"({daily['progress_pct']:.0f}%)\n{bar}\n")
            msg += f"Trades hoy: {daily['trade_count']} | Wins: {daily['win_count']}\n"

        msg += f"\n💰 *PnL total:* `${total_pnl:+.2f}`"
        await update.message.reply_text(msg, parse_mode="Markdown")

    async def daily_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
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
