import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, Request, HTTPException, Query, Header
from fastapi.middleware.cors import CORSMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from bot.config import settings
from bot.state import (
    init_db, get_pool, get_all_positions, count_positions, get_total_pnl,
    get_paper_balance, get_today_stats, get_signal_log,
    is_replay, log_signal, get_current_dd_pct, get_peak_balance,
    ensure_daily_stats,
)
from bot.exchange import binance_exchange
from bot.scanner import load_symbols, scan_all
from bot.fundamental import fundamental_filter
from bot.notifier import notifier

os.makedirs(os.path.dirname(settings.LOG_FILE), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(settings.LOG_FILE),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("scalping_bot")

# Símbolos cargados una vez al startup
_symbols: list[dict] = []


# ── Background tasks ──────────────────────────────────────────────────────────

async def _signal_scanner():
    """Escanea señales Donchian cada SCAN_INTERVAL segundos."""
    global _symbols
    first_run = True
    while True:
        if not first_run:
            await asyncio.sleep(settings.SCAN_INTERVAL)
        first_run = False
        if not _symbols:
            continue
        try:
            logger.info(f"[SCANNER] Iniciando scan de {len(_symbols)} símbolos")
            signals = await scan_all(_symbols)
            logger.info(f"[SCANNER] {len(signals)} señal(es) detectada(s)")

            # Máximo 2 entradas nuevas por ciclo para evitar concentración de riesgo
            new_entries = 0
            for sig in signals:
                if new_entries >= 2:
                    logger.info(f"[SCANNER] Max entradas por ciclo alcanzado, saltando {sig['symbol']}")
                    break
                result = await _process_signal(sig)
                if result == "executed":
                    new_entries += 1

        except Exception as e:
            logger.error(f"[SCANNER-LOOP] {e}")


async def _process_signal(sig: dict):
    symbol    = sig["symbol"]
    side      = sig["side"]
    price     = sig["price"]
    sl_price  = sig["sl_price"]
    tp_price  = sig["tp_price"]
    candle_ts = sig["candle_ts"]
    ts        = datetime.now(timezone.utc).isoformat()

    # Anti-replay por vela
    if await is_replay(symbol, side, candle_ts):
        logger.debug(f"[REPLAY] {symbol} {side} candle={candle_ts}")
        return

    # Max posiciones
    if await count_positions() >= settings.MAX_POSITIONS:
        logger.info(f"[SKIP] {symbol} {side} — max_positions={settings.MAX_POSITIONS}")
        await log_signal(ts, symbol, side, price, sl_price, tp_price,
                         True, "max_positions", 0.0, "skipped_max_positions")
        return

    # Circuit breaker — DD global
    balance = await get_paper_balance() if settings.TESTNET else await binance_exchange.get_balance()
    initial = settings.PAPER_BALANCE
    dd = await get_current_dd_pct(initial, balance)
    if dd >= settings.MAX_DD_PCT:
        peak = await get_peak_balance(initial)
        open_positions = await get_all_positions()
        close_results  = []
        if open_positions:
            logger.error(f"[CIRCUIT BREAKER] Cerrando {len(open_positions)} posición(es) abiertas por DD excesivo")
            close_results = await binance_exchange.emergency_close_all()
        msg = (f"🚨 *CIRCUIT BREAKER SCALPING*\n"
               f"DD: `{dd:.1f}%` ≥ límite `{settings.MAX_DD_PCT}%`\n"
               f"Balance pico: `${peak:,.2f}` | Actual: `${balance:,.2f}`\n"
               f"Señal bloqueada: `{side.upper()}` `{symbol}`\n"
               + (f"⚠️ {len(open_positions)} posición(es) cerradas de emergencia.\n" if open_positions else "")
               + "Usá `/close_all` si queda alguna posición abierta.")
        logger.error(f"[CIRCUIT BREAKER] dd={dd:.1f}% peak=${peak:.2f} current=${balance:.2f}")
        await notifier.notify(msg)
        await log_signal(ts, symbol, side, price, sl_price, tp_price,
                         True, f"circuit_breaker dd={dd:.1f}%", 0.0, "circuit_breaker")
        return

    # Circuit breaker — pérdida diaria
    daily = await get_today_stats()
    if daily and (-daily.get("realized_pnl", 0)) >= (initial * settings.MAX_DAILY_LOSS_PCT / 100):
        await notifier.notify(
            f"⛔️ *MAX DAILY LOSS — Bot pausado hasta mañana*\n"
            f"Pérdida del día: `${daily['realized_pnl']:+.2f}` | Límite: `{settings.MAX_DAILY_LOSS_PCT}%`"
        )
        await log_signal(ts, symbol, side, price, sl_price, tp_price,
                         True, "max_daily_loss", 0.0, "circuit_breaker_daily")
        return

    # Fundamental filter
    _fund_allow  = True
    _fund_reason = ""
    _fund_impact = 0.0
    fund = await fundamental_filter.check(symbol)
    _fund_allow  = fund["allow"]
    _fund_reason = fund["reason"]
    _fund_impact = fund.get("impact_score", 0.0)
    logger.info(f"[FUNDAMENTAL] {symbol}: {fund['reason']}")

    if not fund["allow"]:
        await notifier.notify(
            f"🧠 *FILTRO IA — BLOQUEADA*\n"
            f"`{side.upper()}` `{symbol}` @ `${price:,.4f}`\n"
            f"Score: `{_fund_impact:.1f}` | `{_fund_reason}`"
        )
        await log_signal(ts, symbol, side, price, sl_price, tp_price,
                         False, _fund_reason, _fund_impact, "filtered_fundamental")
        return

    # Ejecutar
    result = await binance_exchange.open_position(
        symbol, side, price, sl_price, tp_price, settings.MAX_LEVERAGE,
        fund_reduce=fund.get("reduce_size", False),
        fund_boost=fund.get("boost_size", False),
    )

    await log_signal(ts, symbol, side, price, sl_price, tp_price,
                     _fund_allow, _fund_reason, _fund_impact,
                     "executed" if result["status"] == "success" else f"error_{result.get('reason','')}",
                     json.dumps(result))

    if result["status"] == "success":
        lev = result.get("leverage", settings.MAX_LEVERAGE)
        liq = price * (1 - 1/lev) if side == "long" else price * (1 + 1/lev)
        sl_str  = f"`${sl_price:,.4f}`"  if sl_price  else "`—`"
        tp_str  = f"`${tp_price:,.4f}`"  if tp_price  else "`—`"
        fund_str = f"Score IA: `{_fund_impact:.1f}`" if _fund_reason else "IA: desactivado"
        mode_str = "PAPER" if settings.TESTNET else "REAL"
        await notifier.notify(
            f"🎯 *[Scalping 15m] {side.upper()}* `{symbol}`\n"
            f"📍 Entrada: `${price:,.4f}`\n"
            f"🛑 Stop Loss: {sl_str}\n"
            f"✅ Take Profit: {tp_str}\n"
            f"💥 Liquidación aprox: `${liq:,.4f}`\n"
            f"⚡ Apalancamiento: `{lev:.0f}x` | Qty: `{result.get('qty', 0):.4f}`\n"
            f"{fund_str} | `{mode_str}` — `{ts[:19]}`"
        )
        if result.get("sl_warning"):
            await notifier.notify(f"🚨 {result['sl_warning']}")
        return "executed"

    elif result["status"] not in ("skipped",):
        logger.error(f"[OPEN] {symbol} {side}: {result}")


async def _position_monitor():
    """Chequea SL y TP: cada 5s en paper (emula exchange), cada 60s en real (backup)."""
    interval = 5 if settings.TESTNET else 60
    while True:
        await asyncio.sleep(interval)
        try:
            positions = [dict(p) for p in await get_all_positions()]
            if not positions:
                continue

            symbols   = list({p["symbol"] for p in positions})
            prices    = await binance_exchange.get_prices(symbols)

            for pos in positions:
                symbol = pos["symbol"]
                side   = pos["side"]
                price  = prices.get(symbol)
                if price is None:
                    continue

                sl = pos.get("sl_price")
                tp = pos.get("tp_price")

                sl_hit = sl and (
                    (side == "long"  and price <= sl) or
                    (side == "short" and price >= sl)
                )
                tp_hit = tp and (
                    (side == "long"  and price >= tp) or
                    (side == "short" and price <= tp)
                )

                if sl_hit or tp_hit:
                    reason      = "tp_hit" if tp_hit else "sl_hit"
                    close_price = (tp if tp_hit else sl)
                    result = await binance_exchange.close_position(symbol, side, close_price, reason)
                    if result["status"] == "success":
                        pnl  = result.get("pnl", 0)
                        icon = "✅" if tp_hit else "🛑"
                        label = "TP ALCANZADO" if tp_hit else "SL TOCADO"
                        await notifier.notify(
                            f"{icon} *[Scalping 15m] {label}*\n"
                            f"`{side.upper()}` `{symbol}`\n"
                            f"📍 Entry: `${pos['entry_price']:,.4f}` → Exit: `${close_price:,.4f}`\n"
                            f"💰 PnL: `{'+'if pnl>=0 else ''}${pnl:.2f}`"
                        )

        except Exception as e:
            logger.error(f"[POSITION-MONITOR] {e}")


async def _daily_tracker():
    """Reporta progreso del objetivo diario cada hora por Telegram."""
    while True:
        await asyncio.sleep(3600)
        try:
            daily = await get_today_stats()
            if not daily or daily.get("trade_count", 0) == 0:
                continue
            bar = "🟩" * min(10, int(daily["progress_pct"] / 10)) + \
                  "⬜" * max(0, 10 - int(daily["progress_pct"] / 10))
            icon = "✅" if daily["reached"] else "📈"
            await notifier.notify(
                f"{icon} *Objetivo diario — update*\n"
                f"PnL: `${daily['realized_pnl']:+.2f}` / `${daily['target_usd']:.2f}` "
                f"({daily['progress_pct']:.0f}%)\n{bar}\n"
                f"Trades: {daily['trade_count']} | Wins: {daily['win_count']}"
            )
        except Exception as e:
            logger.error(f"[DAILY-TRACKER] {e}")


async def _reconcile_loop():
    while True:
        await asyncio.sleep(300)
        try:
            discrepancies = await binance_exchange.reconcile()
            if discrepancies:
                lines = "\n".join(f"• {d['type']}: {d['symbol']} {d['side']}" for d in discrepancies)
                await notifier.notify(f"⚠️ *RECONCILIACIÓN SCALP — {len(discrepancies)} discrepancia(s)*\n{lines}")
        except Exception as e:
            logger.error(f"[RECONCILE] {e}")


# ── App lifespan ──────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _symbols
    db_url = settings.DATABASE_URL
    masked = db_url[:30] + "..." if len(db_url) > 30 else repr(db_url)
    logger.info(f"[STARTUP] DATABASE_URL = {masked}")
    await init_db()
    await ensure_daily_stats()

    if settings.TESTNET:
        from bot.state import get_paper_balance, save_paper_balance
        bal = await get_paper_balance()
        if bal == settings.PAPER_BALANCE:
            await save_paper_balance(settings.PAPER_BALANCE)

    notifier.exchange = binance_exchange
    await notifier.start()

    _symbols = load_symbols()
    logger.info(f"[STARTUP] {len(_symbols)} símbolos activos: {[s['symbol'] for s in _symbols]}")

    discrepancies = await binance_exchange.reconcile()
    if discrepancies:
        lines = "\n".join(f"• {d['type']}: {d['symbol']}" for d in discrepancies)
        await notifier.notify(f"⚠️ *RECONCILIACIÓN STARTUP SCALP*\n{lines}")

    logger.info(f"[FUNDAMENTAL] FUNDAMENTAL_ENABLED={settings.FUNDAMENTAL_ENABLED!r}")
    await fundamental_filter.start()

    scanner_task    = asyncio.create_task(_signal_scanner())
    monitor_task    = asyncio.create_task(_position_monitor())
    daily_task      = asyncio.create_task(_daily_tracker())
    reconcile_task  = asyncio.create_task(_reconcile_loop())

    logger.info("Scalping Bot started")
    yield

    scanner_task.cancel()
    monitor_task.cancel()
    daily_task.cancel()
    reconcile_task.cancel()
    await fundamental_filter.stop()
    await notifier.stop()
    logger.info("Scalping Bot stopped")


# ── FastAPI ───────────────────────────────────────────────────────────────────

limiter = Limiter(key_func=get_remote_address)
app     = FastAPI(lifespan=lifespan)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


def _check_secret(x_secret: str = Header(default="", alias="X-Secret")):
    if settings.WEBHOOK_SECRET and x_secret != settings.WEBHOOK_SECRET:
        raise HTTPException(status_code=403, detail="Forbidden")


@app.get("/health")
async def health():
    return {"status": "ok", "mode": "paper" if settings.TESTNET else "real",
            "symbols": len(_symbols)}


@app.get("/api/positions")
@limiter.limit("30/minute")
async def api_positions(request: Request):
    return [dict(p) for p in await get_all_positions()]


@app.get("/api/trades")
@limiter.limit("30/minute")
async def api_trades(request: Request, limit: int = Query(default=100)):
    async with get_pool().acquire() as conn:
        rows = await conn.fetch("SELECT * FROM trades ORDER BY id DESC LIMIT $1", limit)
    return [dict(r) for r in rows]


@app.get("/api/stats")
@limiter.limit("30/minute")
async def api_stats(request: Request):
    async with get_pool().acquire() as conn:
        row = await conn.fetchrow(
            "SELECT COUNT(*) total, COALESCE(SUM(pnl),0) total_pnl, "
            "SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) wins FROM trades"
        )
        open_count = await conn.fetchval("SELECT COUNT(*) FROM positions")
    total = row["total"] or 0
    wins  = row["wins"]  or 0
    return {
        "total_trades":   total,
        "wins":           wins,
        "losses":         total - wins,
        "win_rate":       round(wins / total * 100, 2) if total else 0,
        "total_pnl":      round(row["total_pnl"] or 0.0, 2),
        "open_positions": open_count,
        "balance":        round(await get_paper_balance(), 2),
        "strategy":       "SCALP",
    }


@app.get("/api/daily")
async def api_daily():
    async with get_pool().acquire() as conn:
        rows = await conn.fetch("SELECT * FROM daily_stats ORDER BY date DESC LIMIT 30")
    today = await get_today_stats()
    return {"today": today, "history": [dict(r) for r in rows]}


@app.get("/api/audit")
async def api_audit(limit: int = Query(default=200)):
    return await get_signal_log(limit=limit)


@app.get("/api/fundamental")
async def api_fundamental():
    from bot.fundamental import fundamental_filter
    fg = fundamental_filter._last_fear_greed
    async with get_pool().acquire() as conn:
        events_24h = await conn.fetchval(
            "SELECT COUNT(*) FROM fundamental_events WHERE ts::timestamptz >= NOW() - INTERVAL '24 hours' AND category = 'news'"
        )
        last_news = await conn.fetchrow(
            "SELECT title, impact, sentiment, ts FROM fundamental_events WHERE category = 'news' ORDER BY ts DESC LIMIT 1"
        )
        last_fg = await conn.fetchrow(
            "SELECT title, ts FROM fundamental_events WHERE category = 'sentiment' ORDER BY ts DESC LIMIT 1"
        )
    return {
        "fear_greed": {
            "value": fg["value"] if fg else None,
            "label": fg["label"] if fg else None,
            "last_update": dict(last_fg) if last_fg else None,
        },
        "news": {
            "newsapi_configured": bool(settings.NEWSAPI_KEY),
            "groq_configured":    bool(settings.GROQ_API_KEY),
            "events_last_24h":    events_24h or 0,
            "last_event":         dict(last_news) if last_news else None,
        },
        "poll_interval_s": settings.FUNDAMENTAL_POLL_INTERVAL,
        "enabled":         settings.FUNDAMENTAL_ENABLED,
    }


@app.get("/api/analytics")
async def api_analytics():
    async with get_pool().acquire() as conn:
        # Performance por símbolo (trades ejecutados)
        sym_rows = await conn.fetch("""
            SELECT symbol, side,
                COUNT(*) AS total,
                SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN pnl <= 0 THEN 1 ELSE 0 END) AS losses,
                ROUND(SUM(pnl)::numeric, 2) AS total_pnl,
                ROUND(AVG(pnl)::numeric, 2) AS avg_pnl,
                ROUND(AVG(CASE WHEN pnl > 0 THEN pnl END)::numeric, 2) AS avg_win,
                ROUND(AVG(CASE WHEN pnl <= 0 THEN pnl END)::numeric, 2) AS avg_loss,
                SUM(CASE WHEN close_reason='tp_hit' THEN 1 ELSE 0 END) AS tp_count,
                SUM(CASE WHEN close_reason='sl_hit' THEN 1 ELSE 0 END) AS sl_count
            FROM trades GROUP BY symbol, side ORDER BY total_pnl DESC
        """)
        # Señales por símbolo cruzadas con resultado de trades
        sig_sym_rows = await conn.fetch("""
            SELECT s.symbol, s.side,
                COUNT(*) AS señales,
                SUM(CASE WHEN s.verdict = 'executed' THEN 1 ELSE 0 END) AS ejecutadas,
                SUM(CASE WHEN s.verdict != 'executed' THEN 1 ELSE 0 END) AS filtradas,
                SUM(CASE WHEN s.verdict = 'filtered_fundamental' THEN 1 ELSE 0 END) AS filtradas_ia,
                ROUND(AVG(s.fund_impact)::numeric, 2) AS avg_impact
            FROM signal_log s
            GROUP BY s.symbol, s.side ORDER BY señales DESC
        """)
        daily_rows = await conn.fetch("""
            SELECT date, realized_pnl, trade_count, win_count,
                   ROUND((win_count::numeric / NULLIF(trade_count,0) * 100), 1) AS win_rate
            FROM daily_stats ORDER BY date DESC LIMIT 30
        """)
        sig_rows = await conn.fetch("""
            SELECT verdict, COUNT(*) AS cnt,
                   ROUND(AVG(fund_impact)::numeric, 2) AS avg_impact
            FROM signal_log GROUP BY verdict ORDER BY cnt DESC
        """)
        filter_dist = await conn.fetch("""
            SELECT
                CASE WHEN fund_impact >= 7 THEN 'alto (bloqueada)'
                     WHEN fund_impact >= 4 THEN 'medio (reducida)'
                     ELSE 'bajo (ok)'
                END AS bucket,
                COUNT(*) AS cnt
            FROM signal_log WHERE verdict != 'executed'
            GROUP BY bucket
        """)
        hour_rows = await conn.fetch("""
            SELECT EXTRACT(HOUR FROM close_time::timestamptz) AS hour,
                   COUNT(*) AS total,
                   SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) AS wins,
                   ROUND(AVG(pnl)::numeric, 2) AS avg_pnl
            FROM trades WHERE close_time IS NOT NULL
            GROUP BY hour ORDER BY hour
        """)
    return {
        "by_symbol":      [dict(r) for r in sym_rows],
        "signals_symbol": [dict(r) for r in sig_sym_rows],
        "by_day":         [dict(r) for r in daily_rows],
        "by_verdict":     [dict(r) for r in sig_rows],
        "filter_dist":    [dict(r) for r in filter_dist],
        "by_hour":        [dict(r) for r in hour_rows],
    }


@app.post("/admin/reset")
async def api_reset(confirm: str = Query(default="")):
    """Cierra posiciones en el exchange y limpia todas las tablas. Requiere ?confirm=RESET"""
    if confirm != "RESET":
        raise HTTPException(status_code=403, detail="Forbidden: pass ?confirm=RESET")

    # Cerrar posiciones en el exchange
    close_results = []
    try:
        close_results = await binance_exchange.emergency_close_all()
    except Exception as e:
        logger.warning(f"[RESET] Error cerrando posiciones: {e}")

    # Limpiar todas las tablas operativas
    async with get_pool().acquire() as conn:
        await conn.execute("DELETE FROM positions")
        await conn.execute("DELETE FROM trades")
        await conn.execute("DELETE FROM processed_signals")
        await conn.execute("DELETE FROM signal_log")
        await conn.execute("DELETE FROM daily_stats")
        await conn.execute("DELETE FROM account_state")

    logger.warning("[RESET] Base de datos limpiada por solicitud manual")
    return {
        "ok": True,
        "exchange_closes": close_results,
        "message": "DB limpiada. Posiciones, trades y señales borrados.",
    }


@app.post("/admin/reset")
async def admin_reset(x_secret: str = Header(default="", alias="X-Secret")):
    if settings.WEBHOOK_SECRET and x_secret != settings.WEBHOOK_SECRET:
        raise HTTPException(status_code=403, detail="Forbidden")
    import aiosqlite
    async with aiosqlite.connect(settings.DATABASE_PATH) as db:
        await db.execute("DELETE FROM positions")
        await db.execute("DELETE FROM trades")
        await db.execute("DELETE FROM daily_stats")
        await db.commit()
    logger.info("DB reset via /admin/reset")
    return {"status": "reset_ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
