import asyncio
import base64
import json
import logging
import os
import secrets
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, Request, HTTPException, Query, Header, Depends
from fastapi.middleware.cors import CORSMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

import ccxt.async_support as ccxt_async

from bot.config import settings
from bot.state import (
    init_db, get_pool, get_all_positions, count_positions, get_total_pnl,
    get_paper_balance, get_today_stats, get_signal_log,
    is_replay, log_signal, get_current_dd_pct, get_peak_balance,
    ensure_daily_stats, is_blocked_logged, get_pending_blocked_signals,
    update_blocked_outcome, get_position, update_position_sl,
    get_consecutive_losses,
)
from bot.exchange import binance_exchange
from bot.scanner import load_top50_symbols, scan_all
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
            logger.info(f"[SCANNER] Iniciando scan — {len(_symbols)} simbolos 15m")
            signals, blocked = await scan_all(_symbols)
            logger.info(f"[SCANNER] {len(signals)} señal(es), {len(blocked)} bloqueada(s) por tendencia 1H")

            # Loguear señales bloqueadas por tendencia o funding (sin marcarlas como processed)
            for b in blocked:
                if not await is_blocked_logged(b["symbol"], b["side"], b["candle_ts"]):
                    ts_b = datetime.now(timezone.utc).isoformat()
                    reason = b.get("blocked_reason", "trend_1h")
                    verdict = "blocked_funding" if reason.startswith("funding") else "blocked_trend"
                    await log_signal(
                        ts_b, b["symbol"], b["side"], b["price"],
                        b["sl_price"], b["tp_price"],
                        True, f"{reason}|{b['candle_ts']}", 0.0, verdict,
                        json.dumps({"outcome": "pending", "blocked_reason": reason}),
                        b.get("strategy", "donchian"),
                        chop_val=b.get("chop_val"),
                    )

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
    strategy  = sig.get("strategy", "donchian")
    chop_val  = sig.get("chop_val")
    ts        = datetime.now(timezone.utc).isoformat()

    # Anti-replay por vela (incluye strategy para permitir ambas en misma vela)
    if await is_replay(symbol, side, candle_ts, strategy):
        logger.debug(f"[REPLAY] {symbol} {side} candle={candle_ts}")
        return

    # Bloquear solo si hay posición abierta en dirección OPUESTA (anti-hedge)
    opposite_side = "short" if side == "long" else "long"
    if await get_position("binance", symbol, opposite_side):
        logger.debug(f"[SKIP] {symbol} {side} — posición opuesta abierta ({opposite_side}), anti-hedge")
        await log_signal(ts, symbol, side, price, sl_price, tp_price,
                         True, f"conflict|hedge_{opposite_side}|{candle_ts}", 0.0, "blocked_conflict",
                         json.dumps({"outcome": "pending"}), strategy, chop_val=chop_val)
        return

    # Max posiciones por grupo de estrategia
    if strategy == "donchian_1h":
        n_pos = await count_positions(strategies=["donchian_1h"])
        max_pos = settings.MAX_POSITIONS_1H
    else:
        n_pos = await count_positions(strategies=["donchian", "tcp"])
        max_pos = settings.MAX_POSITIONS
    if n_pos >= max_pos:
        logger.info(f"[SKIP] {symbol} {side} — max_positions={max_pos} ({strategy})")
        await log_signal(ts, symbol, side, price, sl_price, tp_price,
                         True, "max_positions", 0.0, "skipped_max_positions", "", strategy)
        return

    # Cooldown tras N pérdidas consecutivas
    if settings.COOLDOWN_LOSSES > 0:
        streak, last_close = await get_consecutive_losses()
        if streak <= -settings.COOLDOWN_LOSSES and last_close:
            try:
                last_dt  = datetime.fromisoformat(last_close.replace("Z", "+00:00"))
                cooldown_until = last_dt + timedelta(minutes=settings.COOLDOWN_MINUTES)
                if datetime.now(timezone.utc) < cooldown_until:
                    remaining = int((cooldown_until - datetime.now(timezone.utc)).total_seconds() / 60)
                    logger.info(f"[COOLDOWN] {symbol} bloqueado — {-streak} pérdidas consecutivas, cooldown {remaining}m restantes")
                    await log_signal(ts, symbol, side, price, sl_price, tp_price,
                                     True, f"cooldown|{-streak}_losses_consecutivas", 0.0,
                                     "blocked_cooldown",
                                     json.dumps({"outcome": "pending", "blocked_reason": "cooldown"}),
                                     strategy, chop_val=chop_val)
                    return "cooldown"
            except Exception:
                pass

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
                         True, f"circuit_breaker dd={dd:.1f}%", 0.0, "circuit_breaker", "", strategy,
                         chop_val=chop_val)
        return

    # Circuit breaker — pérdida diaria
    daily = await get_today_stats()
    if daily and (-daily.get("realized_pnl", 0)) >= (initial * settings.MAX_DAILY_LOSS_PCT / 100):
        await notifier.notify(
            f"⛔️ *MAX DAILY LOSS — Bot pausado hasta mañana*\n"
            f"Pérdida del día: `${daily['realized_pnl']:+.2f}` | Límite: `{settings.MAX_DAILY_LOSS_PCT}%`"
        )
        await log_signal(ts, symbol, side, price, sl_price, tp_price,
                         True, "max_daily_loss", 0.0, "circuit_breaker_daily", "", strategy,
                         chop_val=chop_val)
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
                         False, _fund_reason, _fund_impact, "filtered_fundamental",
                         json.dumps({"outcome": "pending", "blocked_reason": "filtered_fundamental"}), strategy,
                         chop_val=chop_val)
        return

    # Ejecutar
    result = await binance_exchange.open_position(
        symbol, side, price, sl_price, tp_price, settings.MAX_LEVERAGE,
        fund_reduce=fund.get("reduce_size", False),
        fund_boost=fund.get("boost_size", False),
        strategy=strategy,
    )

    await log_signal(ts, symbol, side, price, sl_price, tp_price,
                     _fund_allow, _fund_reason, _fund_impact,
                     "executed" if result["status"] == "success" else f"error_{result.get('reason','')}",
                     json.dumps(result), strategy, chop_val=chop_val)

    if result["status"] == "success":
        lev = result.get("leverage", settings.MAX_LEVERAGE)
        liq = price * (1 - 1/lev) if side == "long" else price * (1 + 1/lev)
        sl_str  = f"`${sl_price:,.4f}`"  if sl_price  else "`—`"
        tp_str  = f"`${tp_price:,.4f}`"  if tp_price  else "`—`"
        fund_str = f"Score IA: `{_fund_impact:.1f}`" if _fund_reason else "IA: desactivado"
        mode_str = "PAPER" if settings.TESTNET else "REAL"
        strat_label = "1H Donchian" if strategy == "donchian_1h" else "TCP 15m" if strategy == "tcp" else "15m Donchian"
        await notifier.notify(
            f"🎯 *[{strat_label}] {side.upper()}* `{symbol}`\n"
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
                strat  = pos.get("strategy", "donchian")
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
                    result = await binance_exchange.close_position(symbol, side, close_price, reason,
                                                                   strategy=strat)
                    if result["status"] == "success":
                        # Verificar que la posición fue eliminada de DB (guard contra inconsistencia)
                        still_open = await get_position("binance", symbol, side, strat)
                        if still_open:
                            logger.warning(f"[MONITOR] {symbol} {side} sigue en DB tras cierre exitoso — forzando limpieza")
                            async with get_pool().acquire() as conn:
                                await conn.execute(
                                    "DELETE FROM positions WHERE exchange='binance' AND symbol=$1 AND side=$2 AND strategy=$3",
                                    symbol, side, strat
                                )
                        pnl  = result.get("pnl", 0)
                        icon = "✅" if tp_hit else "🛑"
                        label = "TP ALCANZADO" if tp_hit else "SL TOCADO"
                        strat_label = "1H Donchian" if strat == "donchian_1h" else "TCP 15m" if strat == "tcp" else "15m Donchian"
                        await notifier.notify(
                            f"{icon} *[{strat_label}] {label}*\n"
                            f"`{side.upper()}` `{symbol}`\n"
                            f"📍 Entry: `${pos['entry_price']:,.4f}` → Exit: `${close_price:,.4f}`\n"
                            f"💰 PnL: `{'+'if pnl>=0 else ''}${pnl:.2f}`"
                        )

                # BE Stop: mover SL a entry+fees cuando precio llega al 50% del TP (una sola vez)
                elif settings.BE_STOP_ENABLED and sl and tp and not pos.get("be_set"):
                    entry    = pos["entry_price"]
                    tp_dist  = abs(tp - entry)
                    if tp_dist > 0:
                        be_triggered = (
                            (side == "long"  and price >= entry + tp_dist * 0.5) or
                            (side == "short" and price <= entry - tp_dist * 0.5)
                        )
                        if be_triggered:
                            fee_rt   = 0.0004 * 2
                            be_price = entry * (1 + fee_rt) if side == "long" else entry * (1 - fee_rt)
                            be_price = round(be_price, 6)
                            await update_position_sl("binance", symbol, side, be_price,
                                                     be_set=True, strategy=strat)
                            logger.info(f"[BE-STOP] {symbol} {side} ({strat}) SL → {be_price:.6f} (entry+fees)")
                            await notifier.notify(
                                f"🔒 *BE-STOP* `{side.upper()}` `{symbol}`\n"
                                f"Precio: `${price:,.4f}` | SL movido a `${be_price:,.4f}` (entry+fees)"
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


async def _trend_outcome_evaluator():
    """Cada 5 min evalúa si las señales bloqueadas por tendencia 1H hubieran ganado o perdido."""
    while True:
        await asyncio.sleep(300)
        try:
            pending = await get_pending_blocked_signals()
            if not pending:
                continue
            ex = ccxt_async.binance({
                "options": {"defaultType": "future"},
                "enableRateLimit": True,
            })
            try:
                for sig in pending:
                    symbol   = sig["symbol"]
                    side     = sig["side"]
                    sl_price = float(sig["sl_price"] or 0)
                    tp_price = float(sig["tp_price"] or 0)
                    if not sl_price or not tp_price:
                        continue
                    try:
                        sig_dt = datetime.fromisoformat(sig["ts"].replace("Z", "+00:00"))
                        sig_ms = int(sig_dt.timestamp() * 1000)
                        ohlcv  = await ex.fetch_ohlcv(symbol, "15m", since=sig_ms, limit=100)
                    except Exception:
                        continue

                    outcome = "pending"
                    for candle in ohlcv:
                        c_ts, _, c_high, c_low, _, _ = candle
                        if c_ts <= sig_ms:
                            continue
                        if side == "long":
                            if c_high >= tp_price:
                                outcome = "would_win"; break
                            if c_low <= sl_price:
                                outcome = "would_lose"; break
                        else:
                            if c_low <= tp_price:
                                outcome = "would_win"; break
                            if c_high >= sl_price:
                                outcome = "would_lose"; break

                    if outcome != "pending":
                        await update_blocked_outcome(sig["id"], outcome)
                        logger.info(f"[TREND-EVAL] {symbol} {side} → {outcome}")
            finally:
                await ex.close()
        except Exception as e:
            logger.error(f"[TREND-EVAL] {e}")


# ── App lifespan ──────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _symbols
    db_url = settings.DATABASE_URL
    masked = db_url[:30] + "..." if len(db_url) > 30 else repr(db_url)
    logger.info(f"[STARTUP] DATABASE_URL = {masked}")
    if not settings.WEBHOOK_SECRET:
        logger.warning("[SECURITY] WEBHOOK_SECRET no configurado — dashboard y APIs sin autenticacion. Configurar antes de produccion.")
    await init_db()
    await ensure_daily_stats()

    if settings.TESTNET:
        from bot.state import get_paper_balance, save_paper_balance
        bal = await get_paper_balance()
        if bal == settings.PAPER_BALANCE:
            await save_paper_balance(settings.PAPER_BALANCE)

    notifier.exchange = binance_exchange
    await notifier.start()

    _symbols = await load_top50_symbols()
    logger.info(f"[STARTUP] {len(_symbols)} símbolos 15m top50: {[s['symbol'] for s in _symbols[:5]]}...")

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
    trend_eval_task = asyncio.create_task(_trend_outcome_evaluator())

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

@app.middleware("http")
async def security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Frame-Options"] = "ALLOWALL"
    response.headers["X-Content-Type-Options"] = "nosniff"
    return response


def _check_secret(x_secret: str = Header(default="", alias="X-Secret")):
    if settings.WEBHOOK_SECRET and not secrets.compare_digest(x_secret, settings.WEBHOOK_SECRET):
        raise HTTPException(status_code=403, detail="Forbidden")


def _check_basic_auth(authorization: str = Header(default="", alias="Authorization")):
    """Basic Auth para el dashboard — usa WEBHOOK_SECRET como contraseña (usuario: admin)."""
    if not settings.WEBHOOK_SECRET:
        return  # sin secreto configurado, acceso libre (modo dev)
    valid = False
    if authorization.startswith("Basic "):
        try:
            decoded = base64.b64decode(authorization[6:]).decode("utf-8", errors="replace")
            _, _, password = decoded.partition(":")
            valid = secrets.compare_digest(password, settings.WEBHOOK_SECRET)
        except Exception:
            pass
    if not valid:
        raise HTTPException(
            status_code=401,
            detail="Unauthorized",
            headers={"WWW-Authenticate": 'Basic realm="Scalping Bot"'},
        )


@app.get("/health")
async def health():
    return {"status": "ok", "mode": "paper" if settings.TESTNET else "real",
            "symbols": len(_symbols)}


@app.get("/dashboard", response_class=__import__("fastapi.responses", fromlist=["HTMLResponse"]).HTMLResponse)
async def dashboard():
    from fastapi.responses import HTMLResponse
    bal   = await get_paper_balance() if settings.TESTNET else await binance_exchange.get_balance()
    pos   = [dict(p) for p in await get_all_positions()]
    daily = await get_today_stats() or {}
    signals = await get_signal_log(limit=50)
    from bot.state import ARG_TZ
    today_str = datetime.now(ARG_TZ).strftime("%Y-%m-%d")
    async with get_pool().acquire() as conn:
        trades = [dict(r) for r in await conn.fetch(
            "SELECT * FROM trades ORDER BY id DESC LIMIT 50")]
        today_breakdown = dict(await conn.fetchrow(f"""
            SELECT
                COALESCE(SUM(CASE WHEN close_reason='tp_hit' THEN 1 ELSE 0 END),0)            wins_hoy,
                COALESCE(SUM(CASE WHEN close_reason='sl_hit' AND pnl>=0 THEN 1 ELSE 0 END),0) be_hoy,
                COALESCE(SUM(CASE WHEN close_reason='sl_hit' AND pnl<0  THEN 1 ELSE 0 END),0) losses_hoy,
                COUNT(*) closed_hoy
            FROM trades
            WHERE close_time::timestamptz AT TIME ZONE 'America/Argentina/Buenos_Aires' >= '{today_str}'::date
        """))
        open_hoy = await conn.fetchval(f"""
            SELECT COUNT(*) FROM positions
            WHERE open_time::timestamptz AT TIME ZONE 'America/Argentina/Buenos_Aires' >= '{today_str}'::date
        """)
        strat_rows = [dict(r) for r in await conn.fetch("""
            SELECT strategy,
                   COUNT(*) trades,
                   SUM(CASE WHEN close_reason='tp_hit' THEN 1 ELSE 0 END) wins,
                   SUM(CASE WHEN close_reason='sl_hit' AND pnl<0  THEN 1 ELSE 0 END) losses,
                   SUM(CASE WHEN close_reason='sl_hit' AND pnl>=0 THEN 1 ELSE 0 END) be_hits,
                   ROUND(SUM(pnl)::numeric,2) total_pnl,
                   SUM(CASE WHEN close_reason='tp_hit' THEN 1 ELSE 0 END) tp_hits,
                   SUM(CASE WHEN close_reason='sl_hit' AND pnl<0  THEN 1 ELSE 0 END) sl_hits
            FROM trades GROUP BY strategy ORDER BY strategy
        """)]
        chop_analysis = [dict(r) for r in await conn.fetch("""
            SELECT
                CASE
                    WHEN chop_val IS NULL  THEN '— sin dato (ADX bloqueó antes)'
                    WHEN chop_val >= 61.8 THEN '≥61.8 lateral (CHOP bloqueó)'
                    WHEN chop_val >= 55   THEN '55–61.8 casi lateral'
                    WHEN chop_val >= 40   THEN '40–55 tendencia moderada'
                    ELSE                       '<40 tendencia fuerte'
                END AS rango,
                CASE
                    WHEN chop_val IS NULL  THEN 5
                    WHEN chop_val >= 61.8 THEN 1
                    WHEN chop_val >= 55   THEN 2
                    WHEN chop_val >= 40   THEN 3
                    ELSE                       4
                END AS orden,
                COUNT(*) total,
                SUM(CASE WHEN verdict='executed' THEN 1 ELSE 0 END) ejecutados,
                SUM(CASE WHEN result_json LIKE '%would_lose%' THEN 1 ELSE 0 END) correcto,
                SUM(CASE WHEN result_json LIKE '%would_win%'  THEN 1 ELSE 0 END) error,
                SUM(CASE WHEN result_json LIKE '%pending%'    THEN 1 ELSE 0 END) pendiente,
                ROUND(AVG(chop_val)::numeric, 1) chop_avg
            FROM signal_log
            GROUP BY rango, orden
            ORDER BY orden
        """)]
        filter_summary = [dict(r) for r in await conn.fetch("""
            SELECT verdict,
                   COUNT(*) total,
                   SUM(CASE WHEN result_json LIKE '%would_lose%' THEN 1 ELSE 0 END) correcto,
                   SUM(CASE WHEN result_json LIKE '%would_win%'  THEN 1 ELSE 0 END) error,
                   SUM(CASE WHEN result_json LIKE '%pending%'    THEN 1 ELSE 0 END) pendiente
            FROM signal_log
            WHERE verdict IN ('blocked_trend','blocked_funding','blocked_conflict',
                              'filtered_fundamental','blocked_cooldown')
            GROUP BY verdict ORDER BY total DESC
        """)]
        filter_detail = [dict(r) for r in await conn.fetch("""
            SELECT SPLIT_PART(fund_reason, '|', 1) razon,
                   COUNT(*) total,
                   SUM(CASE WHEN result_json LIKE '%would_lose%' THEN 1 ELSE 0 END) correcto,
                   SUM(CASE WHEN result_json LIKE '%would_win%'  THEN 1 ELSE 0 END) error,
                   SUM(CASE WHEN result_json LIKE '%pending%'    THEN 1 ELSE 0 END) pendiente
            FROM signal_log
            WHERE verdict IN ('blocked_trend','blocked_funding','blocked_conflict',
                              'filtered_fundamental','blocked_cooldown')
            GROUP BY razon ORDER BY total DESC
        """)]
        cooldown_blocks_total = await conn.fetchval(
            "SELECT COUNT(*) FROM signal_log WHERE verdict='blocked_cooldown'"
        ) or 0
        cooldown_blocks_24h = await conn.fetchval("""
            SELECT COUNT(*) FROM signal_log
            WHERE verdict='blocked_cooldown'
            AND ts::timestamptz >= NOW() - INTERVAL '24 hours'
        """) or 0
        cooldown_last = await conn.fetchrow("""
            SELECT fund_reason, ts FROM signal_log
            WHERE verdict='blocked_cooldown' ORDER BY id DESC LIMIT 1
        """)
        fund_status_row = await conn.fetchrow("""
            SELECT title fg_label,
                   (magnitude::numeric) fg_value,
                   ts fg_ts
            FROM fundamental_events WHERE category='sentiment' ORDER BY ts DESC LIMIT 1
        """)
        fund_news_24h = await conn.fetchval("""
            SELECT COUNT(*) FROM fundamental_events
            WHERE category='news' AND ts::timestamptz >= NOW() - INTERVAL '24 hours'
        """) or 0
        fund_last_news = await conn.fetchrow("""
            SELECT title, impact, sentiment, ts FROM fundamental_events
            WHERE category='news' ORDER BY ts DESC LIMIT 1
        """)
        fund_blocks_total = await conn.fetchval(
            "SELECT COUNT(*) FROM signal_log WHERE verdict='filtered_fundamental'"
        ) or 0
        fund_blocks_24h = await conn.fetchval("""
            SELECT COUNT(*) FROM signal_log
            WHERE verdict='filtered_fundamental' AND ts::timestamptz >= NOW() - INTERVAL '24 hours'
        """) or 0

    mode      = "PAPER" if settings.TESTNET else "REAL"
    pnl_d     = daily.get("realized_pnl", 0)
    pnl_class = "pos" if pnl_d >= 0 else "neg"

    STRATS = [
        {"key": "donchian", "label": "15m Donchian", "color": "#00bcd4", "sym": len(_symbols)},
        {"key": "tcp",      "label": "TCP 15m",      "color": "#ff9800", "sym": len(_symbols)},
    ]
    strat_by_key = {r["strategy"]: r for r in strat_rows}

    def badge(strat):
        cfg = next((s for s in STRATS if s["key"] == strat), None)
        c, lbl = (cfg["color"], cfg["label"]) if cfg else ("#aaa", strat)
        return f'<span style="background:{c}22;color:{c};padding:2px 7px;border-radius:4px;font-size:10px;font-weight:bold">{lbl}</span>'

    def strat_card(s):
        r   = strat_by_key.get(s["key"])
        t   = int(r["trades"])   if r else 0
        w   = int(r["wins"])     if r else 0
        be  = int(r["be_hits"])  if r else 0
        lo  = int(r["losses"])   if r else 0
        pnl = float(r["total_pnl"] or 0) if r else 0.0
        wr  = w / (w + lo) * 100 if (w + lo) else 0
        wr_color  = "#00e676" if wr >= 40 else ("#ffa500" if wr >= 30 else "#ff5252")
        pnl_color = "#00e676" if pnl >= 0 else "#ff5252"
        c = s["color"]
        return f"""
        <div class="scard" data-strat="{s['key']}" style="border-top:3px solid {c}">
            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px">
                <span style="color:{c};font-weight:bold;font-size:13px">{s['label']}</span>
                <span style="color:#555;font-size:10px">{s['sym']} simbolos</span>
            </div>
            <div style="display:flex;gap:16px;flex-wrap:wrap">
                <div><div class="sval">{t}</div><div class="slbl">trades</div></div>
                <div><div class="sval" style="color:{wr_color}">{wr:.1f}%</div><div class="slbl">WR</div></div>
                <div><div class="sval" style="color:{pnl_color}">${pnl:+.2f}</div><div class="slbl">PnL</div></div>
                <div><div class="sval" style="color:#00e676">{w}</div><div class="slbl">wins</div></div>
                <div><div class="sval" style="color:#ffd740">{be}</div><div class="slbl">BE</div></div>
                <div><div class="sval" style="color:#ff5252">{lo}</div><div class="slbl">losses</div></div>
            </div>
        </div>"""

    strat_cards_html = "".join(strat_card(s) for s in STRATS)

    def rows_pos(items):
        if not items:
            return "<tr><td colspan=12 style='color:#555;text-align:center;padding:20px'>Sin posiciones abiertas</td></tr>"
        rows = []
        for p in items:
            sym_id = p['symbol'].replace('/', '').replace(':', '')
            rows.append(f"""<tr data-strat="{p.get('strategy','')}" data-sym="{p['symbol']}" data-entry="{p['entry_price']}" data-qty="{p.get('qty',0)}" data-side="{p['side']}">
            <td>{badge(p.get('strategy',''))}</td>
            <td><b>{p['symbol']}</b></td>
            <td style="color:{'#00e676' if p['side']=='long' else '#ff5252'}">{p['side'].upper()}</td>
            <td>${p['entry_price']:,.4f}</td>
            <td id="_px_{sym_id}" style="color:#aaa">—</td>
            <td id="_gross_{sym_id}" style="color:#555">—</td>
            <td id="_fee_{sym_id}" style="color:#555;font-size:10px">—</td>
            <td id="_net_{sym_id}" style="color:#555;font-weight:bold">—</td>
            <td>${p.get('sl_price') or 0:,.4f}</td>
            <td>${p.get('tp_price') or 0:,.4f}</td>
            <td>{p.get('leverage',3):.0f}x</td>
            <td>{str(p.get('open_time',''))[:16]}</td>
        </tr>""")
        return "".join(rows)

    def rows_trades(items):
        if not items:
            return "<tr><td colspan=8 style='color:#555;text-align:center;padding:20px'>Sin historial</td></tr>"
        def _trade_row(t):
            cr  = t.get('close_reason', '')
            pnl = t['pnl'] or 0
            is_tp = cr == 'tp_hit'
            is_be = cr == 'sl_hit' and pnl >= 0
            cr_color  = "#00e676" if is_tp else ("#ffd740" if is_be else "#ff5252")
            pnl_color = "#00e676" if is_tp else ("#ffd740" if is_be else "#ff5252")
            cr_label  = ("TP HIT" if is_tp else ("BE" if is_be else "SL HIT"))
            return f"""<tr data-strat="{t.get('strategy','')}">
            <td>{badge(t.get('strategy',''))}</td>
            <td><b>{t['symbol']}</b></td>
            <td style="color:{'#00e676' if t['side']=='long' else '#ff5252'}">{t['side'].upper()}</td>
            <td>${t['entry_price']:,.4f}</td>
            <td>${t['exit_price']:,.4f}</td>
            <td style="color:{pnl_color}">${pnl:+.2f}</td>
            <td style="color:{cr_color}">{cr_label}</td>
            <td>{str(t.get('close_time',''))[:16]}</td>
        </tr>"""
        return "".join(_trade_row(t) for t in items)

    def rows_signals(items):
        def _outcome_badge(s):
            verdict = s.get('verdict', '')
            rj = s.get('result_json') or ''
            if verdict == 'executed':
                return ''
            if 'would_lose' in rj:
                return '<span style="color:#00e676;font-size:11px" title="Filtro acertó — hubiera perdido">✓ Correcto</span>'
            if 'would_win' in rj:
                return '<span style="color:#ff5252;font-size:11px" title="Filtro erró — hubiera ganado">✗ Error</span>'
            return '<span style="color:#555;font-size:11px">⏳ Pendiente</span>'
        rows = []
        for s in items:
            rows.append(f"""<tr data-strat="{s.get('strategy','')}">
            <td>{str(s.get('ts',''))[:16]}</td>
            <td>{badge(s.get('strategy',''))}</td>
            <td><b>{s.get('symbol','')}</b></td>
            <td style="color:{'#00e676' if s.get('side')=='long' else '#ff5252'}">{(s.get('side') or '').upper()}</td>
            <td>${(s.get('price') or 0):,.4f}</td>
            <td style="color:{'#00e676' if s.get('verdict')=='executed' else '#ffa500'}">{(s.get('verdict') or '').replace('_',' ').upper()}</td>
            <td style="font-size:10px;color:#888">{(s.get('fund_reason') or '')[:50]}</td>
            <td>{_outcome_badge(s)}</td>
        </tr>""")
        return "".join(rows) or "<tr><td colspan=8 style='color:#555;text-align:center;padding:20px'>Sin senales aun</td></tr>"

    _filter_labels = {
        "blocked_trend":        "Tendencia / ADX",
        "blocked_funding":      "Funding Rate",
        "blocked_conflict":     "Conflicto (pos. abierta)",
        "filtered_fundamental": "Filtro Fundamental (IA)",
        "blocked_cooldown":     "Cooldown (racha de pérdidas)",
    }

    def rows_filter_summary():
        if not filter_summary:
            return "<tr><td colspan=6 style='color:#555;text-align:center;padding:16px'>Sin datos aún</td></tr>"
        rows = []
        for r in filter_summary:
            t = int(r["total"]); c = int(r["correcto"]); e = int(r["error"]); p = int(r["pendiente"])
            resolved = c + e
            eff = round(c / resolved * 100, 1) if resolved else 0
            eff_c = "#00e676" if eff >= 55 else ("#ffa500" if eff >= 45 else "#ff5252")
            label = _filter_labels.get(r["verdict"], r["verdict"])
            rows.append(
                f"<tr><td><b>{label}</b></td>"
                f"<td style='color:#aaa'>{t}</td>"
                f"<td style='color:#00e676'>{c}</td>"
                f"<td style='color:#ff5252'>{e}</td>"
                f"<td style='color:#555'>{p}</td>"
                f"<td style='color:{eff_c};font-weight:bold'>{eff}%</td></tr>"
            )
        return "".join(rows)

    def rows_filter_detail():
        if not filter_detail:
            return "<tr><td colspan=6 style='color:#555;text-align:center;padding:16px'>Sin datos aún</td></tr>"
        rows = []
        for r in filter_detail:
            t = int(r["total"]); c = int(r["correcto"]); e = int(r["error"]); p = int(r["pendiente"])
            resolved = c + e
            eff = round(c / resolved * 100, 1) if resolved else 0
            eff_c = "#00e676" if eff >= 55 else ("#ffa500" if eff >= 45 else "#ff5252")
            razon = (r["razon"] or "—").replace("_", " ")
            rows.append(
                f"<tr><td><b>{razon}</b></td>"
                f"<td style='color:#aaa'>{t}</td>"
                f"<td style='color:#00e676'>{c}</td>"
                f"<td style='color:#ff5252'>{e}</td>"
                f"<td style='color:#555'>{p}</td>"
                f"<td style='color:{eff_c};font-weight:bold'>{eff}%</td></tr>"
            )
        return "".join(rows)

    def _build_cooldown_panel():
        streak, last_close = 0, None
        # Calcular streak y cooldown restante en base a datos ya disponibles
        # (usamos get_consecutive_losses que ya fue llamada, pero no está en scope aquí —
        #  en su lugar calculamos desde los trades ya cargados)
        if trades:
            sign = 1 if (trades[0].get("pnl") or 0) > 0 else -1
            for tr in trades:
                pnl = tr.get("pnl") or 0
                if (pnl > 0 and sign == 1) or (pnl <= 0 and sign == -1):
                    streak += sign
                else:
                    break
            last_close = str(trades[0].get("close_time", ""))[:19] if trades else None

        cooldown_active = False
        cooldown_remaining_min = 0
        cooldown_until_str = ""
        if streak <= -settings.COOLDOWN_LOSSES and last_close:
            try:
                last_dt = datetime.fromisoformat(last_close.replace("Z", "+00:00"))
                cooldown_until = last_dt + timedelta(minutes=settings.COOLDOWN_MINUTES)
                now_utc = datetime.now(timezone.utc)
                if now_utc < cooldown_until:
                    cooldown_active = True
                    cooldown_remaining_min = int((cooldown_until - now_utc).total_seconds() / 60)
                    cooldown_until_str = cooldown_until.strftime("%H:%M UTC")
            except Exception:
                pass

        streak_color = "#00e676" if streak > 0 else ("#ff5252" if streak < 0 else "#aaa")
        streak_icon  = "🟢" * min(abs(streak), 5) if streak > 0 else "🔴" * min(abs(streak), 5)
        streak_label = f"+{streak} ganancias" if streak > 0 else (f"{streak} pérdidas" if streak < 0 else "0 (neutro)")

        if cooldown_active:
            cd_html = (f'<div style="margin-top:8px;padding:8px 12px;background:#ff52521a;'
                       f'border:1px solid #ff525244;border-radius:6px;font-size:12px">'
                       f'⛔ <b style="color:#ff5252">COOLDOWN ACTIVO</b> — '
                       f'<b>{cooldown_remaining_min} min restantes</b> (hasta {cooldown_until_str})'
                       f'</div>')
        else:
            cd_html = (f'<div style="margin-top:8px;font-size:11px;color:#555">'
                       f'Sin cooldown activo &nbsp;|&nbsp; '
                       f'Se activa con {settings.COOLDOWN_LOSSES} pérdidas consecutivas, '
                       f'dura {settings.COOLDOWN_MINUTES} min'
                       f'</div>')

        # Último evento de cooldown
        last_cd_html = ""
        if cooldown_last:
            reason = str(cooldown_last["fund_reason"] or "")
            ts_cd  = str(cooldown_last["ts"] or "")[:16]
            last_cd_html = (f'<div style="margin-top:6px;font-size:10px;color:#555">'
                            f'Último bloqueo: <b style="color:#ffa500">{reason}</b> '
                            f'— {ts_cd}'
                            f'</div>')

        return f"""
        <div style="background:#141414;border:1px solid #222;border-radius:10px;padding:14px 18px;margin-bottom:16px">
            <div style="display:flex;gap:24px;flex-wrap:wrap;align-items:flex-start">
                <div>
                    <div style="font-size:10px;color:#555;text-transform:uppercase;letter-spacing:1px;margin-bottom:4px">Racha actual</div>
                    <div style="font-size:20px;font-weight:bold;color:{streak_color}">{streak_icon} {streak_label}</div>
                </div>
                <div>
                    <div style="font-size:10px;color:#555;text-transform:uppercase;letter-spacing:1px;margin-bottom:4px">Señales bloqueadas por cooldown</div>
                    <div style="font-size:20px;font-weight:bold;color:#ffa500">{cooldown_blocks_total}</div>
                    <div style="font-size:10px;color:#444">Total &nbsp;|&nbsp; <b style="color:#ffa500">{cooldown_blocks_24h}</b> últimas 24h</div>
                </div>
                <div style="flex:1">
                    <div style="font-size:10px;color:#555;text-transform:uppercase;letter-spacing:1px;margin-bottom:6px">Configuración</div>
                    <div style="font-size:12px;color:#888;line-height:1.8">
                        Activación: <b style="color:#aaa">{settings.COOLDOWN_LOSSES} pérdidas consecutivas</b><br>
                        Duración: <b style="color:#aaa">{settings.COOLDOWN_MINUTES} minutos</b>
                        {"&nbsp;&nbsp;⏱ <b style='color:#ff5252;font-size:13px'>Restan " + str(cooldown_remaining_min) + " min</b> <span style='color:#555;font-size:10px'>(hasta " + cooldown_until_str + ")</span>" if cooldown_active else "&nbsp;&nbsp;<span style='color:#555;font-size:10px'>— sin cooldown activo</span>"}
                    </div>
                </div>
            </div>
            {cd_html}
            {last_cd_html}
        </div>"""

    def rows_chop_analysis():
        if not chop_analysis:
            return "<tr><td colspan=8 style='color:#555;text-align:center;padding:16px'>Sin datos aún — datos disponibles después del próximo ciclo de scanner</td></tr>"
        rows = []
        for r in chop_analysis:
            t  = int(r["total"])
            ej = int(r["ejecutados"])
            c  = int(r["correcto"])
            e  = int(r["error"])
            p  = int(r["pendiente"])
            resolved = c + e
            eff = round(c / resolved * 100, 1) if resolved else 0
            eff_c = "#00e676" if eff >= 55 else ("#ffa500" if eff >= 45 else ("#ff5252" if resolved else "#555"))
            rango = r["rango"]
            chop_avg = r["chop_avg"]
            avg_str = f"{chop_avg:.1f}" if chop_avg else "—"
            # Colorear el rango según zona
            if "≥61.8" in rango:
                rango_c = "#ff5252"
            elif "55–61.8" in rango:
                rango_c = "#ffa500"
            elif "40–55" in rango:
                rango_c = "#aaa"
            elif "<40" in rango:
                rango_c = "#00e676"
            else:
                rango_c = "#555"
            rows.append(
                f"<tr>"
                f"<td><b style='color:{rango_c}'>{rango}</b></td>"
                f"<td style='color:#888'>{avg_str}</td>"
                f"<td style='color:#aaa'>{t}</td>"
                f"<td style='color:#00bcd4'>{ej}</td>"
                f"<td style='color:#00e676'>{c}</td>"
                f"<td style='color:#ff5252'>{e}</td>"
                f"<td style='color:#555'>{p}</td>"
                f"<td style='color:{eff_c};font-weight:bold'>{eff}%</td>"
                f"</tr>"
            )
        return "".join(rows)

    def _build_fund_panel(fg_row, news_24h, last_news, blocks_total, blocks_24h):
        fg_val   = float(fg_row["fg_value"]) if fg_row else None
        fg_label = str(fg_row["fg_label"]) if fg_row else "—"
        fg_ts    = str(fg_row["fg_ts"])[:16] if fg_row else "—"
        if fg_val is None:
            fg_color = "#555"
            fg_text  = "Sin datos"
        elif fg_val <= 25:
            fg_color = "#ff5252"; fg_text = f"{fg_val:.0f} — Miedo Extremo"
        elif fg_val <= 45:
            fg_color = "#ffa500"; fg_text = f"{fg_val:.0f} — Miedo"
        elif fg_val <= 55:
            fg_color = "#aaa";    fg_text = f"{fg_val:.0f} — Neutral"
        elif fg_val <= 75:
            fg_color = "#00e676"; fg_text = f"{fg_val:.0f} — Codicia"
        else:
            fg_color = "#ff5252"; fg_text = f"{fg_val:.0f} — Codicia Extrema"

        newsapi_ok = bool(settings.NEWSAPI_KEY)
        groq_ok    = bool(settings.GROQ_API_KEY)
        fund_on    = bool(settings.FUNDAMENTAL_ENABLED)

        last_news_html = ""
        if last_news:
            imp = float(last_news["impact"] or 0)
            imp_c = "#ff5252" if imp > 7 else ("#ffa500" if imp > 4 else "#00e676")
            last_news_html = (
                f'<div style="margin-top:10px;padding:8px 12px;background:#1a1a1a;border-radius:6px;font-size:11px">'
                f'<span style="color:#555">Última noticia: </span>'
                f'<b style="color:#e0e0e0">{str(last_news["title"] or "—")[:80]}</b>'
                f' &nbsp;|&nbsp; <span style="color:{imp_c}">Impact: {imp:.1f}</span>'
                f' &nbsp;|&nbsp; <span style="color:#555">{str(last_news["ts"] or "")[:16]}</span>'
                f'</div>'
            )

        def pill(ok, yes_txt, no_txt):
            c = "#00e676" if ok else "#ff5252"
            t = yes_txt if ok else no_txt
            return f'<span style="background:{c}22;color:{c};padding:2px 8px;border-radius:4px;font-size:10px;font-weight:bold">{t}</span>'

        return f"""
        <div style="background:#141414;border:1px solid #222;border-radius:10px;padding:14px 18px;margin-bottom:16px">
            <div style="display:flex;gap:24px;flex-wrap:wrap;align-items:flex-start">
                <div>
                    <div style="font-size:10px;color:#555;text-transform:uppercase;letter-spacing:1px;margin-bottom:4px">Fear &amp; Greed</div>
                    <div style="font-size:20px;font-weight:bold;color:{fg_color}">{fg_text}</div>
                    <div style="font-size:10px;color:#444;margin-top:2px">{fg_ts}</div>
                </div>
                <div>
                    <div style="font-size:10px;color:#555;text-transform:uppercase;letter-spacing:1px;margin-bottom:6px">Servicios</div>
                    <div style="display:flex;gap:6px;flex-wrap:wrap">
                        {pill(fund_on,  "Filtro ON", "Filtro OFF")}
                        {pill(newsapi_ok, "NewsAPI ✓", "NewsAPI ✗")}
                        {pill(groq_ok,   "Groq IA ✓", "Groq IA ✗")}
                    </div>
                </div>
                <div>
                    <div style="font-size:10px;color:#555;text-transform:uppercase;letter-spacing:1px;margin-bottom:4px">Señales bloqueadas</div>
                    <div style="font-size:18px;font-weight:bold;color:#ffa500">{blocks_total}</div>
                    <div style="font-size:10px;color:#444">Total &nbsp;|&nbsp; <b style="color:#ffa500">{blocks_24h}</b> últimas 24h</div>
                </div>
                <div style="flex:1">
                    <div style="font-size:10px;color:#555;text-transform:uppercase;letter-spacing:1px;margin-bottom:4px">Noticias procesadas (24h)</div>
                    <div style="font-size:18px;font-weight:bold;color:#aaa">{news_24h}</div>
                </div>
            </div>
            {last_news_html}
        </div>"""

    html_out = f"""<!DOCTYPE html><html><head>
    <title>Scalping Bot</title>
    <meta charset="utf-8">
    <style>
        *{{box-sizing:border-box}} body{{font-family:'Segoe UI',sans-serif;background:#0a0a0a;color:#e0e0e0;margin:0;padding:20px}}
        .wrap{{max-width:1400px;margin:auto}}
        .hdr{{display:flex;justify-content:space-between;align-items:center;background:#141414;padding:18px 24px;border-radius:14px;border:1px solid #222;margin-bottom:20px}}
        .stats{{display:flex;gap:12px;flex-wrap:wrap;margin-bottom:20px}}
        .card{{background:#161616;border:1px solid #222;border-radius:10px;padding:14px 18px;flex:1;min-width:120px}}
        .val{{font-size:22px;font-weight:bold;color:#00ffcc;margin:4px 0 2px}}
        .lbl{{font-size:10px;color:#555;text-transform:uppercase;letter-spacing:1px}}
        .pos{{color:#00e676}} .neg{{color:#ff5252}}
        h2{{border-left:3px solid #00ffcc;padding-left:12px;margin:20px 0 10px;font-size:13px;color:#aaa;text-transform:uppercase;letter-spacing:1px}}
        table{{width:100%;border-collapse:collapse;background:#111;border-radius:10px;overflow:hidden;margin-bottom:20px}}
        th,td{{padding:10px 13px;text-align:left;border-bottom:1px solid #1a1a1a;font-size:12px}}
        th{{background:#1a1a1a;color:#00ffcc;font-size:10px;text-transform:uppercase;letter-spacing:1px}}
        .scards{{display:flex;gap:14px;flex-wrap:wrap;margin-bottom:20px}}
        .scard{{background:#161616;border:1px solid #222;border-radius:10px;padding:16px;flex:1;min-width:200px}}
        .sval{{font-size:18px;font-weight:bold;color:#00ffcc}}
        .slbl{{font-size:10px;color:#555;margin-top:2px}}
        .tabs{{display:flex;gap:6px;margin-bottom:20px;flex-wrap:wrap}}
        .tab{{padding:7px 16px;border-radius:8px;border:1px solid #333;background:#161616;color:#888;cursor:pointer;font-size:12px;font-weight:bold;transition:all .2s}}
        .tab.active{{border-color:#00ffcc;color:#00ffcc;background:#00ffcc11}}
        tr[data-strat].hidden{{display:none}}
        .refresh-note{{font-size:10px;color:#333;text-align:right;margin-bottom:8px}}
    </style></head><body><div class="wrap">

    <div class="hdr">
        <div>
            <h1 style="margin:0;font-size:18px">SCALPING BOT <span style="color:#555;font-size:12px">{mode}</span></h1>
            <div style="font-size:10px;color:#444;margin-top:3px">
                <span style="color:#00bcd4">15m Donchian</span> ({len(_symbols)} sym) &nbsp;|&nbsp;
                <span style="color:#ff9800">TCP 15m</span> ({len(_symbols)} sym)
            </div>
        </div>
        <div style="text-align:right">
            <div class="lbl">Balance</div>
            <div class="val">${bal:,.2f}</div>
            <button onclick="resetBalance()" style="margin-top:4px;padding:3px 8px;border-radius:6px;border:1px solid #00bcd444;background:#00bcd40d;color:#00bcd4;cursor:pointer;font-size:10px">↺ ${settings.PAPER_BALANCE:.0f}</button>
        </div>
    </div>

    <div style="background:#141414;border:1px solid #222;border-radius:10px;padding:12px 18px;margin-bottom:16px">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
            <span style="font-size:11px;color:#555;text-transform:uppercase;letter-spacing:1px">📈 Objetivo diario {settings.TARGET_DAILY_PCT:.0f}% — ${daily.get('target_usd', 0):.2f}</span>
            <span style="font-size:13px;font-weight:bold;color:{'#00e676' if daily.get('reached') else ('#ffa500' if pnl_d>0 else '#ff5252')}">${pnl_d:+.2f} / ${daily.get('target_usd',0):.2f} ({daily.get('progress_pct',0):.1f}%) {"✅" if daily.get("reached") else ""}</span>
        </div>
        <div style="background:#1a1a1a;border-radius:6px;height:8px;overflow:hidden">
            <div style="width:{min(max(daily.get('progress_pct',0),0),100):.1f}%;height:100%;background:{'#00e676' if daily.get('reached') else ('#ffa500' if pnl_d>0 else '#ff5252')};border-radius:6px"></div>
        </div>
    </div>

    <div class="stats">
        <div class="card"><div class="lbl">PnL hoy</div><div class="val {pnl_class}">${pnl_d:+.2f}</div></div>
        <div class="card"><div class="lbl">Posiciones abiertas</div><div class="val">{len(pos)}</div></div>
        <div class="card"><div class="lbl">Trades hoy</div><div class="val">{today_breakdown['closed_hoy'] + (open_hoy or 0)}</div></div>
        <div class="card"><div class="lbl">Wins hoy</div><div class="val pos">{today_breakdown['wins_hoy']}</div></div>
        <div class="card"><div class="lbl">BE hoy</div><div class="val" style="color:#ffd740">{today_breakdown['be_hoy']}</div></div>
        <div class="card"><div class="lbl">Losses hoy</div><div class="val neg">{today_breakdown['losses_hoy']}</div></div>
    </div>

    <h2>Rendimiento por estrategia</h2>
    <div class="scards">{strat_cards_html}</div>

    <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:10px;margin-bottom:8px">
        <div class="tabs" style="margin-bottom:0">
            <button class="tab active" onclick="filterTab('all',this)">Todas</button>
            <button class="tab" onclick="filterTab('donchian',this)" style="border-color:#00bcd444;color:#00bcd4">15m Donchian</button>
            <button class="tab" onclick="filterTab('tcp',this)" style="border-color:#ff980044;color:#ff9800">TCP 15m</button>
            </div>
        <div style="display:flex;gap:8px">
            <button onclick="resetBot('15m')" style="padding:7px 14px;border-radius:8px;border:1px solid #ff525244;background:#ff52520d;color:#ff5252;cursor:pointer;font-size:11px;font-weight:bold">🗑 Reset 15m Bot</button>
            <button onclick="resetDB()" style="padding:7px 14px;border-radius:8px;border:1px solid #ff525244;background:#ff52520d;color:#ff5252;cursor:pointer;font-size:11px;font-weight:bold">🗑 Reset DB</button>
        </div>
    </div>

    <h2>Posiciones abiertas ({len(pos)})</h2>
    <table id="tbl-pos"><thead><tr><th>Estrategia</th><th>Simbolo</th><th>Lado</th><th>Entry</th><th>Precio</th><th>PnL Bruto</th><th>Fees</th><th>PnL Neto</th><th>SL</th><th>TP</th><th>Lev</th><th>Abierto</th></tr></thead>
    <tbody>{rows_pos(pos)}</tbody></table>

    <h2>Ultimos 50 trades</h2>
    <table id="tbl-trades"><thead><tr><th>Estrategia</th><th>Simbolo</th><th>Lado</th><th>Entry</th><th>Exit</th><th>PnL</th><th>Resultado</th><th>Cierre</th></tr></thead>
    <tbody>{rows_trades(trades)}</tbody></table>

    <h2>Bitacora de senales (ultimas 50)</h2>
    <table id="tbl-sig"><thead><tr><th>Hora</th><th>Estrategia</th><th>Simbolo</th><th>Lado</th><th>Precio</th><th>Veredicto</th><th>Razon</th><th>Resultado Filtro</th></tr></thead>
    <tbody>{rows_signals(signals)}</tbody></table>

    <h2>Cooldown — Racha de pérdidas</h2>
    {_build_cooldown_panel()}

    <h2>Filtro Fundamental (IA)</h2>
    {_build_fund_panel(fund_status_row, fund_news_24h, fund_last_news, fund_blocks_total, fund_blocks_24h)}

    <h2>Efectividad de filtros</h2>
    <div style="font-size:11px;color:#555;margin-bottom:10px">
        <b style="color:#00e676">Correcto</b> = hubiera perdido (filtro acertó) &nbsp;|&nbsp;
        <b style="color:#ff5252">Error</b> = hubiera ganado (filtro bloqueó una ganancia) &nbsp;|&nbsp;
        Efectividad = Correcto / (Correcto + Error)
    </div>
    <table><thead><tr>
        <th>Filtro</th><th>Bloqueadas</th>
        <th style="color:#00e676">Correcto ✓</th>
        <th style="color:#ff5252">Error ✗</th>
        <th>Pendiente</th>
        <th>Efectividad</th>
    </tr></thead>
    <tbody>{rows_filter_summary()}</tbody></table>

    <h2>Desglose por razón de filtro</h2>
    <table><thead><tr>
        <th>Razón</th><th>Bloqueadas</th>
        <th style="color:#00e676">Correcto ✓</th>
        <th style="color:#ff5252">Error ✗</th>
        <th>Pendiente</th>
        <th>Efectividad</th>
    </tr></thead>
    <tbody>{rows_filter_detail()}</tbody></table>

    <h2>Análisis CHOP 1H</h2>
    <div style="font-size:11px;color:#555;margin-bottom:10px">
        El CHOP se calcula solo para señales que pasaron ADX (≥25). Umbral de bloqueo: <b style="color:#ffa500">{settings.REGIME_CHOP_THRESHOLD}</b> &nbsp;|&nbsp;
        <b style="color:#ff5252">≥61.8</b> = lateral = bloqueado &nbsp;|&nbsp;
        <b style="color:#00e676">&lt;40</b> = tendencia fuerte &nbsp;|&nbsp;
        <b>Correcto</b> = filtrado correctamente (hubiera perdido) &nbsp;|&nbsp;
        <b>Error</b> = filtró una ganancia
    </div>
    <table><thead><tr>
        <th>Rango CHOP 1H</th>
        <th>CHOP prom.</th>
        <th>Total señales</th>
        <th style="color:#00bcd4">Ejecutadas</th>
        <th style="color:#00e676">Correcto ✓</th>
        <th style="color:#ff5252">Error ✗</th>
        <th>Pendiente</th>
        <th>Efectividad filtro</th>
    </tr></thead>
    <tbody>{rows_chop_analysis()}</tbody></table>

    <div class="refresh-note">Auto-refresh cada 60s</div>
    </div>

    <div id="_modal" style="display:none;position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,.75);z-index:9999;align-items:center;justify-content:center">
      <div style="background:#1a1a1a;border:1px solid #333;border-radius:12px;padding:24px;width:340px">
        <div id="_mtitle" style="font-size:15px;font-weight:bold;margin-bottom:8px"></div>
        <div id="_mmsg" style="font-size:12px;color:#aaa;margin-bottom:16px;white-space:pre-line"></div>
        <div id="_msec" style="display:none;margin-bottom:16px">
          <div style="font-size:10px;color:#555;margin-bottom:4px">WEBHOOK_SECRET</div>
          <input id="_minput" type="password" placeholder="secret..." style="width:100%;padding:8px;background:#111;border:1px solid #444;border-radius:6px;color:#fff;font-size:13px;box-sizing:border-box">
        </div>
        <div style="display:flex;gap:8px;justify-content:flex-end">
          <button onclick="_mcancel()" style="padding:8px 16px;border-radius:6px;border:1px solid #333;background:transparent;color:#aaa;cursor:pointer">Cancelar</button>
          <button id="_mokbtn" style="padding:8px 16px;border-radius:6px;border:none;background:#ff5252;color:#fff;cursor:pointer;font-weight:bold">Confirmar</button>
        </div>
      </div>
    </div>
    <div id="_toast" style="display:none;position:fixed;bottom:24px;left:50%;transform:translateX(-50%);background:#222;border:1px solid #444;border-radius:8px;padding:12px 20px;font-size:13px;z-index:9999"></div>

    <script>
    let _activeTab = 'all';
    function filterTab(strat, btn) {{
        _activeTab = strat;
        document.querySelectorAll('.tab').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        ['tbl-pos','tbl-trades','tbl-sig'].forEach(id => {{
            document.querySelectorAll('#'+id+' tbody tr[data-strat]').forEach(row => {{
                if (strat === 'all' || row.dataset.strat === strat) row.classList.remove('hidden');
                else row.classList.add('hidden');
            }});
        }});
    }}
    const _hasSecret = {'true' if settings.WEBHOOK_SECRET else 'false'};
    let _mok = null;
    function _showModal(title, msg, okLabel, okColor, cb) {{
        document.getElementById('_mtitle').textContent = title;
        document.getElementById('_mmsg').textContent = msg;
        const sec = document.getElementById('_msec');
        if (_hasSecret) {{ sec.style.display='block'; document.getElementById('_minput').value=''; }}
        else sec.style.display='none';
        const btn = document.getElementById('_mokbtn');
        btn.textContent = okLabel; btn.style.background = okColor;
        document.getElementById('_modal').style.display='flex';
        _mok = cb;
        if (_hasSecret) setTimeout(()=>document.getElementById('_minput').focus(), 50);
    }}
    function _mcancel() {{ document.getElementById('_modal').style.display='none'; _mok=null; }}
    document.getElementById('_modal').addEventListener('click', function(e){{ if(e.target===this) _mcancel(); }});
    document.addEventListener('keydown', function(e){{ if(e.key==='Escape') _mcancel(); if(e.key==='Enter' && _mok) document.getElementById('_mokbtn').click(); }});
    document.getElementById('_mokbtn').addEventListener('click', function(){{
        const secret = _hasSecret ? document.getElementById('_minput').value : '';
        document.getElementById('_modal').style.display='none';
        if (_mok) {{ _mok(secret); _mok=null; }}
    }});
    function _toast(msg, ok) {{
        const t = document.getElementById('_toast');
        t.textContent = msg; t.style.color = ok ? '#00e676' : '#ff5252';
        t.style.display='block';
        setTimeout(()=>{{ t.style.display='none'; if(ok) location.reload(); }}, 2000);
    }}
    function resetBot(group) {{
        const label = group==='15m' ? '15m Bot (Donchian + TCP)' : '1H v2 Bot';
        _showModal('🗑 Resetear ' + label, 'Se borrarán posiciones, trades y señales.', 'Resetear', '#ff5252', function(secret) {{
            fetch('/admin/reset_strategy?group='+group+'&confirm=RESET', {{method:'POST',headers:{{'X-Secret':secret}}}})
                .then(r=>{{ if(!r.ok) throw new Error('Error '+r.status+' — verificá el secret'); return r.json(); }})
                .then(d=>_toast('✅ Reset OK — '+d.closed+' posición(es)', true))
                .catch(e=>_toast('❌ '+e, false));
        }});
    }}
    function resetBalance() {{
        _showModal('↺ Reset Balance', 'Resetea el balance a ${settings.PAPER_BALANCE:.0f}.\\nNo borra posiciones ni trades.', 'Resetear', '#00bcd4', function(secret) {{
            fetch('/admin/reset_balance', {{method:'POST',headers:{{'X-Secret':secret}}}})
                .then(r=>{{ if(!r.ok) throw new Error('Error '+r.status+' — verificá el secret'); return r.json(); }})
                .then(d=>_toast('✅ Balance → $'+d.balance, true))
                .catch(e=>_toast('❌ '+e, false));
        }});
    }}
    function resetDB() {{
        _showModal('🗑 Reset DB COMPLETO', 'Se borrarán TODOS los datos:\\nposiciones, trades, señales y balance.', 'Borrar todo', '#ff5252', function(secret) {{
            fetch('/admin/reset?confirm=RESET', {{method:'POST',headers:{{'X-Secret':secret}}}})
                .then(r=>{{ if(!r.ok) throw new Error('Error '+r.status+' — verificá el secret'); return r.json(); }})
                .then(()=>_toast('✅ DB limpiada', true))
                .catch(e=>_toast('❌ '+e, false));
        }});
    }}
    // Live prices para posiciones abiertas
    const TAKER_FEE = 0.0004;
    const _posData = {{}};
    document.querySelectorAll('#tbl-pos tbody tr[data-sym]').forEach(r => {{
        const symId = r.dataset.sym.replace('/','').replace(':','');
        _posData[symId] = {{ entry: parseFloat(r.dataset.entry), qty: parseFloat(r.dataset.qty), side: r.dataset.side }};
    }});
    function _fmt(v) {{ return '$'+(v>=0?'+':'')+v.toFixed(2); }}
    function _fmtPx(v) {{ return '$'+v.toFixed(v<1?4:v<100?3:2); }}
    async function _livePrices() {{
        const ids = Object.keys(_posData);
        if (!ids.length) return;
        try {{
            const all = await fetch('https://fapi.binance.com/fapi/v1/ticker/price').then(r=>r.json());
            const map = {{}};
            all.forEach(p => map[p.symbol] = parseFloat(p.price));
            ids.forEach(id => {{
                const p = _posData[id];
                const cur = map[id];
                if (!cur) return;
                const gross = (p.side==='long' ? cur-p.entry : p.entry-cur) * p.qty;
                const fees  = (p.entry + cur) * p.qty * TAKER_FEE;
                const net   = gross - fees;
                const pxEl    = document.getElementById('_px_'+id);
                const grossEl = document.getElementById('_gross_'+id);
                const feeEl   = document.getElementById('_fee_'+id);
                const netEl   = document.getElementById('_net_'+id);
                if (pxEl)    pxEl.textContent = _fmtPx(cur);
                if (grossEl) {{ grossEl.textContent = _fmt(gross).replace('$+','+$').replace('$-','-$'); grossEl.style.color = gross>=0?'#00e676':'#ff5252'; }}
                if (feeEl)   {{ feeEl.textContent = '-$'+fees.toFixed(2); feeEl.style.color='#ff9800'; }}
                if (netEl)   {{ netEl.textContent = _fmt(net).replace('$+','+$').replace('$-','-$'); netEl.style.color = net>=0?'#00e676':'#ff5252'; }}
            }});
        }} catch(e) {{}}
    }}
    if (Object.keys(_posData).length) {{ _livePrices(); setInterval(_livePrices, 5000); }}
    setTimeout(() => location.reload(), 60000);
    </script>
    </body></html>"""
    return HTMLResponse(html_out)


@app.get("/api/positions")
@limiter.limit("30/minute")
async def api_positions(request: Request, _: None = Depends(_check_secret)):
    return [dict(p) for p in await get_all_positions()]


@app.get("/api/trades")
@limiter.limit("30/minute")
async def api_trades(request: Request, limit: int = Query(default=100, le=500),
                     _: None = Depends(_check_secret)):
    async with get_pool().acquire() as conn:
        rows = await conn.fetch("SELECT * FROM trades ORDER BY id DESC LIMIT $1", limit)
    return [dict(r) for r in rows]


@app.get("/api/stats")
@limiter.limit("30/minute")
async def api_stats(request: Request, _: None = Depends(_check_secret)):
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
async def api_daily(_: None = Depends(_check_secret)):
    async with get_pool().acquire() as conn:
        rows = await conn.fetch("SELECT * FROM daily_stats ORDER BY date DESC LIMIT 30")
    today = await get_today_stats()
    return {"today": today, "history": [dict(r) for r in rows]}


@app.get("/api/audit")
async def api_audit(limit: int = Query(default=200, le=1000), _: None = Depends(_check_secret)):
    return await get_signal_log(limit=limit)


@app.get("/api/fundamental")
async def api_fundamental(_: None = Depends(_check_secret)):
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
async def api_analytics(_: None = Depends(_check_secret)):
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
        trend_rows = await conn.fetch("""
            SELECT id, ts, symbol, side, price, sl_price, tp_price,
                   CASE WHEN result_json LIKE '%would_win%'  THEN 'filtro_malo'
                        WHEN result_json LIKE '%would_lose%' THEN 'filtro_bueno'
                        ELSE 'pendiente'
                   END AS outcome
            FROM signal_log WHERE verdict = 'blocked_trend'
            ORDER BY id DESC LIMIT 50
        """)
        trend_sum = await conn.fetchrow("""
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN result_json LIKE '%would_win%'  THEN 1 ELSE 0 END) AS filtro_malo,
                   SUM(CASE WHEN result_json LIKE '%would_lose%' THEN 1 ELSE 0 END) AS filtro_bueno,
                   SUM(CASE WHEN result_json NOT LIKE '%would_%'  THEN 1 ELSE 0 END) AS pendiente
            FROM signal_log WHERE verdict = 'blocked_trend'
        """)
        conflict_rows = await conn.fetch("""
            SELECT id, ts, symbol, side, price, sl_price, tp_price, strategy,
                   CASE WHEN result_json LIKE '%would_win%'  THEN 'hubiera_ganado'
                        WHEN result_json LIKE '%would_lose%' THEN 'hubiera_perdido'
                        ELSE 'pendiente'
                   END AS outcome
            FROM signal_log WHERE verdict = 'blocked_conflict'
            ORDER BY id DESC LIMIT 50
        """)
        conflict_sum = await conn.fetchrow("""
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN result_json LIKE '%would_win%'  THEN 1 ELSE 0 END) AS hubiera_ganado,
                   SUM(CASE WHEN result_json LIKE '%would_lose%' THEN 1 ELSE 0 END) AS hubiera_perdido,
                   SUM(CASE WHEN result_json NOT LIKE '%would_%'  THEN 1 ELSE 0 END) AS pendiente
            FROM signal_log WHERE verdict = 'blocked_conflict'
        """)
        funding_rows = await conn.fetch("""
            SELECT id, ts, symbol, side, price, sl_price, tp_price, strategy, fund_reason,
                   CASE WHEN result_json LIKE '%would_win%'  THEN 'filtro_malo'
                        WHEN result_json LIKE '%would_lose%' THEN 'filtro_bueno'
                        ELSE 'pendiente'
                   END AS outcome
            FROM signal_log WHERE verdict = 'blocked_funding'
            ORDER BY id DESC LIMIT 50
        """)
        funding_sum = await conn.fetchrow("""
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN result_json LIKE '%would_win%'  THEN 1 ELSE 0 END) AS filtro_malo,
                   SUM(CASE WHEN result_json LIKE '%would_lose%' THEN 1 ELSE 0 END) AS filtro_bueno,
                   SUM(CASE WHEN result_json NOT LIKE '%would_%'  THEN 1 ELSE 0 END) AS pendiente
            FROM signal_log WHERE verdict = 'blocked_funding'
        """)
        strategy_signals = await conn.fetch("""
            SELECT strategy,
                   COUNT(*) AS total_signals,
                   SUM(CASE WHEN verdict='executed' THEN 1 ELSE 0 END) AS ejecutadas,
                   SUM(CASE WHEN verdict='blocked_trend' THEN 1 ELSE 0 END) AS bloqueadas_trend,
                   SUM(CASE WHEN verdict='filtered_fundamental' THEN 1 ELSE 0 END) AS bloqueadas_ia
            FROM signal_log GROUP BY strategy ORDER BY strategy
        """)
        strategy_trades = await conn.fetch("""
            SELECT strategy,
                   COUNT(*) AS trades,
                   SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) AS wins,
                   ROUND(SUM(pnl)::numeric, 2) AS total_pnl,
                   ROUND(AVG(pnl)::numeric, 2) AS avg_pnl,
                   ROUND(AVG(CASE WHEN pnl > 0 THEN pnl END)::numeric, 2) AS avg_win,
                   ROUND(AVG(CASE WHEN pnl <= 0 THEN pnl END)::numeric, 2) AS avg_loss,
                   SUM(CASE WHEN close_reason='tp_hit' THEN 1 ELSE 0 END) AS tp_hits,
                   SUM(CASE WHEN close_reason='sl_hit' THEN 1 ELSE 0 END) AS sl_hits
            FROM trades GROUP BY strategy ORDER BY strategy
        """)
        # Métricas de rachas: agrupar trades consecutivos y ver distribución
        all_trades_ordered = await conn.fetch(
            "SELECT pnl, close_time FROM trades ORDER BY id ASC"
        )
        # Racha actual (últimos 20 trades)
        streak_val, streak_last = await get_consecutive_losses()
        # Distribución de rachas históricas
        loss_streaks = []
        win_streaks  = []
        cur_streak   = 0
        cur_sign     = None
        for r in all_trades_ordered:
            p = r["pnl"] or 0
            s = 1 if p > 0 else -1
            if cur_sign is None or s == cur_sign:
                cur_streak += 1
                cur_sign    = s
            else:
                if cur_sign == -1: loss_streaks.append(cur_streak)
                else:              win_streaks.append(cur_streak)
                cur_streak = 1
                cur_sign   = s
        if cur_sign == -1: loss_streaks.append(cur_streak)
        elif cur_sign == 1: win_streaks.append(cur_streak)
        streak_stats = {
            "current":           streak_val,
            "cooldown_threshold": settings.COOLDOWN_LOSSES,
            "cooldown_minutes":   settings.COOLDOWN_MINUTES,
            "max_loss_streak":   max(loss_streaks) if loss_streaks else 0,
            "avg_loss_streak":   round(sum(loss_streaks)/len(loss_streaks), 1) if loss_streaks else 0,
            "max_win_streak":    max(win_streaks)  if win_streaks  else 0,
            "loss_streak_dist":  {str(i): loss_streaks.count(i) for i in sorted(set(loss_streaks))},
        }
    # Calcular profit factor y expectancy por estrategia
    strategy_metrics = []
    for r in strategy_trades:
        t     = int(r["trades"])
        w     = int(r["wins"])
        l     = t - w
        aw    = float(r["avg_win"]  or 0)
        al    = float(r["avg_loss"] or 0)
        wr    = w / t if t else 0
        pf    = (aw * w) / (abs(al) * l) if al != 0 and l > 0 else None
        exp   = round(wr * aw + (1 - wr) * al, 3) if aw and al else None
        strategy_metrics.append({
            **dict(r),
            "win_rate":     round(wr * 100, 1),
            "profit_factor": round(pf, 3) if pf else None,
            "expectancy":    exp,
        })

    return {
        "by_symbol":      [dict(r) for r in sym_rows],
        "signals_symbol": [dict(r) for r in sig_sym_rows],
        "by_day":         [dict(r) for r in daily_rows],
        "by_verdict":     [dict(r) for r in sig_rows],
        "filter_dist":    [dict(r) for r in filter_dist],
        "by_hour":        [dict(r) for r in hour_rows],
        "trend_blocked":      [dict(r) for r in trend_rows],
        "trend_summary":      dict(trend_sum) if trend_sum else {},
        "strategy_signals":   [dict(r) for r in strategy_signals],
        "strategy_trades":    strategy_metrics,
        "conflict_blocked":   [dict(r) for r in conflict_rows],
        "conflict_summary":   dict(conflict_sum) if conflict_sum else {},
        "funding_blocked":    [dict(r) for r in funding_rows],
        "funding_summary":    dict(funding_sum) if funding_sum else {},
        "streak_stats":       streak_stats,
    }


@app.post("/admin/reset")
async def api_reset(confirm: str = Query(default=""), _: None = Depends(_check_secret)):
    """Resetea TODOS los bots. Requiere ?confirm=RESET + header X-Secret."""
    if confirm != "RESET":
        raise HTTPException(status_code=403, detail="Forbidden: pass ?confirm=RESET")

    close_results = []
    try:
        close_results = await binance_exchange.emergency_close_all()
    except Exception as e:
        logger.warning(f"[RESET] Error cerrando posiciones: {e}")

    async with get_pool().acquire() as conn:
        await conn.execute("DELETE FROM positions")
        await conn.execute("DELETE FROM trades")
        await conn.execute("DELETE FROM processed_signals")
        await conn.execute("DELETE FROM signal_log")
        await conn.execute("DELETE FROM daily_stats")
        await conn.execute("DELETE FROM account_state")

    logger.warning("[RESET] Base de datos limpiada por solicitud manual")
    return {"ok": True, "exchange_closes": close_results}


@app.post("/admin/reset_balance")
async def reset_balance(_: None = Depends(_check_secret)):
    """Resetea solo el balance a PAPER_BALANCE, sin tocar trades ni posiciones."""
    from bot.state import save_paper_balance
    await save_paper_balance(settings.PAPER_BALANCE)
    return {"ok": True, "balance": settings.PAPER_BALANCE}


@app.post("/admin/reset_strategy")
async def reset_strategy(group: str = Query(default=""), confirm: str = Query(default=""),
                         _: None = Depends(_check_secret)):
    """Resetea solo un bot. group=15m resetea donchian+tcp, group=1h resetea donchian_1h."""
    if confirm != "RESET":
        raise HTTPException(status_code=403, detail="Forbidden: pass ?confirm=RESET")
    if group not in ("15m", "1h"):
        raise HTTPException(status_code=400, detail="group must be '15m' or '1h'")

    strategies = ["donchian", "tcp"] if group == "15m" else ["donchian_1h"]

    positions = [p for p in await get_all_positions() if p.get("strategy") in strategies]
    for pos in positions:
        price = await binance_exchange.get_price(pos["symbol"]) or pos["entry_price"]
        await binance_exchange.close_position(
            pos["symbol"], pos["side"], price, "reset_strategy",
            strategy=pos.get("strategy")
        )

    async with get_pool().acquire() as conn:
        await conn.execute(
            "DELETE FROM positions WHERE strategy = ANY($1::text[])", strategies
        )
        await conn.execute(
            "DELETE FROM trades WHERE strategy = ANY($1::text[])", strategies
        )
        await conn.execute(
            "DELETE FROM signal_log WHERE strategy = ANY($1::text[])", strategies
        )

    logger.warning(f"[RESET-STRATEGY] group={group} strategies={strategies}")
    return {"ok": True, "group": group, "strategies": strategies, "closed": len(positions)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
