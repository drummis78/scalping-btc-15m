import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, Request, HTTPException, Query, Header
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
                        b.get("strategy", "donchian")
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
    ts        = datetime.now(timezone.utc).isoformat()

    # Anti-replay por vela (incluye strategy para permitir ambas en misma vela)
    if await is_replay(symbol, side, candle_ts, strategy):
        logger.debug(f"[REPLAY] {symbol} {side} candle={candle_ts}")
        return

    # Posición ya abierta para esta estrategia en este símbolo/lado
    if await get_position("binance", symbol, side, strategy):
        logger.debug(f"[SKIP] {symbol} {side} — posición ya abierta ({strategy})")
        await log_signal(ts, symbol, side, price, sl_price, tp_price,
                         True, f"conflict|pos_abierta|{candle_ts}", 0.0, "blocked_conflict",
                         json.dumps({"outcome": "pending"}), strategy)
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
                                     "blocked_cooldown", "", strategy)
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
                         True, f"circuit_breaker dd={dd:.1f}%", 0.0, "circuit_breaker", "", strategy)
        return

    # Circuit breaker — pérdida diaria
    daily = await get_today_stats()
    if daily and (-daily.get("realized_pnl", 0)) >= (initial * settings.MAX_DAILY_LOSS_PCT / 100):
        await notifier.notify(
            f"⛔️ *MAX DAILY LOSS — Bot pausado hasta mañana*\n"
            f"Pérdida del día: `${daily['realized_pnl']:+.2f}` | Límite: `{settings.MAX_DAILY_LOSS_PCT}%`"
        )
        await log_signal(ts, symbol, side, price, sl_price, tp_price,
                         True, "max_daily_loss", 0.0, "circuit_breaker_daily", "", strategy)
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
                         False, _fund_reason, _fund_impact, "filtered_fundamental", "", strategy)
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
                     json.dumps(result), strategy)

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


def _check_secret(x_secret: str = Header(default="", alias="X-Secret")):
    if settings.WEBHOOK_SECRET and x_secret != settings.WEBHOOK_SECRET:
        raise HTTPException(status_code=403, detail="Forbidden")


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
    async with get_pool().acquire() as conn:
        trades = [dict(r) for r in await conn.fetch(
            "SELECT * FROM trades ORDER BY id DESC LIMIT 50")]
        strat_rows = [dict(r) for r in await conn.fetch("""
            SELECT strategy,
                   COUNT(*) trades,
                   SUM(CASE WHEN pnl>0 THEN 1 ELSE 0 END) wins,
                   ROUND(SUM(pnl)::numeric,2) total_pnl,
                   SUM(CASE WHEN close_reason='tp_hit' THEN 1 ELSE 0 END) tp_hits,
                   SUM(CASE WHEN close_reason='sl_hit' THEN 1 ELSE 0 END) sl_hits
            FROM trades GROUP BY strategy ORDER BY strategy
        """)]

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
        r = strat_by_key.get(s["key"])
        t  = int(r["trades"])  if r else 0
        w  = int(r["wins"])    if r else 0
        tp = int(r["tp_hits"]) if r else 0
        sl = int(r["sl_hits"]) if r else 0
        pnl = float(r["total_pnl"] or 0) if r else 0.0
        wr  = w / t * 100 if t else 0
        wr_color = "#00e676" if wr >= 40 else ("#ffa500" if wr >= 30 else "#ff5252")
        pnl_color = "#00e676" if pnl >= 0 else "#ff5252"
        c = s["color"]
        return f"""
        <div class="scard" data-strat="{s['key']}" style="border-top:3px solid {c}">
            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px">
                <span style="color:{c};font-weight:bold;font-size:13px">{s['label']}</span>
                <span style="color:#555;font-size:10px">{s['sym']} simbolos</span>
            </div>
            <div style="display:flex;gap:20px">
                <div><div class="sval">{t}</div><div class="slbl">trades</div></div>
                <div><div class="sval" style="color:{wr_color}">{wr:.1f}%</div><div class="slbl">WR</div></div>
                <div><div class="sval" style="color:{pnl_color}">${pnl:+.2f}</div><div class="slbl">PnL</div></div>
                <div><div class="sval" style="color:#888">{tp}/{sl}</div><div class="slbl">TP/SL</div></div>
            </div>
        </div>"""

    strat_cards_html = "".join(strat_card(s) for s in STRATS)

    def rows_pos(items):
        if not items:
            return "<tr><td colspan=8 style='color:#555;text-align:center;padding:20px'>Sin posiciones abiertas</td></tr>"
        return "".join(f"""<tr data-strat="{p.get('strategy','')}">
            <td>{badge(p.get('strategy',''))}</td>
            <td><b>{p['symbol']}</b></td>
            <td style="color:{'#00e676' if p['side']=='long' else '#ff5252'}">{p['side'].upper()}</td>
            <td>${p['entry_price']:,.4f}</td>
            <td>${p.get('sl_price') or 0:,.4f}</td>
            <td>${p.get('tp_price') or 0:,.4f}</td>
            <td>{p.get('leverage',3):.0f}x</td>
            <td>{str(p.get('open_time',''))[:16]}</td>
        </tr>""" for p in items)

    def rows_trades(items):
        if not items:
            return "<tr><td colspan=8 style='color:#555;text-align:center;padding:20px'>Sin historial</td></tr>"
        return "".join(f"""<tr data-strat="{t.get('strategy','')}">
            <td>{badge(t.get('strategy',''))}</td>
            <td><b>{t['symbol']}</b></td>
            <td style="color:{'#00e676' if t['side']=='long' else '#ff5252'}">{t['side'].upper()}</td>
            <td>${t['entry_price']:,.4f}</td>
            <td>${t['close_price']:,.4f}</td>
            <td style="color:{'#00e676' if (t['pnl'] or 0)>=0 else '#ff5252'}">${(t['pnl'] or 0):+.2f}</td>
            <td style="color:{'#00e676' if t.get('close_reason')=='tp_hit' else '#ff5252'}">{t.get('close_reason','').replace('_',' ').upper()}</td>
            <td>{str(t.get('close_time',''))[:16]}</td>
        </tr>""" for t in items)

    def rows_signals(items):
        return "".join(f"""<tr data-strat="{s.get('strategy','')}">
            <td>{str(s.get('ts',''))[:16]}</td>
            <td>{badge(s.get('strategy',''))}</td>
            <td><b>{s.get('symbol','')}</b></td>
            <td style="color:{'#00e676' if s.get('side')=='long' else '#ff5252'}">{(s.get('side') or '').upper()}</td>
            <td>${(s.get('price') or 0):,.4f}</td>
            <td style="color:{'#00e676' if s.get('verdict')=='executed' else '#ffa500'}">{(s.get('verdict') or '').replace('_',' ').upper()}</td>
            <td style="font-size:10px;color:#888">{(s.get('fund_reason') or '')[:50]}</td>
        </tr>""" for s in items) or "<tr><td colspan=7 style='color:#555;text-align:center;padding:20px'>Sin senales aun</td></tr>"

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
        <div class="card"><div class="lbl">Trades hoy</div><div class="val">{daily.get('trade_count',0)}</div></div>
        <div class="card"><div class="lbl">Wins hoy</div><div class="val pos">{daily.get('win_count',0)}</div></div>
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
        </div>
    </div>

    <h2>Posiciones abiertas ({len(pos)})</h2>
    <table id="tbl-pos"><thead><tr><th>Estrategia</th><th>Simbolo</th><th>Lado</th><th>Entry</th><th>SL</th><th>TP</th><th>Lev</th><th>Abierto</th></tr></thead>
    <tbody>{rows_pos(pos)}</tbody></table>

    <h2>Ultimos 50 trades</h2>
    <table id="tbl-trades"><thead><tr><th>Estrategia</th><th>Simbolo</th><th>Lado</th><th>Entry</th><th>Exit</th><th>PnL</th><th>Resultado</th><th>Cierre</th></tr></thead>
    <tbody>{rows_trades(trades)}</tbody></table>

    <h2>Bitacora de senales (ultimas 50)</h2>
    <table id="tbl-sig"><thead><tr><th>Hora</th><th>Estrategia</th><th>Simbolo</th><th>Lado</th><th>Precio</th><th>Veredicto</th><th>Razon</th></tr></thead>
    <tbody>{rows_signals(signals)}</tbody></table>

    <div class="refresh-note">Auto-refresh cada 60s</div>
    </div>

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
    function resetBot(group) {{
        const label = group === '15m' ? '15m Bot (Donchian + TCP)' : '1H v2 Bot';
        if (!confirm('⚠️ Resetear ' + label + '?\nSe borrarán posiciones, trades y señales de este bot.')) return;
        fetch('/admin/reset_strategy?group=' + group + '&confirm=RESET', {{method:'POST'}})
            .then(r => r.json())
            .then(d => {{ alert('✅ Reset OK — ' + d.closed + ' posición(es) cerrada(s)'); location.reload(); }})
            .catch(e => alert('❌ Error: ' + e));
    }}
    setTimeout(() => location.reload(), 60000);
    </script>
    </body></html>"""
    return HTMLResponse(html_out)


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
async def api_reset(confirm: str = Query(default="")):
    """Resetea TODOS los bots. Requiere ?confirm=RESET"""
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


@app.post("/admin/reset_strategy")
async def reset_strategy(group: str = Query(default=""), confirm: str = Query(default="")):
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
