"""
Binance Futures — paper y real.
Hedge Mode: LONG y SHORT simultáneos en el mismo símbolo.
Seguridad: clientOrderId, amount_to_precision, price_to_precision,
           retry con backoff, post-timeout check, SL+TP en exchange.
"""
import asyncio
import logging
import uuid
from typing import Optional

import ccxt.async_support as ccxt_async

from bot.config import settings
from bot.state import (
    get_position, get_all_positions, save_position, remove_position,
    get_paper_balance, save_paper_balance, add_daily_pnl, count_positions,
)

logger = logging.getLogger("scalping_bot.exchange")

EXCHANGE_NAME = "binance"
COMMISSIONS   = 0.0004   # taker futures Binance


def _build_exchange() -> ccxt_async.binance:
    return ccxt_async.binance({
        "apiKey":  settings.BINANCE_API_KEY or None,
        "secret":  settings.BINANCE_API_SECRET or None,
        "options": {
            "defaultType": "future",
            "hedgeMode":   True,
        },
        "enableRateLimit": True,
        "timeout":         10000,
        "verbose":         False,
    })


class BinanceExchange:

    def __init__(self):
        self._lock: dict[str, asyncio.Lock] = {}

    def _get_lock(self, symbol: str, side: str, strategy: str = "") -> asyncio.Lock:
        key = f"{symbol}:{side}:{strategy}"
        if key not in self._lock:
            self._lock[key] = asyncio.Lock()
        return self._lock[key]

    # ── Balance ───────────────────────────────────────────────────────────────

    async def get_balance(self) -> float:
        if settings.TESTNET:
            return await get_paper_balance()
        ex = _build_exchange()
        try:
            bal = await ex.fetch_balance()
            return float(bal["USDT"]["free"])
        finally:
            await ex.close()

    # ── Precio público ────────────────────────────────────────────────────────

    async def get_price(self, symbol: str) -> Optional[float]:
        ex = _build_exchange()
        try:
            ticker = await ex.fetch_ticker(symbol)
            return float(ticker["last"])
        except Exception as e:
            logger.warning(f"[PRICE] {symbol}: {e}")
            return None
        finally:
            await ex.close()

    async def get_prices(self, symbols: list[str]) -> dict[str, float]:
        """Retorna {symbol: price} para múltiples símbolos."""
        results = {}
        ex = _build_exchange()
        try:
            for symbol in symbols:
                try:
                    ticker = await ex.fetch_ticker(symbol)
                    results[symbol] = float(ticker["last"])
                except Exception as e:
                    logger.warning(f"[PRICE] {symbol}: {e}")
        finally:
            await ex.close()
        return results

    # ── Open position ─────────────────────────────────────────────────────────

    async def open_position(self, symbol: str, side: str, price: float,
                            sl_price: float, tp_price: float,
                            leverage: float, fund_reduce: bool = False,
                            fund_boost: bool = False,
                            strategy: str = "donchian") -> dict:
        async with self._get_lock(symbol, side, strategy):
            existing = await get_position(EXCHANGE_NAME, symbol, side, strategy)
            if existing:
                return {"status": "skipped", "reason": "already_open",
                        "symbol": symbol, "side": side}

            balance = await self.get_balance()
            if strategy == "donchian_1h":
                open_count = await count_positions(strategies=["donchian_1h"])
                max_pos    = settings.MAX_POSITIONS_1H
            else:
                open_count = await count_positions(strategies=["donchian", "tcp"])
                max_pos    = settings.MAX_POSITIONS
            if open_count >= max_pos:
                return {"status": "skipped", "reason": "max_positions",
                        "symbol": symbol, "side": side}

            risk = settings.RISK_PER_TRADE
            if fund_reduce:
                risk *= 0.5
            elif fund_boost:
                risk = min(risk * 1.25, 0.10)

            notional = balance * risk * leverage
            qty      = notional / price

            if settings.TESTNET:
                return await self._paper_open(symbol, side, price, qty, leverage, sl_price, tp_price, strategy)
            else:
                return await self._real_open(symbol, side, price, qty, leverage, sl_price, tp_price, strategy)

    async def _paper_open(self, symbol, side, price, qty, leverage, sl_price, tp_price, strategy="donchian") -> dict:
        commission = price * qty * COMMISSIONS
        balance = await get_paper_balance()
        margin  = price * qty / leverage
        if balance < margin + commission:
            return {"status": "error", "reason": "insufficient_balance",
                    "symbol": symbol, "side": side}
        await save_paper_balance(balance - margin - commission)
        await save_position(EXCHANGE_NAME, symbol, side, price, qty, leverage, sl_price, tp_price, strategy=strategy)
        logger.info(f"[PAPER] OPEN {side.upper()} {symbol} qty={qty:.6f} @ {price:.4f} SL={sl_price:.4f} TP={tp_price:.4f}")
        return {"status": "success", "symbol": symbol, "side": side,
                "price": price, "qty": qty, "sl_price": sl_price, "tp_price": tp_price}

    async def _real_open(self, symbol, side, price, qty, leverage, sl_price, tp_price, strategy="donchian") -> dict:
        ex = _build_exchange()
        order_side    = "buy"  if side == "long"  else "sell"
        position_side = "LONG" if side == "long"  else "SHORT"
        sl_side       = "sell" if side == "long"  else "buy"
        client_id     = f"scalp_{symbol.replace('/','').replace(':','')[:8]}_{side[:1]}_{uuid.uuid4().hex[:8]}"
        try:
            # Set leverage
            try:
                await ex.set_leverage(int(leverage), symbol)
            except Exception:
                pass

            qty_str  = ex.amount_to_precision(symbol, qty)
            sl_str   = ex.price_to_precision(symbol, sl_price)
            tp_str   = ex.price_to_precision(symbol, tp_price)

            # Market entry
            entry = await self._retry(
                ex.create_order, symbol, "MARKET", order_side, float(qty_str),
                params={"positionSide": position_side, "newClientOrderId": client_id}
            )
            fill_price = float(entry.get("average") or entry.get("price") or price)

            # SL
            sl_order_id = None
            try:
                sl_ord = await ex.create_order(
                    symbol, "STOP_MARKET", sl_side, float(qty_str),
                    params={"positionSide": position_side,
                            "stopPrice":    sl_str,
                            "closePosition": True,
                            "newClientOrderId": client_id + "_sl"}
                )
                sl_order_id = sl_ord.get("id")
            except Exception as e:
                logger.error(f"[SL] Placement falló {symbol} {side}: {e}")

            # TP
            tp_order_id = None
            try:
                tp_ord = await ex.create_order(
                    symbol, "TAKE_PROFIT_MARKET", sl_side, float(qty_str),
                    params={"positionSide": position_side,
                            "stopPrice":    tp_str,
                            "closePosition": True,
                            "newClientOrderId": client_id + "_tp"}
                )
                tp_order_id = tp_ord.get("id")
            except Exception as e:
                logger.error(f"[TP] Placement falló {symbol} {side}: {e}")

            await save_position(EXCHANGE_NAME, symbol, side, fill_price, float(qty_str),
                                leverage, sl_price, tp_price, sl_order_id, tp_order_id, strategy=strategy)
            return {"status": "success", "symbol": symbol, "side": side,
                    "price": fill_price, "qty": float(qty_str),
                    "sl_price": sl_price, "tp_price": tp_price,
                    "sl_warning": None if sl_order_id else f"SL no colocado en {symbol}"}
        except Exception as e:
            logger.error(f"[OPEN] {symbol} {side}: {e}")
            return {"status": "error", "message": str(e), "symbol": symbol, "side": side}
        finally:
            await ex.close()

    # ── Close position ────────────────────────────────────────────────────────

    async def close_position(self, symbol: str, side: str,
                             price: float, close_reason: str = "",
                             strategy: str = None) -> dict:
        async with self._get_lock(symbol, side, strategy or ""):
            pos = await get_position(EXCHANGE_NAME, symbol, side, strategy)
            if not pos:
                return {"status": "skipped", "reason": "not_found",
                        "symbol": symbol, "side": side}
            pos = dict(pos)

            if settings.TESTNET:
                return await self._paper_close(pos, price, close_reason)
            else:
                return await self._real_close(pos, price, close_reason)

    async def _paper_close(self, pos: dict, price: float, close_reason: str) -> dict:
        if price <= 0:
            price = pos["entry_price"]
        is_be = bool(pos.get("be_set")) and close_reason == "sl_hit"
        pnl = await remove_position(EXCHANGE_NAME, pos["symbol"], pos["side"], price, close_reason,
                                    strategy=pos.get("strategy"), is_be=is_be)
        commission = price * pos["qty"] * COMMISSIONS
        margin     = pos["entry_price"] * pos["qty"] / pos["leverage"]
        balance    = await get_paper_balance()
        await save_paper_balance(balance + margin + pnl - commission)
        await add_daily_pnl(pnl, pnl > 0 and not is_be, is_be=is_be)
        logger.info(f"[PAPER] CLOSE {pos['side'].upper()} {pos['symbol']} @ {price:.4f} PnL={pnl:+.2f} ({close_reason}) be={is_be}")
        return {"status": "success", "symbol": pos["symbol"], "side": pos["side"],
                "price": price, "pnl": pnl, "close_reason": close_reason, "is_be": is_be}

    async def _real_close(self, pos: dict, price: float, close_reason: str) -> dict:
        ex = _build_exchange()
        symbol        = pos["symbol"]
        side          = pos["side"]
        order_side    = "sell" if side == "long" else "buy"
        position_side = "LONG" if side == "long" else "SHORT"
        qty_str       = ex.amount_to_precision(symbol, pos["qty"])
        client_id     = f"scalp_close_{symbol.replace('/','').replace(':','')[:8]}_{uuid.uuid4().hex[:8]}"
        try:
            # Cancelar SL y TP pendientes
            for oid in [pos.get("sl_order_id"), pos.get("tp_order_id")]:
                if oid:
                    try:
                        await ex.cancel_order(oid, symbol)
                    except Exception:
                        pass

            order = await self._retry(
                ex.create_order, symbol, "MARKET", order_side, float(qty_str),
                params={"positionSide": position_side,
                        "reduceOnly": True,
                        "newClientOrderId": client_id}
            )
            fill_price = float(order.get("average") or order.get("price") or price)
            is_be = bool(pos.get("be_set")) and close_reason == "sl_hit"
            pnl = await remove_position(EXCHANGE_NAME, symbol, side, fill_price, close_reason,
                                        strategy=pos.get("strategy"), is_be=is_be)
            await add_daily_pnl(pnl, pnl > 0 and not is_be, is_be=is_be)
            return {"status": "success", "symbol": symbol, "side": side,
                    "price": fill_price, "pnl": pnl, "close_reason": close_reason, "is_be": is_be}
        except Exception as e:
            logger.error(f"[CLOSE] {symbol} {side}: {e}")
            return {"status": "error", "message": str(e), "symbol": symbol, "side": side}
        finally:
            await ex.close()

    # ── Emergency close all ───────────────────────────────────────────────────

    async def emergency_close_all(self) -> list[dict]:
        positions = await get_all_positions()
        results   = []
        for pos in [dict(p) for p in positions]:
            price = await self.get_price(pos["symbol"]) or pos["entry_price"]
            r = await self.close_position(pos["symbol"], pos["side"], price, "emergency_close",
                                          strategy=pos.get("strategy"))
            results.append(r)
        return results

    # ── Reconcile ─────────────────────────────────────────────────────────────

    async def reconcile(self) -> list[dict]:
        """Compara DB vs exchange. Solo en modo real."""
        if settings.TESTNET:
            return []
        ex = _build_exchange()
        discrepancies = []
        try:
            db_positions  = {(dict(p)["symbol"], dict(p)["side"]): dict(p)
                             for p in await get_all_positions()}
            raw_positions = await ex.fetch_positions()
            ex_positions  = {(p["symbol"], p["side"].lower()): p
                             for p in raw_positions if float(p.get("contracts", 0)) > 0}

            for key, pos in db_positions.items():
                if key not in ex_positions:
                    discrepancies.append({"type": "ghost_in_db", "symbol": key[0], "side": key[1]})

            for key, pos in ex_positions.items():
                if key not in db_positions:
                    discrepancies.append({"type": "orphan_on_exchange", "symbol": key[0], "side": key[1]})
        except Exception as e:
            logger.error(f"[RECONCILE] {e}")
        finally:
            await ex.close()
        return discrepancies

    # ── Retry helper ──────────────────────────────────────────────────────────

    async def _retry(self, fn, *args, **kwargs):
        last_exc = None
        for attempt, delay in enumerate([0, 1, 2, 4]):
            if delay:
                await asyncio.sleep(delay)
            try:
                return await fn(*args, **kwargs)
            except (ccxt_async.RateLimitExceeded, ccxt_async.NetworkError) as e:
                last_exc = e
                logger.warning(f"[RETRY] attempt={attempt+1} error={type(e).__name__}")
            except ccxt_async.RequestTimeout as e:
                last_exc = e
                # Timeout ≠ orden fallida — esperar y verificar posición abierta en exchange
                logger.warning(f"[TIMEOUT] attempt={attempt+1} — esperando 5s para verificar posición")
                await asyncio.sleep(5)
                try:
                    ex = _build_exchange()
                    positions = await ex.fetch_positions()
                    await ex.close()
                    # Si encontramos la posición abierta, la orden ejecutó — retornar success sintético
                    symbol = args[0] if args else kwargs.get("symbol", "")
                    side_arg = args[2] if len(args) > 2 else kwargs.get("side", "")
                    pos_side = "LONG" if side_arg == "buy" else "SHORT"
                    matching = [p for p in positions
                                if p.get("symbol") == symbol
                                and p.get("side", "").upper() == pos_side
                                and float(p.get("contracts", 0)) > 0]
                    if matching:
                        logger.info(f"[TIMEOUT-RECOVERY] Orden ejecutó en exchange — posición encontrada {symbol}")
                        return {"average": float(matching[0].get("entryPrice", 0)),
                                "price": float(matching[0].get("entryPrice", 0)),
                                "id": "timeout_recovery"}
                except Exception as verify_err:
                    logger.warning(f"[TIMEOUT-VERIFY] No se pudo verificar posición: {verify_err}")
        raise last_exc


binance_exchange = BinanceExchange()
