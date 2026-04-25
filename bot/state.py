import asyncpg
import hashlib
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional
from bot.config import settings

logger = logging.getLogger("scalping_bot.state")

_pool: Optional[asyncpg.Pool] = None


async def init_pool():
    global _pool
    _pool = await asyncpg.create_pool(settings.DATABASE_URL, min_size=2, max_size=10)


def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("DB pool not initialized")
    return _pool


async def init_db():
    await init_pool()
    async with get_pool().acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS positions (
                exchange    TEXT,
                symbol      TEXT,
                side        TEXT,
                entry_price REAL,
                qty         REAL,
                leverage    REAL,
                sl_price    REAL,
                tp_price    REAL,
                sl_order_id TEXT,
                tp_order_id TEXT,
                open_time   TEXT,
                strategy    TEXT DEFAULT 'donchian',
                be_set      BOOLEAN DEFAULT FALSE,
                PRIMARY KEY (exchange, symbol, side)
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id           SERIAL PRIMARY KEY,
                exchange     TEXT,
                symbol       TEXT,
                side         TEXT,
                entry_price  REAL,
                exit_price   REAL,
                qty          REAL,
                leverage     REAL,
                pnl          REAL,
                close_reason TEXT DEFAULT '',
                open_time    TEXT,
                close_time   TEXT,
                strategy     TEXT DEFAULT 'donchian'
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_signals (
                hash TEXT PRIMARY KEY,
                ts   TEXT NOT NULL
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS account_state (
                exchange TEXT PRIMARY KEY,
                balance  REAL NOT NULL
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS signal_log (
                id          SERIAL PRIMARY KEY,
                ts          TEXT NOT NULL,
                symbol      TEXT NOT NULL,
                side        TEXT NOT NULL,
                price       REAL DEFAULT 0,
                sl_price    REAL DEFAULT 0,
                tp_price    REAL DEFAULT 0,
                fund_allow  INTEGER DEFAULT 1,
                fund_reason TEXT DEFAULT '',
                fund_impact REAL DEFAULT 0,
                verdict     TEXT DEFAULT 'executed',
                result_json TEXT DEFAULT '',
                strategy    TEXT DEFAULT 'donchian'
            )
        """)
        # Migración: agregar columnas strategy a tablas existentes (idempotente)
        for tbl in ("signal_log", "positions", "trades"):
            await conn.execute(
                f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS strategy TEXT DEFAULT 'donchian'"
            )
        await conn.execute(
            "ALTER TABLE positions ADD COLUMN IF NOT EXISTS be_set BOOLEAN DEFAULT FALSE"
        )
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS daily_stats (
                date          TEXT PRIMARY KEY,
                start_balance REAL NOT NULL,
                realized_pnl  REAL DEFAULT 0,
                trade_count   INTEGER DEFAULT 0,
                win_count     INTEGER DEFAULT 0
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS fundamental_events (
                id        SERIAL PRIMARY KEY,
                ts        TEXT NOT NULL,
                category  TEXT NOT NULL,
                source    TEXT NOT NULL,
                title     TEXT,
                sentiment REAL DEFAULT 0,
                magnitude REAL DEFAULT 0,
                impact    REAL DEFAULT 0,
                raw_data  TEXT
            )
        """)
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_fund_ts ON fundamental_events(ts)")


# ── Anti-replay ───────────────────────────────────────────────────────────────

async def is_replay(symbol: str, side: str, candle_ts: str, strategy: str = "donchian", ttl_seconds: int = 300) -> bool:
    key    = hashlib.md5(f"{symbol}|{side}|{candle_ts}|SCALP|{strategy}".encode()).hexdigest()
    cutoff = (datetime.utcnow() - timedelta(seconds=ttl_seconds)).isoformat()
    async with get_pool().acquire() as conn:
        await conn.execute("DELETE FROM processed_signals WHERE ts < $1", cutoff)
        row = await conn.fetchrow("SELECT 1 FROM processed_signals WHERE hash = $1", key)
        if row:
            return True
        await conn.execute("INSERT INTO processed_signals (hash, ts) VALUES ($1, $2)",
                           key, datetime.utcnow().isoformat())
    return False


# ── Positions ─────────────────────────────────────────────────────────────────

async def get_position(exchange: str, symbol: str, side: str) -> Optional[dict]:
    async with get_pool().acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM positions WHERE exchange=$1 AND symbol=$2 AND side=$3",
            exchange, symbol, side
        )
    return dict(row) if row else None


async def get_all_positions() -> list[dict]:
    async with get_pool().acquire() as conn:
        rows = await conn.fetch("SELECT * FROM positions")
    return [dict(r) for r in rows]


async def count_positions() -> int:
    async with get_pool().acquire() as conn:
        return await conn.fetchval("SELECT COUNT(*) FROM positions") or 0


async def save_position(exchange: str, symbol: str, side: str, entry_price: float,
                        qty: float, leverage: float, sl_price: float = None,
                        tp_price: float = None, sl_order_id: str = None,
                        tp_order_id: str = None, strategy: str = "donchian"):
    async with get_pool().acquire() as conn:
        await conn.execute("""
            INSERT INTO positions VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
            ON CONFLICT (exchange, symbol, side) DO UPDATE SET
                entry_price=$4, qty=$5, leverage=$6, sl_price=$7, tp_price=$8,
                sl_order_id=$9, tp_order_id=$10, open_time=$11, strategy=$12
        """, exchange, symbol, side, entry_price, qty, leverage,
             sl_price, tp_price, sl_order_id, tp_order_id,
             datetime.now(timezone.utc).isoformat(), strategy)


async def remove_position(exchange: str, symbol: str, side: str,
                          exit_price: float, close_reason: str = "") -> float:
    pos = await get_position(exchange, symbol, side)
    if not pos:
        return 0.0
    sign    = 1 if side == "long" else -1
    pnl_usd = (exit_price - pos["entry_price"]) * sign * pos["qty"] * pos["leverage"]
    strategy = pos.get("strategy") or "donchian"
    async with get_pool().acquire() as conn:
        await conn.execute("""
            INSERT INTO trades
            (exchange, symbol, side, entry_price, exit_price, qty, leverage, pnl,
             close_reason, open_time, close_time, strategy)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
        """, exchange, symbol, side, pos["entry_price"], exit_price, pos["qty"],
             pos["leverage"], pnl_usd, close_reason, pos["open_time"],
             datetime.now(timezone.utc).isoformat(), strategy)
        await conn.execute(
            "DELETE FROM positions WHERE exchange=$1 AND symbol=$2 AND side=$3",
            exchange, symbol, side
        )
    return pnl_usd


async def update_position_sl(exchange: str, symbol: str, side: str,
                              sl_price: float, be_set: bool = False):
    async with get_pool().acquire() as conn:
        await conn.execute(
            "UPDATE positions SET sl_price=$1, be_set=$2 WHERE exchange=$3 AND symbol=$4 AND side=$5",
            sl_price, be_set, exchange, symbol, side
        )


# ── Paper balance ─────────────────────────────────────────────────────────────

async def get_paper_balance() -> float:
    async with get_pool().acquire() as conn:
        val = await conn.fetchval(
            "SELECT balance FROM account_state WHERE exchange='binance'"
        )
    return val if val is not None else settings.PAPER_BALANCE


async def save_paper_balance(balance: float):
    async with get_pool().acquire() as conn:
        await conn.execute("""
            INSERT INTO account_state (exchange, balance) VALUES ('binance', $1)
            ON CONFLICT (exchange) DO UPDATE SET balance=$1
        """, balance)


# ── Daily stats ───────────────────────────────────────────────────────────────

ARG_TZ = timezone(timedelta(hours=-3))

def _today_utc() -> str:
    return datetime.now(ARG_TZ).strftime("%Y-%m-%d")


async def ensure_daily_stats():
    today = _today_utc()
    balance = await get_paper_balance()
    async with get_pool().acquire() as conn:
        await conn.execute("""
            INSERT INTO daily_stats (date, start_balance) VALUES ($1, $2)
            ON CONFLICT (date) DO NOTHING
        """, today, balance)


async def add_daily_pnl(pnl: float, won: bool):
    today = _today_utc()
    await ensure_daily_stats()
    async with get_pool().acquire() as conn:
        await conn.execute("""
            UPDATE daily_stats
            SET realized_pnl = realized_pnl + $1,
                trade_count  = trade_count + 1,
                win_count    = win_count + $2
            WHERE date = $3
        """, pnl, 1 if won else 0, today)


async def get_today_stats() -> dict:
    today = _today_utc()
    await ensure_daily_stats()
    async with get_pool().acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM daily_stats WHERE date=$1", today)
    if not row:
        return {}
    d = dict(row)
    start      = d["start_balance"] or settings.PAPER_BALANCE
    target_usd = start * (settings.TARGET_DAILY_PCT / 100)
    d["target_usd"]    = round(target_usd, 2)
    d["progress_pct"]  = round(d["realized_pnl"] / target_usd * 100, 1) if target_usd else 0
    d["reached"]       = d["realized_pnl"] >= target_usd
    return d


# ── PnL / DD ──────────────────────────────────────────────────────────────────

async def get_total_pnl() -> float:
    async with get_pool().acquire() as conn:
        return await conn.fetchval("SELECT COALESCE(SUM(pnl), 0) FROM trades") or 0.0


async def get_peak_balance(initial: float) -> float:
    async with get_pool().acquire() as conn:
        rows = await conn.fetch("SELECT pnl FROM trades ORDER BY id ASC")
    balance = peak = initial
    for r in rows:
        balance += (r["pnl"] or 0)
        if balance > peak:
            peak = balance
    return peak


async def get_current_dd_pct(initial: float, current_balance: float) -> float:
    peak = await get_peak_balance(initial)
    if peak <= 0:
        return 0.0
    return max(0.0, (peak - current_balance) / peak * 100)


# ── Signal log ────────────────────────────────────────────────────────────────

async def log_signal(ts: str, symbol: str, side: str, price: float,
                     sl_price: float, tp_price: float,
                     fund_allow: bool, fund_reason: str, fund_impact: float,
                     verdict: str, result_json: str = "", strategy: str = "donchian"):
    async with get_pool().acquire() as conn:
        await conn.execute("""
            INSERT INTO signal_log
            (ts, symbol, side, price, sl_price, tp_price,
             fund_allow, fund_reason, fund_impact, verdict, result_json, strategy)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
        """, ts, symbol, side, price, sl_price, tp_price,
             1 if fund_allow else 0, fund_reason, fund_impact, verdict, result_json, strategy)


async def get_signal_log(limit: int = 200) -> list:
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM signal_log ORDER BY id DESC LIMIT $1", limit
        )
    return [dict(r) for r in rows]


async def is_blocked_logged(symbol: str, side: str, candle_ts: str) -> bool:
    async with get_pool().acquire() as conn:
        row = await conn.fetchrow("""
            SELECT 1 FROM signal_log
            WHERE symbol=$1 AND side=$2 AND verdict='blocked_trend' AND fund_reason LIKE $3
        """, symbol, side, f"%{candle_ts}%")
    return row is not None


async def get_pending_blocked_signals() -> list:
    async with get_pool().acquire() as conn:
        rows = await conn.fetch("""
            SELECT * FROM signal_log
            WHERE verdict IN ('blocked_trend', 'blocked_conflict') AND result_json LIKE '%pending%'
            ORDER BY id ASC LIMIT 100
        """)
    return [dict(r) for r in rows]


async def update_blocked_outcome(signal_id: int, outcome: str):
    checked_at = datetime.now(timezone.utc).isoformat()
    async with get_pool().acquire() as conn:
        await conn.execute(
            "UPDATE signal_log SET result_json = $1 WHERE id = $2",
            json.dumps({"outcome": outcome, "checked_at": checked_at}),
            signal_id
        )
