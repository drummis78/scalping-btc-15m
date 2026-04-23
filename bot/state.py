import aiosqlite
import hashlib
import logging
from datetime import datetime, timedelta, timezone
from bot.config import settings

logger = logging.getLogger("scalping_bot.state")

STRATEGY = "SCALP"


async def init_db():
    async with aiosqlite.connect(settings.DATABASE_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS positions (
                exchange    TEXT,
                symbol      TEXT,
                side        TEXT,
                entry_price REAL,
                qty         REAL,
                leverage    REAL,
                sl_price    REAL DEFAULT NULL,
                tp_price    REAL DEFAULT NULL,
                sl_order_id TEXT DEFAULT NULL,
                tp_order_id TEXT DEFAULT NULL,
                open_time   TEXT,
                PRIMARY KEY (exchange, symbol, side)
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                exchange    TEXT,
                symbol      TEXT,
                side        TEXT,
                entry_price REAL,
                exit_price  REAL,
                qty         REAL,
                leverage    REAL,
                pnl         REAL,
                close_reason TEXT DEFAULT '',
                open_time   TEXT,
                close_time  TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS processed_signals (
                hash TEXT PRIMARY KEY,
                ts   TEXT NOT NULL
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS account_state (
                exchange TEXT PRIMARY KEY,
                balance  REAL NOT NULL
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS signal_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
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
                result_json TEXT DEFAULT ''
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS daily_stats (
                date          TEXT PRIMARY KEY,
                start_balance REAL NOT NULL,
                realized_pnl  REAL DEFAULT 0,
                trade_count   INTEGER DEFAULT 0,
                win_count     INTEGER DEFAULT 0
            )
        """)
        await db.commit()


# ── Anti-replay ──────────────────────────────────────────────────────────────

async def is_replay(symbol: str, side: str, candle_ts: str, ttl_seconds: int = 300) -> bool:
    key = hashlib.md5(f"{symbol}|{side}|{candle_ts}|SCALP".encode()).hexdigest()
    cutoff = (datetime.utcnow() - timedelta(seconds=ttl_seconds)).isoformat()
    async with aiosqlite.connect(settings.DATABASE_PATH) as db:
        await db.execute("DELETE FROM processed_signals WHERE ts < ?", (cutoff,))
        async with db.execute("SELECT 1 FROM processed_signals WHERE hash = ?", (key,)) as cur:
            if await cur.fetchone():
                await db.commit()
                return True
        await db.execute("INSERT INTO processed_signals (hash, ts) VALUES (?, ?)",
                         (key, datetime.utcnow().isoformat()))
        await db.commit()
    return False


# ── Positions ────────────────────────────────────────────────────────────────

async def get_position(exchange: str, symbol: str, side: str):
    async with aiosqlite.connect(settings.DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM positions WHERE exchange=? AND symbol=? AND side=?",
            (exchange, symbol, side)
        ) as cur:
            return await cur.fetchone()


async def get_all_positions():
    async with aiosqlite.connect(settings.DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM positions") as cur:
            return await cur.fetchall()


async def count_positions() -> int:
    async with aiosqlite.connect(settings.DATABASE_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM positions") as cur:
            row = await cur.fetchone()
            return row[0] if row else 0


async def save_position(exchange: str, symbol: str, side: str, entry_price: float,
                        qty: float, leverage: float, sl_price: float = None,
                        tp_price: float = None, sl_order_id: str = None,
                        tp_order_id: str = None):
    async with aiosqlite.connect(settings.DATABASE_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO positions VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (exchange, symbol, side, entry_price, qty, leverage,
             sl_price, tp_price, sl_order_id, tp_order_id,
             datetime.now(timezone.utc).isoformat())
        )
        await db.commit()


async def remove_position(exchange: str, symbol: str, side: str,
                          exit_price: float, close_reason: str = "") -> float:
    pos = await get_position(exchange, symbol, side)
    if not pos:
        return 0.0
    pos = dict(pos)
    sign = 1 if side == "long" else -1
    pnl_usd = (exit_price - pos["entry_price"]) * sign * pos["qty"] * pos["leverage"]

    async with aiosqlite.connect(settings.DATABASE_PATH) as db:
        await db.execute(
            """INSERT INTO trades
               (exchange, symbol, side, entry_price, exit_price, qty, leverage, pnl,
                close_reason, open_time, close_time)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (exchange, symbol, side, pos["entry_price"], exit_price, pos["qty"],
             pos["leverage"], pnl_usd, close_reason, pos["open_time"],
             datetime.now(timezone.utc).isoformat())
        )
        await db.execute(
            "DELETE FROM positions WHERE exchange=? AND symbol=? AND side=?",
            (exchange, symbol, side)
        )
        await db.commit()
    return pnl_usd


# ── Paper balance ─────────────────────────────────────────────────────────────

async def get_paper_balance() -> float:
    async with aiosqlite.connect(settings.DATABASE_PATH) as db:
        async with db.execute(
            "SELECT balance FROM account_state WHERE exchange='binance'"
        ) as cur:
            row = await cur.fetchone()
            return row[0] if row else settings.PAPER_BALANCE


async def save_paper_balance(balance: float):
    async with aiosqlite.connect(settings.DATABASE_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO account_state (exchange, balance) VALUES ('binance', ?)",
            (balance,)
        )
        await db.commit()


# ── Daily stats ───────────────────────────────────────────────────────────────

def _today_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


async def ensure_daily_stats():
    today = _today_utc()
    async with aiosqlite.connect(settings.DATABASE_PATH) as db:
        async with db.execute("SELECT 1 FROM daily_stats WHERE date=?", (today,)) as cur:
            if await cur.fetchone():
                return
        balance = await get_paper_balance()
        await db.execute(
            "INSERT INTO daily_stats (date, start_balance) VALUES (?,?)",
            (today, balance)
        )
        await db.commit()


async def add_daily_pnl(pnl: float, won: bool):
    today = _today_utc()
    await ensure_daily_stats()
    async with aiosqlite.connect(settings.DATABASE_PATH) as db:
        await db.execute(
            """UPDATE daily_stats
               SET realized_pnl = realized_pnl + ?,
                   trade_count  = trade_count + 1,
                   win_count    = win_count + ?
               WHERE date = ?""",
            (pnl, 1 if won else 0, today)
        )
        await db.commit()


async def get_today_stats() -> dict:
    today = _today_utc()
    await ensure_daily_stats()
    async with aiosqlite.connect(settings.DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM daily_stats WHERE date=?", (today,)) as cur:
            row = await cur.fetchone()
    if not row:
        return {}
    d = dict(row)
    start = d["start_balance"] or settings.PAPER_BALANCE
    target_usd = start * (settings.TARGET_DAILY_PCT / 100)
    d["target_usd"] = round(target_usd, 2)
    d["progress_pct"] = round(d["realized_pnl"] / target_usd * 100, 1) if target_usd else 0
    d["reached"] = d["realized_pnl"] >= target_usd
    return d


# ── PnL / DD ─────────────────────────────────────────────────────────────────

async def get_total_pnl() -> float:
    async with aiosqlite.connect(settings.DATABASE_PATH) as db:
        async with db.execute("SELECT SUM(pnl) FROM trades") as cur:
            row = await cur.fetchone()
            return row[0] or 0.0


async def get_peak_balance(initial: float) -> float:
    async with aiosqlite.connect(settings.DATABASE_PATH) as db:
        async with db.execute("SELECT pnl FROM trades ORDER BY id ASC") as cur:
            rows = await cur.fetchall()
    balance = peak = initial
    for (pnl,) in rows:
        balance += (pnl or 0)
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
                     verdict: str, result_json: str = ""):
    async with aiosqlite.connect(settings.DATABASE_PATH) as db:
        await db.execute(
            """INSERT INTO signal_log
               (ts, symbol, side, price, sl_price, tp_price,
                fund_allow, fund_reason, fund_impact, verdict, result_json)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (ts, symbol, side, price, sl_price, tp_price,
             1 if fund_allow else 0, fund_reason, fund_impact, verdict, result_json)
        )
        await db.commit()


async def get_signal_log(limit: int = 200) -> list:
    async with aiosqlite.connect(settings.DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM signal_log ORDER BY id DESC LIMIT ?", (limit,)
        ) as cur:
            rows = await cur.fetchall()
    return [dict(r) for r in rows]
