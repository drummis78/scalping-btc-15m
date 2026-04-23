"""
Fundamental filter para el bot de scalping 15m.
Misma arquitectura que 4H/1H: Fear & Greed + NewsAPI + Groq LLM.
Impacto evaluado en ventana de 4h — relevante para scalping.
"""
import asyncio
import logging
import json
from datetime import datetime, timedelta, timezone
from typing import Optional

import aiohttp
from openai import AsyncOpenAI

from bot.config import settings
from bot.state import get_pool

logger = logging.getLogger("scalping_bot.fundamental")

NEWSAPI_URL    = "https://newsapi.org/v2/everything"
FEAR_GREED_URL = "https://api.alternative.me/fng/?limit=1&format=json"

IMPACT_BLOCK_THRESHOLD   = 7.0
IMPACT_REDUCE_THRESHOLD  = 4.0
FEAR_GREED_EXTREME_FEAR  = 20
FEAR_GREED_EXTREME_GREED = 82
IMPACT_WINDOW_HOURS      = 4

CRITICAL_KEYWORDS = [
    "hacked", "hack", "exploit", "breach",
    "sec lawsuit", "sec charges", "banned", "shutdown",
    "chapter 11", "bankruptcy", "insolvent", "halted",
    "de-peg", "depeg", "bank run", "emergency",
]

_LLM_SYSTEM = """Sos un analista financiero especializado en crypto.
Dado un titular, respondé SOLO con JSON válido:
{"sentiment": <float -1.0 a 1.0>, "impact": <float 0.0 a 10.0>, "reason": "<1 oración>"}
Regulación adversa, hackeos, quiebras → impact alto, sentiment negativo.
ETF aprobado, adopción institucional → impact alto, sentiment positivo.
Noticias de precio, predicciones → impact bajo (< 3)."""


class FundamentalFilter:
    def __init__(self):
        self._last_fear_greed: Optional[dict] = None
        self._poll_task: Optional[asyncio.Task] = None

    async def start(self):
        if not settings.FUNDAMENTAL_ENABLED:
            logger.info("[FUNDAMENTAL] Desactivado")
            return
        await self._ensure_table()
        self._poll_task = asyncio.create_task(self._poll_loop())
        logger.info("[FUNDAMENTAL] Poller iniciado")

    async def stop(self):
        if self._poll_task:
            self._poll_task.cancel()

    async def _ensure_table(self):
        # tabla creada en init_db() — no-op aquí
        pass

    async def _poll_loop(self):
        while True:
            try:
                await self._poll_all()
            except Exception as e:
                logger.error(f"[FUNDAMENTAL] Error en poll: {e}")
            await asyncio.sleep(settings.FUNDAMENTAL_POLL_INTERVAL)

    async def _poll_all(self):
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            await asyncio.gather(
                self._poll_fear_greed(session),
                self._poll_newsapi(session),
                return_exceptions=True
            )

    async def _poll_fear_greed(self, session: aiohttp.ClientSession):
        async with session.get(FEAR_GREED_URL) as resp:
            if resp.status != 200:
                return
            data = await resp.json(content_type=None)
        entry = data["data"][0]
        value = float(entry["value"])
        self._last_fear_greed = {"value": value, "label": entry["value_classification"]}
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:00:00")
        async with get_pool().acquire() as conn:
            await conn.execute("""
                INSERT INTO fundamental_events (ts,category,source,title,sentiment,magnitude,impact,raw_data)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT DO NOTHING
            """, ts, "sentiment", "fear_greed", entry["value_classification"],
                 (value - 50) / 50, value, 0.0, json.dumps(entry))

    async def _poll_newsapi(self, session: aiohttp.ClientSession):
        if not settings.NEWSAPI_KEY:
            return
        from_ts = (datetime.utcnow() - timedelta(hours=6)).strftime("%Y-%m-%dT%H:%M:%S")
        params = {
            "apiKey":   settings.NEWSAPI_KEY,
            "q":        "bitcoin OR BTC OR crypto",
            "language": "en",
            "sortBy":   "publishedAt",
            "pageSize": 20,
            "from":     from_ts,
        }
        async with session.get(NEWSAPI_URL, params=params) as resp:
            if resp.status != 200:
                logger.warning(f"[FUNDAMENTAL] NewsAPI status {resp.status}")
                return
            data = await resp.json(content_type=None)

        events = []
        for item in data.get("articles", []):
            title = (item.get("title") or "")[:200]
            if not title or title == "[Removed]":
                continue
            ts = item.get("publishedAt", datetime.utcnow().isoformat())
            is_critical = any(k in title.lower() for k in CRITICAL_KEYWORDS)
            if settings.GROQ_API_KEY:
                llm = await self._score_with_llm(title)
                sentiment  = llm.get("sentiment", 0.0)
                impact     = llm.get("impact", 0.0)
                if is_critical:
                    impact = max(impact, 6.0)
            else:
                sentiment  = 0.0
                impact     = 6.0 if is_critical else 0.0
            events.append((ts, "news", "newsapi", title, sentiment, 0.0, impact,
                           json.dumps({"url": item.get("url", "")})))

        if events:
            async with get_pool().acquire() as conn:
                await conn.executemany("""
                    INSERT INTO fundamental_events (ts,category,source,title,sentiment,magnitude,impact,raw_data)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT DO NOTHING
                """, events)

    async def _score_with_llm(self, title: str) -> dict:
        if not settings.GROQ_API_KEY:
            return {"sentiment": 0.0, "impact": 0.0, "reason": "no_key"}
        try:
            client = AsyncOpenAI(api_key=settings.GROQ_API_KEY,
                                 base_url="https://api.groq.com/openai/v1")
            resp = await client.chat.completions.create(
                model="llama-3.1-8b-instant",
                max_tokens=150,
                messages=[
                    {"role": "system", "content": _LLM_SYSTEM},
                    {"role": "user",   "content": f"Titular: {title}"},
                ],
            )
            return json.loads(resp.choices[0].message.content.strip())
        except Exception as e:
            logger.warning(f"[FUNDAMENTAL] LLM error: {e}")
            return {"sentiment": 0.0, "impact": 0.0, "reason": f"error:{e}"}

    async def check(self, symbol: str = "BTC") -> dict:
        if not settings.FUNDAMENTAL_ENABLED:
            return {"allow": True, "reduce_size": False, "boost_size": False,
                    "reason": "fundamental_disabled", "impact_score": 0.0}

        cutoff = (datetime.utcnow() - timedelta(hours=IMPACT_WINDOW_HOURS)).isoformat()
        async with get_pool().acquire() as conn:
            result = await conn.fetchrow(
                "SELECT MAX(impact), AVG(sentiment), COUNT(*) FROM fundamental_events WHERE ts >= $1 AND category != 'sentiment'",
                cutoff
            )

        max_impact  = result[0] or 0.0
        avg_sent    = result[1] or 0.0
        event_count = result[2] or 0
        fg_value    = self._last_fear_greed["value"] if self._last_fear_greed else 50.0
        fg_label    = self._last_fear_greed["label"] if self._last_fear_greed else "N/A"
        reason_base = f"F&G={fg_value:.0f}({fg_label}) | impact={max_impact:.1f} | events={event_count}"

        if fg_value >= FEAR_GREED_EXTREME_GREED:
            return {"allow": True, "reduce_size": True, "boost_size": False,
                    "reason": reason_base + " → REDUCE_SIZE (extreme greed)",
                    "impact_score": max_impact}

        if max_impact >= IMPACT_BLOCK_THRESHOLD:
            return {"allow": False, "reduce_size": False, "boost_size": False,
                    "reason": reason_base + " → BLOCKED (high impact news)",
                    "impact_score": max_impact}

        if max_impact >= IMPACT_REDUCE_THRESHOLD:
            return {"allow": True, "reduce_size": True, "boost_size": False,
                    "reason": reason_base + " → REDUCE_SIZE (medium impact)",
                    "impact_score": max_impact}

        if fg_value <= FEAR_GREED_EXTREME_FEAR:
            return {"allow": True, "reduce_size": False, "boost_size": True,
                    "reason": reason_base + " → BOOST_SIZE (extreme fear, WR histórico alto)",
                    "impact_score": max_impact}

        return {"allow": True, "reduce_size": False, "boost_size": False,
                "reason": reason_base + " → OK",
                "impact_score": max_impact}


fundamental_filter = FundamentalFilter()
