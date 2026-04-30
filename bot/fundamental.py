"""
Fundamental filter para el bot de scalping 15m.
Fuentes:
  1. Alternative.me Fear & Greed (diario, gratis)
  2. CryptoPanic  (tiempo real, gratis sin key)
  3. GDELT Project (noticias macro globales, gratis, sin key, cada 15min)
  4. Calendario FOMC/CPI (hardcoded, inyecta eventos de alto impacto)
  5. NewsAPI (complemento si hay key)
Scoring con Groq LLM (gratis, 14k req/día) si se configura GROQ_API_KEY.
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

NEWSAPI_URL     = "https://newsapi.org/v2/everything"
FEAR_GREED_URL  = "https://api.alternative.me/fng/?limit=1&format=json"
CRYPTOPANIC_URL = "https://cryptopanic.com/api/v1/posts/"
GDELT_URL       = "https://api.gdeltproject.org/api/v2/doc/doc"

IMPACT_BLOCK_THRESHOLD   = 7.0
IMPACT_REDUCE_THRESHOLD  = 4.0
FEAR_GREED_EXTREME_FEAR  = 20
FEAR_GREED_EXTREME_GREED = 82
IMPACT_WINDOW_HOURS      = 12
GROQ_CACHE_SECS          = 300  # 5 min — evita calls duplicadas en el mismo ciclo de scan

NEWSAPI_QUERY = (
    "bitcoin OR BTC OR crypto OR ethereum OR "
    "Federal Reserve OR Fed rate OR interest rate OR "
    "Trump tariff OR Iran OR war OR geopolitical OR "
    "recession OR inflation OR CPI OR FOMC"
)

GDELT_QUERY = (
    "bitcoin OR ethereum OR crypto OR cryptocurrency "
    "Federal Reserve OR FOMC OR CPI OR inflation OR recession OR "
    "tariff OR sanctions OR war OR interest rate"
)

CRITICAL_KEYWORDS = [
    "hacked", "hack", "exploit", "breach",
    "sec lawsuit", "sec charges", "banned", "shutdown",
    "chapter 11", "bankruptcy", "insolvent", "halted",
    "de-peg", "depeg", "bank run", "emergency",
    "federal reserve", "fed rate", "interest rate hike", "interest rate cut",
    "fomc", "rate decision",
    "iran", "war", "sanctions", "tariff", "recession",
    "cpi", "inflation", "unemployment",
]

# FOMC meeting final days (Fed anuncia decisión de tasas estos días a las 14:00 ET)
# Fuente: federalreserve.gov — actualizados para 2025-2026
FOMC_DATES = [
    "2025-01-29", "2025-03-19", "2025-05-07", "2025-06-18",
    "2025-07-30", "2025-09-17", "2025-10-29", "2025-12-17",
    "2026-01-28", "2026-03-18", "2026-04-29", "2026-06-17",
    "2026-07-29", "2026-09-16", "2026-10-28", "2026-12-16",
]

# Fechas de release del CPI (BLS, ~2do martes del mes, 08:30 ET = 13:30 UTC)
CPI_DATES = [
    "2025-01-15", "2025-02-12", "2025-03-12", "2025-04-10",
    "2025-05-13", "2025-06-11", "2025-07-11", "2025-08-12",
    "2025-09-10", "2025-10-14", "2025-11-13", "2025-12-10",
    "2026-01-14", "2026-02-11", "2026-03-11", "2026-04-10",
    "2026-05-13", "2026-06-10", "2026-07-15", "2026-08-12",
    "2026-09-09", "2026-10-14", "2026-11-12", "2026-12-09",
]

_LLM_SYSTEM = """Sos un analista financiero especializado en crypto.
Dado un titular, respondé SOLO con JSON válido:
{"sentiment": <float -1.0 a 1.0>, "impact": <float 0.0 a 10.0>, "reason": "<1 oración>"}
Noticias macro negativas (guerra, sanciones, crisis, suba de tasas) → impact 6-9, sentiment negativo.
Regulación adversa, hackeos, quiebras → impact alto, sentiment negativo.
ETF aprobado, adopción institucional → impact alto, sentiment positivo.
FOMC, CPI, datos macro importantes → impact 6-8.
Noticias de precio, predicciones, análisis → impact bajo (< 3)."""

_GROQ_REASONING_SYSTEM = """Sos un analista de riesgo para trading de crypto en futuros perpetuos (15 minutos).
Dado el contexto macro y de noticias, decidí si es seguro operar en este momento.
Respondé SOLO con JSON válido (sin markdown, sin texto extra):
{"action": "ok" | "no_long" | "no_short" | "block", "reason": "<1 oración concisa>", "confidence": <0.0 a 1.0>}

Reglas de decisión:
- "block": Evento macro crítico activo — FOMC en ventana -4h/+2h, CPI en ventana -1h/+2h, noticias impacto >= 8, crisis sistémica, hack de exchange
- "no_long": Sentimiento claramente bajista pero no catastrófico — bear macro, noticias regulatorias negativas, fear extremo
- "no_short": Sentimiento claramente alcista — bull market fuerte, noticias institucionales positivas, greed extremo
- "ok": Sin señales de riesgo claras — operar normal
- confidence: certeza de la decisión (0.0=muy inseguro, 1.0=muy seguro)"""


class FundamentalFilter:
    def __init__(self):
        self._last_fear_greed: Optional[dict] = None
        self._poll_task: Optional[asyncio.Task] = None
        self._groq_cache: Optional[tuple] = None        # (action, reason, confidence)
        self._groq_cache_ts: Optional[datetime] = None

    async def start(self):
        if not settings.FUNDAMENTAL_ENABLED:
            logger.info("[FUNDAMENTAL] Desactivado")
            return
        await self._ensure_table()
        # Inyectar eventos de calendario inmediatamente al startup
        await self._inject_calendar_events()
        self._poll_task = asyncio.create_task(self._poll_loop())
        logger.info("[FUNDAMENTAL] Poller iniciado (CryptoPanic + GDELT + Calendario FOMC/CPI)")

    async def stop(self):
        if self._poll_task:
            self._poll_task.cancel()

    async def _ensure_table(self):
        pass

    async def _poll_loop(self):
        while True:
            try:
                await self._poll_all()
            except Exception as e:
                logger.error(f"[FUNDAMENTAL] Error en poll: {e}")
            await asyncio.sleep(settings.FUNDAMENTAL_POLL_INTERVAL)

    async def _poll_all(self):
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=25),
            headers={"User-Agent": "Mozilla/5.0 TradingBot/1.0"}
        ) as session:
            tasks = [
                self._poll_fear_greed(session),
                self._poll_cryptopanic(session),
                self._poll_gdelt(session),
                self._inject_calendar_events(),
            ]
            if settings.NEWSAPI_KEY:
                tasks.append(self._poll_newsapi(session))
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for i, r in enumerate(results):
                if isinstance(r, Exception):
                    logger.error(f"[FUNDAMENTAL] Poll task[{i}] error: {r}")

    # ── Fear & Greed ──────────────────────────────────────────────────────────

    async def _poll_fear_greed(self, session: aiohttp.ClientSession):
        try:
            async with session.get(FEAR_GREED_URL) as resp:
                if resp.status != 200:
                    logger.warning(f"[FUNDAMENTAL] FearGreed status {resp.status}")
                    return
                data = await resp.json(content_type=None)
            entry = data["data"][0]
            value = float(entry["value"])
            self._last_fear_greed = {"value": value, "label": entry["value_classification"]}
            ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:00:00+00:00")
            async with get_pool().acquire() as conn:
                await conn.execute("""
                    INSERT INTO fundamental_events (ts,category,source,title,sentiment,magnitude,impact,raw_data)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT DO NOTHING
                """, ts, "sentiment", "fear_greed", entry["value_classification"],
                     (value - 50) / 50, value, 0.0, json.dumps(entry))
            logger.debug(f"[FUNDAMENTAL] F&G={value} ({entry['value_classification']})")
        except Exception as e:
            logger.warning(f"[FUNDAMENTAL] FearGreed error: {e}")

    # ── CryptoPanic ───────────────────────────────────────────────────────────

    async def _poll_cryptopanic(self, session: aiohttp.ClientSession):
        """CryptoPanic — gratis sin key, devuelve noticias crypto en tiempo real."""
        params = {"public": "true", "kind": "news"}
        if settings.CRYPTOPANIC_API_KEY:
            params["auth_token"] = settings.CRYPTOPANIC_API_KEY

        try:
            async with session.get(CRYPTOPANIC_URL, params=params) as resp:
                body = await resp.text()
                if resp.status != 200:
                    logger.warning(f"[FUNDAMENTAL] CryptoPanic {resp.status}: {body[:200]}")
                    return
                data = json.loads(body)
        except Exception as e:
            logger.warning(f"[FUNDAMENTAL] CryptoPanic request error: {e}")
            return

        results = data.get("results", [])
        logger.info(f"[FUNDAMENTAL] CryptoPanic: {len(results)} noticias recibidas")
        if not results:
            return

        cutoff = datetime.now(timezone.utc) - timedelta(hours=6)
        events = []
        for item in results:
            title = (item.get("title") or "")[:200].strip()
            if not title:
                continue
            pub = item.get("published_at") or item.get("created_at", "")
            try:
                pub_dt = datetime.fromisoformat(pub.replace("Z", "+00:00"))
                if pub_dt < cutoff:
                    continue
                ts = pub_dt.isoformat()
            except Exception:
                ts = datetime.now(timezone.utc).isoformat()

            is_critical = any(k in title.lower() for k in CRITICAL_KEYWORDS)
            sentiment, impact = await self._score_title(title, is_critical)
            events.append((ts, "news", "cryptopanic", title, sentiment, 0.0, impact,
                           json.dumps({"url": item.get("url", ""),
                                       "source": (item.get("source") or {}).get("title", "")})))

        await self._insert_events(events, "CryptoPanic")

    # ── GDELT ─────────────────────────────────────────────────────────────────

    async def _poll_gdelt(self, session: aiohttp.ClientSession):
        """GDELT Project — noticias globales, gratis, sin key, se actualiza cada 15min."""
        params = {
            "query":      GDELT_QUERY,
            "mode":       "artlist",
            "maxrecords": "20",
            "sort":       "hybridrel",
            "format":     "json",
            "timespan":   "30min",
        }
        try:
            async with session.get(GDELT_URL, params=params) as resp:
                if resp.status != 200:
                    logger.debug(f"[FUNDAMENTAL] GDELT status {resp.status}")
                    return
                body = await resp.text()
                if not body.strip().startswith("{"):
                    return  # GDELT a veces devuelve HTML en error
                data = json.loads(body)
        except Exception as e:
            logger.debug(f"[FUNDAMENTAL] GDELT error: {e}")
            return

        articles = data.get("articles", [])
        logger.info(f"[FUNDAMENTAL] GDELT: {len(articles)} artículos recibidos")
        if not articles:
            return

        events = []
        for item in articles:
            title = (item.get("title") or "")[:200].strip()
            if not title:
                continue
            # GDELT usa formato YYYYMMDDHHMMSS para seendate
            raw_date = item.get("seendate", "")
            try:
                ts = datetime.strptime(raw_date, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc).isoformat()
            except Exception:
                ts = datetime.now(timezone.utc).isoformat()

            is_critical = any(k in title.lower() for k in CRITICAL_KEYWORDS)
            if not is_critical:
                # GDELT trae mucho ruido; sin keyword crítica, impacto bajo
                continue

            sentiment, impact = await self._score_title(title, is_critical)
            events.append((ts, "news", "gdelt", title, sentiment, 0.0, impact,
                           json.dumps({"url": item.get("url", ""), "domain": item.get("domain", "")})))

        await self._insert_events(events, "GDELT")

    # ── Economic Calendar (FOMC / CPI) ────────────────────────────────────────

    async def _inject_calendar_events(self):
        """
        Inyecta eventos de alto impacto basados en el calendario FOMC/CPI.
        Activa 24h antes del evento y durante el día del evento.
        """
        now = datetime.now(timezone.utc)
        events = []

        for date_str in FOMC_DATES:
            event_dt = datetime.fromisoformat(date_str + "T18:00:00+00:00")  # 14:00 ET = 18:00 UTC
            delta = (event_dt - now).total_seconds() / 3600
            if -2 <= delta <= 24:  # ventana: 24h antes hasta 2h después
                title = f"FOMC Meeting — Fed Rate Decision ({date_str})"
                ts    = event_dt.isoformat()
                events.append((ts, "news", "fomc_calendar", title, -0.3, 0.0, 8.0,
                                json.dumps({"type": "fomc", "date": date_str})))

        for date_str in CPI_DATES:
            event_dt = datetime.fromisoformat(date_str + "T13:30:00+00:00")  # 08:30 ET = 13:30 UTC
            delta = (event_dt - now).total_seconds() / 3600
            if -2 <= delta <= 24:
                title = f"CPI Inflation Data Release ({date_str})"
                ts    = event_dt.isoformat()
                events.append((ts, "news", "cpi_calendar", title, -0.2, 0.0, 7.0,
                                json.dumps({"type": "cpi", "date": date_str})))

        if events:
            await self._insert_events(events, f"Calendario ({len(events)} eventos activos)")
            logger.info(f"[FUNDAMENTAL] Calendario: {len(events)} eventos macro activos (FOMC/CPI)")

    # ── NewsAPI ───────────────────────────────────────────────────────────────

    async def _poll_newsapi(self, session: aiohttp.ClientSession):
        from_ts = (datetime.utcnow() - timedelta(hours=6)).strftime("%Y-%m-%dT%H:%M:%S")
        params = {
            "apiKey":   settings.NEWSAPI_KEY,
            "q":        NEWSAPI_QUERY,
            "language": "en",
            "sortBy":   "publishedAt",
            "pageSize": 20,
            "from":     from_ts,
        }
        try:
            async with session.get(NEWSAPI_URL, params=params) as resp:
                body = await resp.text()
                if resp.status != 200:
                    logger.warning(f"[FUNDAMENTAL] NewsAPI {resp.status}: {body[:200]}")
                    return
                data = json.loads(body)
        except Exception as e:
            logger.warning(f"[FUNDAMENTAL] NewsAPI error: {e}")
            return

        articles = data.get("articles", [])
        logger.info(f"[FUNDAMENTAL] NewsAPI: {len(articles)} artículos recibidos")
        events = []
        for item in articles:
            title = (item.get("title") or "")[:200].strip()
            if not title or title == "[Removed]":
                continue
            ts = item.get("publishedAt", datetime.utcnow().isoformat())
            is_critical = any(k in title.lower() for k in CRITICAL_KEYWORDS)
            sentiment, impact = await self._score_title(title, is_critical)
            events.append((ts, "news", "newsapi", title, sentiment, 0.0, impact,
                           json.dumps({"url": item.get("url", "")})))
        await self._insert_events(events, "NewsAPI")

    # ── Helpers ───────────────────────────────────────────────────────────────

    async def _score_title(self, title: str, is_critical: bool) -> tuple[float, float]:
        """Retorna (sentiment, impact) usando Groq si hay key, sino heurística."""
        if settings.GROQ_API_KEY:
            result = await self._score_with_llm(title)
            sentiment = result.get("sentiment", 0.0)
            impact    = result.get("impact", 0.0)
            if is_critical:
                impact = max(impact, 6.0)
        else:
            sentiment = 0.0
            impact    = 6.0 if is_critical else 2.0
        return sentiment, impact

    async def _score_with_llm(self, title: str) -> dict:
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

    async def _insert_events(self, events: list, source_label: str):
        if not events:
            return
        try:
            async with get_pool().acquire() as conn:
                await conn.executemany("""
                    INSERT INTO fundamental_events (ts,category,source,title,sentiment,magnitude,impact,raw_data)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT DO NOTHING
                """, events)
            logger.info(f"[FUNDAMENTAL] {source_label}: {len(events)} eventos guardados en DB")
        except Exception as e:
            logger.error(f"[FUNDAMENTAL] Error insertando eventos ({source_label}): {e}")

    # ── Groq AI reasoning layer ───────────────────────────────────────────────

    def _nearest_event_info(self, dates: list, time_suffix: str, label: str) -> str:
        """Retorna string con proximidad al evento más cercano."""
        now = datetime.now(timezone.utc)
        nearest_delta = None
        nearest_date  = None
        for date_str in dates:
            event_dt = datetime.fromisoformat(date_str + time_suffix)
            delta_h  = (event_dt - now).total_seconds() / 3600
            if nearest_delta is None or abs(delta_h) < abs(nearest_delta):
                nearest_delta = delta_h
                nearest_date  = date_str
        if nearest_delta is None:
            return f"{label}: N/A"
        if nearest_delta > 0:
            return f"{label}: en {nearest_delta:.1f}h ({nearest_date})"
        return f"{label}: hace {abs(nearest_delta):.1f}h ({nearest_date})"

    def _groq_action_to_tuple(self, action: str, reason: str, confidence: float,
                               side: str) -> tuple:
        """Convierte acción Groq a (would_block, direction_filter, label)."""
        short_reason = (reason or "")[:60]
        if action == "block":
            return True,  None,       f"groq_block|{short_reason}"
        if action == "no_long":
            return False, "no_long",  f"groq_no_long|{short_reason}"
        if action == "no_short":
            return False, "no_short", f"groq_no_short|{short_reason}"
        return False, None, f"groq_ok|{short_reason}"

    async def _call_groq_background(self, macro_regime: str, max_impact: float,
                                      avg_sent: float, event_count: int) -> None:
        """Llama a Groq en background y actualiza el cache. Nunca bloquea el caller."""
        now = datetime.now(timezone.utc)
        fg_str    = (f"{self._last_fear_greed['value']:.0f} ({self._last_fear_greed['label']})"
                     if self._last_fear_greed else "N/A")
        fomc_info = self._nearest_event_info(FOMC_DATES, "T18:00:00+00:00", "FOMC")
        cpi_info  = self._nearest_event_info(CPI_DATES,  "T13:30:00+00:00", "CPI")

        try:
            cutoff = (now - timedelta(hours=6)).isoformat()
            async with get_pool().acquire() as conn:
                rows = await conn.fetch(
                    "SELECT title, source, impact, sentiment FROM fundamental_events "
                    "WHERE ts >= $1 AND category != 'sentiment' "
                    "ORDER BY impact DESC LIMIT 10",
                    cutoff,
                )
            news_lines = "\n".join(
                f"  [{r['source']}] {r['title']} "
                f"(impact={float(r['impact']):.1f}, sent={float(r['sentiment']):.2f})"
                for r in rows
            ) or "  (sin noticias recientes)"
        except Exception:
            news_lines = "  (error obteniendo noticias)"

        context = (
            f"Contexto de mercado — {now.strftime('%Y-%m-%d %H:%M UTC')}\n\n"
            f"Fear & Greed Index: {fg_str}\n"
            f"Macro Regime (Bitcoin): {macro_regime}\n"
            f"{fomc_info}\n"
            f"{cpi_info}\n\n"
            f"Noticias relevantes (últimas 6h):\n{news_lines}\n\n"
            f"Estadísticas: impacto_max={max_impact:.1f} | "
            f"sentimiento_avg={avg_sent:.2f} | eventos={event_count}"
        )

        try:
            client = AsyncOpenAI(api_key=settings.GROQ_API_KEY,
                                 base_url="https://api.groq.com/openai/v1",
                                 timeout=15.0)
            resp = await client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                max_tokens=150,
                temperature=0.1,
                messages=[
                    {"role": "system", "content": _GROQ_REASONING_SYSTEM},
                    {"role": "user",   "content": context},
                ],
            )
            raw = resp.choices[0].message.content.strip()
            if raw.startswith("```"):
                parts = raw.split("```")
                raw = parts[1] if len(parts) > 1 else raw
                if raw.startswith("json"):
                    raw = raw[4:]
            parsed     = json.loads(raw)
            action     = parsed.get("action", "ok")
            reason     = parsed.get("reason", "")
            confidence = float(parsed.get("confidence", 0.5))
            self._groq_cache    = (action, reason, confidence)
            self._groq_cache_ts = now
            logger.info(f"[FUNDAMENTAL] Groq (bg) → {action} (conf={confidence:.2f}) | {reason}")
        except Exception as e:
            logger.warning(f"[FUNDAMENTAL] Groq background error: {e}")

    async def _ai_groq_decision(self, side: str, macro_regime: str,
                                 max_impact: float, avg_sent: float,
                                 event_count: int) -> tuple:
        """
        Retorna decisión shadow SIN bloquear el signal processing.
        - Cache hit → usa resultado anterior (instantáneo)
        - Cache miss → devuelve reglas ahora + dispara Groq en background
          El próximo signal (dentro de 5 min) ya tendrá el resultado de Groq.
        """
        if not settings.GROQ_API_KEY:
            block, direction, label = self._compute_shadow_decision(max_impact, avg_sent, event_count)
            return block, direction, label, 0.0

        now = datetime.now(timezone.utc)

        # Cache hit → respuesta instantánea con decisión de Groq
        if (self._groq_cache is not None and self._groq_cache_ts is not None and
                (now - self._groq_cache_ts).total_seconds() < GROQ_CACHE_SECS):
            c_action, c_reason, c_conf = self._groq_cache
            logger.debug(f"[FUNDAMENTAL] Groq cache hit: {c_action} (conf={c_conf:.2f})")
            return self._groq_action_to_tuple(c_action, c_reason, c_conf, side)

        # Cache miss → reglas ahora, Groq en background para próxima señal
        asyncio.create_task(
            self._call_groq_background(macro_regime, max_impact, avg_sent, event_count)
        )
        logger.debug("[FUNDAMENTAL] Groq cache miss — usando reglas, Groq corriendo en background")
        block, direction, label = self._compute_shadow_decision(max_impact, avg_sent, event_count)
        return block, direction, label, 0.0

    # ── Shadow decision matrix (fallback / sin Groq key) ─────────────────────

    def _compute_shadow_decision(self, max_impact: float, avg_sent: float,
                                  event_count: int) -> tuple:
        """
        Retorna (would_block_all, direction_filter, label).
        direction_filter: "no_long" | "no_short" | None
        NUNCA bloquea — puro shadow mode.
        """
        now = datetime.now(timezone.utc)

        # Ventana FOMC (4h antes hasta 2h después del anuncio de la Fed, ~18:00 UTC)
        for date_str in FOMC_DATES:
            fomc_dt = datetime.fromisoformat(date_str + "T18:00:00+00:00")
            delta_h = (fomc_dt - now).total_seconds() / 3600
            if -2.0 <= delta_h <= 4.0:
                return True, None, f"fomc_window|{date_str}"

        # Ventana CPI (1h antes hasta 2h después del release, ~13:30 UTC)
        for date_str in CPI_DATES:
            cpi_dt = datetime.fromisoformat(date_str + "T13:30:00+00:00")
            delta_h = (cpi_dt - now).total_seconds() / 3600
            if -1.0 <= delta_h <= 2.0:
                return True, None, f"cpi_window|{date_str}"

        # Evento crítico de muy alto impacto
        if event_count > 0 and max_impact >= 8.0:
            return True, None, f"critical_event|impact={max_impact:.1f}"

        # Filtro direccional por sentimiento
        if event_count > 0 and max_impact >= 4.0:
            if avg_sent < -0.4:
                return False, "no_long", f"bearish_news|sent={avg_sent:.2f}"
            if avg_sent > 0.4:
                return False, "no_short", f"bullish_news|sent={avg_sent:.2f}"

        return False, None, "ok"

    # ── Check (llamado por _process_signal) ───────────────────────────────────

    async def check(self, symbol: str = "BTC", side: str = "long",
                    macro_regime: str = "unknown") -> dict:
        _shadow_defaults = {
            "shadow_would_block": False,
            "shadow_direction":   None,
            "shadow_label":       "disabled",
            "groq_action":        "disabled",
            "groq_reason":        "",
            "groq_confidence":    0.0,
        }

        if not settings.FUNDAMENTAL_ENABLED:
            return {"allow": True, "reduce_size": False, "boost_size": False,
                    "reason": "fundamental_disabled", "impact_score": 0.0,
                    **_shadow_defaults}

        cutoff = (datetime.now(timezone.utc) - timedelta(hours=IMPACT_WINDOW_HOURS)).isoformat()
        async with get_pool().acquire() as conn:
            result = await conn.fetchrow(
                "SELECT MAX(impact), AVG(sentiment), COUNT(*) FROM fundamental_events "
                "WHERE ts >= $1 AND category != 'sentiment'",
                cutoff
            )

        max_impact  = float(result[0] or 0.0)
        avg_sent    = float(result[1] or 0.0)
        event_count = int(result[2] or 0)
        fg_value    = self._last_fear_greed["value"] if self._last_fear_greed else 50.0
        fg_label    = self._last_fear_greed["label"] if self._last_fear_greed else "N/A"
        reason_base = f"F&G={fg_value:.0f}({fg_label}) | impact={max_impact:.1f} | events={event_count}"

        # Shadow decision — Groq si hay key, sino fallback a reglas
        s_block, s_dir, s_label, s_conf = await self._ai_groq_decision(
            side, macro_regime, max_impact, avg_sent, event_count
        )
        # Extraer groq_action limpio del label
        groq_action = s_label.split("|")[0] if "|" in s_label else s_label
        groq_reason = s_label.split("|", 1)[1] if "|" in s_label else ""
        shadow = {
            "shadow_would_block": s_block,
            "shadow_direction":   s_dir,
            "shadow_label":       s_label,
            "groq_action":        groq_action,
            "groq_reason":        groq_reason,
            "groq_confidence":    round(s_conf, 3),
        }

        if fg_value >= FEAR_GREED_EXTREME_GREED:
            return {"allow": True, "reduce_size": True, "boost_size": False,
                    "reason": reason_base + " → REDUCE_SIZE (extreme greed)",
                    "impact_score": max_impact, **shadow}

        if max_impact >= IMPACT_BLOCK_THRESHOLD:
            return {"allow": True, "reduce_size": True, "boost_size": False,
                    "reason": reason_base + " → HIGH_IMPACT (shadow only)",
                    "impact_score": max_impact, **shadow}

        if max_impact >= IMPACT_REDUCE_THRESHOLD:
            return {"allow": True, "reduce_size": True, "boost_size": False,
                    "reason": reason_base + " → REDUCE_SIZE (medium impact)",
                    "impact_score": max_impact, **shadow}

        if fg_value <= FEAR_GREED_EXTREME_FEAR:
            return {"allow": True, "reduce_size": False, "boost_size": True,
                    "reason": reason_base + " → BOOST_SIZE (extreme fear)",
                    "impact_score": max_impact, **shadow}

        return {"allow": True, "reduce_size": False, "boost_size": False,
                "reason": reason_base + " → OK",
                "impact_score": max_impact, **shadow}


fundamental_filter = FundamentalFilter()
