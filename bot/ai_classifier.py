"""
AI Classifier — shadow mode.

XGBoost que aprende a predecir si una señal Donchian/TCP va a ser
win (tp_hit) o no (sl_hit / BE), combinando todos los features disponibles.

Shadow mode: calcula score pero NUNCA bloquea señales.
Se reentrena automáticamente cada RETRAIN_EVERY trades nuevos.
"""
import asyncio
import glob
import logging
import math
import os
import json
from datetime import datetime, timezone
from typing import Optional

import numpy as np
import pandas as pd
import pandas_ta as ta

logger = logging.getLogger("scalping_bot.ai_classifier")

MODEL_PATH     = "bot/models/xgb_classifier.json"
RETRAIN_EVERY  = 50     # reentrenar cada N trades cerrados nuevos
MIN_SAMPLES    = 40     # mínimo para intentar entrenar
BOOTSTRAP_DIR  = "data/raw_cvd"

# Scanner params (deben coincidir con scanner.py)
LOOKBACK    = 30
SL_MULT     = 1.5
TP_MULT     = 3.5
ATR_LEN     = 14
MIN_BODY    = 0.35

FEATURES = [
    "atr_pct", "chop_1h", "tbr", "vol_ratio",
    "body_pct", "impact_score",
    "side_bin", "strategy_bin",
    "hour_utc", "day_of_week",
    "macro_bin",
]

_model      = None
_model_meta = {
    "trained_at":          None,
    "n_samples":           0,
    "accuracy":            None,
    "f1":                  None,
    "feature_importances": {},
    "trades_since_retrain": 0,
    "status":              "sin modelo — acumulando datos",
}
_retrain_lock = asyncio.Lock()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _safe_float(v, default=0.0) -> float:
    try:
        return float(v) if v is not None else default
    except Exception:
        return default


def _compute_chop(df: pd.DataFrame, n: int = 14) -> float:
    if len(df) < n + 2:
        return 50.0
    try:
        highs  = df["high"].values[-n:]
        lows   = df["low"].values[-n:]
        closes = df["close"].values[-(n+1):]
        trs = []
        for i in range(n):
            tr = max(highs[i], closes[i]) - min(lows[i], closes[i])
            trs.append(tr)
        atr_sum = sum(trs)
        hh = max(highs)
        ll = min(lows)
        if hh == ll or atr_sum <= 0:
            return 50.0
        return round(100 * math.log10(atr_sum / (hh - ll)) / math.log10(n), 2)
    except Exception:
        return 50.0


def _compute_ema(series: pd.Series, n: int) -> pd.Series:
    return series.ewm(span=n, adjust=False).mean()


def _extract_features_from_row(row: dict) -> Optional[dict]:
    """
    Extrae features de una fila de signal_log + result_json.
    Retorna None si faltan datos críticos.
    """
    try:
        rj = {}
        if row.get("result_json"):
            try:
                rj = json.loads(row["result_json"])
            except Exception:
                pass

        ts_str = row.get("ts", "")
        try:
            dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        except Exception:
            dt = datetime.now(timezone.utc)

        side     = row.get("side", "long")
        strategy = row.get("strategy", "donchian")

        return {
            "atr_pct":      _safe_float(rj.get("feat_atr_pct"),   0.0),
            "chop_1h":      _safe_float(row.get("chop_val"),       50.0),
            "tbr":          _safe_float(rj.get("cvd_tbr"),         0.5),
            "vol_ratio":    _safe_float(rj.get("feat_vol_ratio"),  1.0),
            "body_pct":     _safe_float(rj.get("feat_body_pct"),   0.35),
            "impact_score": _safe_float(row.get("impact_score"),   0.0),
            "side_bin":     1.0 if side == "long" else -1.0,
            "strategy_bin": 0.0 if strategy == "donchian" else 1.0,
            "hour_utc":     float(dt.hour),
            "day_of_week":  float(dt.weekday()),
            "macro_bin":    _macro_to_bin(rj.get("macro_regime", "unknown")),
        }
    except Exception as e:
        logger.warning(f"[AI] Error extrayendo features: {e}")
        return None


def _macro_to_bin(regime: str) -> float:
    return {"bull": 1.0, "neutral": 0.0, "bear": -1.0}.get(regime, 0.0)


def features_for_signal(side: str, strategy: str, chop_val: float,
                        cvd_tbr: Optional[float], macro_regime: str,
                        impact_score: float, feat_atr_pct: float,
                        feat_vol_ratio: float, feat_body_pct: float,
                        ts: datetime) -> dict:
    """Construye el dict de features para una señal en tiempo real."""
    return {
        "atr_pct":      feat_atr_pct,
        "chop_1h":      chop_val or 50.0,
        "tbr":          cvd_tbr if cvd_tbr is not None else 0.5,
        "vol_ratio":    feat_vol_ratio,
        "body_pct":     feat_body_pct,
        "impact_score": impact_score,
        "side_bin":     1.0 if side == "long" else -1.0,
        "strategy_bin": 0.0 if strategy == "donchian" else 1.0,
        "hour_utc":     float(ts.hour),
        "day_of_week":  float(ts.weekday()),
        "macro_bin":    _macro_to_bin(macro_regime),
    }


# ── Model I/O ─────────────────────────────────────────────────────────────────

def _load_model():
    global _model
    if not os.path.exists(MODEL_PATH):
        return
    try:
        import xgboost as xgb
        m = xgb.XGBClassifier()
        m.load_model(MODEL_PATH)
        _model = m
        logger.info(f"[AI] Modelo cargado desde {MODEL_PATH}")
    except Exception as e:
        logger.warning(f"[AI] No se pudo cargar modelo: {e}")


def _save_model(model):
    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
    model.save_model(MODEL_PATH)
    logger.info(f"[AI] Modelo guardado en {MODEL_PATH}")


# ── Predict ───────────────────────────────────────────────────────────────────

def get_score(feat: dict) -> Optional[float]:
    """Retorna probabilidad de win (0.0–1.0) o None si no hay modelo."""
    if _model is None:
        return None
    try:
        X = pd.DataFrame([feat])[FEATURES].fillna(0)
        prob = float(_model.predict_proba(X)[0][1])
        return round(prob, 4)
    except Exception as e:
        logger.warning(f"[AI] Error en predict: {e}")
        return None


def get_meta() -> dict:
    return dict(_model_meta)


# ── Training ──────────────────────────────────────────────────────────────────

async def _build_training_data_from_db(pool) -> pd.DataFrame:
    """
    Une signal_log (executed) con trades para obtener label (win=1 / no_win=0).
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT
                sl.ts, sl.side, sl.strategy, sl.chop_val,
                sl.fund_impact AS impact_score, sl.result_json,
                t.close_reason, t.pnl
            FROM signal_log sl
            JOIN trades t ON (
                t.symbol   = sl.symbol AND
                t.side     = sl.side   AND
                t.strategy = sl.strategy AND
                t.open_time::timestamptz BETWEEN
                    sl.ts::timestamptz - INTERVAL '5 minutes' AND
                    sl.ts::timestamptz + INTERVAL '5 minutes'
            )
            WHERE sl.verdict = 'executed'
            ORDER BY sl.id DESC
            LIMIT 5000
        """)

    records = []
    for r in rows:
        feat = _extract_features_from_row(dict(r))
        if feat is None:
            continue
        is_win = 1 if r["close_reason"] == "tp_hit" else 0
        feat["label"] = is_win
        records.append(feat)

    if not records:
        return pd.DataFrame()
    return pd.DataFrame(records)


def _build_bootstrap_dataset() -> pd.DataFrame:
    """
    Genera dataset de entrenamiento desde los CSVs históricos raw_cvd.
    Simula señales Donchian y las labela según TP/SL hit.
    """
    csv_files = glob.glob(os.path.join(BOOTSTRAP_DIR, "*_15m_cvd.csv"))
    if not csv_files:
        logger.warning("[AI] No se encontraron archivos raw_cvd para bootstrap")
        return pd.DataFrame()

    all_records = []

    for fpath in csv_files:
        symbol = os.path.basename(fpath).replace("_15m_cvd.csv", "")
        try:
            df = pd.read_csv(fpath)
            df.columns = [c.lower() for c in df.columns]
            df = df.rename(columns={"taker_buy_volume": "taker_buy"})
            df = df.dropna().reset_index(drop=True)

            if len(df) < LOOKBACK + ATR_LEN + 50:
                continue

            # ATR
            df.ta.atr(length=ATR_LEN, append=True)
            atr_col = [c for c in df.columns if c.startswith("ATR")][0]

            # EMA para tendencia en misma TF (proxy)
            df["ema20"] = _compute_ema(df["close"], 20)
            df["ema50"] = _compute_ema(df["close"], 50)

            # Volumen promedio
            df["vol_avg"] = df["volume"].rolling(20).mean()

            records = _simulate_donchian_signals(df, atr_col, symbol)
            all_records.extend(records)

        except Exception as e:
            logger.warning(f"[AI] Bootstrap error {symbol}: {e}")

    if not all_records:
        return pd.DataFrame()

    df_out = pd.DataFrame(all_records)
    logger.info(f"[AI] Bootstrap: {len(df_out)} señales históricas generadas de {len(csv_files)} símbolos")
    return df_out


def _simulate_donchian_signals(df: pd.DataFrame, atr_col: str, symbol: str) -> list:
    records = []

    for i in range(LOOKBACK + ATR_LEN + 5, len(df) - 50):
        window = df.iloc[i - LOOKBACK: i]
        row    = df.iloc[i]
        atr    = df[atr_col].iloc[i]

        if atr <= 0 or pd.isna(atr):
            continue

        upper = window["high"].max()
        lower = window["low"].min()
        close = row["close"]
        vol   = row["volume"]
        vol_avg = row["vol_avg"]
        body  = abs(row["close"] - row["open"])
        rng   = row["high"] - row["low"]
        body_pct = body / rng if rng > 0 else 0

        # Confirmación de volumen
        if vol_avg <= 0 or vol < vol_avg * 2.0:
            continue

        # Breakout
        if close > upper and body_pct >= MIN_BODY:
            side = "long"
            sl_price = close - atr * SL_MULT
            tp_price = close + atr * TP_MULT
        elif close < lower and body_pct >= MIN_BODY:
            side = "short"
            sl_price = close + atr * SL_MULT
            tp_price = close - atr * TP_MULT
        else:
            continue

        # Label: simular precio futuro hasta TP o SL
        label = _simulate_outcome(df, i, side, sl_price, tp_price)
        if label is None:
            continue

        # Features
        tbr = row["taker_buy"] / vol if vol > 0 and "taker_buy" in df.columns else 0.5
        chop = _compute_chop(df.iloc[max(0, i-14):i+1])
        ts_dt = datetime.fromtimestamp(row["timestamp"] / 1000, tz=timezone.utc) if "timestamp" in df.columns else datetime.now(timezone.utc)
        ema20 = row.get("ema20", close)
        ema50 = row.get("ema50", close)
        macro = "bull" if ema20 > ema50 * 1.01 else ("bear" if ema20 < ema50 * 0.99 else "neutral")

        records.append({
            "atr_pct":      atr / close,
            "chop_1h":      chop,
            "tbr":          tbr,
            "vol_ratio":    vol / vol_avg if vol_avg > 0 else 1.0,
            "body_pct":     body_pct,
            "impact_score": 0.0,
            "side_bin":     1.0 if side == "long" else -1.0,
            "strategy_bin": 0.0,
            "hour_utc":     float(ts_dt.hour),
            "day_of_week":  float(ts_dt.weekday()),
            "macro_bin":    _macro_to_bin(macro),
            "label":        label,
        })

    return records


def _simulate_outcome(df: pd.DataFrame, entry_idx: int,
                      side: str, sl: float, tp: float) -> Optional[int]:
    """Simula si TP o SL se toca primero en las siguientes 50 velas."""
    for j in range(entry_idx + 1, min(entry_idx + 51, len(df))):
        high  = df["high"].iloc[j]
        low   = df["low"].iloc[j]
        if side == "long":
            if low <= sl:
                return 0
            if high >= tp:
                return 1
        else:
            if high >= sl:
                return 0
            if low <= tp:
                return 1
    return None


def _train_model(df: pd.DataFrame) -> Optional[tuple]:
    """Entrena XGBoost con walk-forward validation. Retorna (model, metrics)."""
    try:
        import xgboost as xgb
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import accuracy_score, f1_score

        X = df[FEATURES].fillna(0).values
        y = df["label"].values

        if len(X) < MIN_SAMPLES:
            return None

        X_tr, X_val, y_tr, y_val = train_test_split(
            X, y, test_size=0.25, shuffle=False  # temporal — no shuffle para respetar orden
        )

        model = xgb.XGBClassifier(
            n_estimators=200,
            max_depth=4,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            use_label_encoder=False,
            eval_metric="logloss",
            verbosity=0,
            random_state=42,
        )
        model.fit(X_tr, y_tr,
                  eval_set=[(X_val, y_val)],
                  verbose=False)

        y_pred = model.predict(X_val)
        acc = float(accuracy_score(y_val, y_pred))
        f1  = float(f1_score(y_val, y_pred, zero_division=0))

        fi = dict(zip(FEATURES, model.feature_importances_.tolist()))

        return model, {"accuracy": round(acc, 4), "f1": round(f1, 4),
                       "n_samples": len(df), "feature_importances": fi}
    except Exception as e:
        logger.error(f"[AI] Error entrenando modelo: {e}")
        return None


# ── Public API ────────────────────────────────────────────────────────────────

async def maybe_retrain(pool) -> bool:
    """
    Llama esto después de cada trade cerrado.
    Reentrena si acumuló suficientes trades nuevos.
    """
    global _model, _model_meta

    _model_meta["trades_since_retrain"] += 1

    if _model_meta["trades_since_retrain"] < RETRAIN_EVERY and _model is not None:
        return False

    async with _retrain_lock:
        logger.info(f"[AI] Iniciando reentrenamiento ({_model_meta['trades_since_retrain']} trades nuevos)...")
        try:
            df_live = await _build_training_data_from_db(pool)
            n_live  = len(df_live)
            logger.info(f"[AI] Datos live: {n_live} señales")

            # Bootstrap solo si tenemos pocos datos reales
            if n_live < MIN_SAMPLES:
                logger.info("[AI] Pocos datos live — usando bootstrap CSV")
                df_boot = _build_bootstrap_dataset()
                if not df_boot.empty and not df_live.empty:
                    df = pd.concat([df_boot, df_live], ignore_index=True)
                elif not df_boot.empty:
                    df = df_boot
                else:
                    df = df_live
            else:
                df = df_live

            if len(df) < MIN_SAMPLES:
                logger.info(f"[AI] Insuficientes datos ({len(df)} < {MIN_SAMPLES}), esperando más trades")
                _model_meta["status"] = f"acumulando datos ({len(df)}/{MIN_SAMPLES} mínimo)"
                return False

            result = _train_model(df)
            if result is None:
                return False

            new_model, metrics = result
            old_acc = _model_meta.get("accuracy") or 0.0

            # Reemplazar si mejora accuracy o no hay modelo previo
            if _model is None or metrics["accuracy"] >= old_acc - 0.02:
                _model = new_model
                _save_model(_model)
                _model_meta.update({
                    "trained_at":           datetime.now(timezone.utc).isoformat(),
                    "n_samples":            metrics["n_samples"],
                    "accuracy":             metrics["accuracy"],
                    "f1":                   metrics["f1"],
                    "feature_importances":  metrics["feature_importances"],
                    "trades_since_retrain": 0,
                    "status":               f"activo — {metrics['n_samples']} muestras",
                })
                logger.info(f"[AI] Modelo actualizado — acc={metrics['accuracy']:.3f} f1={metrics['f1']:.3f} n={metrics['n_samples']}")
                return True
            else:
                logger.info(f"[AI] Nuevo modelo peor (acc={metrics['accuracy']:.3f} < {old_acc:.3f}), manteniendo anterior")
                _model_meta["trades_since_retrain"] = 0
                return False

        except Exception as e:
            logger.error(f"[AI] Error en reentrenamiento: {e}")
            return False


async def start(pool):
    """Carga modelo existente y entrena si no hay ninguno."""
    _load_model()
    if _model is None:
        logger.info("[AI] Sin modelo previo — entrenando con bootstrap...")
        _model_meta["trades_since_retrain"] = RETRAIN_EVERY  # fuerza reentrenamiento
        await maybe_retrain(pool)
    else:
        logger.info(f"[AI] Modelo listo. Status: {_model_meta['status']}")
