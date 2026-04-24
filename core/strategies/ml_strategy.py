"""ML strategy using a trained LightGBM model for BTC/USDT 5-min binary prediction.

Returns the IDENTICAL signal dict schema as PatternStrategy.
Uses get_next_slot_info() + get_slot_prices() exactly as PatternStrategy does.
"""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from core.strategies.base import BaseStrategy
from ml import data_fetcher
from ml import features as feat_eng
from ml import model_store
from ml import inference_logger
from db import queries
from polymarket.markets import get_next_slot_info, get_slot_prices
import config as cfg

log = logging.getLogger(__name__)

FEATURE_COLS = feat_eng.FEATURE_COLS
_RELOAD_REQUESTED = False
_PRELOADED_MODEL = None


def set_model(model) -> None:
    global _PRELOADED_MODEL
    _PRELOADED_MODEL = model


def request_model_reload() -> None:
    global _RELOAD_REQUESTED
    _RELOAD_REQUESTED = True


class MLStrategy(BaseStrategy):
    def __init__(self):
        self._model = None
        self._funding_buffer: deque = deque(maxlen=24)
        self._funding_records: deque = deque(maxlen=24)
        self._model_slot = "current"
        self._last_funding_settlement: datetime | None = None
        try:
            self._load_model()
        except Exception:
            log.exception(
                "MLStrategy.__init__: _load_model failed - model will be None; signals will be skipped until loaded"
            )
        try:
            self._seed_funding_buffer()
        except Exception:
            log.exception("MLStrategy.__init__: _seed_funding_buffer failed")

    def _seed_funding_buffer(self) -> None:
        try:
            history = data_fetcher.fetch_live_funding_history(n_periods=24)
            if history is None or history.empty:
                log.warning("MLStrategy: could not seed funding state - no historical data returned")
                return
            seeded = 0
            for row in history.itertuples(index=False):
                rate = float(row.funding_rate)
                ts = pd.Timestamp(row.timestamp)
                if ts.tzinfo is None:
                    ts = ts.tz_localize("UTC")
                else:
                    ts = ts.tz_convert("UTC")
                self._funding_buffer.append(rate)
                self._funding_records.append({"timestamp": ts, "funding_rate": rate})
                seeded += 1
            if seeded:
                self._last_funding_settlement = pd.Timestamp(self._funding_records[-1]["timestamp"]).to_pydatetime()
                log.info("MLStrategy: seeded funding state with %d historical records", seeded)
        except Exception as exc:
            log.warning("MLStrategy: funding_buffer seed failed: %s", exc)

    @staticmethod
    def _current_funding_settlement() -> datetime:
        now = datetime.now(timezone.utc)
        settlement_hour = (now.hour // 8) * 8
        return now.replace(hour=settlement_hour, minute=0, second=0, microsecond=0)

    def _load_model(self) -> None:
        global _RELOAD_REQUESTED, _PRELOADED_MODEL
        if _PRELOADED_MODEL is not None:
            self._model = _PRELOADED_MODEL
            _PRELOADED_MODEL = None
            _RELOAD_REQUESTED = False
            log.info("MLStrategy: model set from preloaded instance")
        else:
            self._model = model_store.load_model("current")
            _RELOAD_REQUESTED = False
            if self._model is None:
                log.warning("MLStrategy: no trained model found at models/model_current.lgb")
            else:
                log.info("MLStrategy: model loaded successfully")

    async def _get_threshold(self) -> float:
        try:
            return await queries.get_ml_threshold()
        except Exception:
            pass
        try:
            val = await queries.get_setting("ml_threshold")
            if val is not None:
                return float(val)
        except Exception:
            pass
        return cfg.ML_DEFAULT_THRESHOLD

    async def _get_down_threshold(self, up_threshold: float) -> float:
        try:
            val = await queries.get_ml_down_threshold()
            if val is not None:
                return val
        except Exception:
            pass
        return round(1.0 - up_threshold, 4)

    def _get_down_enabled(self) -> bool:
        try:
            meta = model_store.load_metadata(self._model_slot)
            if meta is not None:
                if meta.get("down_override", False):
                    return True
                return bool(meta.get("down_enabled", False))
        except Exception:
            pass
        return False

    async def check_signal(self) -> dict[str, Any] | None:
        global _RELOAD_REQUESTED
        if _RELOAD_REQUESTED:
            self._load_model()

        slot_n1 = get_next_slot_info()
        slug = slot_n1["slug"]
        slot_ts = slot_n1["slot_start_ts"]
        slot_start_str = slot_n1["slot_start_str"]
        slot_end_str = slot_n1["slot_end_str"]

        base_fields: dict[str, Any] = {
            "skipped": True,
            "pattern": None,
            "candles_used": 400,
            "slot_n1_start_full": slot_n1["slot_start_full"],
            "slot_n1_end_full": slot_n1["slot_end_full"],
            "slot_n1_start_str": slot_start_str,
            "slot_n1_end_str": slot_end_str,
            "slot_n1_ts": slot_ts,
            "slot_n1_slug": slug,
        }

        if self._model is None:
            self._load_model()
            if self._model is None:
                inference_logger.log_skipped_data(
                    slot_slug=slug,
                    slot_ts=slot_ts,
                    slot_start_str=slot_start_str,
                    slot_end_str=slot_end_str,
                    skip_reason="No model loaded",
                )
                return {**base_fields, "reason": "No model loaded"}

        try:
            slot_end_ms = (int(slot_ts) * 1000) if isinstance(slot_ts, (int, float)) else int(slot_ts.timestamp() * 1000)
            data_start_ms = slot_end_ms - (450 * 5 * 60 * 1000)

            loop = asyncio.get_running_loop()
            df5 = await loop.run_in_executor(
                None,
                lambda: data_fetcher.fetch_live_5m(400, start_ms=data_start_ms, end_ms=slot_end_ms),
            )
            df15, df1h, funding_rate, cvd_live = await asyncio.gather(
                loop.run_in_executor(None, lambda: data_fetcher.fetch_live_15m(100, end_ms=slot_end_ms)),
                loop.run_in_executor(None, lambda: data_fetcher.fetch_live_1h(60, end_ms=slot_end_ms)),
                loop.run_in_executor(None, data_fetcher.fetch_live_funding),
                loop.run_in_executor(None, lambda: data_fetcher.fetch_live_gate_cvd(400, end_ms=slot_end_ms)),
            )

            df5_rows = len(df5) if df5 is not None else 0
            df15_rows = len(df15) if df15 is not None else 0
            df1h_rows = len(df1h) if df1h is not None else 0
            cvd_rows = len(cvd_live) if cvd_live is not None and not cvd_live.empty else 0
            candle_n1_ts = None
            candle_n1_close = None
            candle_n1_vol = None
            if df5_rows >= 2:
                try:
                    n1 = df5.iloc[-2]
                    ts_raw = n1["timestamp"]
                    if isinstance(ts_raw, pd.Timestamp):
                        candle_n1_ts = str(ts_raw.tz_localize("UTC").isoformat() if ts_raw.tzinfo is None else ts_raw.isoformat())
                    candle_n1_close = float(n1["close"])
                    candle_n1_vol = float(n1["volume"])
                except Exception as _e:
                    log.debug("inference_logger: candle_n1 extraction failed: %s", _e)

            funding_feature_records = self._funding_records
            try:
                funding_frame_end = pd.Timestamp(df5["timestamp"].max())
                if funding_frame_end.tzinfo is None:
                    funding_frame_end = funding_frame_end.tz_localize("UTC")
                else:
                    funding_frame_end = funding_frame_end.tz_convert("UTC")
                canonical_funding_hist = data_fetcher.fetch_live_funding_history(n_periods=24, end_ts=funding_frame_end)
                if canonical_funding_hist is not None and not canonical_funding_hist.empty:
                    refreshed_records: deque = deque(maxlen=24)
                    for row in canonical_funding_hist.itertuples(index=False):
                        ts = pd.Timestamp(row.timestamp)
                        if ts.tzinfo is None:
                            ts = ts.tz_localize("UTC")
                        else:
                            ts = ts.tz_convert("UTC")
                        rate = float(row.funding_rate)
                        refreshed_records.append({"timestamp": ts, "funding_rate": rate})
                    funding_feature_records = refreshed_records
                    self._funding_records = deque(refreshed_records, maxlen=24)
                    self._funding_buffer = deque((float(item["funding_rate"]) for item in refreshed_records), maxlen=24)
                    self._last_funding_settlement = pd.Timestamp(refreshed_records[-1]["timestamp"]).to_pydatetime()
                    if funding_rate is None:
                        funding_rate = float(refreshed_records[-1]["funding_rate"])
            except Exception as funding_refresh_exc:
                log.warning(
                    "MLStrategy: canonical funding history refresh failed; using cached records: %s",
                    funding_refresh_exc,
                )

            if funding_rate is not None:
                current_settlement = self._current_funding_settlement()
                if self._last_funding_settlement != current_settlement:
                    self._funding_buffer.append(funding_rate)
                    self._funding_records.append({"timestamp": pd.Timestamp(current_settlement), "funding_rate": float(funding_rate)})
                    funding_feature_records = self._funding_records
                    self._last_funding_settlement = current_settlement

            funding_buf_len = len(funding_feature_records)
            feature_row, nan_features = feat_eng.build_live_features(
                df5, df15, df1h, funding_rate, funding_feature_records, cvd_live
            )
            if feature_row is None:
                inference_logger.log_inference(
                    slot_slug=slug,
                    slot_ts=slot_ts,
                    slot_start_str=slot_start_str,
                    slot_end_str=slot_end_str,
                    df5_rows=df5_rows,
                    df15_rows=df15_rows,
                    df1h_rows=df1h_rows,
                    cvd_rows=cvd_rows,
                    funding_buf_len=funding_buf_len,
                    candle_n1_ts=candle_n1_ts,
                    candle_n1_close=candle_n1_close,
                    candle_n1_vol=candle_n1_vol,
                    feature_names=FEATURE_COLS,
                    feature_row=None,
                    nan_features=nan_features,
                    p_up=None,
                    p_down=None,
                    up_threshold=None,
                    down_threshold=None,
                    down_enabled=False,
                    fired=False,
                    side=None,
                    skip_reason="Insufficient data for features",
                )
                return {**base_fields, "reason": "Insufficient data for features"}

            prob = float(self._model.predict(feature_row)[0])
            up_threshold = await self._get_threshold()
            down_threshold = await self._get_down_threshold(up_threshold)
            prob_down = round(1.0 - prob, 6)
            up_qualifies = prob >= up_threshold
            down_enabled = self._get_down_enabled()
            regime_gate_enabled = True
            try:
                regime_gate_enabled = await queries.get_ml_volatility_gate_enabled()
            except Exception as _gate_setting_exc:
                log.warning(
                    "MLStrategy: volatility gate setting read failed; defaulting enabled: %s",
                    _gate_setting_exc,
                )

            if regime_gate_enabled:
                try:
                    _meta = model_store.load_metadata(self._model_slot)
                    if _meta is not None:
                        _regime_p5 = _meta.get("regime_vol_p5")
                        _regime_p95 = _meta.get("regime_vol_p95")
                        if _regime_p5 is not None and _regime_p95 is not None:
                            _vol_regime_idx = FEATURE_COLS.index("vol_regime")
                            _live_regime = float(feature_row[0, _vol_regime_idx])
                            if not (_regime_p5 <= _live_regime <= _regime_p95):
                                _regime_skip_reason = (
                                    f"Regime gate: vol_regime={_live_regime:.4f} outside training distribution [{_regime_p5:.4f}, {_regime_p95:.4f}] -- signal suppressed"
                                )
                                inference_logger.log_inference(
                                    slot_slug=slug,
                                    slot_ts=slot_ts,
                                    slot_start_str=slot_start_str,
                                    slot_end_str=slot_end_str,
                                    df5_rows=df5_rows,
                                    df15_rows=df15_rows,
                                    df1h_rows=df1h_rows,
                                    cvd_rows=cvd_rows,
                                    funding_buf_len=funding_buf_len,
                                    candle_n1_ts=candle_n1_ts,
                                    candle_n1_close=candle_n1_close,
                                    candle_n1_vol=candle_n1_vol,
                                    feature_names=FEATURE_COLS,
                                    feature_row=feature_row,
                                    nan_features=[],
                                    p_up=prob,
                                    p_down=prob_down,
                                    up_threshold=up_threshold,
                                    down_threshold=down_threshold,
                                    down_enabled=down_enabled,
                                    fired=False,
                                    side=None,
                                    skip_reason=_regime_skip_reason,
                                )
                                return {
                                    **base_fields,
                                    "pattern": f"p={prob:.4f} [regime_gate]",
                                    "reason": _regime_skip_reason,
                                    "ml_p_up": prob,
                                    "ml_p_down": prob_down,
                                    "ml_up_threshold": up_threshold,
                                    "ml_down_threshold": down_threshold,
                                    "ml_down_enabled": down_enabled,
                                }
                except Exception as _rge:
                    log.warning("MLStrategy: regime gate check failed (non-fatal, continuing): %s", _rge)

            down_qualifies = down_enabled and (prob_down >= down_threshold)
            if up_qualifies and down_qualifies:
                up_margin = prob - up_threshold
                down_margin = prob_down - down_threshold
                side = "Up" if up_margin >= down_margin else "Down"
            elif up_qualifies:
                side = "Up"
            elif down_qualifies:
                side = "Down"
            else:
                down_reason = "DOWN disabled (not validated)" if not down_enabled else f"p_down={prob_down:.4f}<{down_threshold:.3f}"
                skip_reason = f"Below threshold (p_up={prob:.4f}<{up_threshold:.3f}, {down_reason})"
                inference_logger.log_inference(
                    slot_slug=slug,
                    slot_ts=slot_ts,
                    slot_start_str=slot_start_str,
                    slot_end_str=slot_end_str,
                    df5_rows=df5_rows,
                    df15_rows=df15_rows,
                    df1h_rows=df1h_rows,
                    cvd_rows=cvd_rows,
                    funding_buf_len=funding_buf_len,
                    candle_n1_ts=candle_n1_ts,
                    candle_n1_close=candle_n1_close,
                    candle_n1_vol=candle_n1_vol,
                    feature_names=FEATURE_COLS,
                    feature_row=feature_row,
                    nan_features=[],
                    p_up=prob,
                    p_down=prob_down,
                    up_threshold=up_threshold,
                    down_threshold=down_threshold,
                    down_enabled=down_enabled,
                    fired=False,
                    side=None,
                    skip_reason=skip_reason,
                )
                return {
                    **base_fields,
                    "pattern": f"p={prob:.4f}<{up_threshold:.3f}",
                    "reason": skip_reason,
                    "ml_p_up": prob,
                    "ml_p_down": prob_down,
                    "ml_up_threshold": up_threshold,
                    "ml_down_threshold": down_threshold,
                    "ml_down_enabled": down_enabled,
                }

            prices = await get_slot_prices(slug)
            if prices is None:
                inference_logger.log_inference(
                    slot_slug=slug,
                    slot_ts=slot_ts,
                    slot_start_str=slot_start_str,
                    slot_end_str=slot_end_str,
                    df5_rows=df5_rows,
                    df15_rows=df15_rows,
                    df1h_rows=df1h_rows,
                    cvd_rows=cvd_rows,
                    funding_buf_len=funding_buf_len,
                    candle_n1_ts=candle_n1_ts,
                    candle_n1_close=candle_n1_close,
                    candle_n1_vol=candle_n1_vol,
                    feature_names=FEATURE_COLS,
                    feature_row=feature_row,
                    nan_features=[],
                    p_up=prob,
                    p_down=prob_down,
                    up_threshold=up_threshold,
                    down_threshold=down_threshold,
                    down_enabled=down_enabled,
                    fired=False,
                    side=side,
                    skip_reason="Market data unavailable (no Polymarket prices)",
                )
                return {
                    **base_fields,
                    "pattern": f"p={prob:.4f}",
                    "reason": "Market data unavailable",
                    "ml_p_up": prob,
                    "ml_p_down": prob_down,
                    "ml_up_threshold": up_threshold,
                    "ml_down_threshold": down_threshold,
                    "ml_down_enabled": down_enabled,
                }

            entry_price = prices["up_price"] if side == "Up" else prices["down_price"]
            opposite_price = prices["down_price"] if side == "Up" else prices["up_price"]
            token_id = prices["up_token_id"] if side == "Up" else prices["down_token_id"]
            opposite_token_id = prices["down_token_id"] if side == "Up" else prices["up_token_id"]
            routing_probability = prob if side == "Up" else prob_down
            routing_bucket = queries.truncate_probability_bucket(routing_probability)

            inference_logger.log_inference(
                slot_slug=slug,
                slot_ts=slot_ts,
                slot_start_str=slot_start_str,
                slot_end_str=slot_end_str,
                df5_rows=df5_rows,
                df15_rows=df15_rows,
                df1h_rows=df1h_rows,
                cvd_rows=cvd_rows,
                funding_buf_len=funding_buf_len,
                candle_n1_ts=candle_n1_ts,
                candle_n1_close=candle_n1_close,
                candle_n1_vol=candle_n1_vol,
                feature_names=FEATURE_COLS,
                feature_row=feature_row,
                nan_features=[],
                p_up=prob,
                p_down=prob_down,
                up_threshold=up_threshold,
                down_threshold=down_threshold,
                down_enabled=down_enabled,
                fired=True,
                side=side,
                skip_reason=None,
            )

            return {
                **base_fields,
                "skipped": False,
                "side": side,
                "model_side": side,
                "entry_price": entry_price,
                "opposite_price": opposite_price,
                "token_id": token_id,
                "opposite_token_id": opposite_token_id,
                "pattern": f"p_up={prob:.4f},p_down={prob_down:.4f}",
                "ml_p_up": prob,
                "ml_p_down": prob_down,
                "ml_up_threshold": up_threshold,
                "ml_down_threshold": down_threshold,
                "ml_down_enabled": down_enabled,
                "routing_probability": routing_probability,
                "routing_bucket": routing_bucket,
            }
        except Exception as exc:
            log.exception("MLStrategy.check_signal failed: %s", exc)
            return None
