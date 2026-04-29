"""CRUD helpers and analytics queries for signals, trades, settings, and redemptions."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN, InvalidOperation
from typing import Any

import aiosqlite
import config as cfg


VALID_THRESHOLD_POLICIES = {"FOLLOW", "BLOCK", "INVERT"}
VALID_THRESHOLD_MODES = {"real", "demo"}
VALID_ROLLING_WR_POLICIES = {"FOLLOW", "SKIP", "INVERT", "WARMUP"}


def make_redemption_key(condition_id: str, outcome_index: int | None = None) -> str:
    normalized = str(condition_id or '').strip().lower()
    if outcome_index is None:
        return normalized
    return f"{normalized}:{int(outcome_index)}"


def _normalize_redemption_attempt_state(success: bool, verified: bool) -> str:
    if verified:
        return "completed"
    if success:
        return "broadcast"
    return "failed"


def _db() -> str:
    return cfg.DB_PATH


def normalize_threshold_policy(value: str | None) -> str:
    normalized = (value or "FOLLOW").strip().upper()
    if normalized not in VALID_THRESHOLD_POLICIES:
        raise ValueError(f"invalid threshold policy: {value}")
    return normalized


def normalize_threshold_mode(value: str) -> str:
    normalized = (value or "").strip().lower()
    if normalized not in VALID_THRESHOLD_MODES:
        raise ValueError(f"invalid threshold mode: {value}")
    return normalized


def normalize_rolling_wr_policy(value: str | None) -> str:
    normalized = (value or "FOLLOW").strip().upper()
    if normalized not in VALID_ROLLING_WR_POLICIES:
        raise ValueError(f"invalid rolling WR policy: {value}")
    return normalized


def truncate_probability_bucket(probability: float | int | str | None) -> str | None:
    if probability is None:
        return None
    try:
        dec = Decimal(str(probability))
    except (InvalidOperation, ValueError, TypeError):
        return None
    if dec < Decimal("0"):
        dec = Decimal("0")
    if dec > Decimal("1"):
        dec = Decimal("1")
    return format(dec.quantize(Decimal("0.00"), rounding=ROUND_DOWN), ".2f")


def invert_side(side: str | None) -> str | None:
    if side == "Up":
        return "Down"
    if side == "Down":
        return "Up"
    return side


def _parse_bool_setting(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default


def _coerce_int(value: str | None, default: int, minimum: int = 1) -> int:
    try:
        parsed = int(float(value)) if value is not None else default
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, parsed)


def _coerce_float(value: str | None, default: float) -> float:
    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

async def get_setting(key: str) -> str | None:
    async with aiosqlite.connect(_db()) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT value FROM settings WHERE key = ?", (key,))
        row = await cursor.fetchone()
        return row["value"] if row else None


async def set_setting(key: str, value: str) -> None:
    async with aiosqlite.connect(_db()) as db:
        await db.execute(
            "INSERT INTO settings (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )
        await db.commit()


async def is_autotrade_enabled() -> bool:
    val = await get_setting("autotrade_enabled")
    return val == "true"


async def get_trade_amount() -> float:
    val = await get_setting("trade_amount_usdc")
    return float(val) if val else cfg.TRADE_AMOUNT_USDC


async def get_trade_mode() -> str:
    val = await get_setting("trade_mode")
    return val if val in ("fixed", "pct") else "fixed"


async def get_trade_pct() -> float:
    try:
        val = await get_setting("trade_pct")
        pct = float(val) if val else cfg.TRADE_PCT
        return pct if 0 < pct <= 100 else cfg.TRADE_PCT
    except (ValueError, TypeError):
        return cfg.TRADE_PCT


async def get_ml_volatility_gate_enabled() -> bool:
    try:
        val = await get_setting("ml_volatility_gate_enabled")
    except Exception:
        return True
    if val is None:
        return True
    normalized = str(val).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return True


async def set_ml_volatility_gate_enabled(enabled: bool) -> None:
    await set_setting("ml_volatility_gate_enabled", "1" if enabled else "0")


async def resolve_trade_amount(poly_client=None, is_demo: bool = False) -> tuple[float, str]:
    import logging
    log = logging.getLogger(__name__)

    mode = await get_trade_mode()
    fixed_amount = await get_trade_amount()

    if mode == "fixed":
        return fixed_amount, f"${fixed_amount:.2f} (fixed)"

    pct = await get_trade_pct()

    try:
        if is_demo:
            balance = await get_demo_bankroll()
        else:
            if poly_client is None:
                log.warning(
                    "resolve_trade_amount: pct mode but poly_client is None - falling back to fixed amount $%.2f",
                    fixed_amount,
                )
                return fixed_amount, f"${fixed_amount:.2f} (fixed, fallback)"

            from polymarket import account as pm_account
            balance = await pm_account.get_balance(poly_client)
            if balance is None:
                log.warning(
                    "resolve_trade_amount: balance fetch returned None - falling back to fixed amount $%.2f",
                    fixed_amount,
                )
                return fixed_amount, f"${fixed_amount:.2f} (fixed, fallback)"
    except Exception as exc:
        log.warning(
            "resolve_trade_amount: balance fetch failed (%s) - falling back to fixed amount $%.2f",
            exc,
            fixed_amount,
        )
        return fixed_amount, f"${fixed_amount:.2f} (fixed, fallback)"

    if balance <= 0:
        return 1.0, f"$1.00 ({pct:.1f}% of ${balance:.2f}, floor applied)"

    raw = balance * (pct / 100.0)
    amount = max(1.0, round(raw, 2))
    label = f"${amount:.2f} ({pct:.1f}% of ${balance:.2f})"
    if raw < 1.0:
        label += " [floor $1.00]"
    return amount, label


async def is_auto_redeem_enabled() -> bool:
    val = await get_setting("auto_redeem_enabled")
    return val == "true"


async def is_invert_trades_enabled() -> bool:
    val = await get_setting("invert_trades_enabled")
    return val == "true"


# ---------------------------------------------------------------------------
# Rolling WR settings and analytics
# ---------------------------------------------------------------------------

async def get_rolling_wr_config() -> dict[str, Any]:
    enabled = _parse_bool_setting(await get_setting("rolling_wr_enabled"), True)
    window_size = _coerce_int(await get_setting("rolling_wr_window_size"), 320, minimum=1)
    follow_below = _coerce_float(await get_setting("rolling_wr_follow_below"), 49.0)
    invert_above = _coerce_float(await get_setting("rolling_wr_invert_above"), 51.0)
    min_samples = _coerce_int(await get_setting("rolling_wr_min_samples"), window_size, minimum=1)
    skip_when_unready = _parse_bool_setting(await get_setting("rolling_wr_skip_when_unready"), True)
    if min_samples > window_size:
        min_samples = window_size
    return {
        "enabled": enabled,
        "window_size": window_size,
        "follow_below": follow_below,
        "invert_above": invert_above,
        "min_samples": min_samples,
        "skip_when_unready": skip_when_unready,
    }


async def set_rolling_wr_enabled(enabled: bool) -> None:
    await set_setting("rolling_wr_enabled", "true" if enabled else "false")


async def set_rolling_wr_window_size(window_size: int) -> None:
    window_size = max(1, int(window_size))
    await set_setting("rolling_wr_window_size", str(window_size))
    current_min = _coerce_int(await get_setting("rolling_wr_min_samples"), window_size, minimum=1)
    if current_min > window_size:
        await set_setting("rolling_wr_min_samples", str(window_size))


async def set_rolling_wr_follow_below(value: float) -> None:
    await set_setting("rolling_wr_follow_below", f"{float(value):.4f}")


async def set_rolling_wr_invert_above(value: float) -> None:
    await set_setting("rolling_wr_invert_above", f"{float(value):.4f}")


async def set_rolling_wr_min_samples(value: int) -> None:
    config = await get_rolling_wr_config()
    value = max(1, min(int(value), config["window_size"]))
    await set_setting("rolling_wr_min_samples", str(value))


async def set_rolling_wr_skip_when_unready(enabled: bool) -> None:
    await set_setting("rolling_wr_skip_when_unready", "true" if enabled else "false")


async def get_rolling_wr_latest_batch() -> dict[str, Any] | None:
    async with aiosqlite.connect(_db()) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM rolling_wr_import_batches ORDER BY id DESC LIMIT 1"
        )
        row = await cursor.fetchone()
        return dict(row) if row else None


async def get_rolling_wr_history_window(limit: int | None = None) -> list[dict[str, Any]]:
    config = await get_rolling_wr_config()
    effective_limit = max(1, int(limit or config["window_size"]))
    async with aiosqlite.connect(_db()) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM rolling_wr_history ORDER BY slot_timestamp DESC, id DESC LIMIT ?",
            (effective_limit,),
        )
        rows = await cursor.fetchall()
        ordered = [dict(r) for r in rows]
    return list(reversed(ordered))


async def get_rolling_wr_recent_history(limit: int = 10) -> list[dict[str, Any]]:
    limit = max(1, int(limit))
    async with aiosqlite.connect(_db()) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM rolling_wr_history ORDER BY slot_timestamp DESC, id DESC LIMIT ?",
            (limit,),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


async def get_rolling_wr_history_counts() -> dict[str, int]:
    async with aiosqlite.connect(_db()) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT source, COUNT(*) AS cnt FROM rolling_wr_history GROUP BY source"
        )
        rows = await cursor.fetchall()
    counts = {"import": 0, "live": 0}
    for row in rows:
        source = (row["source"] or "live").lower()
        if source in counts:
            counts[source] = int(row["cnt"] or 0)
    counts["total"] = counts["import"] + counts["live"]
    return counts


async def evaluate_rolling_wr() -> dict[str, Any]:
    config = await get_rolling_wr_config()
    window_rows = await get_rolling_wr_history_window(config["window_size"])
    sample_size = len(window_rows)
    wins = sum(1 for row in window_rows if int(row.get("is_correct") or 0) == 1)
    losses = sample_size - wins
    win_rate = round((wins / sample_size) * 100, 2) if sample_size else None
    ready = sample_size >= config["min_samples"]

    if not config["enabled"]:
        policy = "FOLLOW"
        reason = "rolling WR disabled"
    elif not ready:
        policy = "SKIP" if config["skip_when_unready"] else "FOLLOW"
        reason = "warming up"
    elif win_rate is not None and win_rate > config["invert_above"]:
        policy = "INVERT"
        reason = f"WR {win_rate:.2f}% above {config['invert_above']:.2f}%"
    elif win_rate is not None and win_rate < config["follow_below"]:
        policy = "FOLLOW"
        reason = f"WR {win_rate:.2f}% below {config['follow_below']:.2f}%"
    else:
        policy = "SKIP"
        reason = f"WR {win_rate:.2f}% in neutral zone" if win_rate is not None else "no rolling WR history"

    counts = await get_rolling_wr_history_counts()
    latest_batch = await get_rolling_wr_latest_batch()
    source_mix = {
        "import_count": counts["import"],
        "live_count": counts["live"],
        "window_live_count": sum(1 for row in window_rows if (row.get("source") or "live") == "live"),
        "window_import_count": sum(1 for row in window_rows if (row.get("source") or "live") == "import"),
    }

    return {
        "enabled": config["enabled"],
        "policy": policy,
        "win_rate": win_rate,
        "sample_size": sample_size,
        "window_size": config["window_size"],
        "min_samples": config["min_samples"],
        "ready": ready,
        "wins": wins,
        "losses": losses,
        "follow_below": config["follow_below"],
        "invert_above": config["invert_above"],
        "skip_when_unready": config["skip_when_unready"],
        "reason": reason,
        "window_rows": window_rows,
        "source_mix": source_mix,
        "counts": counts,
        "latest_batch": latest_batch,
    }


async def append_rolling_wr_live_result(
    *,
    signal_id: int,
    slot_timestamp: int,
    slot_start: str | None,
    signal_slug: str | None,
    original_side: str,
    winner_side: str,
) -> int | None:
    if not original_side or not winner_side:
        return None
    is_correct = 1 if winner_side == original_side else 0
    async with aiosqlite.connect(_db()) as db:
        db.row_factory = aiosqlite.Row
        existing = await (
            await db.execute(
                "SELECT id FROM rolling_wr_history WHERE signal_id = ? AND source = 'live' LIMIT 1",
                (signal_id,),
            )
        ).fetchone()
        if existing:
            return int(existing["id"])
        cursor = await db.execute(
            "INSERT INTO rolling_wr_history (signal_id, slot_timestamp, slot_start, signal_slug, original_side, winner_side, is_correct, source) VALUES (?, ?, ?, ?, ?, ?, ?, 'live')",
            (signal_id, slot_timestamp, slot_start, signal_slug, original_side, winner_side, is_correct),
        )
        await db.commit()
        return cursor.lastrowid


async def get_signal_by_id(signal_id: int) -> dict[str, Any] | None:
    async with aiosqlite.connect(_db()) as db:
        db.row_factory = aiosqlite.Row
        row = await (
            await db.execute("SELECT * FROM signals WHERE id = ?", (signal_id,))
        ).fetchone()
        return dict(row) if row else None


async def update_signal_rolling_wr_fields(
    signal_id: int,
    status: dict[str, Any],
) -> None:
    async with aiosqlite.connect(_db()) as db:
        await db.execute(
            "UPDATE signals SET rolling_wr_policy = ?, rolling_wr_wr = ?, rolling_wr_window_size = ?, rolling_wr_sample_size = ?, rolling_wr_follow_below = ?, rolling_wr_invert_above = ?, rolling_wr_ready = ?, rolling_wr_source = ? WHERE id = ?",
            (
                normalize_rolling_wr_policy(status.get("policy")),
                status.get("win_rate"),
                status.get("window_size"),
                status.get("sample_size"),
                status.get("follow_below"),
                status.get("invert_above"),
                1 if status.get("ready") else 0,
                "live+import",
                signal_id,
            ),
        )
        await db.commit()


async def refresh_signal_rolling_wr_snapshot(signal_id: int) -> None:
    signal = await get_signal_by_id(signal_id)
    if not signal:
        return
    await update_signal_rolling_wr_fields(signal_id, await evaluate_rolling_wr())


async def update_trade_rolling_wr_fields(trade_id: int, status: dict[str, Any]) -> None:
    async with aiosqlite.connect(_db()) as db:
        await db.execute(
            "UPDATE trades SET rolling_wr_policy = ?, rolling_wr_wr = ?, rolling_wr_window_size = ?, rolling_wr_sample_size = ?, rolling_wr_ready = ? WHERE id = ?",
            (
                normalize_rolling_wr_policy(status.get("policy")),
                status.get("win_rate"),
                status.get("window_size"),
                status.get("sample_size"),
                1 if status.get("ready") else 0,
                trade_id,
            ),
        )
        await db.commit()


async def get_rolling_wr_analytics() -> dict[str, Any]:
    status = await evaluate_rolling_wr()
    recent_history = await get_rolling_wr_recent_history(limit=10)
    recent_signals: list[dict[str, Any]] = []
    async with aiosqlite.connect(_db()) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT id, slot_start, rolling_wr_policy, rolling_wr_wr, rolling_wr_sample_size, rolling_wr_ready, side, model_side FROM signals WHERE skipped = 0 AND rolling_wr_policy IS NOT NULL ORDER BY id DESC LIMIT 10"
        )
        rows = await cursor.fetchall()
        recent_signals = [dict(r) for r in rows]

    policy_distribution = {"FOLLOW": 0, "SKIP": 0, "INVERT": 0, "WARMUP": 0}
    for row in recent_signals:
        policy = normalize_rolling_wr_policy(row.get("rolling_wr_policy"))
        policy_distribution[policy] = policy_distribution.get(policy, 0) + 1

    return {
        **status,
        "recent_history": recent_history,
        "recent_signals": recent_signals,
        "policy_distribution": policy_distribution,
    }


async def replace_rolling_wr_import(
    *,
    filename: str | None,
    rows: list[dict[str, Any]],
    window_size_hint: int,
    notes: str | None = None,
) -> dict[str, Any]:
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    accepted = len(rows)
    async with aiosqlite.connect(_db()) as db:
        cursor = await db.execute(
            "INSERT INTO rolling_wr_import_batches (source_filename, replace_existing, row_count, window_size_hint, notes) VALUES (?, 1, ?, ?, ?)",
            (filename, accepted, window_size_hint, notes),
        )
        batch_id = cursor.lastrowid
        await db.execute("DELETE FROM rolling_wr_history WHERE source = 'import'")
        for row in rows:
            await db.execute(
                "INSERT INTO rolling_wr_history (signal_id, slot_timestamp, slot_start, signal_slug, original_side, winner_side, is_correct, source, imported_batch_id, imported_at) VALUES (?, ?, ?, ?, ?, ?, ?, 'import', ?, ?)",
                (
                    None,
                    int(row["slot_timestamp"]),
                    row.get("slot_start"),
                    row.get("signal_slug"),
                    row["original_side"],
                    row["winner_side"],
                    int(row["is_correct"]),
                    batch_id,
                    timestamp,
                ),
            )
        await db.commit()
    await set_setting("rolling_wr_last_imported_at", timestamp)
    await set_setting("rolling_wr_last_import_count", str(accepted))
    await set_setting("rolling_wr_import_source", filename or "uploaded file")
    status = await evaluate_rolling_wr()
    return {
        "batch_id": batch_id,
        "accepted_rows": accepted,
        "status": status,
    }


# ---------------------------------------------------------------------------
# Threshold policy helpers
# ---------------------------------------------------------------------------

async def get_threshold_policy(bucket: str | None, mode: str) -> str:
    normalized_mode = normalize_threshold_mode(mode)
    normalized_bucket = truncate_probability_bucket(bucket)
    if normalized_bucket is None:
        return "FOLLOW"
    async with aiosqlite.connect(_db()) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT policy FROM threshold_policies WHERE probability_bucket = ? AND mode = ?",
            (normalized_bucket, normalized_mode),
        )
        row = await cursor.fetchone()
        if not row:
            return "FOLLOW"
        try:
            return normalize_threshold_policy(row["policy"])
        except ValueError:
            return "FOLLOW"


async def set_threshold_policy(bucket: str | float, mode: str, policy: str) -> None:
    normalized_mode = normalize_threshold_mode(mode)
    normalized_policy = normalize_threshold_policy(policy)
    normalized_bucket = truncate_probability_bucket(bucket)
    if normalized_bucket is None:
        raise ValueError("invalid probability bucket")
    async with aiosqlite.connect(_db()) as db:
        await db.execute(
            "INSERT INTO threshold_policies (probability_bucket, mode, policy, updated_at) VALUES (?, ?, ?, CURRENT_TIMESTAMP) "
            "ON CONFLICT(probability_bucket, mode) DO UPDATE SET policy = excluded.policy, updated_at = CURRENT_TIMESTAMP",
            (normalized_bucket, normalized_mode, normalized_policy),
        )
        await db.commit()


async def clear_threshold_policy(bucket: str | float, mode: str) -> None:
    normalized_mode = normalize_threshold_mode(mode)
    normalized_bucket = truncate_probability_bucket(bucket)
    if normalized_bucket is None:
        raise ValueError("invalid probability bucket")
    async with aiosqlite.connect(_db()) as db:
        await db.execute(
            "DELETE FROM threshold_policies WHERE probability_bucket = ? AND mode = ?",
            (normalized_bucket, normalized_mode),
        )
        await db.commit()


async def list_threshold_policies(mode: str | None = None) -> list[dict[str, Any]]:
    async with aiosqlite.connect(_db()) as db:
        db.row_factory = aiosqlite.Row
        if mode:
            normalized_mode = normalize_threshold_mode(mode)
            cursor = await db.execute(
                "SELECT probability_bucket, mode, policy, updated_at FROM threshold_policies WHERE mode = ? ORDER BY probability_bucket ASC",
                (normalized_mode,),
            )
        else:
            cursor = await db.execute(
                "SELECT probability_bucket, mode, policy, updated_at FROM threshold_policies ORDER BY mode ASC, probability_bucket ASC"
            )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


async def get_threshold_policy_matrix(mode: str | None = None) -> dict[str, dict[str, str]]:
    rows = await list_threshold_policies(mode=mode)
    matrix: dict[str, dict[str, str]] = {"real": {}, "demo": {}}
    for row in rows:
        matrix.setdefault(row["mode"], {})[row["probability_bucket"]] = row["policy"]
    return matrix


async def decide_threshold_route(
    *,
    original_side: str,
    effective_side: str | None = None,
    probability: float | None,
    bucket: str | None,
    mode: str,
    legacy_invert_enabled: bool = False,
    legacy_blocked_ranges: list[tuple[float, float]] | None = None,
) -> dict[str, Any]:
    normalized_mode = normalize_threshold_mode(mode)
    normalized_bucket = truncate_probability_bucket(bucket or probability)
    policy = await get_threshold_policy(normalized_bucket, normalized_mode)
    reasons: list[str] = []
    base_side = effective_side or original_side

    if policy == "FOLLOW":
        if legacy_invert_enabled and normalized_mode == "real":
            policy = "INVERT"
            reasons.append("legacy global invert applied as fallback")
        elif legacy_blocked_ranges and probability is not None:
            for lo, hi in legacy_blocked_ranges:
                if lo <= float(probability) <= hi:
                    policy = "BLOCK"
                    reasons.append(f"legacy blocked threshold range [{lo:.2f}, {hi:.2f}] applied as fallback")
                    break

    routed_side = None if policy == "BLOCK" else base_side
    if policy == "INVERT":
        routed_side = invert_side(base_side)

    return {
        "mode": normalized_mode,
        "policy": policy,
        "bucket": normalized_bucket,
        "probability": probability,
        "original_side": original_side,
        "effective_side": base_side,
        "routed_side": routed_side,
        "blocked": policy == "BLOCK",
        "reason": "; ".join(reasons) if reasons else None,
    }


# ---------------------------------------------------------------------------
# Signal CRUD
# ---------------------------------------------------------------------------

async def insert_signal(
    slot_start: str,
    slot_end: str,
    slot_timestamp: int,
    side: str | None,
    entry_price: float | None,
    opposite_price: float | None,
    skipped: bool = False,
    filter_blocked: bool = False,
    pattern: str | None = None,
    ml_p_up: float | None = None,
    ml_p_down: float | None = None,
    ml_probability_bucket: str | None = None,
    ml_probability_used: float | None = None,
    threshold_policy_real: str | None = None,
    threshold_policy_demo: str | None = None,
    model_side: str | None = None,
    signal_slug: str | None = None,
    rolling_wr_policy: str | None = None,
    rolling_wr_wr: float | None = None,
    rolling_wr_window_size: int | None = None,
    rolling_wr_sample_size: int | None = None,
    rolling_wr_follow_below: float | None = None,
    rolling_wr_invert_above: float | None = None,
    rolling_wr_ready: bool | None = None,
    rolling_wr_source: str | None = None,
) -> int:
    async with aiosqlite.connect(_db()) as db:
        cursor = await db.execute(
            "INSERT INTO signals (slot_start, slot_end, slot_timestamp, side, entry_price, opposite_price, skipped, filter_blocked, pattern, ml_p_up, ml_p_down, ml_probability_bucket, ml_probability_used, threshold_policy_real, threshold_policy_demo, rolling_wr_policy, rolling_wr_wr, rolling_wr_window_size, rolling_wr_sample_size, rolling_wr_follow_below, rolling_wr_invert_above, rolling_wr_ready, rolling_wr_source, model_side, signal_slug) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                slot_start,
                slot_end,
                slot_timestamp,
                side,
                entry_price,
                opposite_price,
                1 if skipped else 0,
                1 if filter_blocked else 0,
                pattern,
                ml_p_up,
                ml_p_down,
                ml_probability_bucket,
                ml_probability_used,
                normalize_threshold_policy(threshold_policy_real) if threshold_policy_real else None,
                normalize_threshold_policy(threshold_policy_demo) if threshold_policy_demo else None,
                normalize_rolling_wr_policy(rolling_wr_policy) if rolling_wr_policy else None,
                rolling_wr_wr,
                rolling_wr_window_size,
                rolling_wr_sample_size,
                rolling_wr_follow_below,
                rolling_wr_invert_above,
                1 if rolling_wr_ready else 0 if rolling_wr_ready is not None else None,
                rolling_wr_source,
                model_side,
                signal_slug,
            ),
        )
        await db.commit()
        return cursor.lastrowid


async def resolve_signal(signal_id: int, outcome: str, is_win: bool) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    async with aiosqlite.connect(_db()) as db:
        await db.execute(
            "UPDATE signals SET outcome = ?, is_win = ?, resolved_at = ? WHERE id = ?",
            (outcome, 1 if is_win else 0, now, signal_id),
        )
        await db.commit()


async def mark_signal_resolved_if_unset(signal_id: int, outcome: str, is_win: bool) -> bool:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    async with aiosqlite.connect(_db()) as db:
        cursor = await db.execute(
            "UPDATE signals SET outcome = ?, is_win = ?, resolved_at = ? WHERE id = ? AND is_win IS NULL",
            (outcome, 1 if is_win else 0, now, signal_id),
        )
        await db.commit()
        return cursor.rowcount > 0


async def get_recent_signals(n: int = 10) -> list[dict[str, Any]]:
    async with aiosqlite.connect(_db()) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM signals ORDER BY id DESC LIMIT ?", (n,))
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


async def get_unresolved_signals() -> list[dict[str, Any]]:
    async with aiosqlite.connect(_db()) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM signals WHERE is_win IS NULL AND skipped = 0 ORDER BY id"
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


async def get_last_signal() -> dict[str, Any] | None:
    async with aiosqlite.connect(_db()) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM signals WHERE skipped = 0 ORDER BY id DESC LIMIT 1"
        )
        row = await cursor.fetchone()
        return dict(row) if row else None


# ---------------------------------------------------------------------------
# Trade CRUD
# ---------------------------------------------------------------------------

async def insert_trade(
    signal_id: int,
    slot_start: str,
    slot_end: str,
    side: str,
    entry_price: float,
    amount_usdc: float,
    order_id: str | None = None,
    fill_price: float | None = None,
    status: str = "pending",
    is_demo: bool = False,
    routing_mode: str | None = None,
    routing_policy: str | None = None,
    original_side: str | None = None,
    routed_side: str | None = None,
    policy_bucket: str | None = None,
    policy_probability: float | None = None,
    rolling_wr_policy: str | None = None,
    rolling_wr_wr: float | None = None,
    rolling_wr_window_size: int | None = None,
    rolling_wr_sample_size: int | None = None,
    rolling_wr_ready: bool | None = None,
) -> int:
    async with aiosqlite.connect(_db()) as db:
        cursor = await db.execute(
            "INSERT INTO trades (signal_id, slot_start, slot_end, side, entry_price, amount_usdc, order_id, fill_price, status, is_demo, routing_mode, routing_policy, original_side, routed_side, policy_bucket, policy_probability, rolling_wr_policy, rolling_wr_wr, rolling_wr_window_size, rolling_wr_sample_size, rolling_wr_ready) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                signal_id,
                slot_start,
                slot_end,
                side,
                entry_price,
                amount_usdc,
                order_id,
                fill_price,
                status,
                1 if is_demo else 0,
                normalize_threshold_mode(routing_mode) if routing_mode else ("demo" if is_demo else "real"),
                normalize_threshold_policy(routing_policy) if routing_policy else "FOLLOW",
                original_side or side,
                routed_side or side,
                truncate_probability_bucket(policy_bucket),
                policy_probability,
                normalize_rolling_wr_policy(rolling_wr_policy) if rolling_wr_policy else None,
                rolling_wr_wr,
                rolling_wr_window_size,
                rolling_wr_sample_size,
                1 if rolling_wr_ready else 0 if rolling_wr_ready is not None else None,
            ),
        )
        await db.commit()
        return cursor.lastrowid


async def update_trade_status(trade_id: int, status: str, order_id: str | None = None) -> None:
    async with aiosqlite.connect(_db()) as db:
        if order_id:
            await db.execute(
                "UPDATE trades SET status = ?, order_id = ? WHERE id = ?",
                (status, order_id, trade_id),
            )
        else:
            await db.execute("UPDATE trades SET status = ? WHERE id = ?", (status, trade_id))
        await db.commit()


async def update_trade_retry(
    trade_id: int,
    status: str,
    retry_count: int,
    order_id: str | None = None,
) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    async with aiosqlite.connect(_db()) as db:
        if order_id:
            await db.execute(
                "UPDATE trades SET status = ?, retry_count = ?, last_retry_at = ?, order_id = ? WHERE id = ?",
                (status, retry_count, now, order_id, trade_id),
            )
        else:
            await db.execute(
                "UPDATE trades SET status = ?, retry_count = ?, last_retry_at = ? WHERE id = ?",
                (status, retry_count, now, trade_id),
            )
        await db.commit()


async def mark_trade_signal_outcome_recorded(trade_id: int) -> None:
    async with aiosqlite.connect(_db()) as db:
        await db.execute(
            "UPDATE trades SET signal_outcome_recorded = 1 WHERE id = ?",
            (trade_id,),
        )
        await db.commit()


async def get_active_trade_for_signal(signal_id: int) -> dict[str, Any] | None:
    async with aiosqlite.connect(_db()) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM trades WHERE signal_id = ? AND status = 'filled' AND is_demo = 0 LIMIT 1",
            (signal_id,),
        )
        row = await cursor.fetchone()
        return dict(row) if row else None


async def resolve_trade(trade_id: int, outcome: str, is_win: bool, pnl: float) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    async with aiosqlite.connect(_db()) as db:
        await db.execute(
            "UPDATE trades SET outcome = ?, is_win = ?, pnl = ?, resolved_at = ? WHERE id = ?",
            (outcome, 1 if is_win else 0, pnl, now, trade_id),
        )
        await db.commit()


async def get_recent_trades(n: int = 10) -> list[dict[str, Any]]:
    async with aiosqlite.connect(_db()) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM trades WHERE is_demo = 0 ORDER BY id DESC LIMIT ?", (n,))
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


async def get_unresolved_trades() -> list[dict[str, Any]]:
    async with aiosqlite.connect(_db()) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM trades WHERE is_win IS NULL AND status IN ('pending', 'filled') ORDER BY id"
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


async def get_trade_by_signal(signal_id: int) -> dict[str, Any] | None:
    trades = await get_trades_by_signal(signal_id)
    return trades[0] if trades else None


async def get_trades_by_signal(signal_id: int) -> list[dict[str, Any]]:
    async with aiosqlite.connect(_db()) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM trades WHERE signal_id = ? ORDER BY is_demo ASC, id ASC",
            (signal_id,),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Redemption CRUD
# ---------------------------------------------------------------------------

async def insert_redemption(
    condition_id: str,
    outcome_index: int | None,
    size: float,
    title: str | None,
    tx_hash: str | None,
    status: str,
    error: str | None = None,
    gas_used: int | None = None,
    dry_run: bool = False,
    verified: bool = False,
    redemption_key: str | None = None,
    attempt_state: str | None = None,
) -> int:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    resolved_at = now if status in ("success", "failed", "verified") else None
    verified_at = now if verified else None
    redemption_key = redemption_key or make_redemption_key(condition_id, outcome_index)
    attempt_state = attempt_state or _normalize_redemption_attempt_state(status in ("success", "verified"), verified)
    async with aiosqlite.connect(_db()) as db:
        cursor = await db.execute(
            "INSERT INTO redemptions (condition_id, outcome_index, size, title, tx_hash, status, error, gas_used, dry_run, resolved_at, verified, verified_at, redemption_key, attempt_state) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                condition_id,
                outcome_index,
                size,
                title,
                tx_hash,
                status,
                error,
                gas_used,
                1 if dry_run else 0,
                resolved_at,
                1 if verified else 0,
                verified_at,
                redemption_key,
                attempt_state,
            ),
        )
        await db.commit()
        return cursor.lastrowid


async def get_recent_redemptions(n: int = 20) -> list[dict[str, Any]]:
    async with aiosqlite.connect(_db()) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM redemptions WHERE dry_run = 0 ORDER BY id DESC LIMIT ?",
            (n,),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


async def redemption_already_recorded(condition_id: str, outcome_index: int | None = None) -> bool:
    redemption_key = make_redemption_key(condition_id, outcome_index)
    async with aiosqlite.connect(_db()) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT id FROM redemptions WHERE redemption_key = ? AND dry_run = 0 AND attempt_state IN ('pending', 'broadcast', 'completed') LIMIT 1",
            (redemption_key,),
        )
        row = await cursor.fetchone()
        return row is not None


async def delete_redemptions_for_condition(condition_id: str) -> int:
    async with aiosqlite.connect(_db()) as db:
        cursor = await db.execute(
            "DELETE FROM redemptions WHERE condition_id = ? AND dry_run = 0",
            (condition_id,),
        )
        await db.commit()
        return cursor.rowcount


async def update_redemption_verified(redemption_id: int) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    async with aiosqlite.connect(_db()) as db:
        await db.execute(
            "UPDATE redemptions SET verified = 1, verified_at = ?, status = 'verified' WHERE id = ?",
            (now, redemption_id),
        )
        await db.commit()


async def get_unverified_success_redemptions() -> list[dict[str, Any]]:
    async with aiosqlite.connect(_db()) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM redemptions WHERE status = 'success' AND verified = 0 AND dry_run = 0 ORDER BY id ASC"
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


async def get_redemption_stats() -> dict[str, Any]:
    async with aiosqlite.connect(_db()) as db:
        db.row_factory = aiosqlite.Row
        total_row = await (await db.execute("SELECT COUNT(*) as cnt FROM redemptions WHERE dry_run = 0")).fetchone()
        success_row = await (await db.execute("SELECT COUNT(*) as cnt FROM redemptions WHERE dry_run = 0 AND status = 'success'" )).fetchone()
        failed_row = await (await db.execute("SELECT COUNT(*) as cnt FROM redemptions WHERE dry_run = 0 AND status = 'failed'" )).fetchone()
        size_row = await (await db.execute("SELECT SUM(size) as total_size FROM redemptions WHERE dry_run = 0 AND status = 'success'" )).fetchone()
    return {
        "total": total_row["cnt"] if total_row else 0,
        "success": success_row["cnt"] if success_row else 0,
        "failed": failed_row["cnt"] if failed_row else 0,
        "total_size": round(float(size_row["total_size"] or 0) if size_row else 0.0, 4),
    }


# ---------------------------------------------------------------------------
# Streak helpers
# ---------------------------------------------------------------------------

def _compute_streaks(results: list[int]) -> dict[str, Any]:
    if not results:
        return {
            "current_streak": 0,
            "current_streak_type": None,
            "best_win_streak": 0,
            "worst_loss_streak": 0,
        }
    best_win = 0
    worst_loss = 0
    streak = 1
    prev = results[0]
    for i in range(len(results)):
        if i == 0:
            streak = 1
        elif results[i] == prev:
            streak += 1
        else:
            streak = 1
        prev = results[i]
        if results[i] == 1:
            best_win = max(best_win, streak)
        else:
            worst_loss = max(worst_loss, streak)
    current_type = results[-1]
    current = 0
    for v in reversed(results):
        if v == current_type:
            current += 1
        else:
            break
    return {
        "current_streak": current,
        "current_streak_type": "W" if current_type == 1 else "L",
        "best_win_streak": best_win,
        "worst_loss_streak": worst_loss,
    }


# ---------------------------------------------------------------------------
# Analytics
# ---------------------------------------------------------------------------

async def get_signal_stats(limit: int | None = None) -> dict[str, Any]:
    async with aiosqlite.connect(_db()) as db:
        db.row_factory = aiosqlite.Row
        total_row = await (await db.execute("SELECT COUNT(*) as cnt FROM signals WHERE skipped = 0")).fetchone()
        skip_row = await (await db.execute("SELECT COUNT(*) as cnt FROM signals WHERE skipped = 1")).fetchone()
        if limit:
            inner = f"SELECT * FROM signals WHERE skipped = 0 AND is_win IS NOT NULL ORDER BY id DESC LIMIT {limit}"
            query = f"SELECT is_win FROM ({inner}) ORDER BY id ASC"
        else:
            query = "SELECT is_win FROM signals WHERE skipped = 0 AND is_win IS NOT NULL ORDER BY id ASC"
        cursor = await db.execute(query)
        rows = await cursor.fetchall()
        results = [r["is_win"] for r in rows]
    wins = sum(1 for r in results if r == 1)
    losses = sum(1 for r in results if r == 0)
    resolved = wins + losses
    win_pct = (wins / resolved * 100) if resolved else 0.0
    streaks = _compute_streaks(results)
    return {
        "total_signals": total_row["cnt"] if total_row else 0,
        "skip_count": skip_row["cnt"] if skip_row else 0,
        "wins": wins,
        "losses": losses,
        "resolved": resolved,
        "win_pct": round(win_pct, 1),
        **streaks,
    }


async def _get_trade_stats_by_demo_flag(is_demo: bool, limit: int | None = None) -> dict[str, Any]:
    async with aiosqlite.connect(_db()) as db:
        db.row_factory = aiosqlite.Row
        flag = 1 if is_demo else 0
        if limit:
            inner = f"SELECT * FROM trades WHERE is_win IS NOT NULL AND is_demo = {flag} ORDER BY id DESC LIMIT {limit}"
            query = f"SELECT is_win, amount_usdc, pnl FROM ({inner}) ORDER BY id ASC"
        else:
            query = f"SELECT is_win, amount_usdc, pnl FROM trades WHERE is_win IS NOT NULL AND is_demo = {flag} ORDER BY id ASC"
        cursor = await db.execute(query)
        rows = await cursor.fetchall()
        total_row = await (await db.execute("SELECT COUNT(*) as cnt FROM trades WHERE is_demo = ?", (flag,))).fetchone()
    results = [r["is_win"] for r in rows]
    wins = sum(1 for r in results if r == 1)
    losses = sum(1 for r in results if r == 0)
    resolved = wins + losses
    win_pct = (wins / resolved * 100) if resolved else 0.0
    total_deployed = sum(r["amount_usdc"] for r in rows)
    total_pnl = sum(r["pnl"] for r in rows if r["pnl"] is not None)
    total_returned = total_deployed + total_pnl
    roi_pct = (total_pnl / total_deployed * 100) if total_deployed else 0.0
    result = {
        "total_trades": total_row["cnt"] if total_row else 0,
        "wins": wins,
        "losses": losses,
        "resolved": resolved,
        "win_pct": round(win_pct, 1),
        "total_deployed": round(total_deployed, 2),
        "total_returned": round(total_returned, 2),
        "net_pnl": round(total_pnl, 2),
        "roi_pct": round(roi_pct, 1),
    }
    if not is_demo:
        result.update(_compute_streaks(results))
    return result


async def get_trade_stats(limit: int | None = None) -> dict[str, Any]:
    return await _get_trade_stats_by_demo_flag(False, limit=limit)


async def get_all_real_trades_for_export() -> list[dict[str, Any]]:
    async with aiosqlite.connect(_db()) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT id, signal_id, slot_start, slot_end, side, entry_price, amount_usdc, order_id, fill_price, status, retry_count, outcome, is_win, pnl, resolved_at, routing_mode, routing_policy, original_side, routed_side, policy_bucket, policy_probability, rolling_wr_policy, rolling_wr_wr, rolling_wr_window_size, rolling_wr_sample_size, rolling_wr_ready FROM trades WHERE is_demo = 0 ORDER BY id ASC"
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


async def get_all_demo_trades_for_export() -> list[dict[str, Any]]:
    async with aiosqlite.connect(_db()) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT id, signal_id, slot_start, slot_end, side, entry_price, amount_usdc, order_id, fill_price, status, retry_count, outcome, is_win, pnl, resolved_at, routing_mode, routing_policy, original_side, routed_side, policy_bucket, policy_probability, rolling_wr_policy, rolling_wr_wr, rolling_wr_window_size, rolling_wr_sample_size, rolling_wr_ready FROM trades WHERE is_demo = 1 ORDER BY id ASC"
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


async def get_all_signals_for_export() -> list[dict[str, Any]]:
    async with aiosqlite.connect(_db()) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT id, slot_start, slot_timestamp, side, model_side, entry_price, is_win, pattern, ml_p_up, ml_p_down, ml_probability_bucket, ml_probability_used, threshold_policy_real, threshold_policy_demo, rolling_wr_policy, rolling_wr_wr, rolling_wr_window_size, rolling_wr_sample_size, rolling_wr_follow_below, rolling_wr_invert_above, rolling_wr_ready FROM signals WHERE skipped = 0 ORDER BY id ASC"
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Demo Trade Settings
# ---------------------------------------------------------------------------

async def is_demo_trade_enabled() -> bool:
    val = await get_setting("demo_trade_enabled")
    return val == "true"


async def get_demo_bankroll() -> float:
    val = await get_setting("demo_bankroll_usdc")
    return float(val) if val else 1000.00


async def set_demo_bankroll(amount: float) -> None:
    await set_setting("demo_bankroll_usdc", f"{amount:.2f}")


async def adjust_demo_bankroll(delta: float) -> float:
    current = await get_demo_bankroll()
    new_balance = max(0.0, round(current + delta, 2))
    await set_setting("demo_bankroll_usdc", f"{new_balance:.2f}")
    return new_balance


async def reset_demo_bankroll(starting_amount: float = 1000.00) -> None:
    await set_setting("demo_bankroll_usdc", f"{starting_amount:.2f}")


# ---------------------------------------------------------------------------
# Demo Trade Stats
# ---------------------------------------------------------------------------

async def get_demo_trade_stats(limit: int | None = None) -> dict[str, Any]:
    return await _get_trade_stats_by_demo_flag(True, limit=limit)


async def get_recent_demo_trades(n: int = 10) -> list[dict[str, Any]]:
    async with aiosqlite.connect(_db()) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM trades WHERE is_demo = 1 ORDER BY id DESC LIMIT ?", (n,))
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


async def delete_failed_redemptions_by_condition(condition_id: str) -> int:
    async with aiosqlite.connect(_db()) as db:
        cursor = await db.execute(
            "DELETE FROM redemptions WHERE condition_id = ? AND dry_run = 0",
            (condition_id,),
        )
        await db.commit()
        return cursor.rowcount


# ---------------------------------------------------------------------------
# Pattern analytics
# ---------------------------------------------------------------------------

async def get_pattern_stats() -> list[dict[str, Any]]:
    async with aiosqlite.connect(_db()) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """
            SELECT
                s.pattern AS pattern,
                COUNT(t.id) AS total_trades,
                SUM(CASE WHEN t.is_win = 1 THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN t.is_win = 0 THEN 1 ELSE 0 END) AS losses,
                SUM(t.amount_usdc) AS total_deployed,
                SUM(COALESCE(t.pnl, 0)) AS net_pnl,
                MAX(s.slot_start) AS last_seen
            FROM trades t
            JOIN signals s ON t.signal_id = s.id
            WHERE t.is_demo = 0
              AND t.is_win IS NOT NULL
              AND s.pattern IS NOT NULL
              AND s.pattern != ''
            GROUP BY s.pattern
            ORDER BY (SUM(CASE WHEN t.is_win = 1 THEN 1 ELSE 0 END) * 1.0 / COUNT(t.id)) DESC,
                     COUNT(t.id) DESC
            """
        )
        rows = await cursor.fetchall()
    result = []
    for r in rows:
        total = r["total_trades"]
        wins = r["wins"]
        losses = r["losses"]
        deployed = float(r["total_deployed"] or 0)
        pnl = float(r["net_pnl"] or 0)
        result.append({
            "pattern": r["pattern"],
            "total_trades": total,
            "wins": wins,
            "losses": losses,
            "win_pct": round(wins / total * 100, 1) if total else 0.0,
            "wl_ratio": round(wins / losses, 2) if losses else float("inf"),
            "total_deployed": round(deployed, 2),
            "net_pnl": round(pnl, 2),
            "roi_pct": round(pnl / deployed * 100, 1) if deployed else 0.0,
            "last_seen": r["last_seen"],
        })
    return result


async def get_pattern_stats_for_export() -> list[dict[str, Any]]:
    return await get_pattern_stats()


# ---------------------------------------------------------------------------
# Threshold analytics
# ---------------------------------------------------------------------------

async def get_threshold_stats(mode: str, limit: int | None = None) -> list[dict[str, Any]]:
    normalized_mode = normalize_threshold_mode(mode)
    policy_col = "threshold_policy_demo" if normalized_mode == "demo" else "threshold_policy_real"
    query = f"""
        SELECT
            s.ml_probability_bucket AS bucket,
            COALESCE(s.{policy_col}, 'FOLLOW') AS policy,
            COUNT(*) AS total_signals,
            SUM(CASE WHEN COALESCE(s.{policy_col}, 'FOLLOW') = 'BLOCK' THEN 1 ELSE 0 END) AS blocked_signals,
            SUM(CASE WHEN COALESCE(s.{policy_col}, 'FOLLOW') != 'BLOCK' THEN 1 ELSE 0 END) AS executed_signals,
            SUM(CASE WHEN t.is_win = 1 THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN t.is_win = 0 THEN 1 ELSE 0 END) AS losses,
            SUM(COALESCE(t.amount_usdc, 0)) AS total_deployed,
            SUM(COALESCE(t.pnl, 0)) AS net_pnl,
            MAX(s.slot_start) AS last_seen
        FROM signals s
        LEFT JOIN trades t
          ON t.signal_id = s.id
         AND t.routing_mode = ?
         AND t.policy_bucket = s.ml_probability_bucket
         AND t.routing_policy = COALESCE(s.{policy_col}, 'FOLLOW')
        WHERE s.ml_probability_bucket IS NOT NULL
        GROUP BY s.ml_probability_bucket, COALESCE(s.{policy_col}, 'FOLLOW')
        ORDER BY s.ml_probability_bucket ASC, policy ASC
    """
    async with aiosqlite.connect(_db()) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(query, (normalized_mode,))
        rows = await cursor.fetchall()
    items = []
    for row in rows:
        executed = int(row["executed_signals"] or 0)
        wins = int(row["wins"] or 0)
        losses = int(row["losses"] or 0)
        deployed = float(row["total_deployed"] or 0)
        pnl = float(row["net_pnl"] or 0)
        items.append({
            "bucket": row["bucket"],
            "policy": row["policy"] or "FOLLOW",
            "total_signals": int(row["total_signals"] or 0),
            "blocked_signals": int(row["blocked_signals"] or 0),
            "executed_signals": executed,
            "total_trades": executed,
            "wins": wins,
            "losses": losses,
            "win_pct": round((wins / executed) * 100, 2) if executed else 0.0,
            "total_deployed": round(deployed, 2),
            "net_pnl": round(pnl, 2),
            "roi_pct": round((pnl / deployed) * 100, 2) if deployed else 0.0,
            "last_seen": row["last_seen"],
        })
    if limit is not None:
        return items[:limit]
    return items


async def get_recent_threshold_routed_trades(mode: str, n: int = 10) -> list[dict[str, Any]]:
    normalized_mode = normalize_threshold_mode(mode)
    is_demo = 1 if normalized_mode == "demo" else 0
    async with aiosqlite.connect(_db()) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM trades WHERE is_demo = ? ORDER BY id DESC LIMIT ?",
            (is_demo, n),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# ML config helpers (ml_config table)
# ---------------------------------------------------------------------------

async def get_ml_config(key: str) -> str | None:
    async with aiosqlite.connect(_db()) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT value FROM ml_config WHERE key = ?", (key,))
        row = await cursor.fetchone()
        return row["value"] if row else None


async def set_ml_config(key: str, value: str) -> None:
    async with aiosqlite.connect(_db()) as db:
        await db.execute(
            "INSERT INTO ml_config (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )
        await db.commit()


async def get_ml_threshold() -> float:
    val = await get_ml_config("ml_threshold")
    if val is not None:
        try:
            return float(val)
        except (ValueError, TypeError):
            pass
    return cfg.ML_DEFAULT_THRESHOLD


async def set_ml_threshold(threshold: float) -> None:
    await set_ml_config("ml_threshold", str(threshold))


async def get_ml_down_threshold() -> float | None:
    val = await get_ml_config("ml_down_threshold")
    if val is not None:
        try:
            return float(val)
        except (ValueError, TypeError):
            pass
    return None


async def set_ml_down_threshold(threshold: float) -> None:
    await set_ml_config("ml_down_threshold", str(threshold))


def _parse_ranges_raw(raw: str | None) -> list[tuple[float, float]]:
    ranges: list[tuple[float, float]] = []
    if not raw or not raw.strip():
        return ranges
    for part in raw.split(","):
        part = part.strip()
        if "-" not in part:
            continue
        lo_str, _, hi_str = part.partition("-")
        try:
            lo = float(lo_str.strip())
            hi = float(hi_str.strip())
        except ValueError:
            continue
        if lo > hi:
            lo, hi = hi, lo
        ranges.append((lo, hi))
    return ranges


def _format_ranges(ranges: list[tuple[float, float]]) -> str:
    return ",".join(f"{lo:.2f}-{hi:.2f}" for lo, hi in ranges)


async def get_blocked_threshold_ranges() -> list[tuple[float, float]]:
    val = await get_ml_config("blocked_threshold_ranges")
    if val is None:
        return cfg.BLOCKED_THRESHOLD_RANGES
    if val == "__NONE__":
        return []
    if not val.strip():
        return []
    parsed = _parse_ranges_raw(val)
    return parsed if parsed else []


async def set_blocked_threshold_ranges(ranges: list[tuple[float, float]]) -> None:
    formatted = "__NONE__" if not ranges else _format_ranges(ranges)
    await set_ml_config("blocked_threshold_ranges", formatted)


# ---------------------------------------------------------------------------
# Model registry helpers
# ---------------------------------------------------------------------------

async def insert_model_registry(
    slot: str,
    train_date: str,
    wr: float,
    precision_score: float,
    trades_per_day: float,
    threshold: float,
    sample_count: int,
    path: str,
    metadata_json: str,
) -> int:
    async with aiosqlite.connect(_db()) as db:
        cursor = await db.execute(
            """INSERT INTO model_registry
               (slot, train_date, wr, precision_score, trades_per_day, threshold,
                sample_count, path, metadata)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (slot, train_date, wr, precision_score, trades_per_day, threshold, sample_count, path, metadata_json),
        )
        await db.commit()
        return cursor.lastrowid


async def get_model_registry(slot: str = "current") -> dict | None:
    async with aiosqlite.connect(_db()) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM model_registry WHERE slot = ? ORDER BY id DESC LIMIT 1",
            (slot,),
        )
        row = await cursor.fetchone()
        return dict(row) if row else None
