"""CRUD helpers and analytics queries for signals, trades, settings, and redemptions."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import aiosqlite
import config as cfg


def _db() -> str:
    return cfg.DB_PATH


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


async def is_auto_redeem_enabled() -> bool:
    """Return True if auto-redeem is toggled on in settings."""
    val = await get_setting("auto_redeem_enabled")
    return val == "true"


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
    filter_blocked: bool = False,  # Kept for DB schema backward-compatibility.
                                   # The pre-trade filter was removed; this
                                   # column is always written as False (0) and
                                   # is never set to True by any active code path.
    pattern: str | None = None,
) -> int:
    async with aiosqlite.connect(_db()) as db:
        cursor = await db.execute(
            "INSERT INTO signals (slot_start, slot_end, slot_timestamp, side, "
            "entry_price, opposite_price, skipped, filter_blocked, pattern) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
            ),
        )
        await db.commit()
        return cursor.lastrowid  # type: ignore[return-value]


async def resolve_signal(signal_id: int, outcome: str, is_win: bool) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    async with aiosqlite.connect(_db()) as db:
        await db.execute(
            "UPDATE signals SET outcome = ?, is_win = ?, resolved_at = ? WHERE id = ?",
            (outcome, 1 if is_win else 0, now, signal_id),
        )
        await db.commit()


async def get_recent_signals(n: int = 10) -> list[dict[str, Any]]:
    async with aiosqlite.connect(_db()) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM signals ORDER BY id DESC LIMIT ?", (n,)
        )
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
) -> int:
    async with aiosqlite.connect(_db()) as db:
        cursor = await db.execute(
            "INSERT INTO trades (signal_id, slot_start, slot_end, side, entry_price, "
            "amount_usdc, order_id, fill_price, status, is_demo) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
            ),
        )
        await db.commit()
        return cursor.lastrowid  # type: ignore[return-value]


async def update_trade_status(trade_id: int, status: str, order_id: str | None = None) -> None:
    async with aiosqlite.connect(_db()) as db:
        if order_id:
            await db.execute(
                "UPDATE trades SET status = ?, order_id = ? WHERE id = ?",
                (status, order_id, trade_id),
            )
        else:
            await db.execute(
                "UPDATE trades SET status = ? WHERE id = ?",
                (status, trade_id),
            )
        await db.commit()


async def update_trade_retry(
    trade_id: int,
    status: str,
    retry_count: int,
    order_id: str | None = None,
) -> None:
    """Update trade with retry information."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    async with aiosqlite.connect(_db()) as db:
        if order_id:
            await db.execute(
                "UPDATE trades SET status = ?, retry_count = ?, last_retry_at = ?, order_id = ? "
                "WHERE id = ?",
                (status, retry_count, now, order_id, trade_id),
            )
        else:
            await db.execute(
                "UPDATE trades SET status = ?, retry_count = ?, last_retry_at = ? "
                "WHERE id = ?",
                (status, retry_count, now, trade_id),
            )
        await db.commit()


async def get_active_trade_for_signal(signal_id: int) -> dict[str, Any] | None:
    """Check if a filled trade already exists for this signal.

    Used as a duplicate guard before retrying FOK orders — if a trade is
    already 'filled' we must not place another order for the same signal.
    """
    async with aiosqlite.connect(_db()) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM trades WHERE signal_id = ? AND status = 'filled' LIMIT 1",
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
        cursor = await db.execute(
            "SELECT * FROM trades WHERE is_demo = 0 ORDER BY id DESC LIMIT ?", (n,)
        )
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
    async with aiosqlite.connect(_db()) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM trades WHERE signal_id = ? LIMIT 1", (signal_id,)
        )
        row = await cursor.fetchone()
        return dict(row) if row else None


# ---------------------------------------------------------------------------
# Redemption CRUD
# ---------------------------------------------------------------------------

async def insert_redemption(
    condition_id: str,
    outcome_index: int,
    size: float,
    title: str | None,
    tx_hash: str | None,
    status: str,
    error: str | None = None,
    gas_used: int | None = None,
    dry_run: bool = False,
    verified: bool = False,
) -> int:
    """Insert a redemption record. Returns the new row id."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    resolved_at = now if status in ("success", "failed", "verified") else None
    verified_at = now if verified else None
    async with aiosqlite.connect(_db()) as db:
        cursor = await db.execute(
            "INSERT INTO redemptions "
            "(condition_id, outcome_index, size, title, tx_hash, status, "
            " error, gas_used, dry_run, resolved_at, verified, verified_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
            ),
        )
        await db.commit()
        return cursor.lastrowid  # type: ignore[return-value]


async def get_recent_redemptions(n: int = 20) -> list[dict[str, Any]]:
    """Return the *n* most recent non-dry-run redemption records."""
    async with aiosqlite.connect(_db()) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM redemptions WHERE dry_run = 0 ORDER BY id DESC LIMIT ?",
            (n,),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


async def redemption_already_recorded(condition_id: str) -> bool:
    """Return True if a verified (non-dry-run) redemption exists for *condition_id*.

    Only suppresses retries for verified redemptions.
    Old 'success' rows with verified=0 are from the buggy EOA path — retry them.
    """
    async with aiosqlite.connect(_db()) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT id FROM redemptions "
            "WHERE condition_id = ? AND dry_run = 0 "
            "AND (status = 'verified' OR (status = 'success' AND verified = 1)) LIMIT 1",
            (condition_id,),
        )
        row = await cursor.fetchone()
        return row is not None


async def delete_redemptions_for_condition(condition_id: str) -> int:
    """Delete ALL redemption records for *condition_id* (any status, non-dry-run).

    Use this to clear incorrectly recorded 'success' entries so the scheduler
    will retry redemption on the next scan.  Returns the number of rows deleted.
    """
    async with aiosqlite.connect(_db()) as db:
        cursor = await db.execute(
            "DELETE FROM redemptions WHERE condition_id = ? AND dry_run = 0",
            (condition_id,),
        )
        await db.commit()
        return cursor.rowcount


async def update_redemption_verified(redemption_id: int) -> None:
    """Mark a redemption row as verified: set verified=1, verified_at=now, status='verified'."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    async with aiosqlite.connect(_db()) as db:
        await db.execute(
            "UPDATE redemptions SET verified = 1, verified_at = ?, status = 'verified' "
            "WHERE id = ?",
            (now, redemption_id),
        )
        await db.commit()


async def get_unverified_success_redemptions() -> list[dict[str, Any]]:
    """Return all non-dry-run redemption rows where status='success' and verified=0.

    These are candidates for re-verification or retry (e.g. rows recorded by
    the buggy EOA path that succeeded on-chain but may not have actually redeemed).
    """
    async with aiosqlite.connect(_db()) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM redemptions "
            "WHERE status = 'success' AND verified = 0 AND dry_run = 0 "
            "ORDER BY id ASC"
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


async def get_redemption_stats() -> dict[str, Any]:
    """Aggregate stats for the /redemptions command dashboard."""
    async with aiosqlite.connect(_db()) as db:
        db.row_factory = aiosqlite.Row

        total_row = await (await db.execute(
            "SELECT COUNT(*) as cnt FROM redemptions WHERE dry_run = 0"
        )).fetchone()
        total = total_row["cnt"] if total_row else 0

        success_row = await (await db.execute(
            "SELECT COUNT(*) as cnt FROM redemptions WHERE dry_run = 0 AND status = 'success'"
        )).fetchone()
        success = success_row["cnt"] if success_row else 0

        failed_row = await (await db.execute(
            "SELECT COUNT(*) as cnt FROM redemptions WHERE dry_run = 0 AND status = 'failed'"
        )).fetchone()
        failed = failed_row["cnt"] if failed_row else 0

        size_row = await (await db.execute(
            "SELECT SUM(size) as total_size FROM redemptions WHERE dry_run = 0 AND status = 'success'"
        )).fetchone()
        total_size = float(size_row["total_size"] or 0) if size_row else 0.0

    return {
        "total":      total,
        "success":    success,
        "failed":     failed,
        "total_size": round(total_size, 4),
    }


# ---------------------------------------------------------------------------
# Streak helpers
# ---------------------------------------------------------------------------

def _compute_streaks(results: list[int]) -> dict[str, Any]:
    """Given a list of 1/0 (win/loss) in chronological order, compute streaks."""
    if not results:
        return {
            "current_streak":      0,
            "current_streak_type": None,
            "best_win_streak":     0,
            "worst_loss_streak":   0,
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

    # Compute current streak from the end.
    current_type = results[-1]
    current = 0
    for v in reversed(results):
        if v == current_type:
            current += 1
        else:
            break

    return {
        "current_streak":      current,
        "current_streak_type": "W" if current_type == 1 else "L",
        "best_win_streak":     best_win,
        "worst_loss_streak":   worst_loss,
    }


# ---------------------------------------------------------------------------
# Analytics
# ---------------------------------------------------------------------------

async def get_signal_stats(limit: int | None = None) -> dict[str, Any]:
    """Return aggregate signal statistics.

    Note: filter_blocked_count is always 0 because the pre-trade filter was
    removed.  The column is kept in the DB schema for backward compatibility
    but no active code path sets it to 1.
    """
    async with aiosqlite.connect(_db()) as db:
        db.row_factory = aiosqlite.Row

        # Total active signals (non-skipped).
        total_row = await (await db.execute(
            "SELECT COUNT(*) as cnt FROM signals WHERE skipped = 0"
        )).fetchone()
        total = total_row["cnt"] if total_row else 0

        # Skip count.
        skip_row = await (await db.execute(
            "SELECT COUNT(*) as cnt FROM signals WHERE skipped = 1"
        )).fetchone()
        skip_count = skip_row["cnt"] if skip_row else 0

        # Resolved signals for win/loss stats (exclude skipped).
        if limit:
            inner = (
                f"SELECT * FROM signals WHERE skipped = 0 AND is_win IS NOT NULL "
                f"ORDER BY id DESC LIMIT {limit}"
            )
            query = f"SELECT is_win FROM ({inner}) ORDER BY id ASC"
        else:
            query = (
                "SELECT is_win FROM signals "
                "WHERE skipped = 0 AND is_win IS NOT NULL "
                "ORDER BY id ASC"
            )

        cursor = await db.execute(query)
        rows = await cursor.fetchall()
        results = [r["is_win"] for r in rows]

    wins = sum(1 for r in results if r == 1)
    losses = sum(1 for r in results if r == 0)
    resolved = wins + losses
    win_pct = (wins / resolved * 100) if resolved else 0.0
    streaks = _compute_streaks(results)

    return {
        "total_signals": total,
        "skip_count":    skip_count,
        "wins":          wins,
        "losses":        losses,
        "resolved":      resolved,
        "win_pct":       round(win_pct, 1),
        **streaks,
    }


async def get_trade_stats(limit: int | None = None) -> dict[str, Any]:
    async with aiosqlite.connect(_db()) as db:
        db.row_factory = aiosqlite.Row

        if limit:
            inner = (
                f"SELECT * FROM trades WHERE is_win IS NOT NULL AND is_demo = 0 "
                f"ORDER BY id DESC LIMIT {limit}"
            )
            query = f"SELECT is_win, amount_usdc, pnl FROM ({inner}) ORDER BY id ASC"
        else:
            query = (
                "SELECT is_win, amount_usdc, pnl FROM trades "
                "WHERE is_win IS NOT NULL AND is_demo = 0 ORDER BY id ASC"
            )

        cursor = await db.execute(query)
        rows = await cursor.fetchall()

        total_row = await (await db.execute(
            "SELECT COUNT(*) as cnt FROM trades WHERE is_demo = 0"
        )).fetchone()
        total_trades = total_row["cnt"] if total_row else 0

    results = [r["is_win"] for r in rows]
    wins = sum(1 for r in results if r == 1)
    losses = sum(1 for r in results if r == 0)
    resolved = wins + losses
    win_pct = (wins / resolved * 100) if resolved else 0.0

    total_deployed = sum(r["amount_usdc"] for r in rows)
    total_pnl = sum(r["pnl"] for r in rows if r["pnl"] is not None)
    total_returned = total_deployed + total_pnl
    roi_pct = (total_pnl / total_deployed * 100) if total_deployed else 0.0

    streaks = _compute_streaks(results)

    return {
        "total_trades":   total_trades,
        "wins":           wins,
        "losses":         losses,
        "resolved":       resolved,
        "win_pct":        round(win_pct, 1),
        "total_deployed": round(total_deployed, 2),
        "total_returned": round(total_returned, 2),
        "net_pnl":        round(total_pnl, 2),
        "roi_pct":        round(roi_pct, 1),
        **streaks,
    }


async def get_all_signals_for_export() -> list[dict[str, Any]]:
    """Return all non-skipped signals ordered by id for CSV/Excel export."""
    async with aiosqlite.connect(_db()) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT id, slot_start, side, entry_price, is_win, pattern "
            "FROM signals WHERE skipped = 0 ORDER BY id ASC"
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Demo Trade Settings
# ---------------------------------------------------------------------------

async def is_demo_trade_enabled() -> bool:
    """Return True if demo trading mode is toggled on in settings."""
    val = await get_setting("demo_trade_enabled")
    return val == "true"


async def get_demo_bankroll() -> float:
    """Return the current demo bankroll balance in USDC."""
    val = await get_setting("demo_bankroll_usdc")
    return float(val) if val else 1000.00


async def set_demo_bankroll(amount: float) -> None:
    """Directly set the demo bankroll to a specific amount."""
    await set_setting("demo_bankroll_usdc", f"{amount:.2f}")


async def adjust_demo_bankroll(delta: float) -> float:
    """Add delta (positive or negative) to the demo bankroll.

    Returns the new balance.  The bankroll is clamped to >= 0.
    """
    current = await get_demo_bankroll()
    new_balance = max(0.0, round(current + delta, 2))
    await set_setting("demo_bankroll_usdc", f"{new_balance:.2f}")
    return new_balance


async def reset_demo_bankroll(starting_amount: float = 1000.00) -> None:
    """Reset the demo bankroll to the given starting amount (default $1000)."""
    await set_setting("demo_bankroll_usdc", f"{starting_amount:.2f}")


# ---------------------------------------------------------------------------
# Demo Trade Stats
# ---------------------------------------------------------------------------

async def get_demo_trade_stats(limit: int | None = None) -> dict[str, Any]:
    """Return aggregate P&L stats for demo trades only."""
    async with aiosqlite.connect(_db()) as db:
        db.row_factory = aiosqlite.Row

        if limit:
            inner = (
                f"SELECT * FROM trades WHERE is_demo = 1 AND is_win IS NOT NULL "
                f"ORDER BY id DESC LIMIT {limit}"
            )
            query = f"SELECT is_win, amount_usdc, pnl FROM ({inner}) ORDER BY id ASC"
        else:
            query = (
                "SELECT is_win, amount_usdc, pnl FROM trades "
                "WHERE is_demo = 1 AND is_win IS NOT NULL ORDER BY id ASC"
            )

        cursor = await db.execute(query)
        rows = await cursor.fetchall()

        total_row = await (await db.execute(
            "SELECT COUNT(*) as cnt FROM trades WHERE is_demo = 1"
        )).fetchone()
        total_trades = total_row["cnt"] if total_row else 0

    results = [r["is_win"] for r in rows]
    wins = sum(1 for r in results if r == 1)
    losses = sum(1 for r in results if r == 0)
    resolved = wins + losses
    win_pct = (wins / resolved * 100) if resolved else 0.0

    total_deployed = sum(r["amount_usdc"] for r in rows)
    total_pnl = sum(r["pnl"] for r in rows if r["pnl"] is not None)
    total_returned = total_deployed + total_pnl
    roi_pct = (total_pnl / total_deployed * 100) if total_deployed else 0.0

    return {
        "total_trades":   total_trades,
        "wins":           wins,
        "losses":         losses,
        "resolved":       resolved,
        "win_pct":        round(win_pct, 1),
        "total_deployed": round(total_deployed, 2),
        "total_returned": round(total_returned, 2),
        "net_pnl":        round(total_pnl, 2),
        "roi_pct":        round(roi_pct, 1),
    }


async def get_recent_demo_trades(n: int = 10) -> list[dict[str, Any]]:
    """Return the n most recent demo trades."""
    async with aiosqlite.connect(_db()) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM trades WHERE is_demo = 1 ORDER BY id DESC LIMIT ?", (n,)
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


async def delete_failed_redemptions_by_condition(condition_id: str) -> int:
    """Delete all non-dry-run redemption records for *condition_id* regardless of status.

    Used to clear incorrectly recorded 'success' entries caused by the
    sig-type-2 / wrong-from-address bug, so the redeemer will attempt them
    again on the next scan.

    Returns the number of rows deleted.
    """
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
    """Return per-pattern performance stats across all resolved real trades.

    Joins signals -> trades on signal_id, groups by pattern, and computes:
      - total trades, wins, losses, win_pct, wl_ratio
      - total_deployed (USDC), net_pnl, roi_pct
      - last_seen (most recent slot_start for that pattern)

    Only resolved real trades (is_demo=0, is_win IS NOT NULL) are counted.
    Patterns are sorted by win_pct DESC, then total_trades DESC.
    """
    async with aiosqlite.connect(_db()) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """
            SELECT
                s.pattern                          AS pattern,
                COUNT(t.id)                        AS total_trades,
                SUM(CASE WHEN t.is_win = 1 THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN t.is_win = 0 THEN 1 ELSE 0 END) AS losses,
                SUM(t.amount_usdc)                 AS total_deployed,
                SUM(COALESCE(t.pnl, 0))            AS net_pnl,
                MAX(s.slot_start)                  AS last_seen
            FROM trades t
            JOIN signals s ON t.signal_id = s.id
            WHERE t.is_demo = 0
              AND t.is_win IS NOT NULL
              AND s.pattern IS NOT NULL
              AND s.pattern != ''
            GROUP BY s.pattern
            ORDER BY
                (SUM(CASE WHEN t.is_win = 1 THEN 1 ELSE 0 END) * 1.0 / COUNT(t.id)) DESC,
                COUNT(t.id) DESC
            """
        )
        rows = await cursor.fetchall()

    result = []
    for r in rows:
        total   = r["total_trades"]
        wins    = r["wins"]
        losses  = r["losses"]
        deployed = float(r["total_deployed"] or 0)
        pnl     = float(r["net_pnl"] or 0)
        win_pct = round(wins / total * 100, 1) if total else 0.0
        wl_ratio = round(wins / losses, 2) if losses else float("inf")
        roi_pct = round(pnl / deployed * 100, 1) if deployed else 0.0
        result.append({
            "pattern":        r["pattern"],
            "total_trades":   total,
            "wins":           wins,
            "losses":         losses,
            "win_pct":        win_pct,
            "wl_ratio":       wl_ratio,
            "total_deployed": round(deployed, 2),
            "net_pnl":        round(pnl, 2),
            "roi_pct":        roi_pct,
            "last_seen":      r["last_seen"],
        })
    return result


async def get_pattern_stats_for_export() -> list[dict[str, Any]]:
    """Same as get_pattern_stats() but returns all fields needed for XLS export."""
    return await get_pattern_stats()
