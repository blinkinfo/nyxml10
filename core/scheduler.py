"""APScheduler loop - syncs to 5-min slot boundaries, fires signals, trades, resolves, redeems."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone, timedelta

from apscheduler.schedulers.asyncio import AsyncIOScheduler

import config as cfg
from core import strategy, trader, resolver
from core import pending_queue
from db import queries
from ml import inference_logger
import html as _html
from polymarket.markets import SLOT_DURATION

log = logging.getLogger(__name__)

SCHEDULER: AsyncIOScheduler | None = None
_tg_app = None
_poly_client = None


def _next_check_time() -> datetime:
    now = datetime.now(timezone.utc)
    epoch = int(now.timestamp())
    current_slot_start = epoch - (epoch % SLOT_DURATION)
    check_epoch = current_slot_start + SLOT_DURATION - cfg.SIGNAL_LEAD_TIME
    if check_epoch <= epoch:
        check_epoch += SLOT_DURATION
    return datetime.fromtimestamp(check_epoch, tz=timezone.utc)


async def _send_telegram(text: str) -> None:
    if _tg_app is None or cfg.TELEGRAM_CHAT_ID is None:
        return
    try:
        await _tg_app.bot.send_message(
            chat_id=int(cfg.TELEGRAM_CHAT_ID),
            text=text,
            parse_mode="HTML",
        )
    except Exception:
        log.exception("Failed to send Telegram message")


def _calculate_resolution_pnl(amount_usdc: float, entry_price: float, is_win: bool) -> float:
    if not is_win:
        return -amount_usdc
    gross_shares = amount_usdc / entry_price
    fee_usdc = gross_shares * 0.072 * entry_price * (1.0 - entry_price)
    return gross_shares - amount_usdc - fee_usdc


def _build_routed_execution(signal: dict, route: dict, mode: str) -> dict | None:
    if route.get("blocked"):
        return None
    routed_side = route["routed_side"]
    entry_price = signal["entry_price"]
    opposite_price = signal["opposite_price"]
    token_id = signal["token_id"]
    if routed_side != signal["model_side"]:
        entry_price, opposite_price = opposite_price, entry_price
        token_id = signal.get("opposite_token_id", token_id)
    return {
        "mode": mode,
        "is_demo": mode == "demo",
        "side": routed_side,
        "entry_price": entry_price,
        "opposite_price": opposite_price,
        "token_id": token_id,
        "policy": route["policy"],
        "bucket": route["bucket"],
        "probability": route["probability"],
        "original_side": route["original_side"],
        "reason": route.get("reason"),
    }


async def _emit_policy_notifications(
    slot_start_str: str,
    slot_end_str: str,
    model_side: str,
    real_route: dict,
    demo_route: dict,
) -> None:
    from bot.formatters import format_threshold_policy_notification

    notifications = []
    if real_route.get("policy") != "FOLLOW":
        notifications.append(
            format_threshold_policy_notification(
                mode="real",
                slot_start_str=slot_start_str,
                slot_end_str=slot_end_str,
                model_side=model_side,
                policy=real_route["policy"],
                routed_side=real_route.get("routed_side"),
                bucket=real_route.get("bucket"),
                probability=real_route.get("probability"),
                note=real_route.get("reason"),
            )
        )
    if demo_route.get("policy") != "FOLLOW":
        notifications.append(
            format_threshold_policy_notification(
                mode="demo",
                slot_start_str=slot_start_str,
                slot_end_str=slot_end_str,
                model_side=model_side,
                policy=demo_route["policy"],
                routed_side=demo_route.get("routed_side"),
                bucket=demo_route.get("bucket"),
                probability=demo_route.get("probability"),
                note=demo_route.get("reason"),
            )
        )
    for msg in notifications:
        await _send_telegram(msg)


async def _resolve_trade_bundle(
    signal_id: int,
    slug: str,
    slot_start: str,
    slot_end: str,
    signal_side: str,
    signal_entry_price: float,
    signal_trade_id: int | None = None,
) -> None:
    from bot.formatters import format_signal_resolution, format_trade_resolution, format_demo_resolution

    winner = await resolver.resolve_slot(slug)
    if winner is None:
        log.warning("Could not resolve slot %s after all attempts - adding to persistent retry queue", slug)
        await pending_queue.add_pending(
            signal_id=signal_id,
            slug=slug,
            side=signal_side,
            entry_price=signal_entry_price,
            slot_start=slot_start,
            slot_end=slot_end,
            trade_id=signal_trade_id,
            amount_usdc=None,
            is_demo=False,
        )
        return

    signal_is_win = winner == signal_side
    await queries.mark_signal_resolved_if_unset(signal_id, winner, signal_is_win)
    inference_logger.log_outcome(slug, winner=winner, is_win=signal_is_win)

    s_start = slot_start.split(" ")[-1] if " " in slot_start else slot_start
    s_end = slot_end.split(" ")[-1] if " " in slot_end else slot_end
    await _send_telegram(
        format_signal_resolution(
            is_win=signal_is_win,
            side=signal_side,
            entry_price=signal_entry_price,
            slot_start_str=s_start,
            slot_end_str=s_end,
        )
    )

    trades = await queries.get_trades_by_signal(signal_id)
    if not trades:
        return

    for trade_row in trades:
        if trade_row.get("is_win") is not None:
            continue
        trade_side = trade_row.get("routed_side") or trade_row.get("side")
        is_win = winner == trade_side
        pnl = round(_calculate_resolution_pnl(trade_row["amount_usdc"], trade_row["entry_price"], is_win), 4)
        await queries.resolve_trade(trade_row["id"], winner, is_win, pnl)
        await queries.mark_trade_signal_outcome_recorded(trade_row["id"])
        if trade_row.get("is_demo"):
            if is_win:
                new_bankroll = await queries.adjust_demo_bankroll(trade_row["amount_usdc"] + pnl)
            else:
                new_bankroll = await queries.get_demo_bankroll()
            await _send_telegram(
                format_demo_resolution(
                    is_win=is_win,
                    side=trade_side,
                    entry_price=trade_row["entry_price"],
                    slot_start_str=s_start,
                    slot_end_str=s_end,
                    pnl=pnl,
                    new_bankroll=new_bankroll,
                    original_side=trade_row.get("original_side") or signal_side,
                    policy=trade_row.get("routing_policy") or "FOLLOW",
                    bucket=trade_row.get("policy_bucket"),
                )
            )
        else:
            await _send_telegram(
                format_trade_resolution(
                    is_win=is_win,
                    side=trade_side,
                    entry_price=trade_row["entry_price"],
                    slot_start_str=s_start,
                    slot_end_str=s_end,
                    pnl=pnl,
                    original_side=trade_row.get("original_side") or signal_side,
                    policy=trade_row.get("routing_policy") or "FOLLOW",
                    bucket=trade_row.get("policy_bucket"),
                )
            )


async def _reconcile_pending() -> None:
    pending = await pending_queue.list_pending()
    if not pending:
        return
    log.info("Reconciler: checking %d pending slot(s)...", len(pending))
    for item in pending:
        try:
            winner, resolved = await resolver.check_resolution(item["slug"])
        except Exception:
            log.exception("Reconciler: error checking slug=%s", item["slug"])
            continue
        if not resolved:
            continue
        signal_side = item["side"]
        signal_id = item["signal_id"]
        await queries.mark_signal_resolved_if_unset(signal_id, winner, winner == signal_side)
        inference_logger.log_outcome(item["slug"], winner=winner, is_win=(winner == signal_side))
        trades = await queries.get_trades_by_signal(signal_id)
        for trade_row in trades:
            if trade_row.get("is_win") is not None:
                continue
            trade_side = trade_row.get("routed_side") or trade_row.get("side")
            is_win = winner == trade_side
            pnl = round(_calculate_resolution_pnl(trade_row["amount_usdc"], trade_row["entry_price"], is_win), 4)
            await queries.resolve_trade(trade_row["id"], winner, is_win, pnl)
            await queries.mark_trade_signal_outcome_recorded(trade_row["id"])
            if trade_row.get("is_demo") and is_win:
                await queries.adjust_demo_bankroll(trade_row["amount_usdc"] + pnl)
        await pending_queue.remove_pending(signal_id)
        log.info("Reconciler: resolved signal %d - winner=%s", signal_id, winner)


async def _auto_redeem_job() -> None:
    from core.redeemer import scan_and_redeem
    from bot.formatters import format_auto_redeem_notification, format_error_alert

    enabled = await queries.is_auto_redeem_enabled()
    if not enabled:
        return
    wallet = cfg.POLYMARKET_FUNDER_ADDRESS
    if not wallet or not cfg.POLYGON_RPC_URL:
        return
    try:
        results = await scan_and_redeem(wallet, dry_run=False)
    except Exception as exc:
        import traceback as _tb
        tb_str = "".join(_tb.format_exception(type(exc), exc, exc.__traceback__))
        await _send_telegram(format_error_alert("auto_redeem_job", f"{type(exc).__name__}: {exc}", tb_str))
        return
    if not results:
        return
    new_results: list[dict] = []
    for r in results:
        cid = r.get("condition_id", "")
        if await queries.redemption_already_recorded(cid):
            continue
        new_results.append(r)
    if not new_results:
        return
    for r in new_results:
        try:
            is_success = bool(r.get("success"))
            is_verified = is_success and bool(r.get("verified_zero_balance"))
            db_status = "verified" if is_verified else ("success" if is_success else "failed")
            await queries.insert_redemption(
                condition_id=r["condition_id"],
                outcome_index=r["outcome_index"],
                size=r["size"],
                title=r.get("title"),
                tx_hash=r.get("tx_hash"),
                status=db_status,
                error=r.get("error"),
                gas_used=r.get("gas_used"),
                dry_run=False,
                verified=is_verified,
            )
        except Exception:
            log.exception("auto_redeem_job: failed to persist redemption for condition=%s", r.get("condition_id"))
    for r in new_results:
        if not r.get("success"):
            err = r.get("error") or "unknown error"
            tb = r.get("error_detail", "")
            detail = tb[-600:] if tb else err[:200]
            title = (r.get("title") or r.get("condition_id", "?"))[:55]
            await _send_telegram(
                f"&#x26A0;&#xFE0F; <b>Redemption Failed</b>\n{_html.escape(title)}\n<pre>{_html.escape(detail)}</pre>"
            )
    await _send_telegram(format_auto_redeem_notification(new_results))


async def _check_and_trade() -> None:
    try:
        now_utc = datetime.now(timezone.utc)
        if now_utc.hour in cfg.BLOCKED_TRADE_HOURS_UTC:
            log.info("Hour filter: skipping slot at %s UTC (blocked hours: %s)", now_utc.strftime("%H:%M"), sorted(cfg.BLOCKED_TRADE_HOURS_UTC))
            return

        from bot.formatters import (
            format_signal,
            format_skip,
            format_ml_signal,
            format_ml_skip,
            format_trade_filled,
            format_trade_unmatched,
            format_trade_aborted,
            format_trade_retrying,
            format_demo_trade_placed,
            format_demo_trade_skipped,
        )
        from core.trade_manager import TradeManager

        signal = await strategy.check_signal()
        if signal is None:
            await _send_telegram("\u274c Strategy error - could not fetch prices. Skipping slot.")
            return

        slot_start_full = signal["slot_n1_start_full"]
        slot_end_full = signal["slot_n1_end_full"]
        slot_start_str = signal["slot_n1_start_str"]
        slot_end_str = signal["slot_n1_end_str"]
        slot_ts = signal["slot_n1_ts"]
        slug = signal.get("slot_n1_slug", f"btc-updown-5m-{slot_ts}")

        if signal["skipped"]:
            await queries.insert_signal(
                slot_start=slot_start_full,
                slot_end=slot_end_full,
                slot_timestamp=slot_ts,
                side=None,
                entry_price=None,
                opposite_price=None,
                skipped=True,
                pattern=signal.get("pattern"),
                ml_p_up=signal.get("ml_p_up"),
                ml_p_down=signal.get("ml_p_down"),
                signal_slug=slug,
            )
            if "ml_p_up" in signal:
                msg = format_ml_skip(
                    slot_start_str=slot_start_str,
                    slot_end_str=slot_end_str,
                    ml_p_up=signal["ml_p_up"],
                    ml_p_down=signal["ml_p_down"],
                    ml_up_threshold=signal["ml_up_threshold"],
                    ml_down_threshold=signal["ml_down_threshold"],
                    ml_down_enabled=signal["ml_down_enabled"],
                )
            else:
                msg = format_skip(
                    slot_start_str=slot_start_str,
                    slot_end_str=slot_end_str,
                    reason=signal.get("reason", "No pattern match"),
                    pattern=signal.get("pattern"),
                )
            await _send_telegram(msg)
            return

        model_side = signal.get("model_side") or signal["side"]
        routing_probability = signal.get("routing_probability")
        routing_bucket = signal.get("routing_bucket")

        legacy_invert_trades = await queries.is_invert_trades_enabled()
        legacy_blocked_ranges = await queries.get_blocked_threshold_ranges()

        real_route = await queries.decide_threshold_route(
            original_side=model_side,
            probability=routing_probability,
            bucket=routing_bucket,
            mode="real",
            legacy_invert_enabled=legacy_invert_trades,
            legacy_blocked_ranges=legacy_blocked_ranges,
        )
        demo_route = await queries.decide_threshold_route(
            original_side=model_side,
            probability=routing_probability,
            bucket=routing_bucket,
            mode="demo",
            legacy_invert_enabled=False,
            legacy_blocked_ranges=legacy_blocked_ranges,
        )

        signal_id = await queries.insert_signal(
            slot_start=slot_start_full,
            slot_end=slot_end_full,
            slot_timestamp=slot_ts,
            side=model_side,
            entry_price=signal["entry_price"],
            opposite_price=signal["opposite_price"],
            skipped=False,
            pattern=signal.get("pattern"),
            ml_p_up=signal.get("ml_p_up"),
            ml_p_down=signal.get("ml_p_down"),
            ml_probability_bucket=routing_bucket,
            ml_probability_used=routing_probability,
            threshold_policy_real=real_route["policy"],
            threshold_policy_demo=demo_route["policy"],
            model_side=model_side,
            signal_slug=slug,
        )

        demo_trade_enabled = await queries.is_demo_trade_enabled()
        await TradeManager.check(signal_side=model_side, current_slot_ts=slot_ts, is_demo=demo_trade_enabled)
        autotrade = await queries.is_autotrade_enabled()
        real_trade_amount, _ = await queries.resolve_trade_amount(poly_client=_poly_client, is_demo=False)
        demo_trade_amount, _ = await queries.resolve_trade_amount(poly_client=_poly_client, is_demo=True)

        if "ml_p_up" in signal:
            msg = format_ml_signal(
                side=model_side,
                entry_price=signal["entry_price"],
                slot_start_str=slot_start_str,
                slot_end_str=slot_end_str,
                ml_p_up=signal["ml_p_up"],
                ml_p_down=signal["ml_p_down"],
                ml_up_threshold=signal["ml_up_threshold"],
                ml_down_threshold=signal["ml_down_threshold"],
                ml_down_enabled=signal.get("ml_down_enabled", False),
                bucket=routing_bucket,
            )
        else:
            msg = format_signal(
                side=model_side,
                entry_price=signal["entry_price"],
                slot_start_str=slot_start_str,
                slot_end_str=slot_end_str,
                pattern=signal.get("pattern"),
            )
        await _send_telegram(msg)
        await _emit_policy_notifications(slot_start_str, slot_end_str, model_side, real_route, demo_route)

        real_execution = _build_routed_execution(signal, real_route, "real")
        demo_execution = _build_routed_execution(signal, demo_route, "demo") if demo_trade_enabled else None
        trade_id_for_watch: int | None = None

        if demo_trade_enabled:
            amount_usdc = round(demo_trade_amount, 2)
            if demo_execution is None:
                await _send_telegram(
                    format_demo_trade_skipped(
                        slot_start_str=slot_start_str,
                        slot_end_str=slot_end_str,
                        reason=f"Threshold policy {demo_route['policy']} blocked demo execution",
                        bucket=demo_route.get("bucket"),
                    )
                )
            else:
                demo_bankroll = await queries.get_demo_bankroll()
                if demo_bankroll < amount_usdc:
                    await _send_telegram(
                        format_demo_trade_skipped(
                            slot_start_str=slot_start_str,
                            slot_end_str=slot_end_str,
                            reason=f"Bankroll ${demo_bankroll:.2f} below required ${amount_usdc:.2f}",
                            bucket=demo_execution.get("bucket"),
                        )
                    )
                else:
                    new_bankroll = await queries.adjust_demo_bankroll(-amount_usdc)
                    demo_trade_id = await queries.insert_trade(
                        signal_id=signal_id,
                        slot_start=slot_start_full,
                        slot_end=slot_end_full,
                        side=demo_execution["side"],
                        entry_price=demo_execution["entry_price"],
                        amount_usdc=amount_usdc,
                        status="filled",
                        is_demo=True,
                        routing_mode="demo",
                        routing_policy=demo_execution["policy"],
                        original_side=model_side,
                        routed_side=demo_execution["side"],
                        policy_bucket=demo_execution["bucket"],
                        policy_probability=demo_execution["probability"],
                    )
                    await _send_telegram(
                        format_demo_trade_placed(
                            side=demo_execution["side"],
                            original_side=model_side,
                            policy=demo_execution["policy"],
                            bucket=demo_execution.get("bucket"),
                            entry_price=demo_execution["entry_price"],
                            amount_usdc=amount_usdc,
                            new_bankroll=new_bankroll,
                        )
                    )

        if autotrade and _poly_client is not None:
            if real_execution is None:
                await _send_telegram(
                    format_trade_aborted(
                        side=model_side,
                        slot_label=f"{slot_start_str}-{slot_end_str}",
                        reason=f"Threshold policy {real_route['policy']} blocked real execution",
                    )
                )
            elif real_execution["token_id"]:
                amount_usdc = round(real_trade_amount, 2)
                trade_id = await queries.insert_trade(
                    signal_id=signal_id,
                    slot_start=slot_start_full,
                    slot_end=slot_end_full,
                    side=real_execution["side"],
                    entry_price=real_execution["entry_price"],
                    amount_usdc=amount_usdc,
                    status="pending",
                    routing_mode="real",
                    routing_policy=real_execution["policy"],
                    original_side=model_side,
                    routed_side=real_execution["side"],
                    policy_bucket=real_execution["bucket"],
                    policy_probability=real_execution["probability"],
                )
                trade_id_for_watch = trade_id
                slot_end_ts = slot_ts + SLOT_DURATION
                slot_label = f"{slot_start_str}-{slot_end_str}"
                max_retries = cfg.FOK_MAX_RETRIES

                async def _place_with_notifications():
                    sent_attempts: set[int] = set()

                    async def _retry_watcher():
                        import asyncio as _asyncio
                        for _ in range(max_retries * 10):
                            await _asyncio.sleep(0.8)
                            try:
                                row = await queries.get_active_trade_for_signal(signal_id)
                                if row is None:
                                    continue
                                retry_count = row.get("retry_count", 0) or 0
                                status = row.get("status", "")
                                if status == "retrying" and retry_count not in sent_attempts:
                                    sent_attempts.add(retry_count)
                                    await _send_telegram(
                                        format_trade_retrying(
                                            side=real_execution["side"],
                                            slot_label=slot_label,
                                            attempt=retry_count + 1,
                                            max_attempts=max_retries,
                                            reason="FOK order not matched - retrying",
                                        )
                                    )
                                if status in ("filled", "unmatched", "aborted", "duplicate_prevented"):
                                    break
                            except Exception:
                                pass

                    watcher_task = asyncio.create_task(_retry_watcher())
                    result = await trader.place_fok_order_with_retry(
                        poly_client=_poly_client,
                        token_id=real_execution["token_id"],
                        amount_usdc=amount_usdc,
                        signal_id=signal_id,
                        trade_id=trade_id,
                        slot_end_ts=slot_end_ts,
                    )
                    watcher_task.cancel()
                    try:
                        await watcher_task
                    except asyncio.CancelledError:
                        pass
                    return result

                result = await _place_with_notifications()
                trade_status = result["status"]
                attempts = result["attempts"]
                reason = result["reason"]
                order_id = result.get("order_id")
                if trade_status == "filled":
                    await _send_telegram(
                        format_trade_filled(
                            side=real_execution["side"],
                            slot_label=slot_label,
                            ask_price=real_execution["entry_price"],
                            amount_usdc=amount_usdc,
                            shares=result.get("shares"),
                            order_id=order_id,
                            attempts=attempts,
                            original_side=model_side,
                            policy=real_execution["policy"],
                            bucket=real_execution.get("bucket"),
                        )
                    )
                elif trade_status == "aborted":
                    await _send_telegram(
                        format_trade_aborted(
                            side=real_execution["side"],
                            slot_label=slot_label,
                            reason=reason,
                        )
                    )
                    trade_id_for_watch = None
                else:
                    await _send_telegram(
                        format_trade_unmatched(
                            side=real_execution["side"],
                            slot_label=slot_label,
                            attempts=attempts,
                            reason=reason,
                        )
                    )
                    trade_id_for_watch = None

        resolve_time = datetime.fromtimestamp(slot_ts + SLOT_DURATION + 30, tz=timezone.utc)
        if SCHEDULER is not None:
            SCHEDULER.add_job(
                _resolve_trade_bundle,
                trigger="date",
                run_date=resolve_time,
                kwargs={
                    "signal_id": signal_id,
                    "slug": slug,
                    "slot_start": slot_start_full,
                    "slot_end": slot_end_full,
                    "signal_side": model_side,
                    "signal_entry_price": signal["entry_price"],
                    "signal_trade_id": trade_id_for_watch,
                },
                id=f"resolve_{signal_id}",
                replace_existing=True,
            )
    except Exception:
        log.exception("_check_and_trade: unhandled exception - rescheduling next check")
        await _send_telegram("\u274c Scheduler error in check_and_trade - see logs. Next check rescheduled.")
    finally:
        _schedule_next()


def _schedule_next() -> None:
    if SCHEDULER is None:
        return
    next_time = _next_check_time()
    SCHEDULER.add_job(
        _check_and_trade,
        trigger="date",
        run_date=next_time,
        id="check_and_trade",
        replace_existing=True,
    )
    log.info("Next check: %s UTC", next_time.strftime("%H:%M:%S"))


async def recover_unresolved() -> None:
    signals = await queries.get_unresolved_signals()
    if not signals:
        log.debug("No unresolved signals to recover.")
    else:
        log.info("Recovering %d unresolved signal(s)...", len(signals))
        for sig in signals:
            slug = sig.get("signal_slug") or f"btc-updown-5m-{sig['slot_timestamp']}"
            resolve_time = datetime.now(timezone.utc) + timedelta(seconds=5)
            if SCHEDULER is not None:
                SCHEDULER.add_job(
                    _resolve_trade_bundle,
                    trigger="date",
                    run_date=resolve_time,
                    kwargs={
                        "signal_id": sig["id"],
                        "slug": slug,
                        "slot_start": sig["slot_start"],
                        "slot_end": sig["slot_end"],
                        "signal_side": sig.get("model_side") or sig["side"],
                        "signal_entry_price": sig["entry_price"],
                        "signal_trade_id": None,
                    },
                    id=f"recover_{sig['id']}",
                    replace_existing=True,
                )
    pending = await pending_queue.list_pending()
    if pending:
        log.info("%d slot(s) remain in persistent retry queue - reconciler will handle them.", len(pending))


async def _feature_drift_check_job() -> None:
    from ml.evaluator import check_feature_drift
    from ml import model_store, inference_logger

    meta = model_store.load_metadata("current")
    if meta is None:
        return
    training_stats = meta.get("training_feature_stats")
    if not training_stats:
        return
    log_path = inference_logger.get_log_path()
    if not log_path:
        return
    result = check_feature_drift(
        inference_log_path=log_path,
        training_feature_stats=training_stats,
        n_recent=500,
        z_threshold=2.0,
    )
    if result.get("error") or not result.get("drifted_features"):
        return
    drift_lines = []
    for d in result["drifted_features"][:10]:
        drift_lines.append(
            f"  <b>{d['feature']}</b>: live={d['live_mean']:.4f} train={d['train_mean']:.4f} z={d['z_score']:+.2f}"
        )
    msg = (
        f"\u26a0\ufe0f <b>Feature Drift Detected</b>\n"
        f"\u2500" * 20 + "\n"
        f"\U0001f4ca Records analyzed: {result['records_analyzed']}\n"
        f"\u26a0\ufe0f Drifted features ({len(result['drifted_features'])}):\n"
        + "\n".join(drift_lines) + "\n"
        + "\u2500" * 20 + "\n"
        + "\U0001f916 Model may be operating on out-of-distribution data.\n"
        + "Consider retraining with /retrain."
    )
    await _send_telegram(msg)


def start_scheduler(tg_app, poly_client) -> AsyncIOScheduler:
    global SCHEDULER, _tg_app, _poly_client
    _tg_app = tg_app
    _poly_client = poly_client
    SCHEDULER = AsyncIOScheduler(timezone="UTC")
    SCHEDULER.start()
    SCHEDULER.add_job(
        _reconcile_pending,
        trigger="interval",
        minutes=5,
        id="reconcile_pending",
        replace_existing=True,
    )
    redeem_interval = cfg.AUTO_REDEEM_INTERVAL_MINUTES
    SCHEDULER.add_job(
        _auto_redeem_job,
        trigger="interval",
        minutes=redeem_interval,
        id="auto_redeem",
        replace_existing=True,
    )
    _schedule_next()
    SCHEDULER.add_job(
        _feature_drift_check_job,
        trigger="cron",
        hour=6,
        minute=0,
        timezone="UTC",
        id="feature_drift_check",
        replace_existing=True,
    )
    log.info("Scheduler started.")
    return SCHEDULER
