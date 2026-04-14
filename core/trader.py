"""FOK market-order execution with robust retry logic.

Key safety features:
- Fill verification: checks CLOB response for MATCHED status
- Retry with backoff: exponential delay between attempts, price refresh each time
- Duplicate guard: checks DB for existing filled trade before each attempt
- Time fence: aborts if too close to slot expiry
"""

from __future__ import annotations

import asyncio
import logging
import time

from typing import Any

from py_clob_client.clob_types import MarketOrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY

import config as cfg
from db import queries
from polymarket.markets import get_clob_best_ask

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Result dataclass-style dict keys:
#   status: "filled" | "unmatched" | "failed" | "aborted"
#   order_id: str | None
#   attempts: int
#   reason: str  (human-readable explanation)
# ---------------------------------------------------------------------------

def _build_result(status: str, order_id: str | None, attempts: int, reason: str) -> dict[str, Any]:
    return {"status": status, "order_id": order_id, "attempts": attempts, "reason": reason}


async def place_fok_order(
    poly_client,
    token_id: str,
    amount_usdc: float,
) -> dict[str, Any]:
    """Place a Fill-Or-Kill market buy order (single attempt, no retry).

    *amount_usdc* is rounded to 2 decimal places to work around
    py-clob-client issue #121 (precision error on fractional amounts).

    The py-clob-client is synchronous, so both steps run inside
    ``asyncio.to_thread`` to keep the event loop responsive.
    """
    amount = round(amount_usdc, 2)
    log.info("Placing FOK order: token=%s  amount=$%.2f", token_id, amount)

    order_args = MarketOrderArgs(
        token_id=token_id,
        amount=amount,
        side=BUY,
        order_type=OrderType.FOK,
    )

    # Step 1 -- sign locally
    signed = await asyncio.to_thread(
        poly_client.client.create_market_order, order_args
    )

    # Step 2 -- post to CLOB
    response = await asyncio.to_thread(
        poly_client.client.post_order, signed, OrderType.FOK
    )

    log.debug("Order response: %s", response)
    return response


def _is_order_matched(response: dict[str, Any]) -> bool:
    """Check if the CLOB response indicates a successful fill.

    The Polymarket CLOB returns a 'status' field. A FOK order is filled only
    when status is 'MATCHED'. Other statuses ('UNMATCHED', 'DELAYED', etc.)
    mean the order did not fill.

    We also accept the legacy pattern where success == True and an orderID is
    present but no explicit status field exists.
    """
    if not isinstance(response, dict):
        return False

    status = response.get("status", "").upper()
    if status == "MATCHED":
        return True

    # Legacy fallback: if no status field but orderID is present and success is True
    if not status and response.get("success") is True and (
        response.get("orderID") or response.get("order_id")
    ):
        return True

    return False


def _extract_order_id(response: dict[str, Any]) -> str | None:
    """Extract the order ID from a CLOB response."""
    if not isinstance(response, dict):
        return None
    return response.get("orderID") or response.get("order_id")


def _seconds_until_slot_end(slot_end_ts: int) -> float:
    """Return seconds remaining until the slot ends."""
    return slot_end_ts - time.time()


async def place_fok_order_with_retry(
    poly_client,
    token_id: str,
    amount_usdc: float,
    signal_id: int,
    trade_id: int,
    slot_end_ts: int,
) -> dict[str, Any]:
    """Place a FOK order with robust retry logic.

    Safety features:
    1. **Fill verification** -- checks CLOB response for MATCHED status
    2. **Retry with backoff** -- up to FOK_MAX_RETRIES attempts with
       exponential delay, refreshing the best ask price each time
    3. **Duplicate guard** -- checks DB for existing filled trade before
       each attempt to prevent double-ordering
    4. **Time fence** -- aborts if within FOK_SLOT_CUTOFF_SECONDS of slot end

    Returns a result dict with keys: status, order_id, attempts, reason.
    """
    max_retries = cfg.FOK_MAX_RETRIES
    delay_base = cfg.FOK_RETRY_DELAY_BASE
    delay_max = cfg.FOK_RETRY_DELAY_MAX
    cutoff = cfg.FOK_SLOT_CUTOFF_SECONDS

    last_order_id: str | None = None
    last_error: str = ""

    for attempt in range(1, max_retries + 1):
        # --- Time fence: abort if too close to slot expiry ---
        remaining = _seconds_until_slot_end(slot_end_ts)
        if remaining < cutoff:
            reason = (
                f"Aborted: only {remaining:.0f}s until slot end "
                f"(cutoff={cutoff}s) after {attempt - 1} attempt(s)"
            )
            log.warning("Trade %d: %s", trade_id, reason)
            await queries.update_trade_retry(trade_id, "aborted", attempt - 1)
            return _build_result("aborted", last_order_id, attempt - 1, reason)

        # --- Duplicate guard: check if this signal already has a filled trade ---
        existing = await queries.get_active_trade_for_signal(signal_id)
        if existing and existing["id"] != trade_id:
            reason = (
                f"Duplicate guard: signal {signal_id} already has filled trade "
                f"{existing['id']} -- aborting"
            )
            log.warning("Trade %d: %s", trade_id, reason)
            await queries.update_trade_retry(trade_id, "duplicate_prevented", attempt - 1)
            return _build_result("aborted", last_order_id, attempt - 1, reason)

        # --- Attempt the FOK order ---
        try:
            log.info(
                "Trade %d: FOK attempt %d/%d (token=%s, amount=$%.2f, %.0fs remaining)",
                trade_id, attempt, max_retries, token_id, amount_usdc, remaining,
            )
            response = await place_fok_order(poly_client, token_id, amount_usdc)
        except Exception as exc:
            last_error = str(exc)
            log.exception(
                "Trade %d: FOK attempt %d/%d raised exception", trade_id, attempt, max_retries
            )
            await queries.update_trade_retry(trade_id, "retrying", attempt)

            if attempt < max_retries:
                delay = min(delay_base * (2 ** (attempt - 1)), delay_max)
                log.info("Trade %d: waiting %.1fs before retry...", trade_id, delay)
                await asyncio.sleep(delay)
            continue

        # --- Verify fill status ---
        last_order_id = _extract_order_id(response)

        if _is_order_matched(response):
            log.info(
                "Trade %d: FILLED on attempt %d/%d (order_id=%s)",
                trade_id, attempt, max_retries, last_order_id,
            )
            await queries.update_trade_retry(trade_id, "filled", attempt, order_id=last_order_id)
            return _build_result("filled", last_order_id, attempt, "Order matched successfully")

        # Not matched -- log the status
        clob_status = response.get("status", "UNKNOWN") if isinstance(response, dict) else "UNKNOWN"
        log.warning(
            "Trade %d: attempt %d/%d NOT matched (CLOB status=%s, order_id=%s)",
            trade_id, attempt, max_retries, clob_status, last_order_id,
        )
        last_error = f"CLOB status: {clob_status}"
        await queries.update_trade_retry(trade_id, "retrying", attempt, order_id=last_order_id)

        # --- If not last attempt, refresh price and wait ---
        if attempt < max_retries:
            delay = min(delay_base * (2 ** (attempt - 1)), delay_max)
            log.info("Trade %d: waiting %.1fs before retry (will refresh price)...", trade_id, delay)
            await asyncio.sleep(delay)

            # Refresh best ask price for the token (liquidity may have changed)
            try:
                import httpx
                async with httpx.AsyncClient(timeout=15) as http_client:
                    new_ask = await get_clob_best_ask(token_id, http_client)
                if new_ask is not None:
                    log.info(
                        "Trade %d: refreshed best ask = %.4f (was placing at $%.2f)",
                        trade_id, new_ask, amount_usdc,
                    )
                    # Note: we don't change amount_usdc or token_id -- the FOK order
                    # amount is in USDC terms, not shares. The CLOB handles the
                    # price matching internally. The refresh is for monitoring only.
            except Exception:
                log.debug("Trade %d: price refresh failed (non-critical)", trade_id)

    # All retries exhausted
    reason = f"All {max_retries} attempts exhausted. Last error: {last_error}"
    log.error("Trade %d: %s", trade_id, reason)
    await queries.update_trade_retry(trade_id, "unmatched", max_retries, order_id=last_order_id)
    return _build_result("unmatched", last_order_id, max_retries, reason)
