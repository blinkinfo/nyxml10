"""Pattern strategy -- historical pattern matching on BTC-USD.

Flow:
1. Fetch the most recently *closed* 5-min BTC-USD candles from Coinbase
2. Read directions of up to 7 most recent fully closed candles
3. Build pattern strings at depth 7, 6, and 5 (longest-first greedy match)
4. Look up string in pattern table
5. If match -> trade predicted direction for N+1 candle
6. If no match -> skip

Candle direction: close >= open means U (up), close < open means D (down).

CANDLE TIMING SAFETY
--------------------
This strategy is called at T-85s (215s into a 300s slot).  At that moment the
current 5-min candle is still open.  Coinbase's /candles endpoint returns only
fully *closed* candles, BUT to be safe we always drop the most-recent candle
returned by the API (it may be the still-open candle if the API races the
clock-boundary) and work exclusively from the confirmed-closed set.
See _fetch_candles() for the implementation detail.
"""

from __future__ import annotations

import logging
import time as _time
from typing import Any

import config as cfg
import httpx
from core.strategies.base import BaseStrategy
from polymarket.markets import get_next_slot_info, get_slot_prices

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pattern table
# ---------------------------------------------------------------------------
# Key format: variable-length string (4-7 characters) where each character
# represents one 5-min BTC-USD candle direction.  Characters are ordered
# LEFT-TO-RIGHT as:
#   [N-1][N-2]...[N-k]
# where N-1 is the most-recently CLOSED candle and N-k is the oldest.
# U = close >= open (up), D = close < open (down).
# Value = predicted direction for the NEXT candle (N+1): "UP" or "DOWN".
#
# Scan order: [_PATTERN_DEPTHS] longest-first (7 > 6 > 5).  Greedy -- the
# first matching depth wins, giving highest specificity to longer patterns.

PATTERN_TABLE: dict[str, str] = {
    # -- 6-char patterns --
    "DDDDDD": "UP",
    "DUUUDU": "DOWN",
    "DUUUUD": "DOWN",
    "DUUUDD": "DOWN",
    "UDDUUD": "UP",
    "DUDUDU": "DOWN",
    "DUDUUD": "UP",
    "UUDUUU": "DOWN",
    "DDUDDU": "UP",
    "UUUDUD": "DOWN",
    "DDDUUD": "DOWN",
    "UDUUDU": "DOWN",
    "DUUDDD": "UP",
    "DDUDDD": "UP",
    "DUDDDU": "DOWN",
    # -- 7-char patterns --
    "DDDUDDD": "UP",
    "DUDDDDD": "UP",
    "UDUUUUU": "UP",
    "UDUUUUD": "DOWN",
    # -- 5-char patterns --
    "DDDUU": "DOWN",
}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

async def _fetch_candles(count: int = 12) -> list[dict[str, float]] | None:
    """Fetch the *count* most recently CONFIRMED-CLOSED 5-min BTC-USD candles.

    Implementation details
    ----------------------
    * Requests a 300-candle window (~25 h) from Coinbase so there is always
      a large pool to draw from.
    * Candles are sorted oldest-first after reversing the Coinbase response.
    * We DROP the final candle in the sorted list (index -1) because at
      T-85s the current 5-min slot is still open and Coinbase may include a
      partially-formed candle at the tail.  Dropping it guarantees we only
      work with fully closed candles.
    * After the safety drop we return the *count* most recent candles
      (oldest first), so callers get exactly what they asked for.

    Returns None on network/parse failure.
    """
    granularity = 300  # 5 minutes in seconds
    end_ts = int(_time.time())
    start_ts = end_ts - 300 * granularity  # ~25-hour window

    params = {
        "granularity": granularity,
        "start": start_ts,
        "end": end_ts,
    }

    try:
        async with httpx.AsyncClient(timeout=15, trust_env=False) as client:
            resp = await client.get(cfg.COINBASE_CANDLE_URL, params=params)
            resp.raise_for_status()
            raw = resp.json()
    except Exception:
        log.exception("Coinbase candle fetch failed")
        return None

    if not raw or not isinstance(raw, list):
        log.error("Coinbase returned empty or invalid candle response")
        return None

    candles: list[dict[str, float]] = []
    for row in raw:
        try:
            candles.append({
                "time":  float(row[0]),
                "low":   float(row[1]),
                "high":  float(row[2]),
                "open":  float(row[3]),
                "close": float(row[4]),
            })
        except (IndexError, ValueError, TypeError):
            continue

    if not candles:
        log.error("Coinbase candle response contained no parseable rows")
        return None

    # Sort oldest-first (Coinbase returns newest-first).
    candles.sort(key=lambda c: c["time"])

    # Drop the last candle: at T-85s the current 5-min slot is still open.
    # The final entry from Coinbase *may* be a still-forming candle.  Removing
    # it is cheap insurance against using dirty data in the pattern.
    confirmed_closed = candles[:-1]

    if len(confirmed_closed) < count:
        log.warning(
            "Not enough confirmed-closed candles: have %d, need %d",
            len(confirmed_closed), count,
        )
        return None

    # Return exactly `count` most-recent confirmed-closed candles, oldest first.
    return confirmed_closed[-count:]


def _build_pattern_string(candles: list[dict[str, float]], depth: int = 6) -> str | None:
    """Build a *depth*-character pattern string from candle directions.

    Expects *candles* sorted oldest-first.  Reads the last *depth* entries.

    Result character order (left to right):
      position 0 = direction of candles[-1]  (N-1, most recent closed)
      position 1 = direction of candles[-2]  (N-2)
      ...
      position k = direction of candles[-k-1] (N-k, oldest in window)

    This matches the PATTERN_TABLE key format: [N-1][N-2]...[N-k].

    Supported depths: 5, 6, 7 (configured via _PATTERN_DEPTHS).
    """
    if len(candles) < depth:
        log.warning(
            "Not enough candles to build pattern: have %d, need %d",
            len(candles), depth,
        )
        return None

    pattern = ""
    for i in range(depth):
        candle = candles[-1 - i]
        direction = "U" if candle["close"] >= candle["open"] else "D"
        pattern += direction

    return pattern


# ---------------------------------------------------------------------------
# Strategy class
# ---------------------------------------------------------------------------

class PatternStrategy(BaseStrategy):
    """Multi-depth historical pattern matching strategy.

    Scans candle history at multiple depths (7, 6, 5) using a longest-first
    greedy match.  Returns either:
      - None                  on hard failure (network, parse error)
      - {"skipped": True, ...}  when no pattern matches at any depth
      - {"skipped": False, ...} with full trade fields when a match is found
    """

    # Number of confirmed-closed candles to fetch from Coinbase.
    # Must be >= max(_PATTERN_DEPTHS) + 1 so the safety drop still leaves enough.
    _CANDLE_FETCH_COUNT: int = 12

    # Depths to scan, checked longest-first (greedy match).
    _PATTERN_DEPTHS: list[int] = [7, 6, 5]

    async def check_signal(self) -> dict[str, Any] | None:
        """Generate a pattern-based signal for slot N+1.

        Called at T-85s before the current slot ends.

        Steps:
          1. Fetch confirmed-closed BTC-USD 5-min candles
          2. Build pattern strings at depths 7, 6, 5 (longest-first)
          3. Look up each pattern in PATTERN_TABLE; first match wins
          4. On match: fetch Polymarket prices, return full signal dict
          5. On no match: return skip dict (no trade placed)
        """
        candles = await _fetch_candles(count=self._CANDLE_FETCH_COUNT)
        if candles is None:
            log.error("PatternStrategy: candle fetch failed \u2014 aborting signal check")
            return None

        # Greedy longest-first match across all configured depths.
        matched_depth: int | None = None
        pattern: str | None = None
        prediction: str | None = None
        shallowest_pattern: str | None = None

        for d in self._PATTERN_DEPTHS:
            p = _build_pattern_string(candles, depth=d)
            if p is None:
                continue
            # Track shallowest for skip logging.
            shallowest_pattern = p
            pred = PATTERN_TABLE.get(p)
            if pred is not None and matched_depth is None:
                matched_depth = d
                prediction = pred
                pattern = p

        if prediction is None:
            # No match at any depth.  Use shallowest_pattern for logging.
            slot_n1 = get_next_slot_info()
            log.info(
                "PatternStrategy: no match at any depth [%s] -> SKIP "
                "(shallowest pattern '%s', slot %s-%s UTC)",
                ", ".join(str(d) for d in self._PATTERN_DEPTHS),
                shallowest_pattern,
                slot_n1["slot_start_str"],
                slot_n1["slot_end_str"],
            )
            return {
                "skipped": True,
                "pattern": shallowest_pattern,  # type: ignore[arg-type]
                "candles_used": self._PATTERN_DEPTHS[-1],
                "slot_n1_start_full": slot_n1["slot_start_full"],
                "slot_n1_end_full":   slot_n1["slot_end_full"],
                "slot_n1_start_str":  slot_n1["slot_start_str"],
                "slot_n1_end_str":    slot_n1["slot_end_str"],
                "slot_n1_ts":         slot_n1["slot_start_ts"],
            }

        # Normalize prediction to Polymarket-style side string.
        side = "Up" if prediction == "UP" else "Down"

        # Fetch live Polymarket prices for N+1 slot.
        slot_n1 = get_next_slot_info()
        prices = await get_slot_prices(slot_n1["slug"])
        if prices is None:
            log.error(
                "PatternStrategy: pattern '%s' (depth %d) -> %s matched but Polymarket price "
                "fetch failed for slot %s \u2014 aborting signal",
                pattern, matched_depth, prediction, slot_n1["slug"],
            )
            return None

        entry_price    = prices["up_price"]    if side == "Up" else prices["down_price"]
        opposite_price = prices["down_price"]  if side == "Up" else prices["up_price"]
        token_id       = prices["up_token_id"] if side == "Up" else prices["down_token_id"]

        log.info(
            "PatternStrategy: MATCH '%s' at depth %d -> %s | slot %s-%s UTC | "
            "entry=$%.4f | token=%s",
            pattern,
            matched_depth,
            prediction,
            slot_n1["slot_start_str"],
            slot_n1["slot_end_str"],
            entry_price,
            token_id,
        )

        return {
            "skipped":            False,
            "side":               side,
            "entry_price":        entry_price,
            "opposite_price":     opposite_price,
            "token_id":           token_id,
            "pattern":            pattern,
            "candles_used":       matched_depth,
            "slot_n1_start_full": slot_n1["slot_start_full"],
            "slot_n1_end_full":   slot_n1["slot_end_full"],
            "slot_n1_start_str":  slot_n1["slot_start_str"],
            "slot_n1_end_str":    slot_n1["slot_end_str"],
            "slot_n1_ts":         slot_n1["slot_start_ts"],
            "slot_n1_slug":       slot_n1["slug"],
        }
