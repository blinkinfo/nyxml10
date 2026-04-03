"""CTF position redeemer — detects resolved winning positions via Polymarket Data API
and redeems them on-chain via the ConditionalTokens (CTF) contract using web3.py.

Flow
----
1. Query https://data-api.polymarket.com/positions?user=<wallet> to find positions
   where size > 0 and the market is resolved (outcome price = 1.0 or 0.0).
2. For each redeemable position, call CTF.redeemPositions() on Polygon.
3. The transaction is signed via EIP-712 / raw private key using web3.py.
4. Results are recorded in the `redemptions` DB table.

Contracts (Polygon mainnet)
---------------------------
CTF (ConditionalTokens):  0x4D97DCd97eC945f40cF65F87097ACe5EA0476045
USDC.e collateral:        0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174
"""

from __future__ import annotations

import asyncio
import logging
import traceback
from typing import Any

import httpx

import config as cfg

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Contract addresses (Polygon mainnet)
# ---------------------------------------------------------------------------
CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
USDC_E_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"

# Minimal ABI — only the methods we actually call
_CTF_ABI = [
    {
        "name": "redeemPositions",
        "type": "function",
        "stateMutability": "nonpayable",
        "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId",        "type": "bytes32"},
            {"name": "indexSets",          "type": "uint256[]"},
        ],
        "outputs": [],
    },
    {
        "name": "payoutDenominator",
        "type": "function",
        "stateMutability": "view",
        "inputs": [{"name": "conditionId", "type": "bytes32"}],
        "outputs": [{"name": "", "type": "uint256"}],
    },
]

# Data API endpoint (module-level constant so tests can patch it)
DATA_API_POSITIONS_URL = "https://data-api.polymarket.com/positions"


# ---------------------------------------------------------------------------
# Web3 helpers (lazy import so the module loads even if web3 is not installed)
# ---------------------------------------------------------------------------

def _get_web3():
    """Return a connected Web3 instance using POLYGON_RPC_URL from config."""
    try:
        from web3 import Web3  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "web3 package is not installed. Add 'web3>=6.0.0' to requirements.txt."
        ) from exc

    rpc_url = cfg.POLYGON_RPC_URL
    if not rpc_url:
        raise RuntimeError("POLYGON_RPC_URL is not set in config / environment.")

    w3 = Web3(Web3.HTTPProvider(rpc_url))
    if not w3.is_connected():
        raise RuntimeError(f"Cannot connect to Polygon RPC at {rpc_url}")
    return w3


def _get_ctf_contract(w3):
    """Return a bound CTF contract instance."""
    from web3 import Web3  # type: ignore
    return w3.eth.contract(
        address=Web3.to_checksum_address(CTF_ADDRESS),
        abi=_CTF_ABI,
    )


# ---------------------------------------------------------------------------
# Data API — position fetching
# ---------------------------------------------------------------------------

async def fetch_positions(wallet_address: str) -> list[dict[str, Any]]:
    """Fetch all open positions for *wallet_address* from the Polymarket Data API.

    Returns a (possibly empty) list of position dicts on success.
    Raises RuntimeError on network failure or unexpected response shape.

    Each dict typically contains:
      proxyWallet, asset, conditionId, size, curPrice, redeemable,
      outcomeIndex, outcome, title, slug, currentValue, initialValue, mergeable,
      negativeRisk, endDate (flat structure -- no nested market object)
    """
    params = {"user": wallet_address, "sizeThreshold": "0.01"}
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.get(DATA_API_POSITIONS_URL, params=params)
            resp.raise_for_status()
            data = resp.json()
    except Exception as exc:
        log.exception("Data API request failed for wallet=%s", wallet_address)
        raise RuntimeError(f"Data API request failed: {exc}") from exc

    if not isinstance(data, list):
        # Some API versions wrap in {"data": [...]}
        if isinstance(data, dict):
            for key in ("data", "positions", "results"):
                if isinstance(data.get(key), list):
                    return data[key]
        err = f"Unexpected Data API response shape: {type(data).__name__}"
        log.error(err)
        raise RuntimeError(err)

    return data


# ---------------------------------------------------------------------------
# Position analysis — which positions are redeemable?
# ---------------------------------------------------------------------------

def find_redeemable_positions(positions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Filter *positions* to those that can be redeemed right now.

    A position is redeemable when ALL of the following hold:
      1. size > 0.001  (we hold tokens)
      2. redeemable == True  (market is resolved, API already computed this)
      3. curPrice >= 0.99  (our outcome won — skip losing positions)

    API schema (flat Polymarket Data API /positions response):
      conditionId, size, curPrice, redeemable, outcomeIndex, outcome, title

    Returns a list of dicts with:
      condition_id, outcome_index, size, title, raw, cur_price
    """
    redeemable: list[dict[str, Any]] = []

    for pos in positions:
        try:
            # 1. Must hold tokens
            size = float(pos.get("size", 0) or 0)
            if size < 0.001:
                continue

            # 2. Market must be resolved (API pre-computed flag)
            if not pos.get("redeemable"):
                continue

            # 3. Our outcome must have won (curPrice >= 0.99)
            cur_price = float(pos.get("curPrice") or 0)
            if cur_price < 0.99:
                continue

            # conditionId — ensure 0x prefix
            condition_id = pos.get("conditionId", "")
            if not condition_id:
                continue
            if not condition_id.startswith("0x"):
                condition_id = "0x" + condition_id

            outcome_index = int(pos["outcomeIndex"])
            title = pos.get("title", condition_id[:16])

            redeemable.append({
                "condition_id": condition_id,
                "outcome_index": outcome_index,
                "size": size,
                "title": title,
                "raw": pos,
                "cur_price": cur_price,
            })

        except Exception:
            log.exception("Error inspecting position %r", pos)
            continue

    return redeemable



# ---------------------------------------------------------------------------
# On-chain redemption
# ---------------------------------------------------------------------------

async def redeem_position(
    condition_id_hex: str,
) -> dict[str, Any]:
    """Call CTF.redeemPositions() on Polygon for one condition.

    Parameters
    ----------
    condition_id_hex : str
        bytes32 condition ID as a 0x-prefixed hex string.
    Returns
    -------
    dict with keys:
      ``success``   bool
      ``tx_hash``   str | None
      ``error``     str | None
      ``gas_used``  int | None
    """
    return await asyncio.to_thread(
        _redeem_position_sync,
        condition_id_hex,
    )


def _redeem_position_sync(
    condition_id_hex: str,
) -> dict[str, Any]:
    """Synchronous inner implementation — runs in a thread pool."""
    try:
        from web3 import Web3  # type: ignore
    except ImportError:
        return {"success": False, "tx_hash": None, "error": "web3 not installed", "gas_used": None}

    try:
        w3 = _get_web3()
    except RuntimeError as exc:
        return {"success": False, "tx_hash": None, "error": str(exc), "gas_used": None}

    private_key = cfg.POLYMARKET_PRIVATE_KEY
    if not private_key:
        return {
            "success": False,
            "tx_hash": None,
            "error": "POLYMARKET_PRIVATE_KEY not set",
            "gas_used": None,
        }

    try:
        ctf = _get_ctf_contract(w3)

        # For signature type 2 (Polymarket proxy wallet), the proxy wallet
        # holds the CTF tokens — NOT the EOA derived from the private key.
        # The private key is used ONLY for signing; the `from` address must
        # be the proxy wallet that actually owns the positions.
        signer_address = w3.eth.account.from_key(private_key).address
        sig_type = cfg.POLYMARKET_SIGNATURE_TYPE
        if sig_type == 2 and cfg.POLYMARKET_FUNDER_ADDRESS:
            account = Web3.to_checksum_address(cfg.POLYMARKET_FUNDER_ADDRESS)
            if account.lower() != signer_address.lower():
                log.warning(
                    "Sig type 2: using proxy wallet %s as `from` (signer EOA is %s). "
                    "Tokens are held by the proxy wallet.",
                    account,
                    signer_address,
                )
        else:
            account = signer_address
            if sig_type == 2 and not cfg.POLYMARKET_FUNDER_ADDRESS:
                log.error(
                    "POLYMARKET_SIGNATURE_TYPE=2 but POLYMARKET_FUNDER_ADDRESS is not set! "
                    "Redemption will use EOA address %s which likely holds no tokens.",
                    account,
                )

        collateral = Web3.to_checksum_address(USDC_E_ADDRESS)

        # parentCollectionId = 0x00...00 (top-level condition)
        parent_collection_id = b"\x00" * 32

        # condition_id as bytes32
        cid_bytes = bytes.fromhex(condition_id_hex.removeprefix("0x"))
        if len(cid_bytes) != 32:
            return {
                "success": False,
                "tx_hash": None,
                "error": f"condition_id must be 32 bytes, got {len(cid_bytes)}",
                "gas_used": None,
            }

        # indexSets — pass [1, 2] to redeem all outcomes per Polymarket docs.
        # The contract burns only winning tokens and ignores losing ones.
        index_sets = [1, 2]

        # --- Check payout denominator to confirm resolution ---
        try:
            payout_denom = ctf.functions.payoutDenominator(cid_bytes).call()
        except Exception:
            log.warning(
                "payoutDenominator check failed for condition %s — proceeding anyway",
                condition_id_hex,
            )
            payout_denom = 1  # assume resolved

        if payout_denom == 0:
            return {
                "success": False,
                "tx_hash": None,
                "error": "Market not yet resolved on-chain (payoutDenominator=0)",
                "gas_used": None,
            }

        # --- Build transaction ---
        nonce = w3.eth.get_transaction_count(account)
        gas_price = w3.eth.gas_price

        # Estimate gas first
        try:
            estimated_gas = ctf.functions.redeemPositions(
                collateral,
                parent_collection_id,
                cid_bytes,
                index_sets,
            ).estimate_gas({"from": account})
            gas_limit = int(estimated_gas * 1.2)  # 20% buffer
        except Exception:
            log.warning("Gas estimation failed — using fallback 200_000")
            gas_limit = 200_000

        tx = ctf.functions.redeemPositions(
            collateral,
            parent_collection_id,
            cid_bytes,
            index_sets,
        ).build_transaction({
            "from":     account,
            "nonce":    nonce,
            "gas":      gas_limit,
            "gasPrice": gas_price,
            "chainId":  137,  # Polygon mainnet
        })

        # --- Sign with private key ---
        signed_tx = w3.eth.account.sign_transaction(tx, private_key=private_key)

        # --- Broadcast ---
        tx_hash = w3.eth.send_raw_transaction(signed_tx.raw_transaction)
        tx_hash_hex = tx_hash.hex()
        log.info("Redemption tx broadcast: %s (condition=%s)", tx_hash_hex, condition_id_hex)

        # --- Wait for receipt (up to 120 seconds) ---
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
        gas_used = receipt.get("gasUsed")

        if receipt["status"] == 1:
            log.info(
                "Redemption confirmed: tx=%s gas_used=%s condition=%s",
                tx_hash_hex, gas_used, condition_id_hex,
            )
            return {
                "success": True,
                "tx_hash": tx_hash_hex,
                "error": None,
                "gas_used": gas_used,
            }
        else:
            log.error("Redemption tx REVERTED: tx=%s condition=%s", tx_hash_hex, condition_id_hex)
            return {
                "success": False,
                "tx_hash": tx_hash_hex,
                "error": "Transaction reverted",
                "gas_used": gas_used,
            }

    except Exception as exc:
        tb_str = traceback.format_exc()
        log.exception("Redemption failed for condition=%s", condition_id_hex)
        return {
            "success": False,
            "tx_hash": None,
            "error": f"{type(exc).__name__}: {exc}",
            "error_detail": tb_str,
            "gas_used": None,
        }


# ---------------------------------------------------------------------------
# High-level: scan and redeem all eligible positions
# ---------------------------------------------------------------------------

async def scan_and_redeem(
    wallet_address: str,
    dry_run: bool = False,
) -> list[dict[str, Any]]:
    """Scan wallet for redeemable positions and redeem each one.

    Parameters
    ----------
    wallet_address : str
        Polygon address to scan.
    dry_run : bool
        If True, detect positions but do NOT send any transactions.
        Useful for the /redeem preview command.

    Returns
    -------
    List of result dicts, one per redeemable position found:
      {
        "condition_id": str,
        "outcome_index": int,
        "size": float,
        "title": str,
        "success": bool,        # always True in dry_run
        "tx_hash": str | None,  # None in dry_run
        "error": str | None,
        "gas_used": int | None,
        "dry_run": bool,
      }
    """
    positions = await fetch_positions(wallet_address)
    redeemable = find_redeemable_positions(positions)

    if not redeemable:
        log.info("scan_and_redeem: no redeemable positions for wallet=%s", wallet_address)
        return []

    log.info(
        "scan_and_redeem: found %d redeemable position(s) for wallet=%s",
        len(redeemable), wallet_address,
    )

    results: list[dict[str, Any]] = []
    for pos in redeemable:
        if dry_run:
            results.append({
                **pos,
                "success": True,
                "tx_hash": None,
                "error": None,
                "gas_used": None,
                "dry_run": True,
            })
            continue

        result = await redeem_position(pos["condition_id"])
        results.append({
            **pos,
            **result,
            "dry_run": False,
        })

    return results
