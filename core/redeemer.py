"""CTF position redeemer — detects resolved winning positions via Polymarket Data API
and redeems them on-chain via the ConditionalTokens (CTF) contract using web3.py.

Flow
----
1. Query https://data-api.polymarket.com/positions?user=<wallet> to find positions
   where size > 0 and the market is resolved (outcome price = 1.0 or 0.0).
2. For each redeemable position, call CTF.redeemPositions() on Polygon.
3. For sig_type==2 (Gnosis Safe), the call is wrapped in Safe.execTransaction()
   so the Safe (which holds the tokens) is msg.sender on the CTF contract.
4. Results are recorded in the `redemptions` DB table.

Contracts (Polygon mainnet)
---------------------------
CTF (ConditionalTokens):  0x4D97DCd97eC945f40cF65F87097ACe5EA0476045
V2 pUSD collateral:       0x466a756E9A7401B5e2444a3fCB3c2C12FBEc0a54
"""

from __future__ import annotations

import asyncio
import logging
import traceback
from collections import defaultdict
from typing import Any

import httpx

import config as cfg

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Contract addresses (Polygon mainnet)
# ---------------------------------------------------------------------------
CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
PUSD_ADDRESS = "0x466a756E9A7401B5e2444a3fCB3c2C12FBEc0a54"


def _resolve_collateral_token(collateral_token: str | None = None) -> str:
    """Return the checksum collateral token address for redemptions.

    Defaults to Polymarket V2 pUSD collateral unless an explicit override is
    provided by the caller.
    """
    from web3 import Web3  # type: ignore

    token = collateral_token or PUSD_ADDRESS
    return Web3.to_checksum_address(token)

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
    {
        "name": "balanceOf",
        "type": "function",
        "stateMutability": "view",
        "inputs": [
            {"name": "account", "type": "address"},
            {"name": "id",      "type": "uint256"},
        ],
        "outputs": [{"name": "", "type": "uint256"}],
    },
    {
        "name": "getPositionId",
        "type": "function",
        "stateMutability": "pure",
        "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "collectionId",    "type": "bytes32"},
        ],
        "outputs": [{"name": "", "type": "uint256"}],
    },
    {
        "name": "getCollectionId",
        "type": "function",
        "stateMutability": "pure",
        "inputs": [
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId",        "type": "bytes32"},
            {"name": "indexSet",           "type": "uint256"},
        ],
        "outputs": [{"name": "", "type": "bytes32"}],
    },
]

# Gnosis Safe ABI — module-level so main.py can import it for the sanity check
_SAFE_ABI = [
    {
        "name": "getTransactionHash",
        "type": "function",
        "stateMutability": "view",
        "inputs": [
            {"name": "to",             "type": "address"},
            {"name": "value",          "type": "uint256"},
            {"name": "data",           "type": "bytes"},
            {"name": "operation",      "type": "uint8"},
            {"name": "safeTxGas",      "type": "uint256"},
            {"name": "baseGas",        "type": "uint256"},
            {"name": "gasPrice",       "type": "uint256"},
            {"name": "gasToken",       "type": "address"},
            {"name": "refundReceiver", "type": "address"},
            {"name": "_nonce",         "type": "uint256"},
        ],
        "outputs": [{"name": "", "type": "bytes32"}],
    },
    {
        "name": "nonce",
        "type": "function",
        "stateMutability": "view",
        "inputs": [],
        "outputs": [{"name": "", "type": "uint256"}],
    },
    {
        "name": "execTransaction",
        "type": "function",
        "stateMutability": "payable",
        "inputs": [
            {"name": "to",             "type": "address"},
            {"name": "value",          "type": "uint256"},
            {"name": "data",           "type": "bytes"},
            {"name": "operation",      "type": "uint8"},
            {"name": "safeTxGas",      "type": "uint256"},
            {"name": "baseGas",        "type": "uint256"},
            {"name": "gasPrice",       "type": "uint256"},
            {"name": "gasToken",       "type": "address"},
            {"name": "refundReceiver", "type": "address"},
            {"name": "signatures",     "type": "bytes"},
        ],
        "outputs": [{"name": "success", "type": "bool"}],
    },
    {
        "name": "ExecutionSuccess",
        "type": "event",
        "anonymous": False,
        "inputs": [{"indexed": True, "name": "txHash", "type": "bytes32"}, {"indexed": False, "name": "payment", "type": "uint256"}],
    },
    {
        "name": "ExecutionFailure",
        "type": "event",
        "anonymous": False,
        "inputs": [{"indexed": True, "name": "txHash", "type": "bytes32"}, {"indexed": False, "name": "payment", "type": "uint256"}],
    },
    {
        "name": "getOwners",
        "type": "function",
        "stateMutability": "view",
        "inputs": [],
        "outputs": [{"name": "", "type": "address[]"}],
    },
    {
        "name": "getThreshold",
        "type": "function",
        "stateMutability": "view",
        "inputs": [],
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

def _normalize_condition_id(condition_id: str | None) -> str | None:
    if not condition_id:
        return None
    normalized = str(condition_id).strip()
    if not normalized:
        return None
    if not normalized.startswith("0x"):
        normalized = "0x" + normalized
    return normalized


def _normalize_collateral_token_for_position(position: dict[str, Any]) -> str | None:
    for key in ("collateralToken", "collateral_token", "collateralAddress", "collateral_address"):
        value = position.get(key)
        if value:
            return str(value)
    return None


def _candidate_index_sets(outcome_index: int | None, outcome_count: int | None) -> list[int]:
    if outcome_count and outcome_count > 0:
        return [1 << idx for idx in range(outcome_count)]
    if outcome_index is None or outcome_index < 0:
        return [1, 2]
    highest = max(outcome_index + 1, 2)
    return [1 << idx for idx in range(highest)]


def _build_redeemable_entry(pos: dict[str, Any]) -> dict[str, Any] | None:
    try:
        size = float(pos.get("size", 0) or 0)
        if size < 0.001:
            return None
        if not pos.get("redeemable"):
            return None

        cur_price = float(pos.get("curPrice") or 0)
        won = cur_price >= 0.99
        lost = cur_price <= 0.01
        if not (won or lost):
            return None

        condition_id = _normalize_condition_id(pos.get("conditionId"))
        if not condition_id:
            return None

        outcome_index_raw = pos.get("outcomeIndex")
        outcome_index = int(outcome_index_raw) if outcome_index_raw is not None else None
        outcome_count_raw = pos.get("outcomeCount") or pos.get("outcomesCount")
        outcome_count = int(outcome_count_raw) if outcome_count_raw not in (None, "") else None
        index_sets = _candidate_index_sets(outcome_index, outcome_count)
        target_index_set = (1 << outcome_index) if outcome_index is not None and outcome_index >= 0 else None
        title = pos.get("title", condition_id[:16])
        asset_id = str(pos.get("asset") or pos.get("asset_id") or "") or None
        collateral_token = _normalize_collateral_token_for_position(pos)

        return {
            "condition_id": condition_id,
            "outcome_index": outcome_index,
            "size": size,
            "title": title,
            "raw": pos,
            "cur_price": cur_price,
            "won": won,
            "asset_id": asset_id,
            "collateral_token": collateral_token,
            "index_sets": index_sets,
            "target_index_set": target_index_set,
            "outcome_count": outcome_count,
        }
    except Exception:
        log.exception("Error inspecting position %r", pos)
        return None


def find_redeemable_positions(positions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return one actionable redemption candidate per condition.

    The Data API reports positions per outcome token, but the on-chain redemption
    call operates at the condition level. We therefore group by condition and keep
    enough metadata to verify that the actual relevant token positions disappear
    after execution.
    """
    grouped: dict[tuple[str, str | None], list[dict[str, Any]]] = defaultdict(list)

    for pos in positions:
        entry = _build_redeemable_entry(pos)
        if entry is None:
            continue
        grouped[(entry["condition_id"], entry.get("collateral_token"))].append(entry)

    redeemable: list[dict[str, Any]] = []
    for (condition_id, collateral_token), entries in grouped.items():
        entries.sort(key=lambda item: (item.get("won") is not True, -(item.get("size") or 0)))
        primary = dict(entries[0])
        unique_index_sets: list[int] = []
        seen_index_sets: set[int] = set()
        outcome_index_entries: list[dict[str, Any]] = []
        for entry in entries:
            target_index_set = entry.get("target_index_set")
            if target_index_set is not None and target_index_set not in seen_index_sets:
                seen_index_sets.add(target_index_set)
                unique_index_sets.append(target_index_set)
                outcome_index_entries.append({
                    "outcome_index": entry.get("outcome_index"),
                    "index_set": target_index_set,
                    "asset_id": entry.get("asset_id"),
                    "size": entry.get("size"),
                    "won": entry.get("won"),
                    "title": entry.get("title"),
                })
        if not unique_index_sets:
            for fallback_index_set in primary.get("index_sets") or [1, 2]:
                if fallback_index_set not in seen_index_sets:
                    seen_index_sets.add(fallback_index_set)
                    unique_index_sets.append(fallback_index_set)

        primary["collateral_token"] = collateral_token
        primary["index_sets"] = unique_index_sets
        primary["positions"] = outcome_index_entries
        primary["asset_ids"] = [p["asset_id"] for p in outcome_index_entries if p.get("asset_id")]
        primary["won_count"] = sum(1 for entry in entries if entry.get("won"))
        primary["lost_count"] = sum(1 for entry in entries if entry.get("won") is False)
        primary["position_count"] = len(entries)
        redeemable.append(primary)

    return redeemable

async def redeem_position(
    condition_id_hex: str,
    collateral_token: str | None = None,
    index_sets: list[int] | None = None,
    positions: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Call CTF.redeemPositions() on Polygon for one condition.

    For sig_type==2 (Gnosis Safe), wraps the call in Safe.execTransaction()
    so the Safe contract is msg.sender and its token balances are used.

    Parameters
    ----------
    condition_id_hex : str
        bytes32 condition ID as a 0x-prefixed hex string.
    Returns
    -------
    dict with keys:
      ``success``               bool
      ``tx_hash``               str | None
      ``error``                 str | None
      ``gas_used``              int | None
      ``safe_exec``             bool  (True only when Safe path was taken)
      ``verified_zero_balance`` bool  (True if post-tx position balances are zero)
      ``verified``              bool  (True if the redemption is considered verified)
    """
    return await asyncio.to_thread(
        _redeem_position_sync,
        condition_id_hex,
        collateral_token,
        index_sets,
        positions,
    )


def _redeem_position_sync(
    condition_id_hex: str,
    collateral_token: str | None = None,
    index_sets: list[int] | None = None,
    positions: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Synchronous inner implementation — runs in a thread pool.

    When cfg.POLYMARKET_SIGNATURE_TYPE == 2 the redemption is routed through
    the Gnosis Safe (cfg.POLYMARKET_FUNDER_ADDRESS) via execTransaction so
    that the Safe — the actual token holder — is msg.sender on the CTF
    contract.  All other sig types keep the existing direct-EOA path.
    """
    try:
        from web3 import Web3  # type: ignore
    except ImportError:
        return {"success": False, "tx_hash": None, "error": "web3 not installed", "gas_used": None,
                "safe_exec": False, "verified_zero_balance": False}

    try:
        w3 = _get_web3()
    except RuntimeError as exc:
        return {"success": False, "tx_hash": None, "error": str(exc), "gas_used": None,
                "safe_exec": False, "verified_zero_balance": False}

    private_key = cfg.POLYMARKET_PRIVATE_KEY
    if not private_key:
        return {
            "success": False,
            "tx_hash": None,
            "error": "POLYMARKET_PRIVATE_KEY not set",
            "gas_used": None,
            "safe_exec": False,
            "verified_zero_balance": False,
        }

    sig_type = cfg.POLYMARKET_SIGNATURE_TYPE

    try:
        ctf = _get_ctf_contract(w3)

        # EOA = the account derived from the private key
        eoa_account = w3.eth.account.from_key(private_key).address

        collateral = _resolve_collateral_token(collateral_token)

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
                "safe_exec": False,
                "verified_zero_balance": False,
            }

        requested_index_sets = [int(v) for v in (index_sets or [1, 2]) if int(v) > 0]
        index_sets = sorted(set(requested_index_sets)) or [1, 2]
        positions = positions or []

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
                "safe_exec": False,
                "verified_zero_balance": False,
            }

        # ---------------------------------------------------------------
        # Build the redeemPositions() calldata (needed for both paths)
        # ---------------------------------------------------------------
        redeem_calldata = ctf.encode_abi(
            "redeemPositions",
            args=[collateral, parent_collection_id, cid_bytes, index_sets],
        )

        # ===================================================================
        # PATH A — Gnosis Safe execTransaction (sig_type == 2)
        # ===================================================================
        if sig_type == 2:
            return _redeem_via_safe(
                w3=w3,
                ctf=ctf,
                eoa_account=eoa_account,
                private_key=private_key,
                redeem_calldata=redeem_calldata,
                collateral=collateral,
                parent_collection_id=parent_collection_id,
                cid_bytes=cid_bytes,
                index_sets=index_sets,
                condition_id_hex=condition_id_hex,
                positions=positions,
            )

        # ===================================================================
        # PATH B — Direct EOA call (sig_type != 2) — original behaviour
        # ===================================================================
        account = eoa_account
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

        # Sign with private key
        signed_tx = w3.eth.account.sign_transaction(tx, private_key=private_key)

        # Broadcast
        tx_hash = w3.eth.send_raw_transaction(signed_tx.raw_transaction)
        tx_hash_hex = tx_hash.hex()
        log.info("Redemption tx broadcast: %s (condition=%s)", tx_hash_hex, condition_id_hex)

        # Wait for receipt (up to 120 seconds)
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
        gas_used = receipt.get("gasUsed")

        if receipt["status"] != 1:
            log.error("Redemption tx REVERTED: tx=%s condition=%s", tx_hash_hex, condition_id_hex)
            return {
                "success": False,
                "tx_hash": tx_hash_hex,
                "error": "Transaction reverted",
                "gas_used": gas_used,
                "safe_exec": False,
                "verified_zero_balance": False,
            }

        log.info(
            "Redemption confirmed: tx=%s gas_used=%s condition=%s",
            tx_hash_hex, gas_used, condition_id_hex,
        )

        # --- Phase 2: Post-tx balance verification (EOA path) ---
        verification = _verify_zero_balance(
            ctf=ctf,
            token_holder=account,
            collateral=collateral,
            parent_collection_id=parent_collection_id,
            cid_bytes=cid_bytes,
            index_sets=index_sets,
            condition_id_hex=condition_id_hex,
            positions=positions,
        )

        return {
            "success": True,
            "tx_hash": tx_hash_hex,
            "error": None,
            "gas_used": gas_used,
            "safe_exec": False,
            "verified_zero_balance": verification["all_zero"],
            "verified": verification["verified"],
            "verification": verification,
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
            "safe_exec": False,
            "verified_zero_balance": False,
        }


def _redeem_via_safe(
    w3,
    ctf,
    eoa_account: str,
    private_key: str,
    redeem_calldata: bytes,
    collateral: str,
    parent_collection_id: bytes,
    cid_bytes: bytes,
    index_sets: list,
    condition_id_hex: str,
    positions: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Execute redeemPositions() through the Gnosis Safe via execTransaction.

    The Safe (cfg.POLYMARKET_FUNDER_ADDRESS) is the token holder and will be
    msg.sender on the CTF contract.  The EOA (derived from POLYMARKET_PRIVATE_KEY)
    signs the Safe transaction hash and pays gas for the outer execTransaction call.
    """
    from web3 import Web3  # type: ignore

    safe_address = Web3.to_checksum_address(cfg.POLYMARKET_FUNDER_ADDRESS)
    ctf_address  = Web3.to_checksum_address(CTF_ADDRESS)
    zero_address = "0x0000000000000000000000000000000000000000"

    safe = w3.eth.contract(address=safe_address, abi=_SAFE_ABI)

    # Safe execTransaction parameters
    to             = ctf_address
    value          = 0
    data           = redeem_calldata
    operation      = 0          # CALL
    safe_tx_gas    = 0
    base_gas       = 0
    gas_price_safe = 0
    gas_token      = zero_address
    refund_receiver = zero_address
    safe_nonce     = safe.functions.nonce().call()

    log.info(
        "Safe execTransaction: safe=%s ctf=%s nonce=%d condition=%s",
        safe_address, ctf_address, safe_nonce, condition_id_hex,
    )

    # --- Get the Safe transaction hash from the contract (correct and canonical) ---
    safe_tx_hash = safe.functions.getTransactionHash(
        to,
        value,
        data,
        operation,
        safe_tx_gas,
        base_gas,
        gas_price_safe,
        gas_token,
        refund_receiver,
        safe_nonce,
    ).call()

    # --- Sign the Safe tx hash with the EOA private key ---
    # signHash returns a SignedMessage with v, r, s
    signed = w3.eth.account._sign_hash(safe_tx_hash, private_key=private_key)
    v = signed.v
    r = signed.r
    s = signed.s
    # Pack as 65 bytes: r (32) + s (32) + v (1), big-endian
    signatures = r.to_bytes(32, "big") + s.to_bytes(32, "big") + v.to_bytes(1, "big")

    # --- Build the execTransaction call ---
    nonce_eoa  = w3.eth.get_transaction_count(eoa_account)
    gas_price  = w3.eth.gas_price

    # Estimate gas for the outer execTransaction
    try:
        exec_tx_for_estimate = safe.functions.execTransaction(
            to,
            value,
            data,
            operation,
            safe_tx_gas,
            base_gas,
            gas_price_safe,
            gas_token,
            refund_receiver,
            signatures,
        )
        estimated_gas = exec_tx_for_estimate.estimate_gas({"from": eoa_account})
        gas_limit = int(estimated_gas * 1.2)  # 20% buffer
    except Exception:
        log.warning("Safe execTransaction gas estimation failed — using fallback 300_000")
        gas_limit = 300_000

    tx = safe.functions.execTransaction(
        to,
        value,
        data,
        operation,
        safe_tx_gas,
        base_gas,
        gas_price_safe,
        gas_token,
        refund_receiver,
        signatures,
    ).build_transaction({
        "from":     eoa_account,
        "nonce":    nonce_eoa,
        "gas":      gas_limit,
        "gasPrice": gas_price,
        "chainId":  137,  # Polygon mainnet
    })

    # --- Sign and broadcast ---
    signed_tx = w3.eth.account.sign_transaction(tx, private_key=private_key)
    tx_hash = w3.eth.send_raw_transaction(signed_tx.raw_transaction)
    tx_hash_hex = tx_hash.hex()
    log.info(
        "Safe execTransaction broadcast: %s (condition=%s)",
        tx_hash_hex, condition_id_hex,
    )

    # --- Wait for receipt ---
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
    gas_used = receipt.get("gasUsed")

    if receipt["status"] != 1:
        log.error(
            "Safe execTransaction REVERTED: tx=%s condition=%s",
            tx_hash_hex, condition_id_hex,
        )
        return {
            "success": False,
            "tx_hash": tx_hash_hex,
            "error": "Safe execTransaction reverted",
            "gas_used": gas_used,
            "safe_exec": True,
            "verified_zero_balance": False,
        }

    log.info(
        "Safe execTransaction confirmed: tx=%s gas_used=%s condition=%s",
        tx_hash_hex, gas_used, condition_id_hex,
    )

    safe_exec_success = False
    safe_exec_failure = False
    try:
        from eth_utils import event_abi_to_log_topic  # type: ignore

        success_topic = event_abi_to_log_topic(_SAFE_ABI[3])
        failure_topic = event_abi_to_log_topic(_SAFE_ABI[4])
        def _norm_hex(value):
            hex_value = value.hex() if hasattr(value, "hex") else str(value)
            hex_value = hex_value.lower()
            return hex_value if hex_value.startswith("0x") else f"0x{hex_value}"

        expected_hash = _norm_hex(safe_tx_hash)
        success_topic_hex = _norm_hex(success_topic)
        failure_topic_hex = _norm_hex(failure_topic)

        for log_entry in receipt.get("logs", []) or []:
            topics = log_entry.get("topics") or []
            if not topics:
                continue
            topic0_hex = _norm_hex(topics[0])
            if topic0_hex not in {success_topic_hex, failure_topic_hex}:
                continue
            if len(topics) < 2:
                continue
            tx_topic_hex = _norm_hex(topics[1])
            if tx_topic_hex != expected_hash:
                continue
            emitter = log_entry.get("address")
            emitter = emitter.lower() if isinstance(emitter, str) else emitter
            if emitter != safe_address.lower():
                continue
            if topic0_hex == success_topic_hex:
                safe_exec_success = True
            elif topic0_hex == failure_topic_hex:
                safe_exec_failure = True
    except Exception as exc:
        log.warning(
            "Could not inspect Safe execution logs for tx=%s condition=%s: %s",
            tx_hash_hex, condition_id_hex, exc,
        )

    if safe_exec_failure or not safe_exec_success:
        error_msg = (
            "Safe inner redeem call failed"
            if safe_exec_failure
            else "Safe execution success could not be confirmed"
        )
        log.error(
            "%s: tx=%s condition=%s safe_tx_hash=%s",
            error_msg,
            tx_hash_hex,
            condition_id_hex,
            safe_tx_hash.hex() if hasattr(safe_tx_hash, "hex") else safe_tx_hash,
        )
        return {
            "success": False,
            "tx_hash": tx_hash_hex,
            "error": error_msg,
            "gas_used": gas_used,
            "safe_exec": True,
            "verified_zero_balance": False,
            "verified": False,
            "verification": {
                "verified": False,
                "all_zero": False,
                "checked_positions": [],
                "safe_exec_success": safe_exec_success,
                "safe_exec_failure": safe_exec_failure,
            },
        }

    # --- Phase 2: Post-tx balance verification (Safe as token holder) ---
    verification = _verify_zero_balance(
        ctf=ctf,
        token_holder=safe_address,
        collateral=collateral,
        parent_collection_id=parent_collection_id,
        cid_bytes=cid_bytes,
        index_sets=index_sets,
        condition_id_hex=condition_id_hex,
        positions=positions,
    )
    verification["safe_exec_success"] = safe_exec_success
    verification["safe_exec_failure"] = safe_exec_failure

    return {
        "success": True,
        "tx_hash": tx_hash_hex,
        "error": None,
        "gas_used": gas_used,
        "safe_exec": True,
        "verified_zero_balance": verification["all_zero"],
        "verified": verification["verified"],
        "verification": verification,
    }


def _verify_zero_balance(
    ctf,
    token_holder: str,
    collateral: str,
    parent_collection_id: bytes,
    cid_bytes: bytes,
    index_sets: list,
    condition_id_hex: str,
    positions: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Check that the relevant post-redemption position balances are actually zero."""
    positions = positions or []
    try:
        checked_positions: list[dict[str, Any]] = []
        all_zero = True
        asset_ids = {str(p.get("asset_id")) for p in positions if p.get("asset_id")}
        relevant_index_sets = sorted({int(p.get("index_set")) for p in positions if p.get("index_set") is not None} or {int(v) for v in index_sets})
        for index_set in relevant_index_sets:
            collection_id = ctf.functions.getCollectionId(
                parent_collection_id, cid_bytes, index_set
            ).call()
            position_id = ctf.functions.getPositionId(collateral, collection_id).call()
            balance = ctf.functions.balanceOf(token_holder, position_id).call()
            position_id_str = str(position_id)
            is_relevant = not asset_ids or position_id_str in asset_ids
            checked_positions.append({
                "index_set": index_set,
                "position_id": position_id_str,
                "balance": int(balance),
                "relevant": is_relevant,
            })
            if is_relevant and balance > 0:
                log.warning(
                    "Post-redemption balance check: holder=%s position_id=%s balance=%d "
                    "(condition=%s indexSet=%d) — relevant tokens remain after redemption",
                    token_holder, position_id, balance, condition_id_hex, index_set,
                )
                all_zero = False

        verified = all_zero and any(item["relevant"] for item in checked_positions)
        if verified:
            log.info(
                "Post-redemption balance check: relevant positions zero for condition=%s holder=%s",
                condition_id_hex, token_holder,
            )
        return {
            "verified": verified,
            "all_zero": all_zero,
            "checked_positions": checked_positions,
        }

    except Exception as exc:
        log.warning(
            "Post-redemption balance verification failed for condition=%s: %s",
            condition_id_hex, exc,
        )
        return {
            "verified": False,
            "all_zero": False,
            "checked_positions": [],
            "error": str(exc),
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
        "success": bool,                # always True in dry_run
        "tx_hash": str | None,          # None in dry_run
        "error": str | None,
        "gas_used": int | None,
        "dry_run": bool,
        "safe_exec": bool,
        "verified_zero_balance": bool,
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
                "safe_exec": False,
                "verified_zero_balance": False,
            })
            continue

        result = await redeem_position(
            pos["condition_id"],
            pos.get("collateral_token"),
            index_sets=pos.get("index_sets"),
            positions=pos.get("positions"),
        )
        results.append({
            **pos,
            **result,
            "dry_run": False,
        })

    return results
