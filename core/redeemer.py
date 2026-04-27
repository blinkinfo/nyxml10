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
Primary USDC collateral:  0x3c499c542cef5e3811e1192ce70d8cc03d5c3359
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
USDC_ADDRESS = "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359"

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

def find_redeemable_positions(positions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Filter *positions* to those that can be redeemed right now.

    A position is redeemable when ALL of the following hold:
      1. size > 0.001  (we hold tokens)
      2. redeemable == True  (market is resolved, API already computed this)
      3. curPrice is settled: >= 0.99 (won) OR <= 0.01 (lost)
         Prices in-between mean the market is still live -- skip those.

    The on-chain redeemPositions() call with index_sets=[1, 2] handles both
    outcomes correctly: winning tokens are redeemed for the current Polymarket USDC collateral, losing tokens
    are burned for $0. Calling it for a lost position is safe and necessary
    to clear worthless ERC-1155 tokens from the wallet.

    API schema (flat Polymarket Data API /positions response):
      conditionId, size, curPrice, redeemable, outcomeIndex, outcome, title

    Returns a list of dicts with:
      condition_id, outcome_index, size, title, raw, cur_price, won
    The `won` field is True when curPrice >= 0.99, False when curPrice <= 0.01.
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

            # 3. Outcome must be settled: won (>=0.99) or lost (<=0.01).
            #    Prices between 0.01 and 0.99 mean the market is still live.
            cur_price = float(pos.get("curPrice") or 0)
            won = cur_price >= 0.99
            lost = cur_price <= 0.01
            if not (won or lost):
                continue

            # conditionId -- ensure 0x prefix
            condition_id = pos.get("conditionId", "")
            if not condition_id:
                continue
            if not condition_id.startswith("0x"):
                condition_id = "0x" + condition_id

            outcome_index = int(pos["outcomeIndex"])
            title = pos.get("title", condition_id[:16])

            redeemable.append({
                "condition_id":  condition_id,
                "outcome_index": outcome_index,
                "size":          size,
                "title":         title,
                "raw":           pos,
                "cur_price":     cur_price,
                "won":           won,
            })

        except Exception:
            log.exception("Error inspecting position %r", pos)
            continue

    return redeemable

async def redeem_position(
    condition_id_hex: str,
    collateral_token: str | None = None,
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
      ``verified_zero_balance`` bool  (True if post-tx balance confirmed zero)
    """
    return await asyncio.to_thread(
        _redeem_position_sync,
        condition_id_hex,
    )


def _redeem_position_sync(
    condition_id_hex: str,
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

        collateral = Web3.to_checksum_address(USDC_ADDRESS)

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
        verified_zero = _verify_zero_balance(
            ctf=ctf,
            token_holder=account,
            collateral=collateral,
            parent_collection_id=parent_collection_id,
            cid_bytes=cid_bytes,
            index_sets=index_sets,
            condition_id_hex=condition_id_hex,
        )

        return {
            "success": True,
            "tx_hash": tx_hash_hex,
            "error": None,
            "gas_used": gas_used,
            "safe_exec": False,
            "verified_zero_balance": verified_zero,
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

    # --- Phase 2: Post-tx balance verification (Safe as token holder) ---
    verified_zero = _verify_zero_balance(
        ctf=ctf,
        token_holder=safe_address,
        collateral=collateral,
        parent_collection_id=parent_collection_id,
        cid_bytes=cid_bytes,
        index_sets=index_sets,
        condition_id_hex=condition_id_hex,
    )

    return {
        "success": True,
        "tx_hash": tx_hash_hex,
        "error": None,
        "gas_used": gas_used,
        "safe_exec": True,
        "verified_zero_balance": verified_zero,
    }


def _verify_zero_balance(
    ctf,
    token_holder: str,
    collateral: str,
    parent_collection_id: bytes,
    cid_bytes: bytes,
    index_sets: list,
    condition_id_hex: str,
) -> bool:
    """Phase 2: Check that all position balances are zero after redemption.

    Returns True if all relevant position balances are confirmed zero.
    Returns False if any balance > 0 remains (partial/failed redemption) or
    if the check itself fails (logs a warning and returns False).
    """
    try:
        all_zero = True
        for index_set in index_sets:
            collection_id = ctf.functions.getCollectionId(
                parent_collection_id, cid_bytes, index_set
            ).call()
            position_id = ctf.functions.getPositionId(collateral, collection_id).call()
            balance = ctf.functions.balanceOf(token_holder, position_id).call()
            if balance > 0:
                log.warning(
                    "Post-redemption balance check: holder=%s position_id=%s balance=%d "
                    "(condition=%s indexSet=%d) — tokens remain after redemption",
                    token_holder, position_id, balance, condition_id_hex, index_set,
                )
                all_zero = False

        if all_zero:
            log.info(
                "Post-redemption balance check: all positions zero for condition=%s holder=%s",
                condition_id_hex, token_holder,
            )
        return all_zero

    except Exception as exc:
        log.warning(
            "Post-redemption balance verification failed for condition=%s: %s — proceeding",
            condition_id_hex, exc,
        )
        return False


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

        result = await redeem_position(pos["condition_id"], pos.get("collateral_token"))
        results.append({
            **pos,
            **result,
            "dry_run": False,
        })

    return results
