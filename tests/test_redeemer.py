import asyncio
import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import config as cfg
from core import redeemer
from db import queries
from db.models import init_db, migrate_db


class _Call:
    def __init__(self, value):
        self._value = value

    def call(self):
        return self._value


class _Functions:
    def __init__(self, balances):
        self._balances = balances

    def getCollectionId(self, parent_collection_id, cid_bytes, index_set):
        return _Call(f"collection-{index_set}")

    def getPositionId(self, collateral, collection_id):
        index_set = int(str(collection_id).split('-')[-1])
        return _Call(self._balances[index_set]["position_id"])

    def balanceOf(self, token_holder, position_id):
        for item in self._balances.values():
            if item["position_id"] == position_id:
                return _Call(item["balance"])
        raise KeyError(position_id)


class _FakeCTF:
    def __init__(self, balances):
        self.functions = _Functions(balances)


def test_find_redeemable_positions_groups_by_condition_and_keeps_relevant_positions():
    condition = "0x" + "ab" * 32
    positions = [
        {
            "conditionId": condition,
            "size": "5",
            "redeemable": True,
            "curPrice": "1.0",
            "outcomeIndex": 0,
            "title": "Market A",
            "asset": "101",
        },
        {
            "conditionId": condition,
            "size": "4",
            "redeemable": True,
            "curPrice": "0.0",
            "outcomeIndex": 1,
            "title": "Market A",
            "asset": "202",
        },
    ]

    redeemable = redeemer.find_redeemable_positions(positions)

    assert len(redeemable) == 1
    item = redeemable[0]
    assert item["condition_id"] == condition
    assert item["position_count"] == 2
    assert item["index_sets"] == [1, 2]
    assert item["asset_ids"] == ["101", "202"]
    assert item["won_count"] == 1
    assert item["lost_count"] == 1


def test_verify_zero_balance_is_strict_about_relevant_tokens_only():
    ctf = _FakeCTF(
        {
            1: {"position_id": 101, "balance": 0},
            2: {"position_id": 202, "balance": 9},
            4: {"position_id": 303, "balance": 7},
        }
    )
    verification = redeemer._verify_zero_balance(
        ctf=ctf,
        token_holder="holder",
        collateral="collateral",
        parent_collection_id=b"\x00" * 32,
        cid_bytes=b"\x11" * 32,
        index_sets=[1, 2, 4],
        condition_id_hex="0x" + "11" * 32,
        positions=[
            {"index_set": 1, "asset_id": "101"},
            {"index_set": 2, "asset_id": "202"},
        ],
    )
    assert verification["verified"] is False
    assert verification["all_zero"] is False
    assert [p["relevant"] for p in verification["checked_positions"]] == [True, True]


def test_verify_zero_balance_ignores_unrelated_candidate_index_sets():
    ctf = _FakeCTF(
        {
            1: {"position_id": 101, "balance": 0},
            2: {"position_id": 202, "balance": 0},
            4: {"position_id": 303, "balance": 99},
        }
    )
    verification = redeemer._verify_zero_balance(
        ctf=ctf,
        token_holder="holder",
        collateral="collateral",
        parent_collection_id=b"\x00" * 32,
        cid_bytes=b"\x11" * 32,
        index_sets=[1, 2, 4],
        condition_id_hex="0x" + "11" * 32,
        positions=[
            {"index_set": 1, "asset_id": "101"},
            {"index_set": 2, "asset_id": "202"},
        ],
    )
    assert verification["verified"] is True
    assert verification["all_zero"] is True


def test_redemption_duplicate_prevention_blocks_pending_and_completed_attempts():
    async def _run():
        fd, path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        old = cfg.DB_PATH
        cfg.DB_PATH = path
        try:
            await init_db(path)
            await migrate_db(path)
            condition = "0xabc"
            assert await queries.redemption_already_recorded(condition, 0) is False
            await queries.insert_redemption(
                condition_id=condition,
                outcome_index=0,
                size=1.0,
                title="Test",
                tx_hash=None,
                status="failed",
                verified=False,
                attempt_state="failed",
            )
            assert await queries.redemption_already_recorded(condition, 0) is False
            await queries.insert_redemption(
                condition_id=condition,
                outcome_index=0,
                size=1.0,
                title="Test",
                tx_hash="0x1",
                status="success",
                verified=False,
                attempt_state="broadcast",
            )
            assert await queries.redemption_already_recorded(condition, 0) is True
        finally:
            cfg.DB_PATH = old
            try:
                os.remove(path)
            except FileNotFoundError:
                pass

    asyncio.run(_run())
