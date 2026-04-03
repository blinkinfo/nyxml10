#!/usr/bin/env python3
"""One-shot utility: delete bad redemption DB records for the 4 conditions
whose transactions were broadcast from the wrong address (EOA instead of the
proxy wallet) due to the sig-type-2 bug.

The transactions were confirmed on-chain but the *from* address was wrong, so
no USDC was actually received.  Deleting these records allows the redeemer to
attempt them again on the next scan (now that redeemer.py uses
POLYMARKET_FUNDER_ADDRESS as the `from` address for sig-type-2).

Condition IDs extracted from deployment_logs_railway_1775229558726.log.txt:
  1. 0x46b556649c109de10c5be1be2dbc4ee3155909fee0d99230e17dbd51020fcb35
  2. 0x1b447392bdf148658a553757511a4a9320ec36486ac42727fbe7c93a192158ae
  3. 0x0fe4e91b6df78899d791e19fdf8176d8bcf242fde888190115fa66dc4b724d85
  4. 0x6daf71ed6a57d96e62563df405159ef67ccfcdd1206e8139ef417c03ba4b26c7

Usage:
    python reset_redemptions.py [--dry-run]
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

# Ensure project root is on sys.path when run directly.
sys.path.insert(0, os.path.dirname(__file__))

import aiosqlite
import config as cfg

# The 4 condition IDs that were incorrectly redeemed from the wrong address.
BAD_CONDITION_IDS = [
    "0x46b556649c109de10c5be1be2dbc4ee3155909fee0d99230e17dbd51020fcb35",
    "0x1b447392bdf148658a553757511a4a9320ec36486ac42727fbe7c93a192158ae",
    "0x0fe4e91b6df78899d791e19fdf8176d8bcf242fde888190115fa66dc4b724d85",
    "0x6daf71ed6a57d96e62563df405159ef67ccfcdd1206e8139ef417c03ba4b26c7",
]


async def show_records(db_path: str) -> None:
    """Print current redemption records for the bad condition IDs."""
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        for cid in BAD_CONDITION_IDS:
            cursor = await db.execute(
                "SELECT id, condition_id, status, tx_hash, dry_run, created_at "
                "FROM redemptions WHERE condition_id = ?",
                (cid,),
            )
            rows = await cursor.fetchall()
            if rows:
                for r in rows:
                    print(
                        f"  id={r['id']} status={r['status']} dry_run={r['dry_run']} "
                        f"tx={r['tx_hash']} created={r['created_at']}"
                    )
                    print(f"    condition={r['condition_id']}")
            else:
                print(f"  (no records) condition={cid}")


async def delete_records(db_path: str) -> int:
    """Delete all non-dry-run redemption records for the bad condition IDs.

    Returns total rows deleted.
    """
    total = 0
    async with aiosqlite.connect(db_path) as db:
        for cid in BAD_CONDITION_IDS:
            cursor = await db.execute(
                "DELETE FROM redemptions WHERE condition_id = ? AND dry_run = 0",
                (cid,),
            )
            deleted = cursor.rowcount
            total += deleted
            print(f"  Deleted {deleted} row(s) for condition {cid}")
        await db.commit()
    return total


async def main(dry_run: bool) -> None:
    db_path = cfg.DB_PATH
    print(f"Database: {db_path}")
    print()

    print("Current records for affected conditions:")
    await show_records(db_path)
    print()

    if dry_run:
        print("[DRY RUN] No changes made. Re-run without --dry-run to delete.")
        return

    print("Deleting bad redemption records...")
    total = await delete_records(db_path)
    print()
    print(f"Done. {total} total row(s) deleted.")
    print("The redeemer will attempt these conditions again on the next scan.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Delete bad redemption DB records.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deleted without making changes.",
    )
    args = parser.parse_args()
    asyncio.run(main(dry_run=args.dry_run))
