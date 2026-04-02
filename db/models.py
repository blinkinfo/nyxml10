"""SQLite schema initialisation -- creates tables and inserts default settings."""

import aiosqlite
import config as cfg

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    slot_start TEXT NOT NULL,
    slot_end TEXT NOT NULL,
    slot_timestamp INTEGER NOT NULL,
    side TEXT,
    entry_price REAL,
    opposite_price REAL,
    outcome TEXT,
    is_win INTEGER,
    resolved_at TIMESTAMP,
    skipped INTEGER DEFAULT 0,
    filter_blocked INTEGER DEFAULT 0,
    pattern TEXT
);

CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_id INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    slot_start TEXT NOT NULL,
    slot_end TEXT NOT NULL,
    side TEXT NOT NULL,
    entry_price REAL NOT NULL,
    amount_usdc REAL NOT NULL,
    order_id TEXT,
    fill_price REAL,
    status TEXT DEFAULT 'pending',
    outcome TEXT,
    is_win INTEGER,
    pnl REAL,
    resolved_at TIMESTAMP,
    retry_count INTEGER DEFAULT 0,
    last_retry_at TIMESTAMP,
    is_demo INTEGER DEFAULT 0,
    FOREIGN KEY (signal_id) REFERENCES signals(id)
);

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS redemptions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    condition_id TEXT NOT NULL,
    outcome_index INTEGER NOT NULL,
    size REAL NOT NULL,
    title TEXT,
    tx_hash TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    error TEXT,
    gas_used INTEGER,
    dry_run INTEGER NOT NULL DEFAULT 0,
    resolved_at TIMESTAMP
);
"""

DEFAULT_SETTINGS = {
    "autotrade_enabled": "false",
    "trade_amount_usdc": str(cfg.TRADE_AMOUNT_USDC),
    "auto_redeem_enabled": "false",
    "demo_trade_enabled": "false",
    "demo_bankroll_usdc": "1000.00",
}


async def init_db(db_path: str | None = None) -> None:
    """Create tables if they don't exist and seed default settings."""
    path = db_path or cfg.DB_PATH
    async with aiosqlite.connect(path) as db:
        await db.executescript(SCHEMA_SQL)
        for key, value in DEFAULT_SETTINGS.items():
            await db.execute(
                "INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)",
                (key, value),
            )
        await db.commit()


async def migrate_db(db_path: str | None = None) -> None:
    """Add new columns to existing tables if they don't exist (safe to run repeatedly)."""
    path = db_path or cfg.DB_PATH
    async with aiosqlite.connect(path) as db:
        # Check existing columns in trades table
        cursor = await db.execute("PRAGMA table_info(trades)")
        columns = {row[1] for row in await cursor.fetchall()}

        if "retry_count" not in columns:
            await db.execute("ALTER TABLE trades ADD COLUMN retry_count INTEGER DEFAULT 0")
        if "last_retry_at" not in columns:
            await db.execute("ALTER TABLE trades ADD COLUMN last_retry_at TIMESTAMP")
        if "is_demo" not in columns:
            await db.execute("ALTER TABLE trades ADD COLUMN is_demo INTEGER DEFAULT 0")

        # Check existing columns in signals table
        cursor2 = await db.execute("PRAGMA table_info(signals)")
        sig_columns = {row[1] for row in await cursor2.fetchall()}
        if "filter_blocked" not in sig_columns:
            await db.execute("ALTER TABLE signals ADD COLUMN filter_blocked INTEGER DEFAULT 0")
        if "pattern" not in sig_columns:
            await db.execute("ALTER TABLE signals ADD COLUMN pattern TEXT")

        # Seed any missing default settings (idempotent)
        for key, value in DEFAULT_SETTINGS.items():
            await db.execute(
                "INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)",
                (key, value),
            )

        await db.commit()
