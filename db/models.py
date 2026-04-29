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
    pattern TEXT,
    ml_p_up REAL,
    ml_p_down REAL,
    ml_probability_bucket TEXT,
    ml_probability_used REAL,
    threshold_policy_real TEXT,
    threshold_policy_demo TEXT,
    rolling_wr_policy TEXT,
    rolling_wr_wr REAL,
    rolling_wr_window_size INTEGER,
    rolling_wr_sample_size INTEGER,
    rolling_wr_follow_below REAL,
    rolling_wr_invert_above REAL,
    rolling_wr_ready INTEGER DEFAULT 0,
    rolling_wr_source TEXT,
    model_side TEXT,
    signal_slug TEXT
);

CREATE TABLE IF NOT EXISTS rolling_wr_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_id INTEGER,
    slot_timestamp INTEGER NOT NULL,
    slot_start TEXT,
    signal_slug TEXT,
    original_side TEXT NOT NULL,
    winner_side TEXT NOT NULL,
    is_correct INTEGER NOT NULL,
    source TEXT NOT NULL DEFAULT 'live',
    imported_batch_id INTEGER,
    imported_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (signal_id) REFERENCES signals(id),
    FOREIGN KEY (imported_batch_id) REFERENCES rolling_wr_import_batches(id)
);

CREATE TABLE IF NOT EXISTS rolling_wr_import_batches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_filename TEXT,
    replace_existing INTEGER NOT NULL DEFAULT 0,
    row_count INTEGER NOT NULL DEFAULT 0,
    window_size_hint INTEGER,
    notes TEXT
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
    routing_mode TEXT,
    routing_policy TEXT,
    original_side TEXT,
    routed_side TEXT,
    policy_bucket TEXT,
    policy_probability REAL,
    rolling_wr_policy TEXT,
    rolling_wr_wr REAL,
    rolling_wr_window_size INTEGER,
    rolling_wr_sample_size INTEGER,
    rolling_wr_ready INTEGER DEFAULT 0,
    signal_outcome_recorded INTEGER DEFAULT 0,
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
    outcome_index INTEGER,
    size REAL NOT NULL,
    title TEXT,
    tx_hash TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    error TEXT,
    gas_used INTEGER,
    dry_run INTEGER NOT NULL DEFAULT 0,
    resolved_at TIMESTAMP,
    verified INTEGER NOT NULL DEFAULT 0,
    verified_at TIMESTAMP,
    redemption_key TEXT,
    attempt_state TEXT NOT NULL DEFAULT 'pending'
);

CREATE TABLE IF NOT EXISTS ml_config (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS threshold_policies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    probability_bucket TEXT NOT NULL,
    mode TEXT NOT NULL,
    policy TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(probability_bucket, mode)
);

CREATE TABLE IF NOT EXISTS model_registry (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    slot TEXT NOT NULL,
    train_date TEXT,
    wr REAL,
    precision_score REAL,
    trades_per_day REAL,
    threshold REAL,
    sample_count INTEGER,
    path TEXT,
    metadata TEXT
);

CREATE TABLE IF NOT EXISTS model_blobs (
    slot TEXT PRIMARY KEY,
    blob BLOB NOT NULL,
    metadata TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_threshold_policies_mode_bucket
ON threshold_policies (mode, probability_bucket);

CREATE INDEX IF NOT EXISTS idx_trades_signal_id ON trades (signal_id);
CREATE INDEX IF NOT EXISTS idx_trades_policy_mode_bucket
ON trades (is_demo, policy_bucket);
CREATE INDEX IF NOT EXISTS idx_trades_signal_outcome_recorded
ON trades (signal_id, signal_outcome_recorded);
CREATE INDEX IF NOT EXISTS idx_rolling_wr_history_slot_timestamp
ON rolling_wr_history (slot_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_rolling_wr_history_signal_id
ON rolling_wr_history (signal_id);
CREATE INDEX IF NOT EXISTS idx_rolling_wr_history_batch_id
ON rolling_wr_history (imported_batch_id);
"""

DEFAULT_SETTINGS = {
    "autotrade_enabled": "false",
    "trade_amount_usdc": str(cfg.TRADE_AMOUNT_USDC),
    "trade_mode": cfg.TRADE_MODE,
    "trade_pct": str(cfg.TRADE_PCT),
    "auto_redeem_enabled": "false",
    "demo_trade_enabled": "false",
    "demo_bankroll_usdc": "1000.00",
    "invert_trades_enabled": "false",
    "ml_volatility_gate_enabled": "true",
    "rolling_wr_enabled": "true",
    "rolling_wr_window_size": "320",
    "rolling_wr_follow_below": "49.0",
    "rolling_wr_invert_above": "51.0",
    "rolling_wr_min_samples": "320",
    "rolling_wr_skip_when_unready": "true",
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
        await db.execute(
            "INSERT OR IGNORE INTO ml_config (key, value) VALUES ('ml_threshold', '0.53')"
        )
        await db.execute(
            "INSERT OR IGNORE INTO ml_config (key, value) VALUES ('ml_down_threshold', '0.47')"
        )
        await db.commit()


_BAD_CONDITION_IDS = [
    "0x46b556649c109de10c5be1be2dbc4ee3155909fee0d99230e17dbd51020fcb35",
    "0x1b447392bdf148658a553757511a4a9320ec36486ac42727fbe7c93a192158ae",
    "0x0fe4e91b6df78899d791e19fdf8176d8bcf242fde888190115fa66dc4b724d85",
    "0x6daf71ed6a57d96e62563df405159ef67ccfcdd1206e8139ef417c03ba4b26c7",
]


async def cleanup_bad_redemptions(db_path: str | None = None) -> int:
    path = db_path or cfg.DB_PATH
    total = 0
    async with aiosqlite.connect(path) as db:
        for cid in _BAD_CONDITION_IDS:
            cursor = await db.execute(
                "DELETE FROM redemptions WHERE condition_id = ? AND dry_run = 0",
                (cid,),
            )
            total += cursor.rowcount
        await db.commit()
    return total


async def migrate_db(db_path: str | None = None) -> None:
    """Add new columns/tables if they don't exist (safe to run repeatedly)."""
    import logging
    log = logging.getLogger(__name__)
    path = db_path or cfg.DB_PATH

    async with aiosqlite.connect(path) as db:
        try:
            cursor = await db.execute("PRAGMA table_info(trades)")
            columns = {row[1] for row in await cursor.fetchall()}
            if "retry_count" not in columns:
                await db.execute("ALTER TABLE trades ADD COLUMN retry_count INTEGER DEFAULT 0")
            if "last_retry_at" not in columns:
                await db.execute("ALTER TABLE trades ADD COLUMN last_retry_at TIMESTAMP")
            if "is_demo" not in columns:
                await db.execute("ALTER TABLE trades ADD COLUMN is_demo INTEGER DEFAULT 0")
            if "routing_mode" not in columns:
                await db.execute("ALTER TABLE trades ADD COLUMN routing_mode TEXT")
            if "routing_policy" not in columns:
                await db.execute("ALTER TABLE trades ADD COLUMN routing_policy TEXT")
            if "original_side" not in columns:
                await db.execute("ALTER TABLE trades ADD COLUMN original_side TEXT")
            if "routed_side" not in columns:
                await db.execute("ALTER TABLE trades ADD COLUMN routed_side TEXT")
            if "policy_bucket" not in columns:
                await db.execute("ALTER TABLE trades ADD COLUMN policy_bucket TEXT")
            if "policy_probability" not in columns:
                await db.execute("ALTER TABLE trades ADD COLUMN policy_probability REAL")
            if "rolling_wr_policy" not in columns:
                await db.execute("ALTER TABLE trades ADD COLUMN rolling_wr_policy TEXT")
            if "rolling_wr_wr" not in columns:
                await db.execute("ALTER TABLE trades ADD COLUMN rolling_wr_wr REAL")
            if "rolling_wr_window_size" not in columns:
                await db.execute("ALTER TABLE trades ADD COLUMN rolling_wr_window_size INTEGER")
            if "rolling_wr_sample_size" not in columns:
                await db.execute("ALTER TABLE trades ADD COLUMN rolling_wr_sample_size INTEGER")
            if "rolling_wr_ready" not in columns:
                await db.execute("ALTER TABLE trades ADD COLUMN rolling_wr_ready INTEGER DEFAULT 0")
            if "signal_outcome_recorded" not in columns:
                await db.execute("ALTER TABLE trades ADD COLUMN signal_outcome_recorded INTEGER DEFAULT 0")
        except Exception as e:
            log.warning("migrate_db: trades column migration failed: %s", e)

        try:
            cursor2 = await db.execute("PRAGMA table_info(signals)")
            sig_columns = {row[1] for row in await cursor2.fetchall()}
            if "filter_blocked" not in sig_columns:
                await db.execute("ALTER TABLE signals ADD COLUMN filter_blocked INTEGER DEFAULT 0")
            if "pattern" not in sig_columns:
                await db.execute("ALTER TABLE signals ADD COLUMN pattern TEXT")
            if "ml_p_up" not in sig_columns:
                await db.execute("ALTER TABLE signals ADD COLUMN ml_p_up REAL")
            if "ml_p_down" not in sig_columns:
                await db.execute("ALTER TABLE signals ADD COLUMN ml_p_down REAL")
            if "ml_probability_bucket" not in sig_columns:
                await db.execute("ALTER TABLE signals ADD COLUMN ml_probability_bucket TEXT")
            if "ml_probability_used" not in sig_columns:
                await db.execute("ALTER TABLE signals ADD COLUMN ml_probability_used REAL")
            if "threshold_policy_real" not in sig_columns:
                await db.execute("ALTER TABLE signals ADD COLUMN threshold_policy_real TEXT")
            if "threshold_policy_demo" not in sig_columns:
                await db.execute("ALTER TABLE signals ADD COLUMN threshold_policy_demo TEXT")
            if "rolling_wr_policy" not in sig_columns:
                await db.execute("ALTER TABLE signals ADD COLUMN rolling_wr_policy TEXT")
            if "rolling_wr_wr" not in sig_columns:
                await db.execute("ALTER TABLE signals ADD COLUMN rolling_wr_wr REAL")
            if "rolling_wr_window_size" not in sig_columns:
                await db.execute("ALTER TABLE signals ADD COLUMN rolling_wr_window_size INTEGER")
            if "rolling_wr_sample_size" not in sig_columns:
                await db.execute("ALTER TABLE signals ADD COLUMN rolling_wr_sample_size INTEGER")
            if "rolling_wr_follow_below" not in sig_columns:
                await db.execute("ALTER TABLE signals ADD COLUMN rolling_wr_follow_below REAL")
            if "rolling_wr_invert_above" not in sig_columns:
                await db.execute("ALTER TABLE signals ADD COLUMN rolling_wr_invert_above REAL")
            if "rolling_wr_ready" not in sig_columns:
                await db.execute("ALTER TABLE signals ADD COLUMN rolling_wr_ready INTEGER DEFAULT 0")
            if "rolling_wr_source" not in sig_columns:
                await db.execute("ALTER TABLE signals ADD COLUMN rolling_wr_source TEXT")
            if "model_side" not in sig_columns:
                await db.execute("ALTER TABLE signals ADD COLUMN model_side TEXT")
            if "signal_slug" not in sig_columns:
                await db.execute("ALTER TABLE signals ADD COLUMN signal_slug TEXT")
        except Exception as e:
            log.warning("migrate_db: signals column migration failed: %s", e)

        try:
            cursor3 = await db.execute("PRAGMA table_info(redemptions)")
            red_columns = {row[1] for row in await cursor3.fetchall()}
            if "verified" not in red_columns:
                await db.execute(
                    "ALTER TABLE redemptions ADD COLUMN verified INTEGER NOT NULL DEFAULT 0"
                )
            if "verified_at" not in red_columns:
                await db.execute(
                    "ALTER TABLE redemptions ADD COLUMN verified_at TIMESTAMP"
                )
            if "redemption_key" not in red_columns:
                await db.execute(
                    "ALTER TABLE redemptions ADD COLUMN redemption_key TEXT"
                )
            if "attempt_state" not in red_columns:
                await db.execute(
                    "ALTER TABLE redemptions ADD COLUMN attempt_state TEXT NOT NULL DEFAULT 'pending'"
                )
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_redemptions_key_attempt_state ON redemptions (redemption_key, dry_run, attempt_state, verified, status)"
            )
        except Exception as e:
            log.warning("migrate_db: redemptions column migration failed: %s", e)

        try:
            await db.execute(
                "CREATE TABLE IF NOT EXISTS ml_config (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
            )
        except Exception as e:
            log.warning("migrate_db: ml_config table creation failed: %s", e)

        try:
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS threshold_policies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    probability_bucket TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    policy TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(probability_bucket, mode)
                )
                """
            )
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_threshold_policies_mode_bucket "
                "ON threshold_policies (mode, probability_bucket)"
            )
        except Exception as e:
            log.warning("migrate_db: threshold_policies migration failed: %s", e)

        try:
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_trades_signal_id ON trades (signal_id)"
            )
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_trades_policy_mode_bucket ON trades (is_demo, policy_bucket)"
            )
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_trades_signal_outcome_recorded ON trades (signal_id, signal_outcome_recorded)"
            )
            await db.execute(
                "CREATE TABLE IF NOT EXISTS rolling_wr_import_batches (id INTEGER PRIMARY KEY AUTOINCREMENT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, source_filename TEXT, replace_existing INTEGER NOT NULL DEFAULT 0, row_count INTEGER NOT NULL DEFAULT 0, window_size_hint INTEGER, notes TEXT)"
            )
            await db.execute(
                "CREATE TABLE IF NOT EXISTS rolling_wr_history (id INTEGER PRIMARY KEY AUTOINCREMENT, signal_id INTEGER, slot_timestamp INTEGER NOT NULL, slot_start TEXT, signal_slug TEXT, original_side TEXT NOT NULL, winner_side TEXT NOT NULL, is_correct INTEGER NOT NULL, source TEXT NOT NULL DEFAULT 'live', imported_batch_id INTEGER, imported_at TIMESTAMP, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY (signal_id) REFERENCES signals(id), FOREIGN KEY (imported_batch_id) REFERENCES rolling_wr_import_batches(id))"
            )
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_rolling_wr_history_slot_timestamp ON rolling_wr_history (slot_timestamp DESC)"
            )
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_rolling_wr_history_signal_id ON rolling_wr_history (signal_id)"
            )
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_rolling_wr_history_batch_id ON rolling_wr_history (imported_batch_id)"
            )
        except Exception as e:
            log.warning("migrate_db: trades indexes migration failed: %s", e)

        try:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS model_registry (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    slot TEXT NOT NULL,
                    train_date TEXT,
                    wr REAL,
                    precision_score REAL,
                    trades_per_day REAL,
                    threshold REAL,
                    sample_count INTEGER,
                    path TEXT,
                    metadata TEXT
                )
            """)
        except Exception as e:
            log.warning("migrate_db: model_registry table creation failed: %s", e)

        try:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS model_blobs (
                    slot TEXT PRIMARY KEY,
                    blob BLOB NOT NULL,
                    metadata TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
        except Exception as e:
            log.warning("migrate_db: model_blobs table creation failed: %s", e)

        try:
            await db.execute(
                "INSERT OR IGNORE INTO ml_config (key, value) VALUES ('ml_threshold', '0.56')"
            )
        except Exception as e:
            log.warning("migrate_db: ml_threshold seed failed: %s", e)

        try:
            await db.execute(
                "INSERT OR IGNORE INTO ml_config (key, value) VALUES ('ml_down_threshold', '0.44')"
            )
        except Exception as e:
            log.warning("migrate_db: ml_down_threshold seed failed: %s", e)

        try:
            default_ranges = ",".join(
                f"{lo:.2f}-{hi:.2f}" for lo, hi in getattr(cfg, "BLOCKED_THRESHOLD_RANGES", [(0.20, 0.22)])
            )
            await db.execute(
                "INSERT OR IGNORE INTO ml_config (key, value) VALUES ('blocked_threshold_ranges', ?)",
                (default_ranges,),
            )
        except Exception as e:
            log.warning("migrate_db: blocked_threshold_ranges seed failed: %s", e)

        for key, value in DEFAULT_SETTINGS.items():
            try:
                await db.execute(
                    "INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)",
                    (key, value),
                )
            except Exception as e:
                log.warning("migrate_db: settings seed failed for key=%s: %s", key, e)

        await db.commit()
