"""Model store — save/load/promote LightGBM models with metadata JSON."""

from __future__ import annotations

import json
import logging
import os
import shutil

import lightgbm as lgb

from ml.features import FEATURE_COLS

log = logging.getLogger(__name__)

_EXPECTED_NUM_FEATURES = len(FEATURE_COLS)


def _validate_feature_count(model: lgb.Booster, slot: str, source: str) -> lgb.Booster | None:
    """Return the model if its feature count matches FEATURE_COLS, else None."""
    n = model.num_feature()
    if n != _EXPECTED_NUM_FEATURES:
        log.warning(
            "%s: model slot=%s has %d features but current FEATURE_COLS expects %d "
            "— discarding stale model (signals will be skipped until retrain)",
            source, slot, n, _EXPECTED_NUM_FEATURES,
        )
        return None
    return model

MODEL_DIR = os.path.join(os.path.dirname(__file__), "..", "models")


def _ensure_dir():
    os.makedirs(MODEL_DIR, exist_ok=True)


def _model_path(slot: str) -> str:
    return os.path.join(MODEL_DIR, f"model_{slot}.lgb")


def _meta_path(slot: str) -> str:
    return os.path.join(MODEL_DIR, f"model_{slot}_meta.json")


def save_model(model: lgb.Booster, slot: str, metadata: dict) -> None:
    """Save model to models/model_{slot}.lgb and metadata JSON."""
    _ensure_dir()
    model_path = _model_path(slot)
    meta_path = _meta_path(slot)

    model.save_model(model_path)
    with open(meta_path, "w") as f:
        json.dump(metadata, f, indent=2)

    log.info("save_model: saved slot=%s path=%s", slot, model_path)


def load_model(slot: str = "current") -> lgb.Booster | None:
    """Load and return model. Returns None if file doesn't exist."""
    path = _model_path(slot)
    if not os.path.exists(path):
        log.debug("load_model: no model file at %s", path)
        return None
    try:
        model = lgb.Booster(model_file=path)
        log.info("load_model: loaded slot=%s", slot)
        return _validate_feature_count(model, slot, "load_model")
    except Exception as e:
        log.error("load_model: failed to load %s: %s", path, e)
        return None


def load_metadata(slot: str = "current") -> dict | None:
    """Load and return metadata JSON. Returns None if not found."""
    path = _meta_path(slot)
    if not os.path.exists(path):
        return None
    try:
        with open(path) as f:
            return json.load(f)
    except Exception as e:
        log.error("load_metadata: failed to load %s: %s", path, e)
        return None


def promote_candidate() -> None:
    """Copy candidate model files to current slot on disk (overwrites current).

    NOTE: This only updates disk files. Call promote_candidate_in_db() afterward
    to also persist the promotion to the database so it survives container restarts.
    """
    _ensure_dir()
    src_model = _model_path("candidate")
    src_meta = _meta_path("candidate")
    dst_model = _model_path("current")
    dst_meta = _meta_path("current")

    if not os.path.exists(src_model):
        raise FileNotFoundError(f"Candidate model not found: {src_model}")

    shutil.copy2(src_model, dst_model)
    if os.path.exists(src_meta):
        shutil.copy2(src_meta, dst_meta)

    log.info("promote_candidate: copied candidate -> current (disk only)")


async def promote_candidate_in_db() -> None:
    """Promote candidate model to current slot in the database.

    Reads the candidate blob from the DB and writes it as the 'current' slot.
    This must be called after promote_candidate() so the promoted model
    survives container restarts on ephemeral filesystems (e.g. Railway).

    Raises KeyError if no candidate blob exists in the DB.
    """
    import aiosqlite
    import config as cfg

    async with aiosqlite.connect(cfg.DB_PATH) as db:
        cursor = await db.execute(
            "SELECT blob, metadata FROM model_blobs WHERE slot = ?", ("candidate",)
        )
        row = await cursor.fetchone()

    if not row:
        raise KeyError("No candidate model found in DB — cannot promote to current")

    blob, meta_json = row
    async with aiosqlite.connect(cfg.DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO model_blobs (slot, blob, metadata)
            VALUES (?, ?, ?)
            ON CONFLICT(slot) DO UPDATE SET
                blob=excluded.blob,
                metadata=excluded.metadata,
                updated_at=CURRENT_TIMESTAMP
            """,
            ("current", blob, meta_json),
        )
        await db.commit()

    log.info("promote_candidate_in_db: candidate promoted to current in DB (%d bytes)", len(blob))


def has_model(slot: str = "current") -> bool:
    """Return True if model file exists for the given slot."""
    return os.path.exists(_model_path(slot))


def delete_model(slot: str) -> None:
    """Delete model file and metadata for the given slot. Safe to call if missing."""
    for path in (_model_path(slot), _meta_path(slot)):
        try:
            os.remove(path)
            log.info("delete_model: removed %s", path)
        except FileNotFoundError:
            pass


async def save_model_to_db(model: lgb.Booster, slot: str, metadata: dict) -> None:
    """Serialize model to bytes and upsert into model_blobs table in SQLite."""
    import tempfile
    import os
    import json
    import aiosqlite
    import config as cfg
    with tempfile.NamedTemporaryFile(suffix=".lgb", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        model.save_model(tmp_path)
        with open(tmp_path, "rb") as f:
            blob = f.read()
    finally:
        os.unlink(tmp_path)
    meta_json = json.dumps(metadata)
    async with aiosqlite.connect(cfg.DB_PATH) as db:
        await db.execute("""
            INSERT INTO model_blobs (slot, blob, metadata)
            VALUES (?, ?, ?)
            ON CONFLICT(slot) DO UPDATE SET
                blob=excluded.blob,
                metadata=excluded.metadata,
                updated_at=CURRENT_TIMESTAMP
        """, (slot, blob, meta_json))
        await db.commit()
    log.info("save_model_to_db: saved slot=%s (%d bytes)", slot, len(blob))


def patch_metadata(slot: str, updates: dict) -> None:
    """Merge *updates* into the on-disk metadata JSON for *slot*.

    Useful for back-filling fields (e.g. ``down_override``) into an already-
    saved metadata file without re-saving the entire model.  No-op if the
    metadata file does not exist yet.
    """
    path = _meta_path(slot)
    if not os.path.exists(path):
        log.debug("patch_metadata: no metadata file for slot=%s, skipping", slot)
        return
    try:
        with open(path) as f:
            meta = json.load(f)
    except Exception as e:
        log.error("patch_metadata: failed to read %s: %s", path, e)
        return
    meta.update(updates)
    try:
        with open(path, "w") as f:
            json.dump(meta, f, indent=2)
        log.info("patch_metadata: patched slot=%s keys=%s", slot, list(updates.keys()))
    except Exception as e:
        log.error("patch_metadata: failed to write %s: %s", path, e)


async def load_model_from_db(slot: str = "current") -> "lgb.Booster | None":
    """Load model blob from SQLite and write to temp disk path for LightGBM to load."""
    import tempfile
    import os
    import aiosqlite
    import config as cfg
    async with aiosqlite.connect(cfg.DB_PATH) as db:
        cursor = await db.execute(
            "SELECT blob FROM model_blobs WHERE slot = ?", (slot,)
        )
        row = await cursor.fetchone()
    if not row:
        log.info("load_model_from_db: no blob found for slot=%s", slot)
        return None
    blob = row[0]
    with tempfile.NamedTemporaryFile(suffix=".lgb", delete=False) as tmp:
        tmp.write(blob)
        tmp_path = tmp.name
    try:
        model = lgb.Booster(model_file=tmp_path)
        log.info("load_model_from_db: loaded slot=%s (%d bytes)", slot, len(blob))
        return _validate_feature_count(model, slot, "load_model_from_db")
    except Exception as e:
        log.error("load_model_from_db: failed to load slot=%s: %s", slot, e)
        return None
    finally:
        os.unlink(tmp_path)
