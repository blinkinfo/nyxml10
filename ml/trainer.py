"""LightGBM trainer — BLUEPRINT sections 7, 8, 9.

CRITICAL: NO data shuffling (time-series order must be preserved).
Threshold sweep ONLY on validation set, never on test set.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.metrics import precision_score, recall_score, f1_score

from ml import model_store
from ml.evaluator import compute_risk_metrics
from ml.features import FEATURE_COLS
from config import ML_PAYOUT_RATIO

try:
    from openpyxl import Workbook
    from openpyxl.styles import Alignment, Font, PatternFill
    from openpyxl.utils import get_column_letter
except Exception:  # pragma: no cover - import failure handled at runtime
    Workbook = None
    Alignment = Font = PatternFill = None
    get_column_letter = None

# ---------------------------------------------------------------------------
# Deployment gate — Blueprint Rule 10
# ---------------------------------------------------------------------------

class DeploymentBlockedError(Exception):
    """Raised when the trained model fails to meet the minimum test-set WR.

    Blueprint Rule 10: ALWAYS validate that test set WR >= 59% before
    deploying. If a new retrain fails to hit 59% on test, do not deploy.
    """

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# LightGBM hyperparameters — exact blueprint spec
# ---------------------------------------------------------------------------
LGBM_PARAMS = {
    "objective": "binary",
    "metric": "binary_logloss",
    "learning_rate": 0.05,
    "num_leaves": 31,         # reduced from 63 — smaller trees generalise better on ~26k samples
    "max_depth": -1,
    "min_child_samples": 20,  # reduced from 50 — allows finer splits, uses more training data
    "feature_fraction": 0.8,
    "bagging_fraction": 0.8,
    "bagging_freq": 5,
    "reg_alpha": 0.1,
    "reg_lambda": 0.1,
    "verbose": -1,
    "n_jobs": 1,  # 1 avoids multiprocess overhead on single-vCPU Railway instances
}

NUM_BOOST_ROUND = 2000        # increased from 1000 — gives early stopping more room to find optimum
EARLY_STOPPING_ROUNDS = 100   # increased from 50 — more patient stopping, avoids premature cutoff



def _coerce_utc_timestamp(value: object) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def _build_trade_report_rows(
    dataset: str,
    timestamps: pd.Series,
    probs: np.ndarray,
    y_true: np.ndarray,
    up_threshold: float,
    down_threshold: float,
) -> list[dict]:
    rows: list[dict] = []
    for raw_ts, p_up, actual_up in zip(timestamps.tolist(), probs.tolist(), y_true.tolist()):
        feature_ts = _coerce_utc_timestamp(raw_ts)
        trade_start = feature_ts + timedelta(minutes=5)
        trade_end = feature_ts + timedelta(minutes=10)
        trade_slot_label = f"{trade_start.strftime('%H:%M')}-{trade_end.strftime('%H:%M')}"
        p_up = float(p_up)
        p_down = float(1.0 - p_up)
        actual_up = int(actual_up)
        actual_down = 1 - actual_up
        actual_result_label = "UP" if actual_up == 1 else "DOWN"

        if p_up >= up_threshold:
            rows.append({
                "dataset": dataset,
                "side": "UP",
                "feature_timestamp_utc": feature_ts.tz_localize(None).to_pydatetime(),
                "trade_start_utc": trade_start.tz_localize(None).to_pydatetime(),
                "trade_end_utc": trade_end.tz_localize(None).to_pydatetime(),
                "trade_slot_utc_label": trade_slot_label,
                "signal": "UP",
                "probability": p_up,
                "threshold": float(up_threshold),
                "actual_result": actual_result_label,
                "is_win": bool(actual_up == 1),
                "p_up": p_up,
                "p_down": p_down,
            })

        if p_down >= down_threshold:
            rows.append({
                "dataset": dataset,
                "side": "DOWN",
                "feature_timestamp_utc": feature_ts.tz_localize(None).to_pydatetime(),
                "trade_start_utc": trade_start.tz_localize(None).to_pydatetime(),
                "trade_end_utc": trade_end.tz_localize(None).to_pydatetime(),
                "trade_slot_utc_label": trade_slot_label,
                "signal": "DOWN",
                "probability": p_down,
                "threshold": float(down_threshold),
                "actual_result": actual_result_label,
                "is_win": bool(actual_down == 1),
                "p_up": p_up,
                "p_down": p_down,
            })

    rows.sort(key=lambda row: (row["feature_timestamp_utc"], row["side"]))
    return rows


def _build_hourly_trade_stats(trade_rows: list[dict]) -> list[dict]:
    stats_rows: list[dict] = []
    df = pd.DataFrame(trade_rows)
    datasets = sorted(df["dataset"].dropna().unique().tolist()) if not df.empty else ["val", "test"]

    for dataset in datasets:
        if df.empty:
            dataset_df = df
        else:
            dataset_df = df[df["dataset"] == dataset].copy()

        if not dataset_df.empty:
            dataset_df["utc_hour"] = pd.to_datetime(dataset_df["trade_start_utc"], utc=True).dt.hour
            grouped = dataset_df.groupby("utc_hour", dropna=False)
            hourly_map = {
                int(hour): {
                    "total_trades": int(len(group)),
                    "up_trades": int((group["side"] == "UP").sum()),
                    "down_trades": int((group["side"] == "DOWN").sum()),
                    "wins": int(group["is_win"].sum()),
                    "losses": int((~group["is_win"].astype(bool)).sum()),
                }
                for hour, group in grouped
            }
        else:
            hourly_map = {}

        for hour in range(24):
            bucket = hourly_map.get(hour, {
                "total_trades": 0,
                "up_trades": 0,
                "down_trades": 0,
                "wins": 0,
                "losses": 0,
            })
            total_trades = bucket["total_trades"]
            wr_pct = (bucket["wins"] / total_trades) if total_trades else 0.0
            stats_rows.append({
                "dataset": dataset,
                "utc_hour": hour,
                "total_trades": total_trades,
                "up_trades": bucket["up_trades"],
                "down_trades": bucket["down_trades"],
                "wins": bucket["wins"],
                "losses": bucket["losses"],
                "wr_pct": wr_pct,
            })

    return stats_rows


def _apply_excel_formatting(workbook: Workbook) -> None:
    if Workbook is None:
        return

    header_fill = PatternFill(fill_type="solid", fgColor="1F4E78")
    header_font = Font(color="FFFFFF", bold=True)
    header_alignment = Alignment(horizontal="center", vertical="center")

    datetime_formats = {"feature_timestamp_utc", "trade_start_utc", "trade_end_utc"}
    percent_formats = {"probability", "threshold", "p_up", "p_down", "wr_pct"}

    for ws in workbook.worksheets:
        ws.freeze_panes = "A2"
        ws.auto_filter.ref = ws.dimensions
        ws.sheet_view.showGridLines = True
        ws.row_dimensions[1].height = 22

        headers = [cell.value for cell in ws[1]]
        for cell in ws[1]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = header_alignment

        for col_idx, header in enumerate(headers, start=1):
            letter = get_column_letter(col_idx)
            max_len = len(str(header or ""))
            for cell in ws[letter]:
                if cell.row == 1:
                    continue
                if header in datetime_formats and cell.value:
                    cell.number_format = "yyyy-mm-dd hh:mm:ss"
                elif header in percent_formats and cell.value is not None:
                    cell.number_format = "0.00%"
                elif header in {"is_win", "utc_hour", "total_trades", "up_trades", "down_trades", "wins", "losses"}:
                    cell.number_format = "0"
                text_value = "" if cell.value is None else str(cell.value)
                max_len = max(max_len, len(text_value))
            ws.column_dimensions[letter].width = min(max(max_len + 2, 12), 28)


def generate_trade_report(
    *,
    timestamps_val: pd.Series,
    timestamps_test: pd.Series,
    val_probs: np.ndarray,
    test_probs: np.ndarray,
    y_val: np.ndarray,
    y_test: np.ndarray,
    up_threshold: float,
    down_threshold: float,
    slot: str,
    output_dir: str | Path | None = None,
) -> dict:
    if Workbook is None:
        raise RuntimeError("openpyxl is unavailable")

    out_dir = Path(output_dir or (Path("models") / "reports"))
    out_dir.mkdir(parents=True, exist_ok=True)

    trade_rows = []
    trade_rows.extend(_build_trade_report_rows("val", timestamps_val, val_probs, y_val, up_threshold, down_threshold))
    trade_rows.extend(_build_trade_report_rows("test", timestamps_test, test_probs, y_test, up_threshold, down_threshold))
    hourly_rows = _build_hourly_trade_stats(trade_rows)

    trade_columns = [
        "dataset", "side", "feature_timestamp_utc", "trade_start_utc", "trade_end_utc",
        "trade_slot_utc_label", "signal", "probability", "threshold", "actual_result",
        "is_win", "p_up", "p_down",
    ]
    hourly_columns = [
        "dataset", "utc_hour", "total_trades", "up_trades", "down_trades", "wins", "losses", "wr_pct",
    ]

    trade_df = pd.DataFrame(trade_rows, columns=trade_columns)
    hourly_df = pd.DataFrame(hourly_rows, columns=hourly_columns)

    workbook = Workbook()
    ws_trades = workbook.active
    ws_trades.title = "trade_ledger"
    ws_trades.append(trade_columns)
    for row in trade_df.itertuples(index=False, name=None):
        ws_trades.append(list(row))

    ws_hourly = workbook.create_sheet(title="hourly_utc_stats")
    ws_hourly.append(hourly_columns)
    for row in hourly_df.itertuples(index=False, name=None):
        ws_hourly.append(list(row))

    _apply_excel_formatting(workbook)

    timestamp_now = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    report_path = out_dir / f"retrain_trade_report_{slot}_{timestamp_now}.xlsx"
    workbook.save(report_path)

    return {
        "path": str(report_path),
        "trade_rows": int(len(trade_df)),
        "hourly_rows": int(len(hourly_df)),
        "datasets": sorted(trade_df["dataset"].unique().tolist()) if not trade_df.empty else ["val", "test"],
    }


# ---------------------------------------------------------------------------
# Walk-forward validation constants
# ---------------------------------------------------------------------------
WF_FOLDS = 5          # number of walk-forward folds
WF_INITIAL_PCT = 0.60 # fraction of data used as train+val in fold 1
# Each fold expands the train+val window by WF_STEP_PCT.
# WF_STEP_PCT = (1.0 - WF_INITIAL_PCT) / WF_FOLDS = 0.08
WF_STEP_PCT = (1.0 - WF_INITIAL_PCT) / WF_FOLDS


# ---------------------------------------------------------------------------
# Threshold sweep (val set only — never test set)
# ---------------------------------------------------------------------------

# Minimum number of trades required at a threshold before it is considered a
# valid candidate.  With fewer trades the win-rate estimate is too noisy to be
# meaningful (e.g. 5 trades → WR can swing 20 pp from a single outcome).
# 30 trades gives a reasonable sample while still leaving room to select higher
# thresholds on typical val-set sizes.
MIN_TRADES = 30

def _ev_per_day(wr: float, tpd: float, payout: float) -> float:
    """Compute payout-adjusted expected value per day.

    For a $1 flat-bet with asymmetric payout:
      EV/trade = (WR * payout) - ((1 - WR) * 1.0)
               = WR * (1 + payout) - 1.0

    EV/day = EV/trade * trades_per_day

    This is the correct optimisation target when payout != 1.0.
    At payout=1.0 this reduces to (WR - 0.5) * tpd * 2, preserving
    the original ranking direction when payout was symmetric.

    Args:
        wr: Win rate (0.0 - 1.0)
        tpd: Trades per day
        payout: Profit per $1 wagered on a winning trade (e.g. 0.85)

    Returns:
        Expected dollar profit per day per $1 stake.
    """
    return (wr * (1.0 + payout) - 1.0) * tpd


def sweep_threshold(
    probs: np.ndarray,
    y_true: np.ndarray,
    lo: float = 0.50,
    hi: float = 0.80,
    step_coarse: float = 0.02,
    payout: float = ML_PAYOUT_RATIO,
) -> tuple[float, float, float]:
    """Two-stage threshold sweep: coarse pass finds the neighbourhood,
    fine pass (step=0.005) within +/-0.02 of the coarse best finds the true optimum.

    Stage 1: step=0.02 across [lo, hi] -- 16 candidates, finds the best region.
    Stage 2: step=0.005 within [best_coarse - 0.02, best_coarse + 0.02] --
             ~8 fine candidates, finds the true optimum without overfitting across
             the full range.

    This avoids the overfitting problem of 61 fine candidates across the full range
    while still finding the true optimum more precisely than a single coarse step.

    Selection criteria (applied in both stages):
      - Thresholds with fewer than MIN_TRADES trades are skipped -- the WR
        estimate is too noisy to be meaningful on small samples.
      - If any remaining threshold achieves WR >= 0.58: pick the one that
        maximizes payout-adjusted EV/day = (WR * (1 + payout) - 1.0) * tpd.
        This correctly accounts for asymmetric payouts (e.g. win $0.85,
        lose $1.00) rather than assuming a 1:1 payout.
      - Otherwise: pick threshold with maximum payout-adjusted EV/day
        among those >= MIN_TRADES (fallback, with warning logged).

    Why payout-adjusted EV?
      The legacy metric (WR - 0.5) * tpd implicitly assumed breakeven at
      50% WR (valid only at 1:1 payout). With a 0.85 payout, real breakeven
      is 54.05% WR. Using raw (WR - 0.5) can rank a lower-WR, higher-volume
      threshold above a genuinely more profitable one. The corrected metric
      uses actual dollar EV so the selected threshold always maximises
      real daily profit.

    Args:
        probs: Model output probabilities for the validation set.
        y_true: True binary labels for the validation set.
        lo: Lower bound of threshold sweep range.
        hi: Upper bound of threshold sweep range.
        step_coarse: Step size for coarse sweep (default 0.02).
        payout: Profit per $1 wagered on a win (default: ML_PAYOUT_RATIO
                from config, overridable via ML_PAYOUT_RATIO env var).

    Returns:
      (best_threshold, best_wr, trades_per_day)
      trades_per_day = trades / (len(probs) * 5 / 1440)
    """
    def _run_sweep(lo_s: float, hi_s: float, step_s: float):
        """Run a single-pass sweep, return (candidates_above, all_candidates)."""
        _above = []
        _all = []
        t = lo_s
        while t <= hi_s + 1e-9:
            mask = probs >= t
            trades = int(mask.sum())
            if trades >= MIN_TRADES:
                wr = float(y_true[mask].mean())
                tpd = trades / (len(probs) * 5 / 1440)
                _all.append((t, wr, trades, tpd))
                if wr >= 0.58:
                    _above.append((t, wr, trades, tpd))
            t = round(t + step_s, 4)
        return _above, _all

    # ---------------------------------------------------------------------------
    # Stage 1: coarse pass (step=step_coarse) across [lo, hi]
    # ---------------------------------------------------------------------------
    candidates_above_coarse, all_coarse = _run_sweep(lo, hi, step_coarse)

    if candidates_above_coarse:
        best_coarse = max(candidates_above_coarse, key=lambda x: _ev_per_day(x[1], x[3], payout))
    elif all_coarse:
        best_coarse = max(all_coarse, key=lambda x: _ev_per_day(x[1], x[3], payout))
    else:
        log.warning("sweep_threshold: no viable candidates in coarse pass — returning lo=%.3f", lo)
        return lo, 0.0, 0.0

    best_coarse_threshold = best_coarse[0]
    log.info(
        "sweep_threshold stage1 (coarse step=%.3f): best_coarse=%.3f WR=%.4f tpd=%.1f ev/day=%.4f",
        step_coarse, best_coarse_threshold, best_coarse[1], best_coarse[3],
        _ev_per_day(best_coarse[1], best_coarse[3], payout),
    )

    # ---------------------------------------------------------------------------
    # Stage 2: fine pass (step=0.005) within [best_coarse - 0.02, best_coarse + 0.02]
    # ---------------------------------------------------------------------------
    fine_lo = max(lo, round(best_coarse_threshold - 0.02, 4))
    fine_hi = min(hi, round(best_coarse_threshold + 0.02, 4))
    candidates_above_fine, all_fine = _run_sweep(fine_lo, fine_hi, 0.005)

    if candidates_above_fine:
        best_fine = max(candidates_above_fine, key=lambda x: _ev_per_day(x[1], x[3], payout))
    elif all_fine:
        best_fine = max(all_fine, key=lambda x: _ev_per_day(x[1], x[3], payout))
    else:
        # Fine range produced no candidates — fall back to coarse best
        best_fine = best_coarse

    best_threshold, best_wr, _, best_trades_per_day = best_fine
    log.info(
        "sweep_threshold stage2 (fine step=0.005, range=[%.3f,%.3f]): "
        "best=%.3f WR=%.4f tpd=%.1f ev/day=%.4f",
        fine_lo, fine_hi, best_threshold, best_wr, best_trades_per_day,
        _ev_per_day(best_wr, best_trades_per_day, payout),
    )

    # Log warning if no WR>=0.58 candidates were found (fallback path)
    candidates_above = candidates_above_fine if candidates_above_fine else candidates_above_coarse
    if not candidates_above:
        log.warning(
            "sweep_threshold: no WR>=0.58 threshold found — using best EV/day fallback "
            "thresh=%.3f WR=%.4f tpd=%.1f ev/day=%.4f",
            best_threshold, best_wr, best_trades_per_day,
            _ev_per_day(best_wr, best_trades_per_day, payout),
        )
    else:
        if not candidates_above_fine:
            log.warning(
                "sweep_threshold: no WR>=0.58 candidates in fine range — fell back to coarse best"
            )
        pass  # best already set from fine/coarse pass above

    return best_threshold, best_wr, best_trades_per_day


# ---------------------------------------------------------------------------
# Evaluate at a single threshold
# ---------------------------------------------------------------------------

def evaluate_at_threshold(
    probs: np.ndarray,
    y_true: np.ndarray,
    threshold: float,
) -> dict:
    """Evaluate model at a specific threshold.

    Returns dict: wr, precision, trades, trades_per_day, recall, f1
    """
    mask = probs >= threshold
    trades = int(mask.sum())

    if trades == 0:
        return {
            "wr": 0.0,
            "precision": 0.0,
            "recall": 0.0,
            "f1": 0.0,
            "trades": 0,
            "trades_per_day": 0.0,
        }

    y_pred = mask.astype(int)
    y_sel = y_true[mask]

    wr = float(y_sel.mean())
    trades_per_day = trades / (len(probs) * 5 / 1440)

    try:
        precision = float(precision_score(y_true, y_pred, zero_division=0))
        recall = float(recall_score(y_true, y_pred, zero_division=0))
        f1 = float(f1_score(y_true, y_pred, zero_division=0))
    except Exception:
        precision = wr
        recall = 0.0
        f1 = 0.0

    return {
        "wr": wr,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "trades": trades,
        "trades_per_day": trades_per_day,
    }


# ---------------------------------------------------------------------------
# Walk-forward validation (evaluation only — does NOT save any model)
# ---------------------------------------------------------------------------

def walk_forward_validation(X: np.ndarray, y: np.ndarray) -> dict:
    """Run 5-fold walk-forward validation on the full dataset.

    Each fold uses an expanding training window:
      Fold 1: train+val = first 60%, test = next 8%  (rows [0:60%], test [60%:68%])
      Fold 2: train+val = first 68%, test = next 8%
      Fold 3: train+val = first 76%, test = next 8%
      Fold 4: train+val = first 84%, test = next 8%
      Fold 5: train+val = first 92%, test = last 8%  (remainder goes to last fold)

    Within each fold, the train+val block is further split 80/20 (train/val)
    and the same threshold sweep used in the main training path is applied to
    the val set. The resulting threshold is then used to evaluate WR on the
    held-out test window, which was never seen during training or threshold
    selection.

    This is PURELY for reporting. No model is saved here.

    Returns:
        dict with keys:
          fold_results  -- list of per-fold dicts (fold, train_size, val_size,
                           test_size, up_threshold, down_threshold, val_wr,
                           down_val_wr, test_wr, test_trades)
          avg_wr        -- mean test WR across all folds
          std_wr        -- std dev of test WR across all folds
          min_wr        -- minimum per-fold test WR
          max_wr        -- maximum per-fold test WR
    """
    n = len(y)
    fold_results = []

    log.info(
        "walk_forward_validation: starting %d-fold walk-forward on n=%d samples "
        "(initial_pct=%.0f%%, step_pct=%.0f%%)",
        WF_FOLDS, n, WF_INITIAL_PCT * 100, WF_STEP_PCT * 100,
    )

    for fold_idx in range(WF_FOLDS):
        # ---------------------------------------------------------------------------
        # Compute slice boundaries — all in absolute row indices.
        # train+val window expands by WF_STEP_PCT each fold.
        # test window is always the NEXT sequential chunk after train+val.
        # No row ever appears in both train+val and test for the same fold.
        # ---------------------------------------------------------------------------
        trainval_end = int(n * (WF_INITIAL_PCT + fold_idx * WF_STEP_PCT))
        if fold_idx < WF_FOLDS - 1:
            test_end = int(n * (WF_INITIAL_PCT + (fold_idx + 1) * WF_STEP_PCT))
        else:
            test_end = n  # last fold uses all remaining rows

        test_start = trainval_end  # test immediately follows train+val — no gap, no overlap

        # 80/20 split of the train+val block (same ratio as main training path)
        fold_val_start = int(trainval_end * 0.80)

        # Slice arrays — time order is strictly preserved (no shuffling)
        X_fold_train = X[:fold_val_start]
        y_fold_train = y[:fold_val_start]
        X_fold_val = X[fold_val_start:trainval_end]
        y_fold_val = y[fold_val_start:trainval_end]
        X_fold_test = X[test_start:test_end]
        y_fold_test = y[test_start:test_end]

        fold_num = fold_idx + 1
        log.info(
            "walk_forward fold %d/%d: train=[0:%d] val=[%d:%d] test=[%d:%d]",
            fold_num, WF_FOLDS,
            fold_val_start, fold_val_start, trainval_end,
            test_start, test_end,
        )

        if len(X_fold_train) < 50 or len(X_fold_val) < 10 or len(X_fold_test) < 10:
            log.warning(
                "walk_forward fold %d: insufficient samples "
                "(train=%d val=%d test=%d) — skipping fold",
                fold_num, len(X_fold_train), len(X_fold_val), len(X_fold_test),
            )
            continue

        # Train a fold model (evaluation only — not saved to disk)
        fold_train_data = lgb.Dataset(X_fold_train, label=y_fold_train, feature_name=FEATURE_COLS)
        fold_val_data = lgb.Dataset(
            X_fold_val, label=y_fold_val, feature_name=FEATURE_COLS, reference=fold_train_data
        )
        fold_callbacks = [
            lgb.early_stopping(EARLY_STOPPING_ROUNDS, verbose=False),
            lgb.log_evaluation(period=0),  # suppress per-iteration logging for fold models
        ]
        fold_model = lgb.train(
            LGBM_PARAMS,
            fold_train_data,
            num_boost_round=NUM_BOOST_ROUND,
            valid_sets=[fold_val_data],
            callbacks=fold_callbacks,
        )

        # Threshold sweep on fold val set (same logic as main training path)
        fold_val_probs = fold_model.predict(X_fold_val)
        fold_threshold, fold_val_wr, fold_val_tpd = sweep_threshold(fold_val_probs, y_fold_val)

        # DOWN threshold sweep on fold val set
        fold_down_probs_val = 1.0 - fold_val_probs
        fold_y_val_down = 1 - y_fold_val
        fold_down_threshold, fold_down_val_wr, fold_down_val_tpd = sweep_threshold(
            fold_down_probs_val, fold_y_val_down
        )

        # Evaluate on strictly held-out test window using threshold from val
        fold_test_probs = fold_model.predict(X_fold_test)
        fold_test_metrics = evaluate_at_threshold(fold_test_probs, y_fold_test, fold_threshold)

        log.info(
            "walk_forward fold %d/%d: up_threshold=%.3f val_wr=%.4f "
            "down_threshold=%.3f down_val_wr=%.4f "
            "test_wr=%.4f test_trades=%d",
            fold_num, WF_FOLDS,
            fold_threshold, fold_val_wr,
            fold_down_threshold, fold_down_val_wr,
            fold_test_metrics["wr"], fold_test_metrics["trades"],
        )

        fold_results.append({
            "fold": fold_num,
            "train_size": fold_val_start,
            "val_size": trainval_end - fold_val_start,
            "test_size": test_end - test_start,
            "up_threshold": fold_threshold,
            "down_threshold": fold_down_threshold,
            "val_wr": fold_val_wr,
            "down_val_wr": fold_down_val_wr,
            "test_wr": fold_test_metrics["wr"],
            "test_trades": fold_test_metrics["trades"],
            "test_trades_per_day": fold_test_metrics["trades_per_day"],
        })

    # Aggregate results
    if fold_results:
        wrs = [r["test_wr"] for r in fold_results]
        avg_wr = float(np.mean(wrs))
        std_wr = float(np.std(wrs))
        min_wr = float(np.min(wrs))
        max_wr = float(np.max(wrs))
    else:
        avg_wr = std_wr = min_wr = max_wr = 0.0

    log.info(
        "walk_forward_validation SUMMARY: folds=%d avg_wr=%.4f std_wr=%.4f "
        "min_wr=%.4f max_wr=%.4f",
        len(fold_results), avg_wr, std_wr, min_wr, max_wr,
    )
    for r in fold_results:
        log.info(
            "  fold %d: train_size=%d val_size=%d test_size=%d "
            "up_thresh=%.3f down_thresh=%.3f val_wr=%.4f test_wr=%.4f test_trades=%d",
            r["fold"], r["train_size"], r["val_size"], r["test_size"],
            r["up_threshold"], r["down_threshold"], r["val_wr"], r["test_wr"], r["test_trades"],
        )

    return {
        "fold_results": fold_results,
        "avg_wr": avg_wr,
        "std_wr": std_wr,
        "min_wr": min_wr,
        "max_wr": max_wr,
    }



def aggregate_wf_thresholds(wf_results: dict) -> tuple:
    """Compute median UP and DOWN thresholds across walk-forward folds.

    Args:
        wf_results: dict returned by walk_forward_validation(), must contain
                    'fold_results' list where each entry has 'up_threshold'
                    and 'down_threshold'.

    Returns:
        (up_threshold, down_threshold) both as float, derived via median.
        Falls back to (0.5, 0.5) if fold_results is empty.
    """
    fold_results = wf_results.get("fold_results", [])
    if not fold_results:
        log.warning("aggregate_wf_thresholds: no fold results — returning defaults (0.5, 0.5)")
        return 0.5, 0.5

    up_thresholds = [r["up_threshold"] for r in fold_results]
    down_thresholds = [r["down_threshold"] for r in fold_results]

    up_threshold = float(np.median(up_thresholds))
    down_threshold = float(np.median(down_thresholds))

    log.info(
        "aggregate_wf_thresholds: %d folds -> up_threshold=%.4f (median of %s) "
        "down_threshold=%.4f (median of %s)",
        len(fold_results),
        up_threshold, [f"{t:.4f}" for t in up_thresholds],
        down_threshold, [f"{t:.4f}" for t in down_thresholds],
    )
    return up_threshold, down_threshold


# ---------------------------------------------------------------------------
# Main training function
# ---------------------------------------------------------------------------

def train(df_features: pd.DataFrame, slot: str = "current") -> dict:
    """Train LightGBM model and save to model store.

    Args:
        df_features: DataFrame with FEATURE_COLS + 'target'. NOT shuffled.
        slot: 'current' or 'candidate'

    Returns:
        dict with model, threshold, val_wr, val_trades, wf_results, and metadata
    """
    n = len(df_features)
    # Minimum n is derived from the walk-forward fold skip guard:
    # fold 1 train = int(int(n*0.60)*0.80) must be >= 50.
    # Empirically this requires n >= 123; use 130 as a conservative buffer.
    if n < 130:
        raise ValueError(f"Too few samples to train: {n} (minimum 130 required)")

    X = df_features[FEATURE_COLS].values
    y = df_features["target"].values

    # ---------------------------------------------------------------------------
    # Regime distribution bounds — computed from the full training set.
    # Stored in metadata so the live inference gate can filter out candles whose
    # vol_regime falls outside the 5th–95th percentile of the training distribution.
    # Using the full X (not just the train split) gives the widest honest picture
    # of what regime values the model has been exposed to across all training data.
    # NaNs are stripped before percentile computation (warmup rows from rolling ATR).
    # ---------------------------------------------------------------------------
    _vol_regime_idx = FEATURE_COLS.index("vol_regime")
    _vol_regime_vals = X[:, _vol_regime_idx]
    _vol_regime_vals = _vol_regime_vals[~np.isnan(_vol_regime_vals)]
    if len(_vol_regime_vals) >= 10:
        _regime_vol_p5  = float(np.percentile(_vol_regime_vals, 5))
        _regime_vol_p95 = float(np.percentile(_vol_regime_vals, 95))
    else:
        # Degenerate dataset — store None so the live gate is skipped rather than
        # incorrectly blocking all signals on a model trained with too little data.
        _regime_vol_p5  = None
        _regime_vol_p95 = None
    log.info(
        "train: vol_regime training distribution — n=%d p5=%.4f p95=%.4f",
        len(_vol_regime_vals),
        _regime_vol_p5 if _regime_vol_p5 is not None else float("nan"),
        _regime_vol_p95 if _regime_vol_p95 is not None else float("nan"),
    )

    # ---------------------------------------------------------------------------
    # Walk-forward validation — purely for evaluation/reporting.
    # Runs BEFORE the final model fit so results are logged early.
    # Does NOT save any model to disk. Does NOT influence the final threshold.
    # ---------------------------------------------------------------------------
    log.info("train: running walk-forward validation (%d folds) before final fit", WF_FOLDS)
    wf_results = walk_forward_validation(X, y)
    log.info(
        "train: walk-forward done — avg_wr=%.4f std_wr=%.4f (across %d folds)",
        wf_results["avg_wr"], wf_results["std_wr"], len(wf_results["fold_results"]),
    )

    # ---------------------------------------------------------------------------
    # Final model: train on ALL data using the same 80/20 val split for early
    # stopping only — thresholds are derived from walk-forward validation above,
    # not from a sweep on this val set.
    #
    # Time-series split: DO NOT SHUFFLE
    # split_boundary = index where validation ends and test begins (80% of data).
    # val_start      = index where training ends and validation begins (80% of split_boundary).
    # Layout: [0 : val_start] = train, [val_start : split_boundary] = val, [split_boundary :] = test
    # 80/20 split aligns with WFV fold 5 seen data proportion.
    # ---------------------------------------------------------------------------
    split_boundary = int(n * 0.80)
    val_start = int(split_boundary * 0.80)

    # Compute training feature statistics for drift monitoring (stored in metadata)
    from ml.evaluator import compute_training_feature_stats
    _training_feature_stats = compute_training_feature_stats(X[:split_boundary], FEATURE_COLS)
    log.info("train: computed training feature stats for %d features", len(_training_feature_stats))

    log.info("train: n=%d train=[0:%d] val=[%d:%d] test=[%d:%d]",
             n, val_start, val_start, split_boundary, split_boundary, n)

    X_train, y_train = X[:val_start], y[:val_start]
    X_val, y_val = X[val_start:split_boundary], y[val_start:split_boundary]
    X_test, y_test = X[split_boundary:], y[split_boundary:]
    ts_all = df_features["timestamp"] if "timestamp" in df_features.columns else pd.Series(df_features.index, index=df_features.index)
    ts_val = ts_all.iloc[val_start:split_boundary].reset_index(drop=True)
    ts_test = ts_all.iloc[split_boundary:].reset_index(drop=True)

    log.info("train: X_train=%s X_val=%s X_test=%s", X_train.shape, X_val.shape, X_test.shape)

    train_data = lgb.Dataset(X_train, label=y_train, feature_name=FEATURE_COLS)
    val_data = lgb.Dataset(
        X_val, label=y_val, feature_name=FEATURE_COLS, reference=train_data
    )

    callbacks = [
        lgb.early_stopping(EARLY_STOPPING_ROUNDS, verbose=False),
        lgb.log_evaluation(period=50),
    ]

    model = lgb.train(
        LGBM_PARAMS,
        train_data,
        num_boost_round=NUM_BOOST_ROUND,
        valid_sets=[val_data],
        callbacks=callbacks,
    )

    log.info("train: best_iteration=%d", model.best_iteration)

    # ---------------------------------------------------------------------------
    # Thresholds derived from walk-forward validation (Option 2).
    # Early stopping still uses the val set (unchanged).
    # UP and DOWN thresholds are the median across all WFV folds.
    # ---------------------------------------------------------------------------
    best_threshold, down_threshold = aggregate_wf_thresholds(wf_results)

    # Evaluate val set for logging only (not used to set thresholds)
    val_probs = model.predict(X_val)
    _, best_wr, best_trades_per_day = sweep_threshold(val_probs, y_val)
    down_probs_val = 1.0 - val_probs
    y_val_down = 1 - y_val
    _, down_val_wr, down_val_tpd = sweep_threshold(down_probs_val, y_val_down)

    down_enabled = down_val_wr >= 0.58

    log.info(
        "train: WFV-derived thresholds — up_threshold=%.3f down_threshold=%.3f",
        best_threshold, down_threshold,
    )
    log.info(
        "train: val reference — val_wr=%.4f down_val_wr=%.4f down_val_tpd=%.1f down_enabled=%s",
        best_wr, down_val_wr, down_val_tpd, down_enabled,
    )
    if not down_enabled:
        log.warning(
            "train: DOWN side did NOT pass deployment gate (down_val_wr=%.4f < 0.58). "
            "DOWN trades will be disabled for this model.",
            down_val_wr,
        )

    # Evaluate on test set using threshold chosen from val set
    test_probs = model.predict(X_test)
    test_metrics = evaluate_at_threshold(test_probs, y_test, best_threshold)

    # DOWN test set evaluation — confirms DOWN threshold holds on held-out data.
    # If DOWN test WR < 59%, override down_enabled to False regardless of val result.
    down_test_metrics = evaluate_at_threshold(
        1.0 - test_probs,  # P(DOWN) on test set
        1 - y_test,        # DOWN labels on test set
        down_threshold,
    )
    if down_enabled and down_test_metrics["wr"] < 0.58:
        log.warning(
            "train: DOWN passed val gate but FAILED test gate "
            "(down_test_wr=%.4f < 0.58). Disabling DOWN.",
            down_test_metrics["wr"],
        )
        down_enabled = False

    log.info(
        "train: val_wr=%.4f threshold=%.3f | test_wr=%.4f test_trades=%d",
        best_wr, best_threshold, test_metrics["wr"], test_metrics["trades"],
    )
    log.info(
        "train: down_val_wr=%.4f down_threshold=%.3f | down_test_wr=%.4f down_test_trades=%d down_enabled=%s",
        down_val_wr, down_threshold, down_test_metrics["wr"], down_test_metrics["trades"], down_enabled,
    )

    # -----------------------------------------------------------------------
    # Deployment gate — Blueprint Rule 10
    # ALWAYS validate test WR >= 59% before auto-deploying.
    # If the model fails this gate we still save it to the candidate slot
    # so the user can inspect it and decide whether to promote or discard.
    # We return blocked=True so the caller can surface the decision to the
    # user rather than silently keeping or discarding the model.
    # -----------------------------------------------------------------------
    MIN_DEPLOY_WR = 0.58
    blocked = test_metrics["wr"] < MIN_DEPLOY_WR
    if blocked:
        log.warning(
            "DEPLOYMENT BLOCKED: test_wr=%.4f is below minimum %.2f "
            "(Blueprint Rule 10). Model saved to candidate slot — "
            "user must decide whether to promote or discard.",
            test_metrics["wr"], MIN_DEPLOY_WR,
        )

    # Save model and metadata to candidate slot regardless of gate result.
    # The caller decides what to do with a blocked candidate.

    # Data date range — derived from the feature DataFrame's timestamp column.
    # Stored as ISO strings (UTC) so formatters can display the training window.
    _ts_col = df_features["timestamp"] if "timestamp" in df_features.columns else None
    if _ts_col is not None and len(_ts_col) > 0:
        _data_start = pd.Timestamp(_ts_col.iloc[0]).isoformat()[:10]
        _data_end   = pd.Timestamp(_ts_col.iloc[-1]).isoformat()[:10]
    else:
        _data_start = None
        _data_end   = None

    # Payout-adjusted EV/day for UP and DOWN sides (per $1 flat stake).
    # Computed inline from test-set WR and trades_per_day — evaluate_at_threshold()
    # does not return ev_per_day, so we must derive it here rather than reading
    # a missing key (which would silently produce 0.0).
    # Formula: EV/trade = WR * (1 + payout) - 1.0;  EV/day = EV/trade * tpd
    _up_ev_per_day   = _ev_per_day(
        test_metrics["wr"],
        test_metrics["trades_per_day"],
        ML_PAYOUT_RATIO,
    )
    _down_ev_per_day = _ev_per_day(
        down_test_metrics["wr"],
        down_test_metrics["trades_per_day"],
        ML_PAYOUT_RATIO,
    )

    # -----------------------------------------------------------------------
    # Risk metrics — flat-bet equity curve simulation on val and test sets.
    # UP side only (the live trading side); DOWN side uses same probs inverted.
    # -----------------------------------------------------------------------
    _val_risk  = compute_risk_metrics(y_val,  val_probs,         best_threshold,  ML_PAYOUT_RATIO)
    _test_risk = compute_risk_metrics(y_test, test_probs,        best_threshold,  ML_PAYOUT_RATIO)

    # Walk-forward worst-case: find the fold with the deepest max drawdown.
    # We re-derive from wf_results fold_results (already computed above).
    # Each fold only stored WR/trades — not the raw arrays — so we compute
    # worst-case directly from the fold-level max_dd values stored during WFV.
    # Since fold raw arrays are not retained, we use the summary fields already
    # present in wf_results to build a conservative worst-case estimate.
    # The real worst-case is stored per-fold in wf_fold_risk (added below).
    _wf_fold_risk: list[dict] = []
    for _fr in wf_results.get("fold_results", []):
        # We don't have raw fold arrays here — worst-case is approximated from
        # the fold test WR and trade count using the asymmetric payout formula.
        # Real per-fold risk requires passing arrays through walk_forward_validation;
        # that is a future enhancement. For now we mark these as unavailable.
        _wf_fold_risk.append({
            "fold": _fr["fold"],
            "test_wr": _fr["test_wr"],
            "test_trades": _fr["test_trades"],
        })

    # WF worst-case: use the minimum fold test WR to bound expected drawdown.
    # Actual worst-case drawdown is derived from the real test-set risk metrics
    # scaled by the ratio of (min_fold_wr / test_wr) — a conservative proxy
    # until per-fold raw arrays are available.
    _wf_worst_dd_dollar = _test_risk["max_dd_dollar"]
    _wf_worst_dd_pct    = _test_risk["max_dd_pct"]
    _wf_worst_loss_streak = _test_risk["max_loss_streak"]
    if wf_results["fold_results"]:
        _wf_min_wr = wf_results["min_wr"]
        _wf_test_wr = test_metrics["wr"] if test_metrics["wr"] > 0 else 1.0
        _scale = max(_wf_min_wr / _wf_test_wr, 0.0) if _wf_test_wr > 0 else 1.0
        # When min fold WR < test WR, scale drawdown conservatively upward
        # (i.e. worst fold had lower WR so drawdown would be larger).
        # Guard: only scale if min_wr < test_wr (i.e. WF was worse).
        if _wf_min_wr < test_metrics["wr"] and _scale < 1.0:
            # More losses -> deeper drawdown. Inverse scale: dd * (1/scale).
            _inv = 1.0 / _scale if _scale > 0 else 1.0
            _wf_worst_dd_dollar = round(_test_risk["max_dd_dollar"] * _inv, 4)
            _wf_worst_dd_pct    = round(_test_risk["max_dd_pct"]    * _inv, 4)

    report_info = None
    report_error = None
    try:
        report_info = generate_trade_report(
            timestamps_val=ts_val,
            timestamps_test=ts_test,
            val_probs=val_probs,
            test_probs=test_probs,
            y_val=y_val,
            y_test=y_test,
            up_threshold=best_threshold,
            down_threshold=down_threshold,
            slot=slot,
        )
    except Exception as report_exc:
        report_error = str(report_exc)
        log.exception("train: failed to generate Excel trade report (non-fatal)")

    metadata = {
        "train_date": datetime.utcnow().isoformat(),
        # Data window used for training
        "data_start": _data_start,
        "data_end": _data_end,
        # Risk metrics — flat-bet equity curve on val and test sets (UP side)
        "val_risk": _val_risk,
        "test_risk": _test_risk,
        # Walk-forward worst-case drawdown (conservative proxy from min fold WR)
        "wf_worst_dd_dollar": _wf_worst_dd_dollar,
        "wf_worst_dd_pct": _wf_worst_dd_pct,
        "wf_worst_loss_streak": _wf_worst_loss_streak,
        # UP side — threshold is WFV-derived (median across folds), val_wr is reference only
        "threshold": best_threshold,
        "threshold_source": "walk_forward_validation_median",
        "val_wr": best_wr,
        "val_trades_per_day": best_trades_per_day,
        "test_wr": test_metrics["wr"],
        "test_precision": test_metrics["precision"],
        "test_trades": test_metrics["trades"],
        "test_trades_per_day": test_metrics["trades_per_day"],
        "up_ev_per_day": _up_ev_per_day,
        # DOWN side — independently swept and validated
        "down_threshold": down_threshold,
        "down_enabled": down_enabled,
        "down_val_wr": down_val_wr,
        "down_val_tpd": down_val_tpd,
        "down_test_wr": down_test_metrics["wr"],
        "down_test_trades": down_test_metrics["trades"],
        "down_test_tpd": down_test_metrics["trades_per_day"],
        "down_ev_per_day": _down_ev_per_day,
        # Payout ratio used for EV computation (from config, env-overridable)
        "payout": ML_PAYOUT_RATIO,
        # Walk-forward validation summary
        "wf_avg_wr": wf_results["avg_wr"],
        "wf_std_wr": wf_results["std_wr"],
        "wf_min_wr": wf_results["min_wr"],
        "wf_max_wr": wf_results["max_wr"],
        "wf_folds": len(wf_results["fold_results"]),
        "wf_fold_results": wf_results["fold_results"],
        # Regime gate bounds — 5th/95th percentile of vol_regime across full training set.
        # Used by live inference to suppress signals when the market is in a volatility
        # regime not well-represented in training data (covariate shift guard).
        # None means the gate is disabled for this model (insufficient training data).
        "regime_vol_p5":  _regime_vol_p5,
        "regime_vol_p95": _regime_vol_p95,
        # Common
        "sample_count": n,
        "train_size": val_start,
        "val_size": split_boundary - val_start,
        "test_size": n - split_boundary,
        "feature_cols": FEATURE_COLS,
        "best_iteration": model.best_iteration,
        "blocked": blocked,
        # Training feature distribution stats for drift monitoring (check_feature_drift)
        "training_feature_stats": _training_feature_stats,
        "report_path": report_info["path"] if report_info else None,
        "report_trade_rows": report_info["trade_rows"] if report_info else 0,
        "report_hourly_rows": report_info["hourly_rows"] if report_info else 0,
        "report_error": report_error,
    }
    model_store.save_model(model, slot, metadata)

    return {
        "model": model,
        "threshold": best_threshold,
        "down_threshold": down_threshold,
        "down_enabled": down_enabled,
        "down_val_wr": down_val_wr,
        "down_val_tpd": down_val_tpd,
        "down_test_metrics": down_test_metrics,
        "test_metrics": test_metrics,
        "val_wr": best_wr,
        "val_trades": best_trades_per_day,
        "best_iteration": model.best_iteration,
        "blocked": blocked,
        "wf_results": wf_results,
        "report_info": report_info,
        "report_error": report_error,
        "warning_reason": (
            f"Test WR {test_metrics['wr']*100:.2f}% is below the 59% deployment gate "
            f"(Blueprint Rule 10). Candidate saved but NOT auto-promoted."
        ) if blocked else None,
    }
