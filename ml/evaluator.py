"""Model evaluator — full hold-out test evaluation with metrics table."""

from __future__ import annotations

import logging

import numpy as np
from sklearn.metrics import (
    confusion_matrix,
    precision_score,
    recall_score,
    f1_score,
)

from config import ML_PAYOUT_RATIO

log = logging.getLogger(__name__)


def evaluate(
    model,
    X_test: np.ndarray,
    y_test: np.ndarray,
    threshold: float,
    test_period_days: float = 37,
    payout: float = ML_PAYOUT_RATIO,
) -> dict:
    """Full evaluation of a trained LightGBM model on a hold-out test set.

    Prints a clear summary table and returns a metrics dict.

    Args:
        model: lgb.Booster instance
        X_test: Feature matrix (n_samples, 22)
        y_test: True binary labels
        threshold: Decision threshold from val-set sweep
        test_period_days: How many days the test set covers (for trades/day)
        payout: Profit per $1 wagered on a winning trade (default:
                ML_PAYOUT_RATIO from config, overridable via ML_PAYOUT_RATIO
                env var). Used to compute payout-adjusted EV/day.

    Returns:
        dict with: wr, precision, recall, f1, trades, trades_per_day,
                   ev_per_trade, ev_per_day, brier_score, calibration_mean,
                   confusion_matrix, threshold, payout
    """
    probs = model.predict(X_test)
    mask = probs >= threshold
    trades = int(mask.sum())

    if trades == 0:
        result = {
            "wr": 0.0,
            "precision": 0.0,
            "recall": 0.0,
            "f1": 0.0,
            "trades": 0,
            "trades_per_day": 0.0,
            "ev_per_trade": 0.0,
            "ev_per_day": 0.0,
            "brier_score": float(np.mean((probs - y_test) ** 2)),
            "calibration_mean": float(np.mean(probs)),
            "confusion_matrix": [[0, 0], [0, 0]],
            "threshold": threshold,
            "payout": payout,
        }
        _print_table(result)
        return result

    y_pred = mask.astype(int)
    y_sel = y_test[mask]

    wr = float(y_sel.mean())
    trades_per_day = trades / test_period_days if test_period_days > 0 else 0.0

    # Payout-adjusted EV: accounts for asymmetric win/loss payouts.
    # EV/trade = (WR * payout) - ((1 - WR) * 1.0) = WR * (1 + payout) - 1.0
    ev_per_trade = float(wr * (1.0 + payout) - 1.0)
    ev_per_day = ev_per_trade * trades_per_day

    precision = float(precision_score(y_test, y_pred, zero_division=0))
    recall = float(recall_score(y_test, y_pred, zero_division=0))
    f1 = float(f1_score(y_test, y_pred, zero_division=0))

    # Brier score (calibration quality)
    brier = float(np.mean((probs - y_test) ** 2))
    calib_mean = float(np.mean(probs[mask]))

    cm = confusion_matrix(y_test, y_pred).tolist()

    result = {
        "wr": wr,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "trades": trades,
        "trades_per_day": trades_per_day,
        "ev_per_trade": ev_per_trade,
        "ev_per_day": ev_per_day,
        "brier_score": brier,
        "calibration_mean": calib_mean,
        "confusion_matrix": cm,
        "threshold": threshold,
        "payout": payout,
    }

    _print_table(result)
    return result


def _print_table(m: dict) -> None:
    """Print a readable evaluation summary."""
    print("\n" + "=" * 52)
    print("  MODEL EVALUATION (HOLD-OUT TEST SET)")
    print("=" * 52)
    print(f"  Threshold          : {m['threshold']:.3f}")
    print(f"  Payout ratio       : {m.get('payout', ML_PAYOUT_RATIO):.2f}")
    print(f"  Win Rate (WR)      : {m['wr']:.4f}  ({m['wr']*100:.2f}%)")
    print(f"  Precision          : {m['precision']:.4f}")
    print(f"  Recall             : {m['recall']:.4f}")
    print(f"  F1                 : {m['f1']:.4f}")
    print(f"  Trades total       : {m['trades']}")
    print(f"  Trades / day       : {m['trades_per_day']:.2f}")
    print(f"  EV / trade ($1)    : {m.get('ev_per_trade', 0.0):+.4f}")
    print(f"  EV / day ($1 flat) : {m.get('ev_per_day', 0.0):+.4f}")
    print(f"  Brier score        : {m['brier_score']:.4f}")
    print(f"  Mean prob (trades) : {m['calibration_mean']:.4f}")
    if m.get("confusion_matrix"):
        cm = m["confusion_matrix"]
        if len(cm) == 2 and len(cm[0]) == 2:
            print(f"  Confusion matrix   :")
            print(f"    TN={cm[0][0]}  FP={cm[0][1]}")
            print(f"    FN={cm[1][0]}  TP={cm[1][1]}")
    print("=" * 52 + "\n")
