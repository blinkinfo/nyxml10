"""Inline keyboard layouts for the Telegram bot."""

from __future__ import annotations

from telegram import InlineKeyboardButton, InlineKeyboardMarkup


# ---------------------------------------------------------------------------
# Consistent filter button helper
# ---------------------------------------------------------------------------

def _filter_btn(label: str, callback_data: str, active: str) -> InlineKeyboardButton:
    """Return a filter tab button with ✅ prefix when active."""
    prefix = "✅ " if callback_data.endswith(f"_{active}") else ""
    return InlineKeyboardButton(f"{prefix}{label}", callback_data=callback_data)


# ---------------------------------------------------------------------------
# Main menu
# ---------------------------------------------------------------------------

def main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("\U0001f4ca Status", callback_data="cmd_status"),
            InlineKeyboardButton("\U0001f4e1 Signals", callback_data="cmd_signals"),
        ],
        [
            InlineKeyboardButton("\U0001f4b9 Trades", callback_data="cmd_trades"),
            InlineKeyboardButton("\U0001f9e9 Patterns", callback_data="cmd_patterns"),
        ],
        [
            InlineKeyboardButton("\U0001f4b8 Redeem", callback_data="cmd_redeem"),
            InlineKeyboardButton("\U0001f4dc Redemptions", callback_data="cmd_redemptions"),
        ],
        [
            InlineKeyboardButton("\u2699\ufe0f Settings", callback_data="cmd_settings"),
            InlineKeyboardButton("\U0001f9ea Demo", callback_data="cmd_demo"),
        ],
        [
            InlineKeyboardButton("\U0001f916 ML Model", callback_data="cmd_ml"),
            InlineKeyboardButton("\U0001f500 Thresholds", callback_data="cmd_thresholds"),
        ],
        [
            InlineKeyboardButton("\u2753 Help", callback_data="cmd_help"),
            InlineKeyboardButton("\U0001f3e0 Home", callback_data="cmd_menu"),
        ],
    ])


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

def settings_keyboard(
    autotrade_on: bool,
    trade_amount: float,
    auto_redeem_on: bool = False,
    demo_trade_on: bool = False,
    demo_bankroll: float = 1000.00,
    trade_mode: str = "fixed",
    trade_pct: float = 5.0,
    invert_trades_on: bool = False,
    ml_volatility_gate_enabled: bool = True,
) -> InlineKeyboardMarkup:
    # Row 1: paired toggles — related switches side-by-side
    at_label = f"\U0001f916 AutoTrade: {'ON' if autotrade_on else 'OFF'}"
    ar_label = f"{'\U0001f4b0' if auto_redeem_on else '\U0001f4e6'} Auto-Redeem: {'ON' if auto_redeem_on else 'OFF'}"

    # Row 2: mode toggle + value input
    if trade_mode == "pct":
        mode_label = "\U0001f4ca Mode: PCT"
        value_label = f"\U0001f4b5 {trade_pct:.1f}%"
    else:
        mode_label = "\U0001f4ca Mode: FIXED"
        value_label = f"\U0001f4b5 ${trade_amount:.2f}"

    # Row 3: demo toggle + bankroll display
    dt_label = f"\U0001f9ea Demo: {'ON' if demo_trade_on else 'OFF'}"
    db_label = f"\U0001f4b5 Bankroll: ${demo_bankroll:.2f}"

    return InlineKeyboardMarkup([
        # Paired toggles
        [
            InlineKeyboardButton(at_label, callback_data="toggle_autotrade"),
            InlineKeyboardButton(ar_label, callback_data="toggle_auto_redeem"),
        ],
        # Mode + value (side by side)
        [
            InlineKeyboardButton(mode_label, callback_data="toggle_trade_mode"),
            InlineKeyboardButton(value_label, callback_data="change_amount"),
        ],
        # Demo section
        [
            InlineKeyboardButton(dt_label, callback_data="toggle_demo_trade"),
            InlineKeyboardButton(db_label, callback_data="set_demo_bankroll"),
        ],
        # Destructive action — full-width, alone
        [InlineKeyboardButton("\U0001f504 Reset Bankroll", callback_data="reset_demo_bankroll")],
        # ML volatility gate
        [InlineKeyboardButton(
            f"\U0001f6e1 ML Volatility Gate: {'ON' if ml_volatility_gate_enabled else 'OFF'}",
            callback_data="toggle_ml_volatility_gate",
        )],
        # Back
        [InlineKeyboardButton("\U0001f519 Back to Menu", callback_data="cmd_menu")],
    ])


# ---------------------------------------------------------------------------
# Filter rows (Last 10 / Last 50 / All Time)
# ---------------------------------------------------------------------------

def signal_filter_row(active: str = "all") -> InlineKeyboardMarkup:
    buttons = [
        _filter_btn("Last 10", "signals_10", active),
        _filter_btn("Last 50", "signals_50", active),
        _filter_btn("All Time", "signals_all", active),
    ]
    return InlineKeyboardMarkup([
        buttons,
        [
            InlineKeyboardButton("\U0001f4c4 CSV", callback_data="download_csv"),
            InlineKeyboardButton("\U0001f4ca Excel", callback_data="download_xlsx"),
        ],
        [InlineKeyboardButton("\U0001f519 Back to Menu", callback_data="cmd_menu")],
    ])


def trade_filter_row(active: str = "all") -> InlineKeyboardMarkup:
    buttons = [
        _filter_btn("Last 10", "trades_10", active),
        _filter_btn("Last 50", "trades_50", active),
        _filter_btn("All Time", "trades_all", active),
    ]
    return InlineKeyboardMarkup([
        buttons,
        [InlineKeyboardButton("\U0001f4ca Excel", callback_data="download_trades_xlsx")],
        [InlineKeyboardButton("\U0001f519 Back to Menu", callback_data="cmd_menu")],
    ])


# ---------------------------------------------------------------------------
# Back button only
# ---------------------------------------------------------------------------

def back_to_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("\U0001f519 Back to Menu", callback_data="cmd_menu")],
    ])


# ---------------------------------------------------------------------------
# Download keyboard (standalone)
# ---------------------------------------------------------------------------

def download_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("\U0001f4c4 CSV", callback_data="download_csv"),
            InlineKeyboardButton("\U0001f4ca Excel", callback_data="download_xlsx"),
        ],
        [InlineKeyboardButton("\U0001f519 Back to Menu", callback_data="cmd_menu")],
    ])


# ---------------------------------------------------------------------------
# Redeem keyboards
# ---------------------------------------------------------------------------

def redeem_confirm_keyboard() -> InlineKeyboardMarkup:
    """Shown after a dry-run scan — lets user confirm or cancel."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("\u2705 Confirm & Redeem All", callback_data="redeem_confirm")],
        [
            InlineKeyboardButton("\u274c Cancel", callback_data="redeem_cancel"),
            InlineKeyboardButton("\U0001f519 Menu", callback_data="cmd_menu"),
        ],
    ])


def redeem_done_keyboard() -> InlineKeyboardMarkup:
    """Shown after redemptions complete."""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("\U0001f4dc History", callback_data="cmd_redemptions"),
            InlineKeyboardButton("\U0001f519 Menu", callback_data="cmd_menu"),
        ],
    ])


def demo_filter_row(active: str = "all") -> InlineKeyboardMarkup:
    """Filter row for the /demo dashboard."""
    return InlineKeyboardMarkup([
        [
            _filter_btn("Last 10", "demo_10", active),
            _filter_btn("Last 50", "demo_50", active),
            _filter_btn("All Time", "demo_all", active),
        ],
        [InlineKeyboardButton("\U0001f4ca Excel", callback_data="download_demo_trades_xlsx")],
        [InlineKeyboardButton("\U0001f519 Back to Menu", callback_data="cmd_menu")],
    ])


def pattern_filter_row() -> InlineKeyboardMarkup:
    """Keyboard for the /patterns dashboard — kept for backward compat."""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("\U0001f4ca Excel", callback_data="download_pattern_xlsx"),
        ],
        [InlineKeyboardButton("\U0001f519 Back to Menu", callback_data="cmd_menu")],
    ])


# ---------------------------------------------------------------------------
# Pattern performance keyboard
# ---------------------------------------------------------------------------

def pattern_keyboard() -> InlineKeyboardMarkup:
    """Keyboard for the /patterns dashboard."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("\U0001f4ca Excel", callback_data="download_pattern_xlsx")],
        [InlineKeyboardButton("\U0001f519 Back to Menu", callback_data="cmd_menu")],
    ])


# ---------------------------------------------------------------------------
# ML Model submenu
# ---------------------------------------------------------------------------

def down_override_keyboard() -> InlineKeyboardMarkup:
    """Shown after an auto-promote (or force-promote) when the DOWN side failed
    its own 59 % gate.

    The UP model has already been promoted to current.  The user now chooses
    whether to enable the DOWN signal anyway or to leave it disabled.
    """
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("\u26a0\ufe0f Enable DOWN Anyway", callback_data="ml_down_override_anyway"),
            InlineKeyboardButton("\u274c Keep DOWN Disabled", callback_data="ml_down_override_skip"),
        ],
        [InlineKeyboardButton("\U0001f519 Back to Menu", callback_data="cmd_menu")],
    ])


def retrain_blocked_keyboard() -> InlineKeyboardMarkup:
    """Shown after a retrain that failed the 59% deployment gate.

    Candidate is already saved — user picks Promote Anyway or Discard.
    """
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("\u26a0\ufe0f Promote Anyway", callback_data="ml_promote_anyway"),
            InlineKeyboardButton("\U0001f5d1 Discard Candidate", callback_data="ml_discard_candidate"),
        ],
        [InlineKeyboardButton("\U0001f519 Back to Menu", callback_data="cmd_menu")],
    ])


def ml_menu() -> InlineKeyboardMarkup:
    """Inline keyboard for the ML Model submenu."""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("\U0001f4ca Model Status", callback_data="ml_status"),
            InlineKeyboardButton("\U0001f4cf Compare Models", callback_data="ml_compare"),
        ],
        [
            InlineKeyboardButton("\u2b06\ufe0f Promote Candidate", callback_data="ml_promote"),
            InlineKeyboardButton("\U0001f504 Retrain", callback_data="ml_retrain"),
        ],
        [
            InlineKeyboardButton("\u2699\ufe0f Set UP Threshold", callback_data="ml_set_threshold"),
            InlineKeyboardButton("\u2699\ufe0f Set DOWN Threshold", callback_data="ml_set_down_threshold"),
        ],
        [InlineKeyboardButton("\U0001f519 Back to Menu", callback_data="cmd_menu")],
    ])


def ml_volatility_gate_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Disable Gate", callback_data="confirm_disable_ml_volatility_gate")],
        [InlineKeyboardButton("Keep Gate Enabled", callback_data="cancel_disable_ml_volatility_gate")],
        [InlineKeyboardButton("Back to Settings", callback_data="cmd_settings")],
    ])


def threshold_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton('\U0001f4ca Real Policies', callback_data='thresholds_real'),
            InlineKeyboardButton('\U0001f9ea Demo Policies', callback_data='thresholds_demo'),
        ],
        [
            InlineKeyboardButton('\U0001f4c8 Real Analytics', callback_data='threshold_stats_real'),
            InlineKeyboardButton('\U0001f4c9 Demo Analytics', callback_data='threshold_stats_demo'),
        ],
        [InlineKeyboardButton('\U0001f3e0 Back to Menu', callback_data='cmd_menu')],
    ])


def threshold_mode_keyboard(
    mode: str,
    page: int = 1,
    total_pages: int = 1,
) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton('\u270f\ufe0f Set Policy', callback_data=f'threshold_set_{mode}'),
            InlineKeyboardButton('\U0001f5d1 Clear Policy', callback_data=f'threshold_clear_{mode}'),
        ],
    ]
    if total_pages > 1:
        prev_page = page - 1 if page > 1 else total_pages
        next_page = page + 1 if page < total_pages else 1
        rows.append([
            InlineKeyboardButton('\u25c0 Prev', callback_data=f'thresholds_{mode}_p{prev_page}'),
            InlineKeyboardButton(f'{page}/{total_pages}', callback_data='noop'),
            InlineKeyboardButton('Next \u25b6', callback_data=f'thresholds_{mode}_p{next_page}'),
        ])
    rows.append([InlineKeyboardButton('\u2190 Back to Hub', callback_data='cmd_thresholds')])
    return InlineKeyboardMarkup(rows)


def threshold_analytics_keyboard(
    mode: str,
    page: int = 1,
    total_pages: int = 1,
) -> InlineKeyboardMarkup:
    rows = []
    if total_pages > 1:
        prev_page = page - 1 if page > 1 else total_pages
        next_page = page + 1 if page < total_pages else 1
        rows.append([
            InlineKeyboardButton('\u25c0 Prev', callback_data=f'threshold_stats_{mode}_p{prev_page}'),
            InlineKeyboardButton(f'{page}/{total_pages}', callback_data='noop'),
            InlineKeyboardButton('Next \u25b6', callback_data=f'threshold_stats_{mode}_p{next_page}'),
        ])
    rows.append([InlineKeyboardButton('\u2190 Back to Hub', callback_data='cmd_thresholds')])
    return InlineKeyboardMarkup(rows)


def threshold_policy_choice_keyboard(mode: str, bucket: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton('\U0001f7e2 FOLLOW', callback_data=f'threshold_policy_{mode}_{bucket}_FOLLOW'),
            InlineKeyboardButton('\U0001f534 BLOCK',  callback_data=f'threshold_policy_{mode}_{bucket}_BLOCK'),
            InlineKeyboardButton('\U0001f501 INVERT', callback_data=f'threshold_policy_{mode}_{bucket}_INVERT'),
        ],
        [InlineKeyboardButton('\u2190 Back', callback_data=f'thresholds_{mode}')],
    ])


def threshold_cancel_keyboard(mode: str) -> InlineKeyboardMarkup:
    """Shown during bucket-input prompts so the user can abort."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('\u274c Cancel', callback_data=f'thresholds_{mode}')],
    ])
