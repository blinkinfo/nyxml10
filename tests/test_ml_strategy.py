import sys
sys.path.insert(0, '/home/nebula/nyxtest4')

from ml.features import FEATURE_COLS

REQUIRED_SKIP_KEYS = {'skipped', 'pattern', 'candles_used', 'slot_n1_start_full',
                       'slot_n1_end_full', 'slot_n1_start_str', 'slot_n1_end_str', 'slot_n1_ts'}
REQUIRED_TRADE_KEYS = REQUIRED_SKIP_KEYS | {'side', 'entry_price', 'opposite_price', 'token_id', 'slot_n1_slug'}


def test_feature_count():
    assert len(FEATURE_COLS) == 26


def test_feature_cols_are_strings():
    for col in FEATURE_COLS:
        assert isinstance(col, str)


def test_skip_signal_schema():
    skip_signal = {
        'skipped': True,
        'pattern': 'p=0.4500<0.535',
        'reason': 'Below threshold',
        'candles_used': 50,
        'slot_n1_start_full': '2025-01-01 09:00:00 UTC',
        'slot_n1_end_full': '2025-01-01 09:05:00 UTC',
        'slot_n1_start_str': '09:00',
        'slot_n1_end_str': '09:05',
        'slot_n1_ts': 1735722000,
        'slot_n1_slug': 'btc-updown-5m-1735722000',
    }
    for key in REQUIRED_SKIP_KEYS:
        assert key in skip_signal, f"Missing key: {key}"


def test_trade_signal_schema():
    trade_signal = {
        'skipped': False,
        'side': 'Up',
        'entry_price': 0.72,
        'opposite_price': 0.28,
        'token_id': 'abc123',
        'pattern': 'p=0.6200',
        'candles_used': 50,
        'slot_n1_start_full': '2025-01-01 09:00:00 UTC',
        'slot_n1_end_full': '2025-01-01 09:05:00 UTC',
        'slot_n1_start_str': '09:00',
        'slot_n1_end_str': '09:05',
        'slot_n1_ts': 1735722000,
        'slot_n1_slug': 'btc-updown-5m-1735722000',
    }
    for key in REQUIRED_TRADE_KEYS:
        assert key in trade_signal, f"Missing key: {key}"
    assert trade_signal['side'] in ('Up', 'Down')
    assert 0 < trade_signal['entry_price'] < 1
    assert 0 < trade_signal['opposite_price'] < 1


def test_default_threshold():
    import config as cfg
    assert hasattr(cfg, 'ML_DEFAULT_THRESHOLD')
    assert cfg.ML_DEFAULT_THRESHOLD == 0.535


def test_strategy_name_is_ml():
    import config as cfg
    assert cfg.STRATEGY_NAME == 'ml'


def test_ml_strategy_importable():
    from core.strategies.ml_strategy import MLStrategy
    assert MLStrategy is not None


def test_ml_strategy_registered():
    from core.strategies import get_strategy
    s = get_strategy('ml')
    assert s is not None
