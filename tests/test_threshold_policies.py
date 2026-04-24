import asyncio
import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import config as cfg
from db.models import init_db, migrate_db
from db import queries


def test_truncate_probability_bucket():
    assert queries.truncate_probability_bucket(0.5399) == "0.53"
    assert queries.truncate_probability_bucket("0.5300") == "0.53"
    assert queries.truncate_probability_bucket(1.0) == "1.00"


def test_threshold_policy_roundtrip_and_stats():
    async def _run():
        fd, path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        old = cfg.DB_PATH
        cfg.DB_PATH = path
        try:
            await init_db(path)
            await migrate_db(path)
            await queries.set_threshold_policy('0.53', 'real', 'INVERT')
            await queries.set_threshold_policy('0.53', 'demo', 'BLOCK')
            real = await queries.decide_threshold_route(original_side='Up', probability=0.5381, bucket='0.53', mode='real')
            demo = await queries.decide_threshold_route(original_side='Up', probability=0.5381, bucket='0.53', mode='demo')
            assert real['routed_side'] == 'Down'
            assert real['policy'] == 'INVERT'
            assert demo['blocked'] is True
            sid = await queries.insert_signal(
                '2026-01-01 00:00:00 UTC', '2026-01-01 00:05:00 UTC', 123,
                'Up', 0.6, 0.4, False, False, 'p', 0.61, 0.39, '0.61', 0.61,
                'FOLLOW', 'INVERT', 'Up', 'slug'
            )
            t1 = await queries.insert_trade(
                sid, '2026-01-01 00:00:00 UTC', '2026-01-01 00:05:00 UTC', 'Up',
                0.6, 10, status='filled', is_demo=False, routing_mode='real',
                routing_policy='FOLLOW', original_side='Up', routed_side='Up',
                policy_bucket='0.61', policy_probability=0.61
            )
            t2 = await queries.insert_trade(
                sid, '2026-01-01 00:00:00 UTC', '2026-01-01 00:05:00 UTC', 'Down',
                0.4, 10, status='filled', is_demo=True, routing_mode='demo',
                routing_policy='INVERT', original_side='Up', routed_side='Down',
                policy_bucket='0.61', policy_probability=0.61
            )
            await queries.resolve_trade(t1, 'Up', True, 5.0)
            await queries.resolve_trade(t2, 'Up', False, -10.0)
            real_stats = await queries.get_threshold_stats('real')
            demo_stats = await queries.get_threshold_stats('demo')
            assert real_stats[0]['bucket'] == '0.61'
            assert demo_stats[0]['policy'] == 'INVERT'
            assert len(await queries.get_trades_by_signal(sid)) == 2
        finally:
            cfg.DB_PATH = old
            try:
                os.remove(path)
            except FileNotFoundError:
                pass
    asyncio.run(_run())
