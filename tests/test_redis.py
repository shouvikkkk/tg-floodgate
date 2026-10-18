"""RedisBackend against fakeredis.

fakeredis does not implement EVAL, so these tests exercise the WATCH/MULTI
fallback path — including the automatic downgrade detection.
"""

import pytest

fakeredis = pytest.importorskip("fakeredis.aioredis")

from tg_floodgate import RedisBackend


@pytest.fixture
async def rdb():
    client = fakeredis.FakeRedis()
    b = RedisBackend(redis=client)
    yield b
    await b.close()


async def test_falls_back_from_lua(rdb):
    # first acquire triggers the eval attempt -> downgrade
    assert (await rdb.try_acquire_multi([("k", 1, 10.0)], 100.0))[0]
    assert rdb._lua_ok is False


async def test_sliding_window_semantics(rdb):
    assert (await rdb.try_acquire_multi([("k", 2, 10.0)], 100.0))[0]
    assert (await rdb.try_acquire_multi([("k", 2, 10.0)], 100.0))[0]
    allowed, wait, key = await rdb.try_acquire_multi([("k", 2, 10.0)], 100.0)
    assert not allowed and key == "k"
    assert wait == pytest.approx(10.0, abs=0.5)


async def test_window_expiry(rdb):
    assert (await rdb.try_acquire_multi([("k", 1, 10.0)], 100.0))[0]
    assert not (await rdb.try_acquire_multi([("k", 1, 10.0)], 105.0))[0]
    assert (await rdb.try_acquire_multi([("k", 1, 10.0)], 111.0))[0]


async def test_atomic_multi_scope(rdb):
    assert (await rdb.try_acquire_multi([("chat:1", 1, 10.0)], 100.0))[0]
    allowed, _, _ = await rdb.try_acquire_multi(
        [("g", 30, 1.0), ("chat:1", 1, 10.0)], 100.0
    )
    assert not allowed
    for _ in range(30):
        assert (await rdb.try_acquire_multi([("g", 30, 1.0)], 100.0))[0]
    assert not (await rdb.try_acquire_multi([("g", 30, 1.0)], 100.0))[0]


async def test_blocks_roundtrip(rdb):
    await rdb.block_until("k", 200.0)
    await rdb.block_until("k", 150.0)  # must not shorten
    assert await rdb.blocked_remaining("k", 100.0) == pytest.approx(100.0)
    await rdb.clear_block("k")
    assert await rdb.blocked_remaining("k", 100.0) == 0.0


async def test_concurrent_acquires_share_one_slot(rdb):
    """Two coroutines racing for a single slot: exactly one wins."""
    import asyncio

    results = await asyncio.gather(
        rdb.try_acquire_multi([("race", 1, 60.0)], 100.0),
        rdb.try_acquire_multi([("race", 1, 60.0)], 100.0),
    )
    assert sum(1 for ok, _, _ in results if ok) == 1
