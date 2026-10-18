"""SQLiteBackend: same sliding-window semantics + persistence + kv."""

import pytest

from tg_floodgate import SQLiteBackend


@pytest.fixture
def db(tmp_path):
    b = SQLiteBackend(str(tmp_path / "t.db"))
    yield b


async def test_sliding_window_semantics(db):
    assert (await db.try_acquire_multi([("k", 2, 10.0)], 100.0))[0]
    assert (await db.try_acquire_multi([("k", 2, 10.0)], 100.0))[0]
    allowed, wait, key = await db.try_acquire_multi([("k", 2, 10.0)], 100.0)
    assert not allowed and key == "k"
    assert wait == pytest.approx(10.0)
    # exact window boundary
    assert (await db.try_acquire_multi([("k", 1, 10.0)], 110.0))[0]


async def test_atomic_multi_scope_no_phantom(db):
    assert (await db.try_acquire_multi([("chat:1", 1, 10.0)], 100.0))[0]
    allowed, _, _ = await db.try_acquire_multi(
        [("g", 30, 1.0), ("chat:1", 1, 10.0)], 100.0
    )
    assert not allowed
    for _ in range(30):
        assert (await db.try_acquire_multi([("g", 30, 1.0)], 100.0))[0]
    assert not (await db.try_acquire_multi([("g", 30, 1.0)], 100.0))[0]


async def test_blocks_survive_reopen(tmp_path):
    p = str(tmp_path / "t.db")
    b1 = SQLiteBackend(p)
    await b1.block_until("tgf:default:chat:9", 500.0)
    await b1.close()
    b2 = SQLiteBackend(p)
    assert await b2.blocked_remaining("tgf:default:chat:9", 100.0) == pytest.approx(400.0)
    await b2.close()


async def test_events_survive_reopen_and_prune(tmp_path):
    p = str(tmp_path / "t.db")
    b1 = SQLiteBackend(p)
    assert (await b1.try_acquire_multi([("k", 1, 10.0)], 100.0))[0]
    await b1.close()
    b2 = SQLiteBackend(p)
    assert not (await b2.try_acquire_multi([("k", 1, 10.0)], 105.0))[0]
    assert (await b2.try_acquire_multi([("k", 1, 10.0)], 110.0))[0]
    await b2.close()


async def test_kv_roundtrip(db):
    assert await db.kv_get("nope") is None
    await db.kv_set("a", "1")
    assert await db.kv_get("a") == "1"
    await db.kv_set("a", "2")
    assert await db.kv_get("a") == "2"
    await db.kv_delete("a")
    assert await db.kv_get("a") is None


async def test_block_never_shortens(db):
    await db.block_until("k", 200.0)
    await db.block_until("k", 150.0)
    assert await db.blocked_remaining("k", 100.0) == pytest.approx(100.0)
