"""MemoryBackend: sliding-window semantics, driven by a fake clock."""

import pytest

from tg_floodgate import MemoryBackend


@pytest.fixture
def mem():
    return MemoryBackend()


async def test_allows_up_to_limit_then_denies_with_wait(mem):
    assert (await mem.try_acquire_multi([("k", 2, 10.0)], 100.0))[0]
    assert (await mem.try_acquire_multi([("k", 2, 10.0)], 100.0))[0]
    allowed, wait, key = await mem.try_acquire_multi([("k", 2, 10.0)], 100.0)
    assert not allowed
    assert key == "k"
    assert wait == pytest.approx(10.0)


async def test_window_boundary_is_exact(mem):
    # event at t=100, limit 1, window 10: expires exactly at t=110 (<= cutoff)
    assert (await mem.try_acquire_multi([("k", 1, 10.0)], 100.0))[0]
    assert not (await mem.try_acquire_multi([("k", 1, 10.0)], 109.999))[0]
    assert (await mem.try_acquire_multi([("k", 1, 10.0)], 110.0))[0]


async def test_denied_multi_acquire_leaves_no_phantom_slots(mem):
    assert (await mem.try_acquire_multi([("chat:1", 1, 10.0)], 100.0))[0]
    allowed, _, _ = await mem.try_acquire_multi(
        [("g", 30, 1.0), ("chat:1", 1, 10.0)], 100.0
    )
    assert not allowed
    # "g" must be untouched: a full 30-slot burst still fits
    for _ in range(30):
        assert (await mem.try_acquire_multi([("g", 30, 1.0)], 100.0))[0]
    assert not (await mem.try_acquire_multi([("g", 30, 1.0)], 100.0))[0]


async def test_reports_slowest_scope(mem):
    # chat:1 frees at t=105, g frees at t=110 -> wait ~10 on "g"
    await mem.try_acquire_multi([("g", 1, 10.0)], 100.0)
    await mem.try_acquire_multi([("chat:1", 1, 5.0)], 100.0)
    allowed, wait, key = await mem.try_acquire_multi(
        [("g", 1, 10.0), ("chat:1", 1, 5.0)], 100.0
    )
    assert not allowed
    assert key == "g"
    assert wait == pytest.approx(10.0)


async def test_block_never_shortens_and_clears(mem):
    await mem.block_until("k", 200.0)
    await mem.block_until("k", 150.0)  # shorter block must not win
    assert await mem.blocked_remaining("k", 100.0) == pytest.approx(100.0)
    await mem.clear_block("k")
    assert await mem.blocked_remaining("k", 100.0) == 0.0


async def test_expired_block_reads_zero(mem):
    await mem.block_until("k", 150.0)
    assert await mem.blocked_remaining("k", 200.0) == 0.0
