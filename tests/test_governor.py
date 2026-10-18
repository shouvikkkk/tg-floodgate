"""Governor: scopes, flood learning, priority, pacing, queue limits."""

import asyncio
import time

import pytest

from tg_floodgate import (
    FloodBlocked,
    FloodSignal,
    Governor,
    GovernorConfig,
    Limits,
    MemoryBackend,
    Priority,
    QueueFull,
    QueueTimeout,
    Scope,
    SQLiteBackend,
)


def make_gov(**kw):
    cfg = GovernorConfig(
        limits=Limits(
            # stay out of the way unless a test wants it
            global_per_second=kw.pop("global_per_second", 1000),
            chat_per_second=kw.pop("chat_per_second", 1),
            chat_window=kw.pop("chat_window", 0.3),
            broadcast_interval=kw.pop("broadcast_interval", 0.3),
        ),
        max_wait=kw.pop("max_wait", 5.0),
        poll_interval=0.01,
        release_jitter=0.0,  # deterministic in tests
        **kw,
    )
    return Governor(MemoryBackend(), cfg)


async def test_chat_scope_paces_second_send():
    gov = make_gov()
    t0 = time.monotonic()
    await gov.acquire(Scope.chat(1))
    await gov.acquire(Scope.chat(1))
    assert time.monotonic() - t0 >= 0.25


async def test_different_chats_do_not_block_each_other():
    gov = make_gov()
    t0 = time.monotonic()
    await gov.acquire(Scope.chat(1))
    await gov.acquire(Scope.chat(2))
    assert time.monotonic() - t0 < 0.25


async def test_global_cap_spans_chats():
    gov = make_gov(global_per_second=2)
    t0 = time.monotonic()
    await gov.acquire(Scope.chat(1))
    await gov.acquire(Scope.chat(2))
    await gov.acquire(Scope.chat(3))  # global window full -> waits
    assert time.monotonic() - t0 >= 0.9


async def test_run_returns_result_and_supports_sync_fn():
    gov = make_gov()

    async def afunc(a, b):
        return a + b

    assert await gov.run(afunc, Scope.chat(1), Priority.NORMAL, 2, 3) == 5
    assert await gov.run(lambda x: x * 2, Scope.chat(1), Priority.NORMAL, 21) == 42


async def test_429_is_learned_and_blocks_scope():
    floods = []
    waits = []
    gov = make_gov(
        on_flood=lambda s, ra: floods.append((s, ra)),
        on_wait=lambda s, w: waits.append((s, w)),
    )

    async def boom():
        raise FloodSignal(5, "too many requests")

    with pytest.raises(FloodSignal):
        await gov.run(boom, Scope.chat(7))

    assert floods == [(Scope.chat(7), 5.0)]
    # the scope is now hard-blocked: acquire raises immediately, no waiting
    with pytest.raises(FloodBlocked) as ei:
        await gov.acquire(Scope.chat(7))
    assert ei.value.retry_after == pytest.approx(5.0, abs=0.5)
    # conservative by design: the global key is blocked too, so even an
    # unrelated chat raises (on the global key) until the block lifts
    with pytest.raises(FloodBlocked) as ei:
        await gov.acquire(Scope.chat(8))
    assert "global" in ei.value.scope_key


async def test_two_instances_share_global_cap_via_sqlite(tmp_path):
    """The distributed story: two processes, one backend, one global budget."""
    backend = SQLiteBackend(str(tmp_path / "shared.db"))

    def gov():
        return Governor(
            backend,
            GovernorConfig(
                limits=Limits(global_per_second=2, chat_per_second=100),
                poll_interval=0.01,
                release_jitter=0.0,
                max_wait=5.0,
            ),
        )

    g1, g2 = gov(), gov()
    t0 = time.monotonic()
    await g1.acquire(Scope.chat(1))
    await g2.acquire(Scope.chat(2))
    # global window (2/sec) is full across *both* instances -> this waits ~1s
    await g1.acquire(Scope.chat(3))
    assert time.monotonic() - t0 >= 0.9
    await g1.close()


async def test_flood_block_shared_across_instances(tmp_path):
    backend = SQLiteBackend(str(tmp_path / "s.db"))
    g1 = Governor(backend, GovernorConfig(release_jitter=0.0))
    g2 = Governor(backend, GovernorConfig(release_jitter=0.0))
    await g1.note_flood(Scope.group(-100), 30)
    with pytest.raises(FloodBlocked) as ei:
        await g2.acquire(Scope.group(-100))
    assert ei.value.retry_after == pytest.approx(30.0, abs=1.0)
    await g1.close()


async def test_aiogram_style_error_shapes_learned():
    gov = make_gov()

    class TelegramRetryAfter(Exception):
        def __init__(self, retry_after):
            self.retry_after = retry_after

    class ParamsError(Exception):
        def __init__(self):
            self.parameters = {"retry_after": 12}

    # each error shape is learned through run() ...
    for exc, secs in ((TelegramRetryAfter(7), 7.0), (ParamsError(), 12.0)):
        gov2 = make_gov()

        async def boom(exc=exc):
            raise exc

        with pytest.raises(Exception):
            await gov2.run(boom, Scope.chat(3))
        with pytest.raises(FloodBlocked) as ei:
            await gov2.acquire(Scope.chat(3))
        assert ei.value.retry_after == pytest.approx(secs, abs=1.0)
    # ... and a longer block never gets shortened by a shorter one
    await gov.note_flood(Scope.chat(3), 12)
    await gov.note_flood(Scope.chat(3), 7)
    with pytest.raises(FloodBlocked) as ei:
        await gov.acquire(Scope.chat(3))
    assert ei.value.retry_after == pytest.approx(12.0, abs=1.0)


async def test_priority_high_jumps_queue():
    gov = make_gov(chat_window=0.6)
    order = []

    await gov.acquire(Scope.chat(1))  # consume the single slot

    async def waiter(prio, name):
        await gov.acquire(Scope.chat(1), prio)
        order.append(name)

    t_low = asyncio.create_task(waiter(Priority.LOW, "low"))
    await asyncio.sleep(0.05)  # low registers first...
    t_high = asyncio.create_task(waiter(Priority.HIGH, "high"))
    await asyncio.gather(t_low, t_high)
    assert order == ["high", "low"]  # ...but high is served first


async def test_broadcast_pacing_is_deterministic():
    gov = make_gov(broadcast_interval=0.3)
    stamps = []

    async def send(item):
        stamps.append(time.monotonic())
        return item

    results = [
        r async for _, r in gov.map_broadcast(
            [1, 2, 3], send, lambda i: Scope.chat(100 + i)
        )
    ]
    assert results == [1, 2, 3]
    gaps = [b - a for a, b in zip(stamps, stamps[1:])]
    assert all(g >= 0.25 for g in gaps), gaps
    assert stamps[-1] - stamps[0] >= 0.55


async def test_queue_timeout():
    gov = make_gov(chat_window=30.0, max_wait=0.2)
    await gov.acquire(Scope.chat(1))
    with pytest.raises(QueueTimeout):
        await gov.acquire(Scope.chat(1))


async def test_queue_full():
    gov = make_gov(chat_window=30.0, max_wait=5.0, max_queue_depth=1)
    await gov.acquire(Scope.chat(1))

    async def waiter():
        await gov.acquire(Scope.chat(1), Priority.LOW)

    t = asyncio.create_task(waiter())
    await asyncio.sleep(0.1)  # waiter is now queued
    with pytest.raises(QueueFull):
        await gov.acquire(Scope.chat(1))
    t.cancel()
    try:
        await t
    except asyncio.CancelledError:
        pass


async def test_on_wait_hook_fires():
    seen = []
    gov = make_gov(chat_window=0.3, on_wait=lambda s, w: seen.append((s, w)))
    await gov.acquire(Scope.chat(1))
    await gov.acquire(Scope.chat(1))
    assert seen and seen[0][0] == Scope.chat(1)
    assert seen[0][1] > 0


async def test_group_scope_uses_per_minute_limit():
    gov = make_gov()
    plan = gov._plan(Scope.group(-42))
    keys = [k for k, _, _ in plan]
    assert any("group:-42" in k for k in keys)
    by_key = {k: (lim, win) for k, lim, win in plan}
    gkey = next(k for k in by_key if "group:-42" in k)
    assert by_key[gkey] == (20, 60.0)


async def test_unknown_scope_is_paced_not_open():
    gov = make_gov()
    plan = gov._plan("mystery")
    assert len(plan) == 2  # global + paced fallback
