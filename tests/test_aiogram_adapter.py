"""aiogram adapter: make_request wrapping with a fake session (no aiogram needed)."""

import pytest

from tg_floodgate import (
    FloodBlocked,
    FloodSignal,
    Governor,
    GovernorConfig,
    MemoryBackend,
    Priority,
    Scope,
    install_aiogram,
)


class FakeMethod:
    def __init__(self, chat_id=None):
        self.chat_id = chat_id


class FakeSession:
    def __init__(self):
        self.calls = []

    async def make_request(self, bot, method, timeout=None):
        self.calls.append((method, timeout))
        return {"ok": True, "method": method}


class FakeBot:
    def __init__(self):
        self.session = FakeSession()


async def test_install_paces_outbound_calls():
    gov = Governor(MemoryBackend(), GovernorConfig(release_jitter=0.0))
    bot = FakeBot()
    install_aiogram(gov, bot)

    method = FakeMethod(chat_id=5)
    res = await bot.session.make_request(bot, method)
    assert res["ok"] is True
    assert len(bot.session.calls) == 1
    # scope was derived from chat_id
    assert gov._plan(Scope.chat(5))[1][0].endswith("chat:5")


async def test_install_learns_429_from_wrapped_call():
    gov = Governor(MemoryBackend(), GovernorConfig(release_jitter=0.0))

    class FlakySession(FakeSession):
        async def make_request(self, bot, method, timeout=None):
            raise FloodSignal(4, "retry later")

    bot = FakeBot()
    bot.session = FlakySession()
    install_aiogram(gov, bot)

    with pytest.raises(FloodSignal):
        await bot.session.make_request(bot, FakeMethod(chat_id=9))
    with pytest.raises(FloodBlocked):
        await gov.acquire(Scope.chat(9))


async def test_custom_scope_and_priority():
    gov = Governor(MemoryBackend(), GovernorConfig(release_jitter=0.0))
    bot = FakeBot()
    seen = {}
    orig_run = gov.run

    async def spy_run(func, scope, priority, *a, **k):
        seen["scope"] = scope
        seen["priority"] = priority
        return await orig_run(func, scope, priority, *a, **k)

    gov.run = spy_run
    install_aiogram(
        gov, bot,
        scope=lambda m: Scope.broadcast(),
        priority=lambda m: Priority.LOW,
    )
    await bot.session.make_request(bot, FakeMethod())
    assert seen["scope"] == Scope.broadcast()
    assert seen["priority"] == Priority.LOW


async def test_method_without_chat_id_uses_global_scope():
    gov = Governor(MemoryBackend(), GovernorConfig(release_jitter=0.0))
    bot = FakeBot()
    install_aiogram(gov, bot)
    await bot.session.make_request(bot, FakeMethod(chat_id=None))
    assert len(bot.session.calls) == 1
