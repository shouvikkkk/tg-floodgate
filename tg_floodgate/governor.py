"""Outbound send governor: capacity checks before the API call happens."""

from __future__ import annotations

import asyncio
import heapq
import inspect
import random
import time
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple, Union

from .errors import FloodBlocked, QueueFull, QueueTimeout
from .limits import Limits
from .scopes import Scope
from .storage import Storage


class Priority(IntEnum):
    """Send priority. Lower value = served first when capacity is contended."""

    HIGH = 0      # interactive replies — never starved by campaigns
    NORMAL = 1    # regular bot traffic
    LOW = 2       # broadcasts / bulk notifications


@dataclass
class GovernorConfig:
    limits: Limits = field(default_factory=Limits)
    namespace: str = "default"  # isolates keys per bot token / deployment
    max_wait: float = 120.0     # give up waiting for capacity after this
    max_queue_depth: int = 2000  # waiters per scope before QueueFull
    poll_interval: float = 0.05  # waiter turn-check cadence
    release_jitter: float = 0.75  # random extra block time after a 429
    on_flood: Optional[Callable[..., Any]] = None  # (scope, retry_after)
    on_wait: Optional[Callable[..., Any]] = None   # (scope, wait_seconds)


def _retry_after_of(exc: BaseException) -> Optional[float]:
    """Extract ``retry_after`` from the shapes Telegram errors come in."""
    for attr in ("retry_after",):
        v = getattr(exc, attr, None)
        if isinstance(v, (int, float)) and v >= 0:
            return float(v)
    params = getattr(exc, "parameters", None)
    if isinstance(params, dict):
        v = params.get("retry_after")
        if isinstance(v, (int, float)) and v >= 0:
            return float(v)
    # some wrappers nest the raw response
    resp = getattr(exc, "response", None)
    if isinstance(resp, dict):
        params = resp.get("parameters") or {}
        v = params.get("retry_after")
        if isinstance(v, (int, float)) and v >= 0:
            return float(v)
    return None


class Governor:
    """Paces outbound Telegram API calls across scopes, processes and priorities.

    Usage::

        gov = Governor(MemoryBackend())
        await gov.run(bot.send_message, Scope.chat(chat_id), Priority.HIGH,
                      chat_id, "hello")
    """

    def __init__(self, storage: Storage, config: Optional[GovernorConfig] = None):
        self.storage = storage
        self.config = config or GovernorConfig()
        # scope key -> heap of (priority, seq) waiters queued for a turn
        self._waiters: Dict[str, list] = {}
        self._seq = 0

    # -- scope planning --------------------------------------------------------

    def _plan(self, scopes: Union[str, List[str]]) -> List[Tuple[str, int, float]]:
        """Expand scope(s) into ordered ``(backend_key, limit, window)`` checks.

        Every send is always checked against the per-bot global cap first,
        then its own scope — mirroring how Telegram enforces the limits.
        """
        if isinstance(scopes, str):
            scopes = [scopes]
        lim = self.config.limits
        ns = self.config.namespace
        plan: List[Tuple[str, int, float]] = []
        seen = set()

        def add(kind: str, limit: int, window: float) -> None:
            key = f"tgf:{ns}:{kind}"
            if key not in seen:
                seen.add(key)
                plan.append((key, limit, window))

        for s in scopes:
            if s == Scope.GLOBAL:
                add("global", lim.global_per_second, 1.0)
            elif s.startswith("chat:"):
                add("global", lim.global_per_second, 1.0)
                add(s, lim.chat_per_second, lim.chat_window)
            elif s.startswith("group:"):
                add("global", lim.global_per_second, 1.0)
                add(s, lim.group_per_minute, lim.group_window)
            elif s == Scope.BROADCAST:
                add("global", lim.global_per_second, 1.0)
                add(s, 1, lim.broadcast_interval)
            else:  # unknown scope: treat chat-like, never silently unpaced
                add("global", lim.global_per_second, 1.0)
                add(s, lim.chat_per_second, lim.chat_window)
        return plan

    # -- public API ------------------------------------------------------------

    async def acquire(
        self, scope: Union[str, List[str]], priority: Priority = Priority.NORMAL
    ) -> None:
        """Wait for send capacity on ``scope``, then record the send.

        Raises :class:`FloodBlocked` if the scope is under a learned 429
        block, :class:`QueueFull` if too many waiters pile up, and
        :class:`QueueTimeout` if capacity never frees within ``max_wait``.
        """
        if isinstance(priority, int):
            priority = Priority(priority)
        plan = self._plan(scope)
        deadline = time.monotonic() + self.config.max_wait
        waiter_key: Optional[str] = None
        entry: Optional[tuple] = None
        try:
            while True:
                now = time.monotonic()
                for key, _limit, _window in plan:
                    remaining = await self.storage.blocked_remaining(key, now)
                    if remaining > 0:
                        raise FloodBlocked(scope, key, remaining)
                allowed, wait, contended = await self.storage.try_acquire_multi(
                    plan, now
                )
                if allowed:
                    return
                await self._emit(self.config.on_wait, scope, wait)
                key = contended or plan[0][0]
                if waiter_key != key:
                    # contended scope changed: move the waiter registration
                    if entry is not None:
                        self._unregister_waiter(waiter_key, entry)
                    entry = self._register_waiter(key, priority)
                    waiter_key = key
                await self._await_turn(waiter_key, entry, deadline)
                # Brief pause before re-probing. sleep(0) still yields to the
                # event loop, so even a zero-wait deny can't hot-spin.
                await asyncio.sleep(min(max(wait, 0.0), self.config.poll_interval))
        finally:
            if entry is not None:
                self._unregister_waiter(waiter_key, entry)

    async def run(
        self,
        func: Callable[..., Any],
        scope: Union[str, List[str]],
        priority: Priority = Priority.NORMAL,
        *args,
        **kwargs,
    ) -> Any:
        """Acquire capacity, call ``func(*args, **kwargs)`` (sync or async),
        and learn from 429s: any exception carrying ``retry_after`` hard-blocks
        the scope via :meth:`note_flood` before the error is re-raised."""
        await self.acquire(scope, priority)
        try:
            res = func(*args, **kwargs)
            if inspect.isawaitable(res):
                res = await res
            return res
        except Exception as exc:
            retry_after = _retry_after_of(exc)
            if retry_after is not None:
                await self.note_flood(scope, retry_after)
            raise

    async def note_flood(
        self, scope: Union[str, List[str]], retry_after: float,
        jitter: Optional[float] = None,
    ) -> float:
        """Record that Telegram answered 429 with ``retry_after`` seconds.

        Hard-blocks the scope (and the global key) until expiry, with a
        little jitter so a fleet of instances doesn't stampede the moment the
        block lifts. Returns the effective block length.
        """
        plan = self._plan(scope)
        j = self.config.release_jitter if jitter is None else jitter
        extra = random.random() * j if j and j > 0 else 0.0
        until = time.monotonic() + max(0.0, retry_after) + extra
        for key, _limit, _window in plan:
            await self.storage.block_until(key, until)
        await self._emit(self.config.on_flood, scope, retry_after)
        return until - time.monotonic()

    async def map_broadcast(self, items, send, scope_of,
                            priority: Priority = Priority.LOW):
        """Drain ``items`` one by one at broadcast priority.

        Each item is sent via ``await send(item)`` under
        ``[scope_of(item), Scope.broadcast()]`` — per-chat limits, campaign
        pacing and the global cap all apply. Because broadcasts queue at
        :attr:`Priority.LOW`, interactive HIGH/NORMAL traffic always jumps the
        queue instead of stalling behind a campaign.

        Async generator yielding ``(item, result)``.
        """
        for item in items:
            result = await self.run(
                send, [scope_of(item), Scope.broadcast()], priority, item
            )
            yield item, result

    async def close(self) -> None:
        await self.storage.close()

    # -- priority turn queue ---------------------------------------------------

    def _register_waiter(self, key: str, priority: Priority) -> tuple:
        """Queue one waiter for ``key``; the entry stays put across probes."""
        queue = self._waiters.setdefault(key, [])
        if len(queue) >= self.config.max_queue_depth:
            raise QueueFull(
                f"floodgate: {len(queue)} waiters queued for {key} "
                f"(max_queue_depth={self.config.max_queue_depth})"
            )
        self._seq += 1
        entry = (int(priority), self._seq)
        heapq.heappush(queue, entry)
        return entry

    def _unregister_waiter(self, key: Optional[str], entry: tuple) -> None:
        queue = self._waiters.get(key) if key else None
        if not queue:
            return
        try:
            queue.remove(entry)
        except ValueError:
            pass
        else:
            heapq.heapify(queue)
        if not queue:
            self._waiters.pop(key, None)

    async def _await_turn(self, key: str, entry: tuple, deadline: float) -> None:
        """Block until this waiter is the highest-priority waiter for ``key``.

        Strict priority: a HIGH waiter that arrives later still probes for
        capacity before an already-queued LOW waiter. Within one priority,
        FIFO by arrival sequence. The entry stays registered the whole time,
        so only the head waiter ever probes the backend.
        """
        while True:
            if time.monotonic() >= deadline:
                raise QueueTimeout(
                    f"floodgate: no capacity on {key} within "
                    f"{self.config.max_wait}s"
                )
            queue = self._waiters.get(key, [])
            if queue and queue[0] == entry:
                return
            await asyncio.sleep(self.config.poll_interval)

    # -- hooks -----------------------------------------------------------------

    async def _emit(self, hook: Optional[Callable[..., Any]], *args) -> None:
        if hook is None:
            return
        res = hook(*args)
        if inspect.isawaitable(res):
            await res
