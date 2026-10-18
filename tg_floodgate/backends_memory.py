"""In-process backend: zero dependencies, asyncio-safe."""

import asyncio
from collections import deque
from typing import Dict, List, Optional, Tuple

from .storage import Storage


class MemoryBackend(Storage):
    """Sliding windows + flood blocks in plain Python structures.

    Guarded by a single ``asyncio.Lock`` so concurrent coroutines in one
    process see atomic check-and-record. For multi-process / multi-instance
    deployments use :class:`RedisBackend` or :class:`SQLiteBackend` instead.
    """

    def __init__(self) -> None:
        self._events: Dict[str, deque] = {}
        self._blocks: Dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def try_acquire_multi(
        self, specs: List[Tuple[str, int, float]], now: float
    ) -> Tuple[bool, float, Optional[str]]:
        async with self._lock:
            waits = []
            for key, limit, window in specs:
                dq = self._events.setdefault(key, deque())
                cutoff = now - window
                while dq and dq[0] <= cutoff:
                    dq.popleft()
                if len(dq) >= limit:
                    waits.append((dq[0] + window - now, key))
            if waits:
                waits.sort(key=lambda w: w[0])
                wait, key = waits[-1]
                return False, max(wait, 0.0), key
            for key, _limit, _window in specs:
                self._events[key].append(now)
            return True, 0.0, None

    async def block_until(self, key: str, until_ts: float) -> None:
        async with self._lock:
            self._blocks[key] = max(self._blocks.get(key, 0.0), until_ts)

    async def blocked_remaining(self, key: str, now: float) -> float:
        async with self._lock:
            return max(0.0, self._blocks.get(key, 0.0) - now)

    async def clear_block(self, key: str) -> None:
        async with self._lock:
            self._blocks.pop(key, None)
