"""Storage interface shared by all backends.

Backends keep two things:

* **sliding-window event logs** — one timestamped entry per permitted send,
  per scope key; and
* **flood blocks** — hard "do not send until T" markers learned from 429s,
  shared across every process using the same backend.

Time is always passed in explicitly (``time.monotonic()`` seconds) so tests
can drive backends with a fake clock.
"""

from abc import ABC, abstractmethod
from typing import List, Optional, Tuple


class Storage(ABC):
    """Async storage contract for the governor."""

    #: True when the backend also offers the kv_* helpers (used by the CLI
    #: to persist broadcast progress).
    supports_kv: bool = False

    @abstractmethod
    async def try_acquire_multi(
        self, specs: List[Tuple[str, int, float]], now: float
    ) -> Tuple[bool, float, Optional[str]]:
        """Atomically check *all* ``(key, limit, window)`` specs.

        Records one event per key only if every scope has capacity —
        all-or-nothing, so a denied acquire never leaves phantom slots.

        Returns ``(allowed, wait_seconds, contended_key)``. When denied,
        ``wait_seconds`` is the longest time until any scope frees up and
        ``contended_key`` names that scope.
        """

    @abstractmethod
    async def block_until(self, key: str, until_ts: float) -> None:
        """Hard-block ``key`` until ``until_ts`` (monotonic seconds).

        Never shortens an existing longer block.
        """

    @abstractmethod
    async def blocked_remaining(self, key: str, now: float) -> float:
        """Seconds left on ``key``'s block, or 0.0."""

    @abstractmethod
    async def clear_block(self, key: str) -> None:
        """Remove ``key``'s block, if any."""

    # -- optional key/value helpers (broadcast resume state) -----------------

    async def kv_get(self, k: str) -> Optional[str]:
        raise NotImplementedError

    async def kv_set(self, k: str, v: str) -> None:
        raise NotImplementedError

    async def kv_delete(self, k: str) -> None:
        raise NotImplementedError

    async def close(self) -> None:
        """Release backend resources (connections, files)."""
