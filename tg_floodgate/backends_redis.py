"""Distributed backend: atomic sliding windows in Redis.

Primary path is a single Lua script that prunes, checks **and** records
every scope in one atomic step — no phantom slots when an acquire spans
several scopes. Some Redis-compatible stores (and test doubles) don't
implement scripting; on the first ``unknown command 'eval'`` the backend
permanently falls back to an optimistic-locking WATCH/MULTI implementation
with identical semantics.
"""

import asyncio
import time
import uuid
from typing import List, Optional, Tuple

from .storage import Storage

# KEYS: one sorted-set per scope.
# ARGV: now, (window, limit) per key..., member.
# Returns {1, 0, 0} on success, {0, max_wait, 1-based index of slowest key} on deny.
_LUA_ACQUIRE = """
local now = tonumber(ARGV[1])
local nkeys = #KEYS
local member = ARGV[#ARGV]
local m = 0
local mi = 0
for i = 1, nkeys do
  local window = tonumber(ARGV[1 + (i - 1) * 2 + 1])
  local limit = tonumber(ARGV[1 + (i - 1) * 2 + 2])
  redis.call('ZREMRANGEBYSCORE', KEYS[i], 0, now - window)
  local count = redis.call('ZCARD', KEYS[i])
  if count >= limit then
    local oldest = redis.call('ZRANGE', KEYS[i], 0, 0, 'WITHSCORES')
    local w = 0
    if #oldest >= 2 then
      w = tonumber(oldest[2]) + window - now
      if w < 0 then w = 0 end
    end
    if w >= m then
      m = w
      mi = i
    end
  end
end
if mi > 0 then
  return {0, m, mi}
end
for i = 1, nkeys do
  local window = tonumber(ARGV[1 + (i - 1) * 2 + 1])
  redis.call('ZADD', KEYS[i], now, member .. ':' .. i)
  redis.call('PEXPIRE', KEYS[i], math.ceil(window * 1000) + 2000)
end
return {1, 0, 0}
"""

_LUA_BLOCK = (
    "local cur = tonumber(redis.call('GET', KEYS[1]) or 0) "
    "local new = tonumber(ARGV[1]) "
    "if new > cur then "
    "  redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2]) "
    "end "
    "return 1"
)


class RedisBackend(Storage):
    """Redis-backed storage. ``redis`` may be an existing
    ``redis.asyncio.Redis`` client, or pass ``url=`` / ``kwargs`` to build one
    (requires the ``redis`` package — an optional extra).
    """

    def __init__(self, redis=None, url: str = "redis://localhost:6379/0",
                 key_prefix: str = "tgf", **kwargs) -> None:
        self._client = redis
        self._url = url
        self._kwargs = kwargs
        self._prefix = key_prefix
        self._lua_ok: Optional[bool] = None  # unknown until first attempt

    # -- setup ---------------------------------------------------------------

    async def _ensure(self):
        if self._client is None:
            try:
                import redis.asyncio as aioredis
            except ImportError as exc:
                raise ImportError(
                    "RedisBackend needs the 'redis' package: "
                    "pip install tg-floodgate[redis]"
                ) from exc
            self._client = aioredis.from_url(self._url, **self._kwargs)
        return self._client

    def _ekey(self, key: str) -> str:
        return f"{self._prefix}:win:{key}"

    def _bkey(self, key: str) -> str:
        return f"{self._prefix}:block:{key}"

    @staticmethod
    def _no_script(exc: Exception) -> bool:
        msg = str(exc).lower()
        return "unknown command" in msg and "eval" in msg

    # -- sliding window --------------------------------------------------------

    async def try_acquire_multi(
        self, specs: List[Tuple[str, int, float]], now: float
    ) -> Tuple[bool, float, Optional[str]]:
        client = await self._ensure()
        keys = [self._ekey(k) for k, _, _ in specs]
        if self._lua_ok is not False:
            try:
                return await self._via_lua(client, keys, specs, now)
            except Exception as exc:
                if self._no_script(exc):
                    self._lua_ok = False
                else:
                    raise
        return await self._via_watch(client, keys, specs, now)

    async def _via_lua(self, client, keys, specs, now):
        argv = [repr(now)]
        for _k, limit, window in specs:
            argv += [repr(window), repr(limit)]
        argv.append(f"{now}:{uuid.uuid4().hex}")
        allowed, wait, idx = await client.eval(_LUA_ACQUIRE, len(keys), *keys, *argv)
        self._lua_ok = True
        if int(allowed) == 1:
            return True, 0.0, None
        contended = specs[int(idx) - 1][0] if idx else specs[0][0]
        return False, float(wait), contended

    async def _via_watch(self, client, keys, specs, now):
        """Optimistic locking: WATCH the keys, read, then MULTI/EXEC the
        writes. A concurrent writer aborts us with WatchError and we retry."""
        member = f"{now}:{uuid.uuid4().hex}"
        for _attempt in range(8):
            pipe = client.pipeline(transaction=True)
            try:
                await pipe.watch(*keys)
                # immediate-execution mode: reads run straight away
                counts = []
                for bkey, (_k, _limit, window) in zip(keys, specs):
                    await pipe.zremrangebyscore(bkey, 0, now - window)
                    counts.append(await pipe.zcard(bkey))
                waits = []
                for bkey, count, (_k, limit, window) in zip(keys, counts, specs):
                    if int(count) >= limit:
                        oldest = await pipe.zrange(bkey, 0, 0, withscores=True)
                        w = (
                            max(oldest[0][1] + window - now, 0.0)
                            if oldest
                            else 0.0
                        )
                        waits.append((w, _k))
                if waits:
                    waits.sort(key=lambda w: w[0])
                    await pipe.unwatch()
                    return False, waits[-1][0], waits[-1][1]
                pipe.multi()
                for i, (bkey, (_k, _limit, window)) in enumerate(zip(keys, specs)):
                    pipe.zadd(bkey, {f"{member}:{i}": now})
                    pipe.pexpire(bkey, int(window * 1000) + 2000)
                await pipe.execute()
                return True, 0.0, None
            except Exception as exc:
                if type(exc).__name__ == "WatchError":
                    await asyncio.sleep(0.005)
                    continue
                raise
            finally:
                try:
                    await pipe.reset()
                except Exception:
                    pass
        # absurdly contended: ask the caller to back off briefly
        return False, 0.05, specs[0][0]

    # -- flood blocks ----------------------------------------------------------

    async def block_until(self, key: str, until_ts: float) -> None:
        client = await self._ensure()
        bkey = self._bkey(key)
        px = max(int((until_ts - time.monotonic()) * 1000), 50)
        try:
            await client.eval(_LUA_BLOCK, 1, bkey, repr(until_ts), px)
        except Exception as exc:
            if self._no_script(exc):
                cur = await client.get(bkey)
                if cur is None or float(cur) < until_ts:
                    await client.set(bkey, repr(until_ts), px=px)
            else:
                raise

    async def blocked_remaining(self, key: str, now: float) -> float:
        client = await self._ensure()
        raw = await client.get(self._bkey(key))
        if raw is None:
            return 0.0
        return max(0.0, float(raw) - now)

    async def clear_block(self, key: str) -> None:
        client = await self._ensure()
        await client.delete(self._bkey(key))

    async def close(self) -> None:
        if self._client is not None:
            try:
                aclose = getattr(self._client, "aclose", None)
                if aclose is not None:
                    await aclose()
                else:
                    await self._client.close()
            except Exception:
                pass
