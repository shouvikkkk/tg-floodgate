"""Local-first persistent backend: stdlib sqlite3, no server needed.

Shares sliding windows and flood blocks between processes on the same
machine (e.g. a bot plus a broadcast CLI), and survives restarts — the
event log doubles as the resume state for campaigns.
"""

import asyncio
import sqlite3
import threading
from typing import List, Optional, Tuple

from .storage import Storage

_SCHEMA = """
CREATE TABLE IF NOT EXISTS fg_events (
    scope TEXT NOT NULL,
    ts REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS fg_events_scope_ts ON fg_events (scope, ts);
CREATE TABLE IF NOT EXISTS fg_blocks (
    scope TEXT PRIMARY KEY,
    until_ts REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS fg_kv (
    k TEXT PRIMARY KEY,
    v TEXT NOT NULL
);
"""


class SQLiteBackend(Storage):
    supports_kv = True

    def __init__(self, path: str = "floodgate.db") -> None:
        self._path = path
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA busy_timeout=5000;")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()
        # One lock: sqlite serialises writers anyway; the lock keeps our
        # BEGIN IMMEDIATE / COMMIT pairs atomic across threads.
        self._lock = threading.Lock()

    # -- sync core (runs in a worker thread) --------------------------------

    def _acquire_multi(
        self, specs: List[Tuple[str, int, float]], now: float
    ) -> Tuple[bool, float, Optional[str]]:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("BEGIN IMMEDIATE;")
            try:
                waits = []
                for key, limit, window in specs:
                    cur.execute(
                        "DELETE FROM fg_events WHERE scope = ? AND ts <= ?",
                        (key, now - window),
                    )
                    n = cur.execute(
                        "SELECT COUNT(*) FROM fg_events WHERE scope = ?",
                        (key,),
                    ).fetchone()[0]
                    if n >= limit:
                        oldest = cur.execute(
                            "SELECT MIN(ts) FROM fg_events WHERE scope = ?",
                            (key,),
                        ).fetchone()[0]
                        waits.append((max(oldest + window - now, 0.0), key))
                if waits:
                    cur.execute("ROLLBACK;")
                    waits.sort(key=lambda w: w[0])
                    wait, key = waits[-1]
                    return False, wait, key
                for key, _limit, _window in specs:
                    cur.execute(
                        "INSERT INTO fg_events (scope, ts) VALUES (?, ?)",
                        (key, now),
                    )
                cur.execute("COMMIT;")
                return True, 0.0, None
            except Exception:
                cur.execute("ROLLBACK;")
                raise

    def _block_until(self, key: str, until_ts: float) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO fg_blocks (scope, until_ts) VALUES (?, ?) "
                "ON CONFLICT(scope) DO UPDATE SET "
                "until_ts = MAX(until_ts, excluded.until_ts)",
                (key, until_ts),
            )
            self._conn.commit()

    def _blocked_remaining(self, key: str, now: float) -> float:
        with self._lock:
            row = self._conn.execute(
                "SELECT until_ts FROM fg_blocks WHERE scope = ?", (key,)
            ).fetchone()
            return max(0.0, row[0] - now) if row else 0.0

    def _clear_block(self, key: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM fg_blocks WHERE scope = ?", (key,))
            self._conn.commit()

    def _kv_get(self, k: str) -> Optional[str]:
        with self._lock:
            row = self._conn.execute(
                "SELECT v FROM fg_kv WHERE k = ?", (k,)
            ).fetchone()
            return row[0] if row else None

    def _kv_set(self, k: str, v: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO fg_kv (k, v) VALUES (?, ?) "
                "ON CONFLICT(k) DO UPDATE SET v = excluded.v",
                (k, v),
            )
            self._conn.commit()

    def _kv_delete(self, k: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM fg_kv WHERE k = ?", (k,))
            self._conn.commit()

    def _close(self) -> None:
        with self._lock:
            self._conn.commit()
            self._conn.close()

    # -- async façade --------------------------------------------------------

    async def try_acquire_multi(self, specs, now):
        return await asyncio.to_thread(self._acquire_multi, specs, now)

    async def block_until(self, key, until_ts):
        await asyncio.to_thread(self._block_until, key, until_ts)

    async def blocked_remaining(self, key, now):
        return await asyncio.to_thread(self._blocked_remaining, key, now)

    async def clear_block(self, key):
        await asyncio.to_thread(self._clear_block, key)

    async def kv_get(self, k):
        return await asyncio.to_thread(self._kv_get, k)

    async def kv_set(self, k, v):
        await asyncio.to_thread(self._kv_set, k, v)

    async def kv_delete(self, k):
        return await asyncio.to_thread(self._kv_delete, k)

    async def close(self):
        await asyncio.to_thread(self._close)
