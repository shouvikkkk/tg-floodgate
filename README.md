# tg-floodgate 🚦

A drop-in **outbound send governor** for Telegram bots: capacity checks *before* the API call, so the 429 never happens in the first place.

## The problem

Telegram's Bot API rate limits are brutal and poorly forgiving. The documented caps are roughly **1 message/sec per chat**, **20 messages/min per group**, and **~30 messages/sec across all chats combined** — and when you exceed them, Telegram doesn't just drop the extra messages:

> *"After roughly 30 messages, Telegram blocks your bot entirely."*
> — [gramio.dev/rate-limits](https://gramio.dev/rate-limits)

Worse, the standard recovery advice most bots implement — catch the 429 and retry — can backfire:

> *"If you ignore this value and send again immediately, you may extend the block in the worst case."*
> — [watch-and-alert.com](https://watch-and-alert.com/blog/telegram-bot-rate-limits-umgehen.html)

So the failure mode looks like this: your bot runs fine at low traffic, then during a spike (a broadcast campaign, a viral moment, N replicas of the same bot behind a load balancer) the instances collectively blow the 30 msg/sec global cap, Telegram blocks the **entire bot**, and every developer hand-rolls their own `retry_after` handling to dig out of it. There was no dominant Python-native, distributed, scope-aware, `retry_after`-learning outbound governor — tg-floodgate is that missing piece.

## What it does

- **Scope-aware limits** — every send is checked against the per-bot global cap (~30/s) *and* its own scope (~1/s per chat, ~20/min per group), all configurable. Unknown scopes are paced chat-like, never left open.
- **Check-before-send** — `acquire()` records the send in a sliding window *before* your code touches the network, so floods are prevented, not recovered from.
- **`retry_after` learning** — when a 429 does surface (via `run()` or `note_flood()`), the scope *and* the global key are hard-blocked until expiry, with jitter so a fleet doesn't stampede the moment the block lifts. `acquire()` raises `FloodBlocked` instead of silently queueing behind a penalty.
- **Distributed backends** — `MemoryBackend` (zero deps), `RedisBackend` (atomic Lua sliding-window across instances, with a WATCH/MULTI fallback for stores without scripting), `SQLiteBackend` (local-first, persistent, shared between processes).
- **Priority queuing** — `HIGH` (interactive replies) > `NORMAL` > `LOW` (broadcasts). A campaign draining in the background never starves a user waiting for a reply; within a priority, FIFO.
- **Atomic multi-scope acquire** — one backend round-trip checks and records *all* scopes or none, so a denied send never leaves phantom slots.
- **Adapters** — a generic `run(any_callable, ...)` wrapper plus an aiogram v3 adapter that paces `bot.session.make_request` (both optional; core is stdlib-only).
- **CLI** — `floodgate broadcast` paces a campaign from a chats file with a progress bar and SQLite-backed **resume** (kill it mid-run, re-run, already-sent chats are skipped).
- **Observability hooks** — `on_flood(scope, retry_after)` / `on_wait(scope, seconds)` for wiring Telegram alerts.

## Setup

```bash
git clone https://github.com/shouvikkkk/tg-floodgate.git
cd tg-floodgate
pip install .            # core: stdlib only, Python 3.10+
pip install ".[redis]"   # RedisBackend
pip install ".[aiogram]" # aiogram v3 adapter
pip install ".[test]"    # pytest suite
```

## Usage

```python
import asyncio
from tg_floodgate import Governor, MemoryBackend, Scope, Priority

gov = Governor(MemoryBackend())

async def main():
    # paced: at most ~1 msg/sec to this chat, ~30/sec globally
    await gov.run(send_message, Scope.chat(chat_id), Priority.HIGH,
                  chat_id, "hello")

    # broadcast that never starves interactive traffic
    async for item, msg_id in gov.map_broadcast(items, send_one,
                                                lambda c: Scope.chat(c)):
        print("sent", item)

asyncio.run(main())
```

Multi-instance (two replicas, one Redis — flood blocks and windows are shared):

```python
from tg_floodgate import Governor, RedisBackend

gov = Governor(RedisBackend(url="redis://localhost:6379/0"))

async def on_flood(scope, retry_after):
    await admin_alert(f"🌊 flood on {scope}: backing off {retry_after}s")

gov.config.on_flood = on_flood
```

aiogram v3 — pace every `Bot` method automatically:

```python
from aiogram import Bot
from tg_floodgate import Governor, RedisBackend, install_aiogram, Priority

bot = Bot(token="...")
install_aiogram(Governor(RedisBackend()), bot,
                priority=lambda m: Priority.LOW)  # broadcast-only bot
await bot.send_message(chat_id, "paced!")
```

CLI campaign:

```bash
export TG_BOT_TOKEN=123:ABC
floodgate broadcast --chats chats.txt --message "Sale is live! 🔥"
# resume is automatic — state lives in floodgate-state.db
```

## vs existing tools

| | tg-floodgate | aiogram-sentinel | limited-aiogram | hand-rolled `retry_after` |
|---|---|---|---|---|
| Direction | **outbound** (your API calls) | inbound (user spam) | outbound | outbound |
| Multi-instance | ✅ Redis / SQLite | n/a | ❌ in-process only | ❌ |
| Scope-aware (chat/group/global) | ✅ | ❌ | partial (group "coming soon") | usually ❌ |
| Learns 429 `retry_after` | ✅ hard-block + jitter | n/a | ❌ | sometimes |
| Priority lanes | ✅ | n/a | ❌ | ❌ |
| Maintained | ✅ (this repo) | ✅ | ❌ (0.1.1, stale) | — |

**aiogram-sentinel** solves a different problem (throttling *incoming* user messages to protect your bot from spam) — it does nothing for your own outbound API calls. **limited-aiogram** patches aiogram's `Bot` for outbound limiting but keeps all state in-process, so two replicas each think they have the full 30 msg/sec budget, and its group limits were never finished. Hand-rolled `retry_after` handling is exactly the boilerplate this package deletes.

## Honest limitations

- The default limits are **Telegram's documented heuristics and can change** without notice; the governor adapts via `retry_after` learning, but if Telegram silently tightens a cap you'll eat one 429 before it learns.
- The **paid-broadcast path is not managed** — `allow_paid_broadcast` (up to ~1000 msg/sec at 0.1 Stars/message) bypasses these limits by design and is out of scope.
- `MemoryBackend` is single-process; use Redis for real multi-instance deployments (SQLite works for processes on one machine).
- Flood blocks are conservative: a 429 on one chat also blocks the global key briefly, so unrelated chats pause too. Correctness over throughput, by design.
- The aiogram adapter wraps `session.make_request` — it survives aiogram v3's generated methods, but a future aiogram major could move the choke point.

## Tests

```bash
pip install ".[test]"
pytest tests/ -q   # 42 tests: sliding-window edges, multi-scope atomicity,
                   # flood learning, priority ordering, broadcast pacing, CLI resume
```

## License

MIT — see [LICENSE](LICENSE).
