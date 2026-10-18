"""CLI: chat loading, paced broadcast, 429 recovery, resume."""

import asyncio
import time

import pytest

from tg_floodgate import FloodSignal
from tg_floodgate.cli import broadcast_campaign, load_chats, main


def write_chats(path, lines):
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return str(path)


def test_load_chats_parses_skips_dedupes(tmp_path):
    p = tmp_path / "chats.txt"
    write_chats(p, ["# comment", "", "123", "123", "@mychannel", "  456  "])
    assert load_chats(str(p)) == [123, "@mychannel", 456]


async def test_broadcast_paces_and_recovers_from_429(tmp_path):
    p = tmp_path / "chats.txt"
    write_chats(p, ["1", "2", "3"])
    calls = []
    attempts = {}

    async def sender(chat_id, text):
        attempts[chat_id] = attempts.get(chat_id, 0) + 1
        calls.append((chat_id, time.monotonic()))
        if chat_id == 2 and attempts[chat_id] == 1:
            raise FloodSignal(0.3, "too many requests")
        return 1000 + chat_id

    sent, skipped, failed = await broadcast_campaign(
        "TOKEN", [1, 2, 3], "hello",
        state_path=str(tmp_path / "state.db"),
        broadcast_interval=0.2,
        sender=sender,
    )
    assert (sent, skipped, failed) == (3, 0, 0)
    # chat 2 needed a retry after its 429
    assert attempts == {1: 1, 2: 2, 3: 1}
    # first-attempt order is preserved ...
    firsts = list(dict.fromkeys(c for c, _ in calls))
    assert firsts == [1, 2, 3]
    # ... and pacing held between sends (the 429 sleep dominates one gap)
    gaps = [b - a for (_, a), (_, b) in zip(calls, calls[1:])]
    paced_gaps = [g for g in gaps if g < 1.0]
    assert paced_gaps and all(g >= 0.15 for g in paced_gaps), gaps


async def test_broadcast_resumes_skipping_done(tmp_path):
    p = tmp_path / "chats.txt"
    write_chats(p, ["1", "2"])
    seen = []

    async def sender(chat_id, text):
        seen.append(chat_id)
        if chat_id == 2:
            raise RuntimeError("boom")  # permanent failure, recorded

    sent, skipped, failed = await broadcast_campaign(
        "TOKEN", [1, 2], "hello",
        state_path=str(tmp_path / "state.db"),
        broadcast_interval=0.05,
        sender=sender,
    )
    assert (sent, skipped, failed) == (1, 0, 1)

    seen.clear()
    sent, skipped, failed = await broadcast_campaign(
        "TOKEN", [1, 2], "hello",
        state_path=str(tmp_path / "state.db"),
        broadcast_interval=0.05,
        sender=sender,
    )
    # chat 1 skipped (done); chat 2 retried (recorded as error, not done)
    assert (sent, skipped, failed) == (0, 1, 1)
    assert seen == [2]


def test_dry_run_sends_nothing(tmp_path, capsys):
    p = tmp_path / "chats.txt"
    write_chats(p, ["1", "2", "3"])
    rc = main([
        "broadcast", "--chats", str(p), "--message", "hi",
        "--state", str(tmp_path / "s.db"), "--dry-run", "--interval", "0.01",
    ])
    assert rc == 0
    out = capsys.readouterr()
    assert "sent=3" in out.out


def test_missing_message_is_an_error(tmp_path):
    p = tmp_path / "chats.txt"
    write_chats(p, ["1"])
    with pytest.raises(SystemExit):
        main(["broadcast", "--chats", str(p), "--token", "x",
              "--state", str(tmp_path / "s.db"), "--interval", "0.01"])
