"""``floodgate`` CLI — paced broadcast campaigns with resume.

Example::

    floodgate broadcast --token 123:ABC --chats chats.txt --message "Sale is live!"

``chats.txt`` holds one chat id per line (``#`` comments and blanks ignored).
Progress is stored in a SQLite state file, so killing the campaign and
re-running it resumes where it left off instead of double-sending.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sys
import time
import urllib.error
import urllib.request

from .backends_sqlite import SQLiteBackend
from .errors import FloodSignal
from .governor import Governor, GovernorConfig, Priority
from .limits import Limits
from .scopes import Scope


def _post_message(token, chat_id, text, parse_mode=None,
                  disable_preview=False, timeout=30):
    """Synchronous stdlib-only sendMessage call. Raises FloodSignal on 429."""
    payload = {"chat_id": chat_id, "text": text,
               "disable_web_page_preview": disable_preview}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/sendMessage",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode() or "{}")
    except urllib.error.HTTPError as e:
        try:
            data = json.loads(e.read().decode() or "{}")
        except Exception:
            data = {}
        if e.code == 429:
            retry_after = (data.get("parameters") or {}).get("retry_after", 5)
            raise FloodSignal(retry_after, data.get("description", ""))
        raise RuntimeError(f"Telegram API HTTP {e.code}: {data}")
    if not data.get("ok"):
        raise RuntimeError(f"Telegram API error: {data}")
    return data["result"]["message_id"]


def load_chats(path: str):
    chats = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            try:
                chats.append(int(line))
            except ValueError:
                chats.append(line)  # @channelusername also works
    # de-dupe, keep order
    return list(dict.fromkeys(chats))


def load_message(args) -> str:
    if args.message:
        return args.message
    if args.message_file:
        with open(args.message_file, encoding="utf-8") as f:
            return f.read().strip()
    raise SystemExit("provide --message or --message-file")


def _bar(done: int, total: int, width: int = 30) -> str:
    filled = int(width * done / max(total, 1))
    return "[" + "#" * filled + "-" * (width - filled) + f"] {done}/{total}"


async def broadcast_campaign(
    token: str,
    chats,
    message: str,
    *,
    state_path: str = "floodgate-state.db",
    campaign: str | None = None,
    parse_mode=None,
    disable_preview: bool = False,
    broadcast_interval: float = 1.1,
    max_retries: int = 5,
    sender=None,
    quiet: bool = False,
):
    """Paced broadcast with resume. Returns ``(sent, skipped, failed)``.

    ``sender`` is an ``async (chat_id, text) -> Any`` hook for tests;
    the default sender calls the real Telegram API.
    """
    if sender is None:
        async def sender(chat_id, text):  # noqa: F811
            return await asyncio.to_thread(
                _post_message, token, chat_id, text,
                parse_mode, disable_preview,
            )

    fingerprint = hashlib.sha1(
        ("|".join(map(str, chats)) + "\n" + message).encode()
    ).hexdigest()[:12]
    campaign_id = campaign or fingerprint

    backend = SQLiteBackend(state_path)
    config = GovernorConfig(
        limits=Limits(broadcast_interval=broadcast_interval),
        namespace=f"cli-{campaign_id}",
        on_flood=lambda scope, ra: print(
            f"\n! flood on {scope}: backing off {ra:.0f}s", file=sys.stderr
        ),
        on_wait=lambda scope, w: None,
    )
    gov = Governor(backend, config)

    def log(msg, end="\n"):
        if not quiet:
            print(msg, file=sys.stderr, end=end)

    sent = skipped = failed = 0
    total = len(chats)
    try:
        for chat_id in chats:
            key = f"campaign:{campaign_id}:{chat_id}"
            if await backend.kv_get(key) == "done":
                skipped += 1
                log(f"\r{_bar(sent + skipped + failed, total)} (resumed, skipping sent)")
                continue
            attempt = 0
            while True:
                attempt += 1
                try:
                    await gov.run(
                        sender, [Scope.chat(chat_id), Scope.broadcast()],
                        Priority.LOW, chat_id, message,
                    )
                    await backend.kv_set(key, "done")
                    sent += 1
                    break
                except FloodSignal as e:
                    # run() already learned the block; wait it out, then retry
                    wait = e.retry_after + 1.0
                    log(f"\n! 429 for {chat_id}: sleeping {wait:.0f}s "
                        f"(attempt {attempt}/{max_retries})")
                    if attempt >= max_retries:
                        await backend.kv_set(key, f"error:429x{attempt}")
                        failed += 1
                        break
                    await asyncio.sleep(wait)
                except Exception as e:  # noqa: BLE001 - record and move on
                    await backend.kv_set(key, f"error:{type(e).__name__}:{e}"[:200])
                    log(f"\n! {chat_id} failed: {e}")
                    failed += 1
                    break
            log(f"\r{_bar(sent + skipped + failed, total)}", end="")
        log("")
    finally:
        await gov.close()
    return sent, skipped, failed


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="floodgate",
        description="Paced, resumable Telegram broadcast campaigns.",
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    b = sub.add_parser("broadcast", help="send a paced campaign")
    b.add_argument("--token", default=os.environ.get("TG_BOT_TOKEN"),
                   help="bot token (or TG_BOT_TOKEN env)")
    b.add_argument("--chats", required=True, help="file with one chat id per line")
    b.add_argument("--message", help="message text")
    b.add_argument("--message-file", help="file with message text")
    b.add_argument("--state", default="floodgate-state.db",
                   help="SQLite state file for resume")
    b.add_argument("--campaign", default=None, help="campaign name for resume")
    b.add_argument("--parse-mode", default=None,
                   choices=["MarkdownV2", "HTML", "Markdown"])
    b.add_argument("--disable-preview", action="store_true")
    b.add_argument("--interval", type=float, default=1.1,
                   help="min seconds between sends (default 1.1)")
    b.add_argument("--dry-run", action="store_true",
                   help="pace without sending (validates chats file + state)")
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    if args.cmd == "broadcast":
        if not args.token and not args.dry_run:
            raise SystemExit("--token is required (or TG_BOT_TOKEN)")
        chats = load_chats(args.chats)
        message = load_message(args)
        print(f"{len(chats)} chats loaded", file=sys.stderr)
        if args.dry_run:
            print("dry run: pacing only, nothing sent", file=sys.stderr)
            async def _noop(chat_id, text):
                return None
            sender = _noop
            token = "dry-run"
        else:
            sender = None
            token = args.token
        t0 = time.monotonic()
        sent, skipped, failed = asyncio.run(broadcast_campaign(
            token, chats, message,
            state_path=args.state,
            campaign=args.campaign,
            parse_mode=args.parse_mode,
            disable_preview=args.disable_preview,
            broadcast_interval=args.interval,
            sender=sender,
        ))
        dt = time.monotonic() - t0
        print(f"done in {dt:.1f}s — sent={sent} skipped={skipped} failed={failed}")
        return 1 if failed else 0
    raise SystemExit("unknown command")  # pragma: no cover


if __name__ == "__main__":
    sys.exit(main())
