"""Scope key builders.

A *scope* is the unit a Telegram rate limit applies to. The governor turns
each scope into one or more backend keys (always including the per-bot global
key) and enforces every matching limit before a send happens.
"""


class Scope:
    """Build scope keys for :meth:`Governor.acquire` / :meth:`Governor.run`."""

    GLOBAL = "global"
    BROADCAST = "broadcast"

    @staticmethod
    def global_() -> str:
        """The whole bot: ~30 messages/sec across all chats (free tier)."""
        return Scope.GLOBAL

    @staticmethod
    def chat(chat_id) -> str:
        """One private chat: ~1 message/sec (bursts allowed briefly)."""
        return f"chat:{chat_id}"

    @staticmethod
    def group(chat_id) -> str:
        """One group/channel: ~20 messages/min."""
        return f"group:{chat_id}"

    @staticmethod
    def broadcast() -> str:
        """Campaign pacing lane: one send per ``broadcast_interval``."""
        return Scope.BROADCAST
