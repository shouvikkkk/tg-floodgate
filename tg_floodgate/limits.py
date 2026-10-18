"""Default limits, matching Telegram's documented Bot API caps."""

from dataclasses import dataclass


@dataclass
class Limits:
    """Rate limits enforced per scope. All values are configurable.

    Defaults follow the Telegram Bots FAQ / Bot API reference as documented
    in October 2026. They are heuristics Telegram can change at any time —
    when in doubt, keep the defaults and let ``retry_after`` learning
    (see :meth:`Governor.note_flood`) absorb the difference.
    """

    global_per_second: int = 30   # ~30 msg/sec bulk, all chats combined
    chat_per_second: int = 1      # ~1 msg/sec to a single chat
    chat_window: float = 1.0
    group_per_minute: int = 20    # ~20 msg/min to a single group/channel
    group_window: float = 60.0
    broadcast_interval: float = 1.1  # min seconds between campaign sends
