"""Exceptions raised by tg-floodgate."""


class FloodgateError(Exception):
    """Base class for all tg-floodgate errors."""


class FloodBlocked(FloodgateError):
    """A scope is hard-blocked because Telegram answered 429.

    Raised by :meth:`Governor.acquire` instead of silently waiting out a
    flood block: a 429 means Telegram already penalised this scope, so the
    caller should back off loudly rather than queue behind the block.
    """

    def __init__(self, scope, scope_key, retry_after):
        self.scope = scope
        self.scope_key = scope_key
        self.retry_after = retry_after
        super().__init__(
            f"scope {scope_key!r} is flood-blocked for {retry_after:.1f}s "
            f"(Telegram 429 on {scope!r})"
        )


class QueueFull(FloodgateError):
    """Too many waiters are queued for one scope (see ``max_queue_depth``)."""


class QueueTimeout(FloodgateError):
    """Gave up waiting for capacity (see ``max_wait``)."""


class FloodSignal(FloodgateError):
    """Transport-level signal: the Telegram API answered HTTP 429.

    Raise this from your send function (the CLI does it automatically)
    with the ``retry_after`` value from the API response so the governor
    can learn the block instead of hammering a penalised scope.
    """

    def __init__(self, retry_after, description=""):
        self.retry_after = float(retry_after)
        self.description = description
        super().__init__(
            f"Telegram 429: retry after {self.retry_after}s. {description}".strip()
        )
