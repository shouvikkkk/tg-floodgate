"""Optional aiogram v3 adapter: pace every outbound Telegram API call.

aiogram v3 has no first-class outbound middleware hook, so this wraps
``bot.session.make_request`` — the single choke point every generated
``Bot`` method (``send_message``, ``send_photo``, ...) funnels through.
The wrapper resolves a scope per method (``chat_id`` when the method
carries one, global otherwise) and routes the call through
:meth:`Governor.run`, so 429s are learned automatically.

aiogram is an optional extra (``pip install tg-floodgate[aiogram]``);
this module itself imports nothing from aiogram, so it also works with
test doubles that expose a ``session.make_request`` coroutine.
"""

from typing import Callable, Optional

from .governor import Governor, Priority
from .scopes import Scope


def _default_scope(method) -> str:
    chat_id = getattr(method, "chat_id", None)
    if chat_id is None:
        return Scope.global_()
    return Scope.chat(chat_id)


def install(
    governor: Governor,
    bot,
    *,
    scope: Optional[Callable[[object], str]] = None,
    priority: Optional[Callable[[object], Priority]] = None,
):
    """Install the governor on an aiogram v3 ``Bot`` instance.

    * ``scope`` — ``method -> scope key``; defaults to per-chat when the
      method has ``chat_id``, else the global scope.
    * ``priority`` — ``method -> Priority``; defaults to ``NORMAL``.
      Pass e.g. ``lambda m: Priority.LOW`` for a broadcast-only bot.

    Returns the bot, for chaining.
    """
    session = bot.session
    original = session.make_request

    async def make_request(b, method, timeout=None):
        sc = scope(method) if callable(scope) else (scope or _default_scope(method))
        pr = (
            priority(method)
            if callable(priority)
            else (priority if priority is not None else Priority.NORMAL)
        )
        return await governor.run(lambda: original(b, method, timeout), sc, pr)

    session.make_request = make_request
    return bot
