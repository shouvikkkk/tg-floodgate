"""tg-floodgate — distributed, scope-aware outbound send governor for Telegram bots.

Quick start::

    from tg_floodgate import Governor, MemoryBackend, Scope, Priority

    gov = Governor(MemoryBackend())
    await gov.run(send_message, Scope.chat(chat_id), Priority.HIGH, chat_id, "hi")
"""

from .aiogram_adapter import install as install_aiogram
from .backends_memory import MemoryBackend
from .backends_redis import RedisBackend
from .backends_sqlite import SQLiteBackend
from .errors import (
    FloodBlocked,
    FloodgateError,
    FloodSignal,
    QueueFull,
    QueueTimeout,
)
from .governor import Governor, GovernorConfig, Priority
from .limits import Limits
from .scopes import Scope
from .storage import Storage

__all__ = [
    "Governor",
    "GovernorConfig",
    "Priority",
    "Scope",
    "Limits",
    "Storage",
    "MemoryBackend",
    "SQLiteBackend",
    "RedisBackend",
    "FloodgateError",
    "FloodBlocked",
    "FloodSignal",
    "QueueFull",
    "QueueTimeout",
    "install_aiogram",
]

__version__ = "0.1.0"
