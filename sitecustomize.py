"""
Workaround for Python 3.14+ asyncio behavior changes.

Some third-party packages (notably eventkit, a dependency of ib_insync) call
asyncio.get_event_loop() at import time. On Python 3.14 this raises unless a
current loop is set on the main thread.

Python auto-imports sitecustomize (if present on sys.path) during startup,
so placing this file in the project directory makes imports robust when
running scripts from this folder.
"""

from __future__ import annotations

import asyncio


def _ensure_event_loop() -> None:
    try:
        asyncio.get_event_loop_policy().get_event_loop()
        return
    except RuntimeError:
        pass

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)


_ensure_event_loop()

