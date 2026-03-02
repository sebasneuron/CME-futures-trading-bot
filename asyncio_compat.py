from __future__ import annotations

import asyncio


def ensure_event_loop() -> None:
    """
    Ensure a current event loop exists on the main thread.

    Python 3.14+ raises RuntimeError if get_event_loop() is called with no loop set.
    Some dependencies (eventkit/ib_insync) do this at import time, so we must
    create/set a loop before importing them.
    """
    try:
        asyncio.get_event_loop_policy().get_event_loop()
        return
    except RuntimeError:
        pass

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

