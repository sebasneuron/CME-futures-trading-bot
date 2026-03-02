from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from asyncio_compat import ensure_event_loop

if TYPE_CHECKING:
    from ib_insync import IB


@dataclass(frozen=True)
class IBConnectionParams:
    host: str
    port: int
    client_id: int


class IBConnection:
    def __init__(self, params: IBConnectionParams, logger: logging.Logger):
        self.params = params
        self.log = logger
        self.ib: Any = self._make_ib()
        self._lock = asyncio.Lock()

    @staticmethod
    def _make_ib() -> "IB":
        ensure_event_loop()
        from ib_insync import IB

        return IB()

    async def connect(self) -> None:
        async with self._lock:
            if self.ib.isConnected():
                return
            self.log.info("Connecting to IB %s:%s clientId=%s", self.params.host, self.params.port, self.params.client_id)
            await self.ib.connectAsync(self.params.host, self.params.port, clientId=self.params.client_id, timeout=5)
            self.log.info("Connected to IB")

    async def disconnect(self) -> None:
        async with self._lock:
            if self.ib.isConnected():
                self.ib.disconnect()
                self.log.info("Disconnected from IB")

    async def ensure_connected(self, *, retry_delay: float = 2.0) -> None:
        while not self.ib.isConnected():
            try:
                await self.connect()
            except Exception as e:
                self.log.warning("IB connect failed: %s; retrying in %.1fs", e, retry_delay)
                await asyncio.sleep(retry_delay)

    async def run_watchdog(self, *, poll_seconds: float = 2.0) -> None:
        while True:
            if not self.ib.isConnected():
                self.log.warning("IB disconnected; reconnecting...")
                await self.ensure_connected()
            await asyncio.sleep(poll_seconds)

