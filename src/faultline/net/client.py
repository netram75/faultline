"""A small Redis client: enough for the tests, the chaos runs and the
benchmark. Any real Redis client works with faultline too."""

import asyncio
import contextlib
from typing import Self

from faultline.net.node import Address
from faultline.net.resp import Parser, Value, encode


class Client:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._reader = reader
        self._writer = writer
        self._parser = Parser()
        self._replies: list[Value] = []

    @classmethod
    async def connect(cls, address: Address) -> Self:
        reader, writer = await asyncio.open_connection(address.host, address.port)
        return cls(reader, writer)

    async def call(self, *args: bytes | str) -> Value:
        """Send one command and wait for its reply."""
        [reply] = await self.pipeline([args])
        return reply

    async def pipeline(self, commands: list[tuple[bytes | str, ...]]) -> list[Value]:
        """Send several commands at once, then read all their replies."""
        for command in commands:
            parts: list[Value] = [a.encode() if isinstance(a, str) else a for a in command]
            self._writer.write(encode(parts))
        await self._writer.drain()
        while len(self._replies) < len(commands):
            data = await self._reader.read(64 * 1024)
            if not data:
                raise ConnectionError("the server closed the connection")
            self._replies += self._parser.feed(data)
        replies = self._replies[: len(commands)]
        del self._replies[: len(commands)]
        return replies

    async def close(self) -> None:
        self._writer.close()
        with contextlib.suppress(ConnectionError):
            await self._writer.wait_closed()
