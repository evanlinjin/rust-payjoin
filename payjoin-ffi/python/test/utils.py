"""In-memory drain helpers used by the test suite.

After the FFI lost SessionPersister, persistence is driven directly: each action
method pushes events into an EventBuffer, and the caller writes those events to
storage in any way it likes. These helpers wrap that pattern: `drain(buf)` peeks
the queued events, appends them to an in-memory list, then commits.
"""


class _InMemoryEventLog:
    def __init__(self):
        self.events = []
        self.closed = False

    def save(self, json_event: str):
        self.events.append(json_event)

    def load(self):
        return list(self.events)

    def close(self):
        self.closed = True


class InMemoryReceiverPersister(_InMemoryEventLog):
    """Drains a `ReceiverEventBuffer` into an in-memory event log."""

    def drain(self, buf):
        events = buf.peek()
        for json_event in events:
            self.save(json_event)
        buf.commit(len(events))


class InMemorySenderPersister(_InMemoryEventLog):
    """Drains a `SenderEventBuffer` into an in-memory event log."""

    def drain(self, buf):
        events = buf.peek()
        for json_event in events:
            self.save(json_event)
        buf.commit(len(events))


class _InMemoryEventLogAsync:
    def __init__(self):
        self.events = []
        self.closed = False

    async def save(self, json_event: str):
        self.events.append(json_event)

    async def load(self):
        return list(self.events)

    async def close(self):
        self.closed = True


class InMemoryReceiverPersisterAsync(_InMemoryEventLogAsync):
    """Async-drain a `ReceiverEventBuffer` into an in-memory log."""

    async def drain(self, buf):
        events = buf.peek()
        for json_event in events:
            await self.save(json_event)
        buf.commit(len(events))


class InMemorySenderPersisterAsync(_InMemoryEventLogAsync):
    """Async-drain a `SenderEventBuffer` into an in-memory log."""

    async def drain(self, buf):
        events = buf.peek()
        for json_event in events:
            await self.save(json_event)
        buf.commit(len(events))
