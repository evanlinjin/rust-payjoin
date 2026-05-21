"""In-memory drain helpers used by the test suite.

After the FFI lost SessionPersister, persistence is driven directly: each action
method pushes events into an EventBuffer, and the caller writes those events to
storage in any way it likes. These helpers wrap that pattern: `drain(buf)` peeks
the queued events, serializes them to JSON, appends them to an in-memory list,
then commits.

`parse_receiver_events` / `parse_sender_events` round-trip a stored JSON log
back into typed events for `replay_*_event_log`.
"""

import payjoin


def parse_receiver_events(json_events):
    """Deserialize a stored JSON log into typed ReceiverSessionEvents."""
    return [payjoin.ReceiverSessionEvent.from_json(s) for s in json_events]


def parse_sender_events(json_events):
    """Deserialize a stored JSON log into typed SenderSessionEvents."""
    return [payjoin.SenderSessionEvent.from_json(s) for s in json_events]


class _InMemoryEventLog:
    def __init__(self):
        self.events = []

    def save(self, json_event: str):
        self.events.append(json_event)

    def load(self):
        return list(self.events)


class InMemoryReceiverPersister(_InMemoryEventLog):
    """Drains a `ReceiverEventBuffer` into an in-memory event log."""

    def drain(self, buf):
        events = buf.peek()
        for event in events:
            self.save(event.to_json())
        buf.commit(len(events))


class InMemorySenderPersister(_InMemoryEventLog):
    """Drains a `SenderEventBuffer` into an in-memory event log."""

    def drain(self, buf):
        events = buf.peek()
        for event in events:
            self.save(event.to_json())
        buf.commit(len(events))


class _InMemoryEventLogAsync:
    def __init__(self):
        self.events = []

    async def save(self, json_event: str):
        self.events.append(json_event)

    async def load(self):
        return list(self.events)


class InMemoryReceiverPersisterAsync(_InMemoryEventLogAsync):
    """Async-drain a `ReceiverEventBuffer` into an in-memory log."""

    async def drain(self, buf):
        events = buf.peek()
        for event in events:
            await self.save(event.to_json())
        buf.commit(len(events))


class InMemorySenderPersisterAsync(_InMemoryEventLogAsync):
    """Async-drain a `SenderEventBuffer` into an in-memory log."""

    async def drain(self, buf):
        events = buf.peek()
        for event in events:
            await self.save(event.to_json())
        buf.commit(len(events))
