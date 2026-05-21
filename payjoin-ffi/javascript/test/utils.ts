import { payjoin } from "payjoin";

// Drain helpers used by the test suite. After the FFI lost SessionPersister,
// persistence is driven directly: each action method pushes events into an
// EventBuffer, and the caller writes those events to storage in any way it
// likes. These helpers wrap that pattern: `drain(buf)` peeks the queued
// events, appends them to an in-memory list, then commits.

class MemoryEventLog {
    readonly events: string[] = [];

    save(event: string): void {
        this.events.push(event);
    }

    load(): string[] {
        return [...this.events];
    }
}

class MemoryEventLogAsync {
    readonly events: string[] = [];

    async save(event: string): Promise<void> {
        this.events.push(event);
    }

    async load(): Promise<string[]> {
        return [...this.events];
    }
}

export class InMemoryReceiverPersister extends MemoryEventLog {
    /** Drains a `ReceiverEventBuffer` into this in-memory log. */
    drain(buf: payjoin.ReceiverEventBuffer): void {
        const events = buf.peek();
        for (const json of events) this.save(json);
        buf.commit(BigInt(events.length));
    }
}

export class InMemorySenderPersister extends MemoryEventLog {
    /** Drains a `SenderEventBuffer` into this in-memory log. */
    drain(buf: payjoin.SenderEventBuffer): void {
        const events = buf.peek();
        for (const json of events) this.save(json);
        buf.commit(BigInt(events.length));
    }
}

export class InMemoryReceiverPersisterAsync extends MemoryEventLogAsync {
    /** Async-drain a `ReceiverEventBuffer` into this in-memory log. */
    async drain(buf: payjoin.ReceiverEventBuffer): Promise<void> {
        const events = buf.peek();
        for (const json of events) await this.save(json);
        buf.commit(BigInt(events.length));
    }
}

export class InMemorySenderPersisterAsync extends MemoryEventLogAsync {
    /** Async-drain a `SenderEventBuffer` into this in-memory log. */
    async drain(buf: payjoin.SenderEventBuffer): Promise<void> {
        const events = buf.peek();
        for (const json of events) await this.save(json);
        buf.commit(BigInt(events.length));
    }
}
