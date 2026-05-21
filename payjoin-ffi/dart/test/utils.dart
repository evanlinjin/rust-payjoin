import "package:payjoin/payjoin.dart" as payjoin;

// Drain helpers used by the test suite. After the FFI lost SessionPersister,
// persistence is driven directly: each action method pushes events into an
// EventBuffer, and the caller writes those events to storage in any way it
// likes. These helpers wrap that pattern: `drain(buf)` peeks the queued
// events, appends them to an in-memory list, then commits.

class _InMemoryEventLog {
  final List<String> events = [];
  bool closed = false;

  void save(String event) {
    events.add(event);
  }

  List<String> load() {
    return List.of(events);
  }

  void close() {
    closed = true;
  }
}

class _InMemoryEventLogAsync {
  final List<String> events = [];
  bool closed = false;

  Future<void> save(String event) async {
    events.add(event);
  }

  Future<List<String>> load() async {
    return List.of(events);
  }

  Future<void> close() async {
    closed = true;
  }
}

class InMemoryReceiverPersister extends _InMemoryEventLog {
  /// Drains a `ReceiverEventBuffer` into this in-memory log.
  void drain(payjoin.ReceiverEventBuffer buf) {
    final events = buf.peek();
    for (final json in events) {
      save(json);
    }
    buf.commit(n: BigInt.from(events.length));
  }
}

class InMemorySenderPersister extends _InMemoryEventLog {
  /// Drains a `SenderEventBuffer` into this in-memory log.
  void drain(payjoin.SenderEventBuffer buf) {
    final events = buf.peek();
    for (final json in events) {
      save(json);
    }
    buf.commit(n: BigInt.from(events.length));
  }
}

class InMemoryReceiverPersisterAsync extends _InMemoryEventLogAsync {
  /// Async-drain a `ReceiverEventBuffer` into this in-memory log.
  Future<void> drain(payjoin.ReceiverEventBuffer buf) async {
    final events = buf.peek();
    for (final json in events) {
      await save(json);
    }
    buf.commit(n: BigInt.from(events.length));
  }
}

class InMemorySenderPersisterAsync extends _InMemoryEventLogAsync {
  /// Async-drain a `SenderEventBuffer` into this in-memory log.
  Future<void> drain(payjoin.SenderEventBuffer buf) async {
    final events = buf.peek();
    for (final json in events) {
      await save(json);
    }
    buf.commit(n: BigInt.from(events.length));
  }
}
