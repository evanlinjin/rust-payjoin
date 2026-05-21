# Migration guide: SessionPersister → EventBuffer

This guide covers migration for both the `payjoin` Rust library and the
`payjoin-ffi` FFI (Python / JavaScript / Dart bindings).

## What changed

The `SessionPersister` / `AsyncSessionPersister` callback traits and all
`*Transition` wrapper objects have been removed. Persistence is now driven
by a caller-owned `EventBuffer`:

- Action methods take `&mut EventBuffer` and push events into it
- The caller drains the buffer through their own storage (sync or async)
- One typestate path serves both sync and async callers — no `_async` fork

Externally-observable side effects (URI emission, directory POSTs, finalized
PSBT posts, sender broadcast) are gated by `Provisional<T>`: the inner value
isn't reachable until the producing event is durably persisted in the
buffer.

## Library migration (Rust)

### Build pattern

```rust
// Before:
let receiver = ReceiverBuilder::new(...)?.build().save(&persister)?;

// After:
let mut buf = EventBuffer::new();
let provisional = ReceiverBuilder::new(...)?.build(&mut buf);
persister.drain(&mut buf)?;
let receiver = provisional.confirm(&buf).expect("just drained");
```

### Action methods

```rust
// Before:
let next = receiver.check_inputs_not_owned(&mut |i| ..., persister)?;

// After:
let next = receiver.check_inputs_not_owned(&mut |i| ..., &mut buf)?;
persister.drain(&mut buf)?;
```

### Cancel

```rust
// Before:
let fallback = receiver.cancel().save(&persister)?;

// After:
let fallback = receiver.cancel(&mut buf);
persister.drain(&mut buf)?;
```

### Replay

```rust
// Before:
let (state, history) = replay_event_log(&persister)?;
let (state, history) = replay_event_log_async(&persister).await?;

// After:
let events = persister.load()?;  // or .load().await? for async storage
let (state, history) = replay_event_log(events)?;
let buf = EventBuffer::after_replay(history.event_count());
```

### Errors

`PersistedError<…>` is gone. Action methods return `Result<T, ApiError<E>>`
or `Result<T, ApiError<E, ErrorState>>`. Storage errors no longer flow
through the library — handle them at your drain call site with your own
error type.

## FFI migration (Python / JavaScript / Dart)

The FFI mirrors the library shape. Pattern is identical across languages.

### Build pattern (Python shown)

```python
# Before:
initialized = ReceiverBuilder(...).build().save(persister)

# After:
buf = ReceiverEventBuffer()
provisional = ReceiverBuilder(...).build(buf)
persister.drain(buf)
initialized = provisional.confirm(buf)
```

### Action methods

```python
# Before:
res = receiver.process_response(body, ctx).save(persister)

# After:
outcome = receiver.process_response(body, ctx, buf)
persister.drain(buf)
```

### Cancel

```python
# Before:
fallback = receiver.cancel().save(persister)        # Optional[bytes]
fallback = sender.cancel().save(persister)          # bytes

# After:
fallback = receiver.cancel(buf); persister.drain(buf)   # Optional[bytes]
fallback = sender.cancel(buf);   persister.drain(buf)   # bytes
```

### Sender build & PSBT confirm

The sender's `build_*` methods and `process_response` now return
provisionals — the directory POST and final PSBT are gated behind a
durable persistence event.

```python
# Build a sender:
buf = SenderEventBuffer()
provisional = SenderBuilder(...).build_recommended(1000, buf)
persister.drain(buf)
sender = provisional.confirm(buf)

# Poll for proposal & get the final signed PSBT:
outcome = sender.process_response(body, ctx, buf)
persister.drain(buf)
if outcome.is_PROGRESS():
    psbt_base64 = outcome.inner.confirm(buf)   # Provisional<Psbt>
    # now safe to sign and broadcast
```

### Finalize proposal (receiver)

```python
# Before:
payjoin = proposal.finalize_proposal(cb).save(persister)

# After:
provisional = proposal.finalize_proposal(cb, buf)
persister.drain(buf)
payjoin = provisional.confirm(buf)
```

### Replay

```python
# Before:
result = replay_receiver_event_log(persister)
result = await replay_receiver_event_log_async(persister)

# After: load events, parse them back to typed, replay, then mint a
# matching buffer for the resumed session.
events = parse_receiver_events(persister.load())  # or async load
result  = replay_receiver_event_log(events)
buf     = result.new_event_buffer()
```

### Drain loop (write your own)

```python
class InMemoryReceiverPersister:
    def __init__(self):
        self.events = []

    def save(self, json_event):
        self.events.append(json_event)

    def load(self):
        return list(self.events)

    def drain(self, buf):
        events = buf.peek()                          # typed events
        for event in events:
            self.save(event.to_json())               # serialize at boundary
        buf.commit(len(events))


class InMemoryReceiverPersisterAsync:
    async def drain(self, buf):
        events = buf.peek()
        for event in events:
            await self.save(event.to_json())
        buf.commit(len(events))
```

See `payjoin-ffi/{python,javascript,dart}/test/utils.{py,ts,dart}` for the
canonical reference implementations in each language.

## Provisional confirm failures

`Provisional::confirm` can fail in three distinct ways. Foreign code can
match on the variant to drive recovery:

- **`NotYetPersisted`** — the producing event isn't yet durable in the
  buffer. Drain more events through storage, then call confirm again.
  Benign retry condition.
- **`WrongBuffer`** — the supplied buffer's id doesn't match the buffer
  this provisional was minted against. Programmer error; retrying with
  the same buffer will never succeed. Typically caused by passing a
  freshly-constructed unrelated buffer (e.g. forgetting to thread the
  session's buffer through).
- **`AlreadyConsumed`** — confirm was already called successfully and
  the provisional was consumed. Programmer error.

## Session metadata accessors

Wallet UIs needing to display "expires in X" / "expects Y sats" can read
metadata directly from any receiver/sender typestate (without parsing
the payjoin URI):

```python
initialized.address()             # bitcoin address (string)
initialized.directory()           # directory URL (string)
initialized.expiration_unix_secs() # u64 Unix timestamp
initialized.amount_sats()         # Optional[int]

sender.endpoint()
sender.expiration_unix_secs()
```

## Downstream tracking issues

If you're a downstream integrator (nolooking, bitmask-core, mobile
wallets, etc.) hitting issues during migration, please open an issue at
<https://github.com/payjoin/rust-payjoin/issues>.
