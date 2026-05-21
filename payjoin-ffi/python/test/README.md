# payjoin-ffi Python test scaffolding

`test/utils.py` is the canonical reference implementation of the persistence
drain pattern for Python integrators. The unit and integration tests in this
directory use it directly.

**You can copy `utils.py` into your own project** as a starting point for
testing against the FFI: it provides an in-memory `InMemoryReceiverPersister` /
`InMemorySenderPersister` (sync and async variants) with a `drain(buf)`
method, plus `parse_receiver_events` / `parse_sender_events` helpers that
round-trip stored JSON back into typed events for `replay_*_event_log`.

For production, swap the in-memory list out for your real storage (SQLite,
IndexedDB, etc.) — keep the `drain(buf)` shape and the parse helpers.

## What's here

- `utils.py` — the canonical drain + parse helpers
- `test_payjoin_unit_test.py` — unit tests, includes per-variant
  `ProvisionalConfirmError` tests
- `test_payjoin_integration_test.py` — full sender + receiver flow with
  real bitcoind and OHTTP services
