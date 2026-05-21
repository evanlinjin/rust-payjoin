# payjoin-ffi JavaScript test scaffolding

`test/utils.ts` is the canonical reference implementation of the persistence
drain pattern for TypeScript/JavaScript integrators. The unit and integration
tests in this directory use it directly.

**You can copy `utils.ts` into your own project** as a starting point for
testing against the FFI: it provides in-memory `InMemoryReceiverPersister` /
`InMemorySenderPersister` (sync and async variants) with a `drain(buf)`
method, plus `parseReceiverEvents` / `parseSenderEvents` helpers that
round-trip stored JSON back into typed events for `replay*EventLog`.

For production, swap the in-memory array out for your real storage
(IndexedDB, SQLite, etc.) — keep the `drain(buf)` shape and the parse
helpers.

## What's here

- `utils.ts` — the canonical drain + parse helpers
- `unit.test.ts` — unit tests, includes per-variant `ProvisionalConfirmError`
  tests
- `integration.test.ts` — full sender + receiver flow with real bitcoind
  and OHTTP services
