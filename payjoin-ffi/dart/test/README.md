# payjoin-ffi Dart test scaffolding

`test/utils.dart` is the canonical reference implementation of the
persistence drain pattern for Dart integrators. The unit and integration
tests in this directory use it directly.

**You can copy `utils.dart` into your own project** as a starting point for
testing against the FFI: it provides in-memory `InMemoryReceiverPersister` /
`InMemorySenderPersister` (sync and async variants) with a `drain(buf)`
method, plus `parseReceiverEvents` / `parseSenderEvents` helpers that
round-trip stored JSON back into typed events for `replay*EventLog`.

For production, swap the in-memory list out for your real storage (SQLite,
Hive, etc.) — keep the `drain(buf)` shape and the parse helpers.

## What's here

- `utils.dart` — the canonical drain + parse helpers
- `test_payjoin_unit_test.dart` — unit tests, includes per-variant
  `ProvisionalConfirmException` tests
- `test_payjoin_integration_test.dart` — full sender + receiver flow with
  real bitcoind and OHTTP services
- `fetch_ohttp_keys_http_test.dart` — OHTTP key fetching
