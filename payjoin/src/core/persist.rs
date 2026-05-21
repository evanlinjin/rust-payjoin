//! State machine persistence for payjoin sessions.
//!
//! The receiver and sender v2 state machines are driven by events. An event
//! contains all the information to transition into the next state, which
//! means that the session's full state can be computed by "replaying" the
//! events. Session history is therefore recorded as an append-only log of
//! events.
//!
//! # The persistence API
//!
//! Sessions are driven through two primitives:
//!
//! - [`EventBuffer<E>`] — a caller-owned, sans-IO event buffer. Action
//!   methods on receiver / sender typestates push events into it; the
//!   caller drains it through whatever storage it likes (sync, async,
//!   batched, transactional). Drain is two-phase: [`EventBuffer::peek`]
//!   borrows pending events without mutating, then
//!   [`EventBuffer::commit`] drops the first `n` after storage acks.
//!   A panic or async cancellation between writes leaves the buffer
//!   consistent with what's on disk — the un-persisted suffix stays
//!   queued.
//!
//! - [`Provisional<T>`] — a witness type that gates access to a value
//!   produced by a transition until the producing event has been
//!   durably persisted in the same buffer. Returned by every transition
//!   whose result, if observed, would mint an externally-visible side
//!   effect (e.g. a payjoin URI, an HTTP request to the directory).
//!   [`Provisional::confirm`] succeeds only when the buffer's
//!   `committed_count` has reached the recorded sequence number AND the
//!   buffer's id matches the one the provisional was minted against.
//!
//! There is no `SessionPersister`-style trait — that abstraction was
//! removed in favour of [`EventBuffer`]. The buffer IS the persistence
//! interface; sync vs async lives in caller code, not in the library's
//! typestate path. [`InMemoryPersister`] is provided as a concrete
//! reference implementation for tests and as a starting point for
//! callers; it exposes inherent `save_event` / `load` / `drain` methods
//! without going through any trait.
//!
//! # Backwards and forwards compatibility
//!
//! If any new fields are added to events, backwards compatibility must be
//! maintained, which means that new fields are necessarily `Option<T>`
//! defaulting to `None`, allowing old event data to be still be processed.
//! Forward compatibility in general is not appropriate since old state machines
//! will not know the meaning of the new fields, and ignoring them may lead to a
//! transition to an invalid state, inconsistent with the state machine of any
//! later version of the code that persisted this event data.
//!
//! If any new event types are added, presumably extending the state machine
//! with additional transitions and states, the same logic applies: old sessions
//! will simply not contain this new type of event and therefore only explore
//! the subgraph of the state machine diagram which corresponds to the older
//! version of the state machine. New sessions which do contain this event will
//! not be interpretable by the old code.

use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

/// Process-unique identifier for an [`EventBuffer`] instance.
///
/// [`Provisional`] stamps the id of the buffer it was minted against; on
/// [`Provisional::confirm`] the recorded id must match the buffer's, otherwise
/// the wrong-buffer attempt is rejected. Buffer ids are issued by a static
/// counter at construction and are never reused within a process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BufferId(u64);

impl BufferId {
    fn next() -> Self {
        // SeqCst is overkill but unambiguous; ids are issued rarely.
        static NEXT: AtomicU64 = AtomicU64::new(1);
        BufferId(NEXT.fetch_add(1, Ordering::SeqCst))
    }
}

/// Witness recording where (which buffer) and when (which seq number) an
/// event was staged. Returned by [`EventBuffer::push`] and stamped into
/// [`Provisional`] so a confirm against an unrelated buffer can be rejected.
#[derive(Debug, Clone, Copy)]
pub(crate) struct EventStamp {
    buffer_id: BufferId,
    seq: u64,
}

/// Caller-owned, sans-IO event buffer. Transitions push events into it;
/// the caller's persister drains it two-phase (peek + commit).
///
/// Generic over the event type so receiver and sender sessions use distinct,
/// non-interchangeable buffer types.
///
/// Each buffer carries a process-unique [`BufferId`] issued at construction.
/// A [`Provisional`] minted against one buffer can only be confirmed against
/// the same buffer instance — confirming against a freshly created or
/// unrelated buffer is rejected, so the persist-before-expose invariant is
/// structural rather than positional.
///
/// # Two-phase drain
///
/// The drain loop is `peek` to read pending events, write each one to storage,
/// then `commit(n)` to drop the persisted prefix. A panic or async
/// cancellation between writes leaves the buffer consistent with what is
/// already on disk: the un-persisted suffix remains queued.
///
/// # Replay
///
/// After replaying a previously persisted log, construct the buffer with
/// [`EventBuffer::after_replay`] so [`Self::committed_count`] reflects the
/// number of events already durable.
pub struct EventBuffer<E> {
    id: BufferId,
    pending: VecDeque<E>,
    /// Monotonic count of events ever pushed across the buffer's lifetime.
    /// `committed_count() == pushed_total - pending.len()`. Used by
    /// `Provisional` to gate side-effect-bearing accessors.
    pushed_total: u64,
}

impl<E> EventBuffer<E> {
    /// Construct an empty buffer with a fresh [`BufferId`].
    pub fn new() -> Self {
        Self { id: BufferId::next(), pending: VecDeque::new(), pushed_total: 0 }
    }

    /// Construct a buffer reflecting that `replayed` events were already
    /// persisted before this session woke up. Use after replaying a log.
    pub fn after_replay(replayed: u64) -> Self {
        Self { id: BufferId::next(), pending: VecDeque::new(), pushed_total: replayed }
    }

    /// Identifier issued at construction; stable for the buffer's lifetime.
    pub fn id(&self) -> BufferId { self.id }

    /// Returns `true` if no events are queued for persistence.
    pub fn is_empty(&self) -> bool { self.pending.is_empty() }

    /// Number of events queued for persistence (not yet committed).
    pub fn len(&self) -> usize { self.pending.len() }

    /// Total events durably persisted. `Provisional::confirm` reads this.
    pub fn committed_count(&self) -> u64 { self.pushed_total - self.pending.len() as u64 }

    /// Borrow events for writing to storage. The buffer is *not* mutated
    /// until `commit` is called.
    pub fn peek(&self) -> impl Iterator<Item = &E> + '_ { self.pending.iter() }

    /// Drop the first `n` events. Call *only* after storage commits.
    ///
    /// If `n` exceeds the number of queued events, the buffer is fully
    /// drained without panicking.
    pub fn commit(&mut self, n: usize) {
        for _ in 0..n.min(self.pending.len()) {
            self.pending.pop_front();
        }
    }

    /// Crate-internal — transitions call this. Returns an [`EventStamp`]
    /// recording this buffer's id and the seq number the event landed at;
    /// `Provisional` records this so `confirm` can reject an unrelated
    /// buffer with a coincidentally-matching seq.
    pub(crate) fn push(&mut self, e: E) -> EventStamp {
        self.pending.push_back(e);
        self.pushed_total += 1;
        EventStamp { buffer_id: self.id, seq: self.pushed_total }
    }
}

impl<E> Default for EventBuffer<E> {
    fn default() -> Self { Self::new() }
}

/// Type alias for an [`EventBuffer`] holding receiver session events.
pub type ReceiverEventBuffer = EventBuffer<crate::receive::v2::SessionEvent>;

/// Type alias for an [`EventBuffer`] holding sender session events.
pub type SenderEventBuffer = EventBuffer<crate::send::v2::SessionEvent>;

/// Wraps a typestate that mints an externally-visible side effect (a payjoin
/// URI, an HTTP request to a directory). The inner value is reachable only
/// after the event that produced it is durable in the same [`EventBuffer`]
/// the `Provisional` was minted against.
///
/// `Provisional` is stamped with the buffer's [`BufferId`] and the seq number
/// the producing event landed at. [`Self::confirm`] checks both: the buffer's
/// id must match (so a confirm against a freshly-created or unrelated buffer
/// is rejected even if its `committed_count` happens to be large enough), and
/// the buffer's `committed_count` must have reached the recorded seq.
///
/// Holding a `Provisional<T>` across an `await` or across threads is fine —
/// it's a plain value, not a borrow.
#[derive(Debug)]
pub struct Provisional<T> {
    inner: T,
    stamp: EventStamp,
}

impl<T> Provisional<T> {
    /// Crate-internal — transitions construct these from the stamp returned
    /// by [`EventBuffer::push`] on the buffer they pushed into.
    pub(crate) fn new(inner: T, stamp: EventStamp) -> Self { Self { inner, stamp } }

    /// Confirm once the producing event is durable in `buf`. On failure,
    /// returns a [`ConfirmFailure`] that carries the original `Provisional`
    /// back to the caller along with a kind indicating *why* the confirm
    /// failed: a `NotYetPersisted` failure is a retry-after-drain signal,
    /// whereas a `WrongBuffer` failure is a programmer bug (the caller passed
    /// a freshly-created or unrelated buffer instead of the one this
    /// `Provisional` was minted against).
    pub fn confirm<E>(self, buf: &EventBuffer<E>) -> Result<T, ConfirmFailure<T>> {
        if buf.id() != self.stamp.buffer_id {
            return Err(ConfirmFailure {
                kind: ConfirmFailureKind::WrongBuffer,
                provisional: self,
            });
        }
        if buf.committed_count() >= self.stamp.seq {
            Ok(self.inner)
        } else {
            Err(ConfirmFailure { kind: ConfirmFailureKind::NotYetPersisted, provisional: self })
        }
    }

    /// Inspect the inner value without consuming.
    ///
    /// This does NOT bypass the durability gate — it borrows the inner value
    /// behind a layer of indirection. Use sparingly: producing externally
    /// visible side effects from a borrowed inner before [`Self::confirm`]
    /// defeats the persist-before-expose guarantee.
    pub fn peek_inner(&self) -> &T { &self.inner }
}

/// Failure returned by [`Provisional::confirm`]. The returned `Provisional` is
/// available for retry, and `kind` discriminates between the two failure
/// modes so foreign code can distinguish a benign "drain more and retry"
/// from a programmer bug.
#[derive(Debug)]
pub struct ConfirmFailure<T> {
    /// Why confirm failed.
    pub kind: ConfirmFailureKind,
    /// The original [`Provisional`], returned so the caller can retry (for
    /// `NotYetPersisted`) or recover (for `WrongBuffer`).
    pub provisional: Provisional<T>,
}

/// Discriminator for [`ConfirmFailure`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfirmFailureKind {
    /// The supplied [`EventBuffer`]'s id did not match the buffer this
    /// `Provisional` was minted against. Retrying with the same buffer will
    /// never succeed; this is a programmer error (e.g. a freshly-constructed
    /// buffer was passed by mistake).
    WrongBuffer,
    /// The producing event has not yet reached this buffer's
    /// `committed_count()`. Drain more events through storage and call
    /// [`Provisional::confirm`] again.
    NotYetPersisted,
}

/// Protocol-level error returned by action methods that push events into an
/// [`EventBuffer`]. Storage errors are not represented here — those surface
/// from the caller's drain loop.
#[derive(Debug)]
pub enum ApiError<Err, ErrorState = ()> {
    /// Retry from the same state.
    Transient(Err),
    /// Session is terminally closed.
    Fatal(Err),
    /// Fatal error that also produced a state transition to `ErrorState`.
    FatalWithState(Err, ErrorState),
}

impl<Err: std::error::Error, ErrorState: fmt::Debug> fmt::Display for ApiError<Err, ErrorState> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApiError::Transient(err) => write!(f, "Transient error: {err}"),
            ApiError::Fatal(err) | ApiError::FatalWithState(err, _) =>
                write!(f, "Fatal error: {err}"),
        }
    }
}

impl<Err: std::error::Error, ErrorState: fmt::Debug> std::error::Error
    for ApiError<Err, ErrorState>
{
}

/// Represents a state transition that either progresses to a new state or maintains the current state
#[derive(Debug, PartialEq)]
pub enum OptionalTransitionOutcome<NextState, CurrentState> {
    /// A successful state transition that returned a next state
    Progress(NextState),
    /// A state transition returned no value. Caller should resume from the current state
    Stasis(CurrentState),
}

/// In-memory event log used by tests and as a reference implementation for
/// callers that don't yet have their own backing storage.
///
/// `InMemoryPersister` is a plain value with inherent `save_event`, `load`,
/// and `drain` methods — there is no `SessionPersister` trait. Action methods
/// in the receiver/sender API take an [`EventBuffer`] directly; the user's
/// drain code (sync or async) is plain Rust, not a trait method.
#[derive(Clone)]
pub struct InMemoryPersister<V> {
    pub(crate) inner: std::sync::Arc<std::sync::RwLock<InnerStorage<V>>>,
}

impl<V> Default for InMemoryPersister<V> {
    fn default() -> Self {
        Self { inner: std::sync::Arc::new(std::sync::RwLock::new(InnerStorage::default())) }
    }
}

#[derive(Clone)]
pub(crate) struct InnerStorage<V> {
    pub(crate) events: std::sync::Arc<Vec<V>>,
}

impl<V> Default for InnerStorage<V> {
    fn default() -> Self { Self { events: std::sync::Arc::new(vec![]) } }
}

impl<V> InMemoryPersister<V>
where
    V: Clone + 'static,
{
    /// Append an event to the in-memory log.
    pub fn save_event(&self, event: V) -> Result<(), std::convert::Infallible> {
        let mut inner = self.inner.write().expect("Lock should not be poisoned");
        std::sync::Arc::make_mut(&mut inner.events).push(event);
        Ok(())
    }

    /// Iterate the persisted events in append order.
    pub fn load(&self) -> Result<Box<dyn Iterator<Item = V>>, std::convert::Infallible> {
        let inner = self.inner.read().expect("Lock should not be poisoned");
        let events = std::sync::Arc::clone(&inner.events);
        Ok(Box::new(
            std::sync::Arc::try_unwrap(events).unwrap_or_else(|arc| (*arc).clone()).into_iter(),
        ))
    }

    /// Drain `buf` into the in-memory log, committing each event after it lands.
    pub fn drain(&self, buf: &mut EventBuffer<V>) -> Result<(), std::convert::Infallible> {
        loop {
            let Some(event) = buf.peek().next().cloned() else {
                return Ok(());
            };
            self.save_event(event)?;
            buf.commit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::*;

    #[test]
    fn event_buffer_push_increments_committed_only_after_commit() {
        let mut buf: EventBuffer<&'static str> = EventBuffer::new();
        assert_eq!(buf.committed_count(), 0);
        assert!(buf.is_empty());

        let stamp = buf.push("a");
        assert_eq!(stamp.seq, 1);
        assert_eq!(stamp.buffer_id, buf.id());
        assert_eq!(buf.len(), 1);
        assert_eq!(buf.committed_count(), 0, "push alone does not commit");

        buf.commit(1);
        assert_eq!(buf.committed_count(), 1);
        assert!(buf.is_empty());
    }

    #[test]
    fn event_buffer_peek_is_non_destructive() {
        let mut buf: EventBuffer<&'static str> = EventBuffer::new();
        buf.push("a");
        buf.push("b");

        let first: Vec<&&'static str> = buf.peek().collect();
        let second: Vec<&&'static str> = buf.peek().collect();
        assert_eq!(first, second);
        assert_eq!(first.len(), 2);
        assert_eq!(buf.len(), 2, "peek does not mutate");
    }

    #[test]
    fn event_buffer_commit_past_end_is_noop() {
        let mut buf: EventBuffer<&'static str> = EventBuffer::new();
        buf.push("a");
        buf.push("b");

        buf.commit(100);
        assert!(buf.is_empty());
        assert_eq!(buf.committed_count(), 2);

        // Committing again on an empty buffer is also a no-op, not a panic.
        buf.commit(5);
        assert!(buf.is_empty());
        assert_eq!(buf.committed_count(), 2);
    }

    #[test]
    fn provisional_confirm_before_and_after_commit() {
        let mut buf: EventBuffer<&'static str> = EventBuffer::new();
        let stamp = buf.push("create");
        let provisional = Provisional::new(42_u32, stamp);

        // Before commit: returns NotYetPersisted with the provisional back.
        let provisional = match provisional.confirm(&buf) {
            Ok(_) => panic!("confirmed before commit"),
            Err(f) => {
                assert_eq!(f.kind, ConfirmFailureKind::NotYetPersisted);
                f.provisional
            }
        };

        buf.commit(1);

        // After commit: returns inner.
        let inner =
            provisional.confirm(&buf).unwrap_or_else(|_| panic!("not confirmed after commit"));
        assert_eq!(inner, 42);
    }

    #[test]
    fn provisional_retry_loop_pattern() {
        let mut buf: EventBuffer<&'static str> = EventBuffer::new();
        let stamp = buf.push("create");
        let mut p = Provisional::new(42_u32, stamp);

        // Loop until confirm succeeds, persisting one event per iteration.
        let inner = loop {
            match p.confirm(&buf) {
                Ok(v) => break v,
                Err(f) => {
                    assert_eq!(f.kind, ConfirmFailureKind::NotYetPersisted);
                    p = f.provisional;
                    buf.commit(1);
                }
            }
        };
        assert_eq!(inner, 42);
    }

    #[test]
    fn after_replay_sets_committed_count() {
        let buf: EventBuffer<&'static str> = EventBuffer::after_replay(5);
        assert_eq!(buf.committed_count(), 5);
        assert_eq!(buf.len(), 0);
        assert!(buf.is_empty());
    }

    #[test]
    fn after_replay_then_push_continues_sequence() {
        let mut buf: EventBuffer<&'static str> = EventBuffer::after_replay(3);
        let stamp = buf.push("next");
        assert_eq!(stamp.seq, 4, "seq numbers continue from replayed count");
        assert_eq!(buf.committed_count(), 3);

        buf.commit(1);
        assert_eq!(buf.committed_count(), 4);
    }

    #[test]
    fn provisional_peek_inner_does_not_consume() {
        let mut buf: EventBuffer<&'static str> = EventBuffer::new();
        let stamp = buf.push("create");
        let p = Provisional::new("hidden".to_string(), stamp);

        assert_eq!(p.peek_inner(), "hidden");
        assert_eq!(p.peek_inner(), "hidden");
    }

    /// Provisional rejects confirm against a freshly created buffer even if
    /// that buffer's committed_count happens to reach the stamp's seq, and
    /// surfaces that as `WrongBuffer` (not `NotYetPersisted`).
    #[test]
    fn provisional_confirm_against_fresh_buffer_is_rejected() {
        let mut buf1: EventBuffer<&'static str> = EventBuffer::new();
        let stamp = buf1.push("create");
        let provisional = Provisional::new(42_u32, stamp);
        buf1.commit(1);
        // buf1 would confirm, but the user passes buf2 by mistake.
        let buf2: EventBuffer<&'static str> = EventBuffer::new();
        let failure = provisional.confirm(&buf2).expect_err("wrong buffer should fail");
        assert_eq!(
            failure.kind,
            ConfirmFailureKind::WrongBuffer,
            "fresh unrelated buffer must surface WrongBuffer, never NotYetPersisted"
        );
    }

    /// Provisional rejects confirm against an unrelated buffer with a
    /// coincidentally-matching committed_count. Without buffer-id witness
    /// this was the silently-accepts case that defeated the gate.
    #[test]
    fn provisional_confirm_against_unrelated_buffer_with_same_seq_is_rejected() {
        let mut buf_a: EventBuffer<&'static str> = EventBuffer::new();
        let stamp = buf_a.push("gate");
        let provisional = Provisional::new(99_u32, stamp);
        // buf_a is dropped without draining — the gate event was never persisted.
        drop(buf_a);

        // A different buffer happens to have one committed event.
        let mut buf_b: EventBuffer<&'static str> = EventBuffer::new();
        buf_b.push("unrelated");
        buf_b.commit(1);
        assert_eq!(buf_b.committed_count(), 1);

        // Even though the seq counts line up, the buffer ids do not — and the
        // failure kind must reflect that (so retrying isn't pointless).
        let failure = provisional.confirm(&buf_b).expect_err("wrong buffer should fail");
        assert_eq!(failure.kind, ConfirmFailureKind::WrongBuffer);
    }

    /// Demonstrates that an arbitrary drain loop — sync or async — works
    /// on the same buffer value. The library ships no trait-bound bridge:
    /// `EventBuffer` is the unified API, callers own the drain.
    #[tokio::test]
    async fn caller_owned_drain_loops_share_one_buffer_shape() {
        // Sync drain into a Vec<E>: peek the front event, write it (here,
        // append to a Vec), commit one. No SessionPersister involvement.
        let mut buf: EventBuffer<String> = EventBuffer::new();
        buf.push("a".to_string());
        let seq = buf.push("b".to_string());
        let provisional = Provisional::new("uri".to_string(), seq);

        let mut storage: Vec<String> = Vec::new();
        loop {
            let Some(event) = buf.peek().next().cloned() else { break };
            storage.push(event);
            buf.commit(1);
        }
        assert_eq!(storage, vec!["a".to_string(), "b".to_string()]);
        assert!(buf.is_empty());
        assert_eq!(buf.committed_count(), 2);
        let uri = provisional.confirm(&buf).unwrap_or_else(|_| panic!("confirmed"));
        assert_eq!(uri, "uri");

        // Async drain into an Arc<Mutex<Vec<E>>>: identical shape, just
        // awaiting the write. Same EventBuffer<E> type, same Provisional<T>.
        let mut buf: EventBuffer<String> = EventBuffer::new();
        let seq = buf.push("only".to_string());
        let provisional = Provisional::new(7_u32, seq);

        let storage = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::<String>::new()));
        loop {
            let Some(event) = buf.peek().next().cloned() else { break };
            storage.lock().await.push(event);
            buf.commit(1);
        }
        assert_eq!(storage.lock().await.as_slice(), ["only"]);
        let inner = provisional.confirm(&buf).unwrap_or_else(|_| panic!("confirmed"));
        assert_eq!(inner, 7);
    }
}
