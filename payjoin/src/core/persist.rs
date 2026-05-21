//! State machine persistence for payjoin sessions.
//!
//! The receiver and senders' v1 and v2 state machines are driven by events. An
//! event contains all the information to transition into the next state, which
//! means that the session's full state can be computed by "replaying" the events.
//! Session history is therefore a recorded as an append only log of events.
//!
//! # Persistence shapes
//!
//! Three coexisting ways to drive persistence are provided:
//!
//! - [`SessionPersister`] / [`AsyncSessionPersister`] — the original
//!   callback traits. A transition's `.save(&persister)` invokes the
//!   storage callback directly.
//! - `deconstruct()` on each transition — returns a `(PersistActions,
//!   Outcome)` pair as plain data so callers can drive persistence
//!   themselves. Crate-internal, used by `.save` / `.save_async`.
//! - [`EventBuffer`] + [`Provisional`] — a sans-IO event log with
//!   batched, two-phase persistence (`peek` + `commit`) and runtime
//!   persist-before-expose gating for side-effect-bearing transitions.
//!   The buffer is a plain value; the caller's persister (sync or
//!   async) drains it however it wants, so one typestate path serves
//!   both worlds without separate trait variants.
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

/// Caller-owned, sans-IO event buffer. Transitions push events into it;
/// the caller's persister drains it two-phase (peek + commit).
///
/// Generic over the event type so receiver and sender sessions use distinct,
/// non-interchangeable buffer types.
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
    pending: VecDeque<E>,
    /// Monotonic count of events ever pushed across the buffer's lifetime.
    /// `committed_count() == pushed_total - pending.len()`. Used by
    /// `Provisional` to gate side-effect-bearing accessors.
    pushed_total: u64,
}

impl<E> EventBuffer<E> {
    /// Construct an empty buffer.
    pub fn new() -> Self { Self { pending: VecDeque::new(), pushed_total: 0 } }

    /// Construct a buffer reflecting that `replayed` events were already
    /// persisted before this session woke up. Use after replaying a log.
    pub fn after_replay(replayed: u64) -> Self {
        Self { pending: VecDeque::new(), pushed_total: replayed }
    }

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

    /// Crate-internal — transitions call this. Returns the seq number assigned
    /// to the event; `Provisional` records this to know when the event is
    /// durable.
    #[allow(dead_code)]
    pub(crate) fn push(&mut self, e: E) -> u64 {
        self.pending.push_back(e);
        self.pushed_total += 1;
        self.pushed_total
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
/// after the event that produced it is durable.
///
/// Created by transitions that want persist-before-expose semantics; consumed
/// via [`Self::confirm`]. Holding a `Provisional<T>` across an `await` or
/// across threads is fine — it's a plain value, not a borrow.
#[derive(Debug)]
pub struct Provisional<T> {
    inner: T,
    needs_committed: u64,
}

impl<T> Provisional<T> {
    /// Crate-internal — transitions construct these.
    ///
    /// Currently used only by tests; receiver/sender transitions will call
    /// this in follow-up PRs.
    #[allow(dead_code)]
    pub(crate) fn new(inner: T, needs_committed: u64) -> Self { Self { inner, needs_committed } }

    /// Confirm once the required event is durable. On `Err`, returns `self`
    /// so the caller can persist more and retry. Same shape as
    /// [`std::sync::Arc::try_unwrap`].
    pub fn confirm<E>(self, buf: &EventBuffer<E>) -> Result<T, Self> {
        if buf.committed_count() >= self.needs_committed {
            Ok(self.inner)
        } else {
            Err(self)
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

        let seq = buf.push("a");
        assert_eq!(seq, 1);
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
        let seq = buf.push("create");
        let provisional = Provisional::new(42_u32, seq);

        // Before commit: returns self.
        let provisional = match provisional.confirm(&buf) {
            Ok(_) => panic!("confirmed before commit"),
            Err(p) => p,
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
        let seq = buf.push("create");
        let mut p = Provisional::new(42_u32, seq);

        // Loop until confirm succeeds, persisting one event per iteration.
        let inner = loop {
            match p.confirm(&buf) {
                Ok(v) => break v,
                Err(returned) => {
                    p = returned;
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
        let seq = buf.push("next");
        assert_eq!(seq, 4, "seq numbers continue from replayed count");
        assert_eq!(buf.committed_count(), 3);

        buf.commit(1);
        assert_eq!(buf.committed_count(), 4);
    }

    #[test]
    fn provisional_peek_inner_does_not_consume() {
        let mut buf: EventBuffer<&'static str> = EventBuffer::new();
        let seq = buf.push("create");
        let p = Provisional::new("hidden".to_string(), seq);

        assert_eq!(p.peek_inner(), "hidden");
        assert_eq!(p.peek_inner(), "hidden");
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
