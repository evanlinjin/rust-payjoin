//! In-tree persister used solely for `resume_from_events`.
//!
//! `payjoin::receive::v2::replay_event_log` / `send::v2::replay_event_log`
//! consume a `SessionPersister` to load the stored event log. The live forward
//! flow no longer needs a persister — each typestate transition uses
//! [`deconstruct`](payjoin::persist::MaybeFatalTransition::deconstruct) to
//! surface the [`PersistAction`](payjoin::persist::PersistAction) as data.

use std::convert::Infallible;
use std::sync::Mutex;

use payjoin::persist::SessionPersister;

/// Replays a fixed event log when payjoin's `replay_event_log` asks the
/// persister to `load()`. Used by `*Session::resume_from_events`.
///
/// `save_event` is a no-op — replay is read-only.
pub(crate) struct Replay<E> {
    pub(crate) events: Mutex<Option<Vec<E>>>,
}

impl<E> Replay<E> {
    pub(crate) fn new(events: Vec<E>) -> Self {
        Self {
            events: Mutex::new(Some(events)),
        }
    }
}

impl<E: Send + 'static> SessionPersister for Replay<E> {
    type InternalStorageError = Infallible;
    type SessionEvent = E;

    fn save_event(&self, _event: E) -> Result<(), Self::InternalStorageError> {
        Ok(())
    }

    fn load(
        &self,
    ) -> Result<Box<dyn Iterator<Item = Self::SessionEvent>>, Self::InternalStorageError> {
        let taken = self
            .events
            .lock()
            .expect("replay-events mutex poisoned")
            .take()
            .unwrap_or_default();
        Ok(Box::new(taken.into_iter()))
    }

    fn close(&self) -> Result<(), Self::InternalStorageError> {
        Ok(())
    }
}
