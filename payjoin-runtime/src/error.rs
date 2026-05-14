use core::fmt;

use payjoin::persist::ApiError;

/// Errors surfaced by the runtime.
///
/// `Error` is intentionally coarse: the underlying payjoin crate has a deep
/// error hierarchy, but the runtime collapses it into a handful of semantic
/// buckets to keep match arms manageable. The original error message is
/// preserved in the `Payjoin(_)` / `Wallet(_)` payloads for diagnostics.
#[derive(Debug)]
pub enum Error {
    /// A payjoin protocol-level error (encapsulation, directory response,
    /// state-machine violation, etc.).
    Payjoin(String),
    /// A wallet-side failure raised by the caller while handling a
    /// [`ReceiverStep`](crate::ReceiverStep) / [`SenderStep`](crate::SenderStep).
    Wallet(String),
    /// The session has already terminated (Done or Failed). All subsequent
    /// `poll` / `feed_*` calls return this error.
    Terminated,
}

impl Error {
    pub(crate) fn payjoin<E: fmt::Display>(e: E) -> Self {
        Self::Payjoin(e.to_string())
    }

    /// Map an `ApiError` from a transition's `deconstruct` outcome into our
    /// flat `Error::Payjoin`. The error-state half (where present) is folded
    /// into the message via its `Debug` impl.
    pub(crate) fn from_api<Err, ES>(e: ApiError<Err, ES>) -> Self
    where
        Err: fmt::Display,
        ES: fmt::Debug,
    {
        match e {
            ApiError::Transient(err) => Self::Payjoin(format!("transient: {err}")),
            ApiError::Fatal(err) => Self::Payjoin(format!("fatal: {err}")),
            ApiError::FatalWithState(err, st) => {
                Self::Payjoin(format!("fatal {err} ({st:?})"))
            }
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Payjoin(s) => write!(f, "payjoin protocol error: {s}"),
            Error::Wallet(s) => write!(f, "wallet error: {s}"),
            Error::Terminated => write!(f, "session already terminated"),
        }
    }
}

impl std::error::Error for Error {}
