//! Sans-IO state machines for the payjoin v2 protocol.
//!
//! `payjoin_runtime` exposes a driver per role ([`ReceiverSession`],
//! [`SenderSession`]) that owns the multi-stage payjoin typestate, the OHTTP
//! polling cycle, and the `.save(&persister)` ceremony. The caller drives each
//! session by alternating [`poll`](ReceiverSession::poll) — which yields a
//! [`ReceiverStep`] / [`SenderStep`] describing the next side effect — and one
//! of the `feed_*` methods, which delivers the answer back to the runtime.
//!
//! Every external side effect (HTTP, sleep, wallet decision, signing) is
//! surfaced as its own [`ReceiverStep`] / [`SenderStep`] variant. The runtime
//! itself never performs IO and never calls back into a wallet trait; the
//! caller routes those decisions wherever they wish (a worker thread, an async
//! task, a hardware signer) and feeds the answer back when ready.
//!
//! # Receiver sketch
//!
//! ```ignore
//! let mut session = ReceiverSession::new(builder, relay, fee_range)?;
//! loop {
//!     match session.poll() {
//!         ReceiverStep::Save(events) => persist_atomically(events),
//!         ReceiverStep::SendRequest(req) => {
//!             let resp = http.post(req).await?;
//!             session.feed_response(resp.bytes().to_vec())?;
//!         }
//!         ReceiverStep::Backoff => sleep(Duration::from_secs(2)).await,
//!         ReceiverStep::CheckBroadcast(tx) => {
//!             let ok = mempool_accepts(&tx).await?;
//!             session.feed_broadcast_check(ok)?;
//!         }
//!         ReceiverStep::ResolveOwned(spks) => {
//!             let answers = spks.iter().map(|s| wallet.owns(s)).collect();
//!             session.feed_owned(answers)?;
//!         }
//!         ReceiverStep::Contribute => {
//!             let inputs = wallet.pick_payjoin_inputs()?;
//!             session.feed_contribute(inputs)?;
//!         }
//!         ReceiverStep::SignAndFinalize(mut psbt) => {
//!             wallet.sign(&mut psbt)?;
//!             session.feed_signed_psbt(psbt)?;
//!         }
//!         ReceiverStep::Done => break,
//!         ReceiverStep::Failed(e) => return Err(e.into()),
//!     }
//! }
//! ```

#![warn(missing_docs)]

mod error;
mod persister;
mod psbt;
mod receiver;
mod sender;

pub use error::Error;
pub use psbt::restore_psbt_utxos;
pub use receiver::{ReceiverSession, ReceiverStep};
pub use sender::{SenderSession, SenderStep};

// Re-exports so consumers can build the runtime's inputs without a direct
// `payjoin` dependency.
pub use payjoin::receive::v2::{ReceiverBuilder, SessionEvent as ReceiverSessionEvent};
pub use payjoin::receive::InputPair;
pub use payjoin::send::v2::{SenderBuilder, SessionEvent as SenderSessionEvent};
pub use payjoin::{ImplementationError, OhttpKeys, PjUri, Request, Uri, UriExt};

use bitcoin::FeeRate;

/// Fee-range bounds passed by the receiver to payjoin's `apply_fee_range`.
///
/// Both endpoints are optional: `None` for `min` means "accept payjoin's
/// recommended minimum (broadcast-min)"; `None` for `max` means "the receiver
/// will not pay for any of the network fee".
#[derive(Debug, Clone, Copy, Default)]
pub struct FeeRange {
    /// Minimum effective feerate the receiver accepts on the proposal.
    pub min: Option<FeeRate>,
    /// Maximum effective feerate the receiver is willing to pay for their own
    /// contributed input/output. `None` opts out of receiver-paid fees.
    pub max: Option<FeeRate>,
}
