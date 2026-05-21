use std::str::FromStr;
use std::sync::{Arc, Mutex};

pub use error::{
    BuildSenderError, CreateRequestError, EncapsulationError, PsbtParseError, ResponseError,
    SenderApiError, SenderInputError,
};

pub use crate::error::{ImplementationError, ProvisionalConfirmError, SerdeJsonError};
use crate::ohttp::ClientResponse;
use crate::request::Request;
use crate::send::error::SenderReplayError;
use crate::uri::PjUri;
use crate::validation::{validate_amount_sat, validate_fee_rate_sat_per_kwu};

pub mod error;

// =============================================================================
// EventBuffer for sender session events
// =============================================================================

/// Caller-owned, sans-IO event buffer for sender session events.
///
/// Action methods push events into this buffer; the caller drains it through
/// their storage (sync or async).
#[derive(uniffi::Object)]
pub struct SenderEventBuffer {
    pub(crate) inner: Mutex<payjoin::persist::EventBuffer<payjoin::send::v2::SessionEvent>>,
}

#[uniffi::export]
impl SenderEventBuffer {
    /// Construct an empty buffer with a fresh id.
    #[uniffi::constructor]
    pub fn new() -> Arc<Self> {
        Arc::new(Self { inner: Mutex::new(payjoin::persist::EventBuffer::new()) })
    }

    /// Construct a buffer reflecting that `replayed` events were already
    /// persisted before this session woke up. Use after replaying a log.
    #[uniffi::constructor]
    pub fn after_replay(replayed: u64) -> Arc<Self> {
        Arc::new(Self { inner: Mutex::new(payjoin::persist::EventBuffer::after_replay(replayed)) })
    }

    /// Returns true if no events are queued for persistence.
    pub fn is_empty(&self) -> bool { self.inner.lock().expect("poisoned").is_empty() }

    /// Number of events queued for persistence (not yet committed).
    pub fn len(&self) -> u64 { self.inner.lock().expect("poisoned").len() as u64 }

    /// Total events durably persisted across the buffer's lifetime.
    pub fn committed_count(&self) -> u64 { self.inner.lock().expect("poisoned").committed_count() }

    /// Borrow pending events as typed [`SenderSessionEvent`]s. The buffer is
    /// not mutated; call `commit(n)` after writing the first `n` of these to
    /// storage. Foreign code typically serializes each event with
    /// [`SenderSessionEvent::to_json`] at the storage boundary.
    pub fn peek(&self) -> Vec<Arc<SenderSessionEvent>> {
        let g = self.inner.lock().expect("poisoned");
        g.peek().map(|e| Arc::new(SenderSessionEvent::from(e.clone()))).collect()
    }

    /// Drop the first `n` events. Call only after storage commits.
    pub fn commit(&self, n: u64) { self.inner.lock().expect("poisoned").commit(n as usize); }
}

// =============================================================================
// SessionEvent wrapper
// =============================================================================

#[derive(uniffi::Object, Debug, Clone)]
pub struct SenderSessionEvent(payjoin::send::v2::SessionEvent);

impl From<SenderSessionEvent> for payjoin::send::v2::SessionEvent {
    fn from(value: SenderSessionEvent) -> Self { value.0 }
}

impl From<payjoin::send::v2::SessionEvent> for SenderSessionEvent {
    fn from(value: payjoin::send::v2::SessionEvent) -> Self { SenderSessionEvent(value) }
}

#[uniffi::export]
impl SenderSessionEvent {
    pub fn to_json(&self) -> Result<String, SerdeJsonError> {
        serde_json::to_string(&self.0).map_err(Into::into)
    }

    #[uniffi::constructor]
    pub fn from_json(json: String) -> Result<Self, SerdeJsonError> {
        let event: payjoin::send::v2::SessionEvent = serde_json::from_str(&json)?;
        Ok(SenderSessionEvent(event))
    }
}

// =============================================================================
// SessionOutcome + SendSession enum
// =============================================================================

#[derive(Clone, uniffi::Object)]
pub struct SenderSessionOutcome(payjoin::send::v2::SessionOutcome);

impl From<payjoin::send::v2::SessionOutcome> for SenderSessionOutcome {
    fn from(value: payjoin::send::v2::SessionOutcome) -> Self { Self(value) }
}

impl From<SenderSessionOutcome> for payjoin::send::v2::SessionOutcome {
    fn from(value: SenderSessionOutcome) -> Self { value.0 }
}

#[uniffi::export]
impl SenderSessionOutcome {
    pub fn is_success(&self) -> bool {
        matches!(self.0, payjoin::send::v2::SessionOutcome::Success(_))
    }

    pub fn success_psbt_base64(&self) -> Option<String> {
        match &self.0 {
            payjoin::send::v2::SessionOutcome::Success(psbt) => Some(psbt.to_string()),
            _ => None,
        }
    }

    pub fn is_failure(&self) -> bool {
        matches!(self.0, payjoin::send::v2::SessionOutcome::Failure)
    }

    pub fn is_cancelled(&self) -> bool {
        matches!(self.0, payjoin::send::v2::SessionOutcome::Cancel)
    }
}

#[derive(Clone, uniffi::Enum)]
pub enum SendSession {
    WithReplyKey { inner: Arc<WithReplyKey> },
    PollingForProposal { inner: Arc<PollingForProposal> },
    Closed { inner: Arc<SenderSessionOutcome> },
}

impl From<payjoin::send::v2::SendSession> for SendSession {
    fn from(value: payjoin::send::v2::SendSession) -> Self {
        use payjoin::send::v2::SendSession;
        match value {
            SendSession::WithReplyKey(inner) =>
                Self::WithReplyKey { inner: Arc::new(inner.into()) },
            SendSession::PollingForProposal(inner) =>
                Self::PollingForProposal { inner: Arc::new(inner.into()) },
            SendSession::Closed(session_outcome) =>
                Self::Closed { inner: Arc::new(session_outcome.into()) },
        }
    }
}

// =============================================================================
// Replay
// =============================================================================

#[derive(uniffi::Object)]
pub struct SenderReplayResult {
    state: SendSession,
    session_history: SenderSessionHistory,
    event_count: u64,
}

#[uniffi::export]
impl SenderReplayResult {
    pub fn state(&self) -> SendSession { self.state.clone() }

    pub fn session_history(&self) -> SenderSessionHistory { self.session_history.clone() }

    /// Number of events that were replayed.
    pub fn event_count(&self) -> u64 { self.event_count }

    /// Construct a fresh [`SenderEventBuffer`] whose `committed_count`
    /// matches the number of replayed events. Equivalent to
    /// [`SenderEventBuffer::after_replay`] with [`Self::event_count`], but
    /// avoids the two-step ceremony at the call site.
    pub fn into_buffer(&self) -> Arc<SenderEventBuffer> {
        SenderEventBuffer::after_replay(self.event_count)
    }
}

/// Replay the persisted event log into a starting [`SendSession`] and
/// [`SenderSessionHistory`]. The caller loads its events from storage
/// however it likes (sync or async, native code), deserializes each one via
/// [`SenderSessionEvent::from_json`], and passes them in here; the library is
/// sans-IO.
#[uniffi::export]
pub fn replay_sender_event_log(
    events: Vec<Arc<SenderSessionEvent>>,
) -> Result<SenderReplayResult, SenderReplayError> {
    let parsed: Vec<payjoin::send::v2::SessionEvent> =
        events.into_iter().map(|e| Arc::unwrap_or_clone(e).into()).collect();
    let event_count = parsed.len() as u64;
    let (state, session_history) = payjoin::send::v2::replay_event_log(parsed)?;
    Ok(SenderReplayResult {
        state: state.into(),
        session_history: session_history.into(),
        event_count,
    })
}

// =============================================================================
// SessionStatus + SessionHistory
// =============================================================================

#[derive(uniffi::Object)]
pub struct SenderSessionStatus {
    inner: payjoin::send::v2::SessionStatus,
}

impl From<payjoin::send::v2::SessionStatus> for SenderSessionStatus {
    fn from(value: payjoin::send::v2::SessionStatus) -> Self { Self { inner: value } }
}

impl From<SenderSessionStatus> for payjoin::send::v2::SessionStatus {
    fn from(value: SenderSessionStatus) -> Self { value.inner }
}

#[derive(uniffi::Object)]
pub struct PjParam(payjoin::uri::v2::PjParam);

impl From<payjoin::uri::v2::PjParam> for PjParam {
    fn from(value: payjoin::uri::v2::PjParam) -> Self { Self(value) }
}

impl From<PjParam> for payjoin::uri::v2::PjParam {
    fn from(value: PjParam) -> Self { value.0 }
}

#[derive(uniffi::Object, Clone)]
pub struct SenderSessionHistory(pub payjoin::send::v2::SessionHistory);

impl From<payjoin::send::v2::SessionHistory> for SenderSessionHistory {
    fn from(value: payjoin::send::v2::SessionHistory) -> Self { Self(value) }
}

impl From<SenderSessionHistory> for payjoin::send::v2::SessionHistory {
    fn from(value: SenderSessionHistory) -> Self { value.0 }
}

#[uniffi::export]
impl SenderSessionHistory {
    /// Fallback transaction from the session.
    pub fn fallback_tx(&self) -> Vec<u8> {
        payjoin::bitcoin::consensus::encode::serialize(&self.0.fallback_tx())
    }

    pub fn pj_param(&self) -> Arc<PjParam> { Arc::new(self.0.pj_param().to_owned().into()) }

    pub fn status(&self) -> SenderSessionStatus { self.0.status().into() }
}

// =============================================================================
// Sender typestates
// =============================================================================

/// Helper macro to add a `cancel` method to each sender typestate.
macro_rules! impl_cancel_for_sender {
    ($ty:ident) => {
        #[uniffi::export]
        impl $ty {
            /// Cancel the Payjoin session immediately.
            ///
            /// Pushes a `Closed(Cancel)` event into `buf` and returns the
            /// fallback transaction as consensus-encoded raw bytes. The fallback
            /// is the sender's original transaction that should be broadcast to
            /// complete the payment without Payjoin.
            ///
            /// This is a terminal action — the session cannot be used after
            /// cancellation. The caller is expected to drain `buf` and treat the
            /// `Closed` event as the session boundary.
            pub fn cancel(&self, buf: &SenderEventBuffer) -> Vec<u8> {
                let mut g = buf.inner.lock().expect("poisoned");
                let tx = self.0.clone().cancel(&mut *g);
                payjoin::bitcoin::consensus::serialize(&tx)
            }
        }
    };
}

// -----------------------------------------------------------------------------
// SenderBuilder
// -----------------------------------------------------------------------------

#[derive(Clone, uniffi::Object)]
pub struct SenderBuilder(payjoin::send::v2::SenderBuilder);

impl From<payjoin::send::v2::SenderBuilder> for SenderBuilder {
    fn from(value: payjoin::send::v2::SenderBuilder) -> Self { Self(value) }
}

#[uniffi::export]
impl SenderBuilder {
    /// Prepare an HTTP request and request context to process the response.
    ///
    /// Call [`SenderBuilder::build_recommended()`] or other `build` methods
    /// to create a [`WithReplyKey`].
    #[uniffi::constructor]
    pub fn new(psbt: String, uri: Arc<PjUri>) -> Result<Self, SenderInputError> {
        let psbt = payjoin::bitcoin::psbt::Psbt::from_str(psbt.as_str())
            .map_err(PsbtParseError::from)
            .map_err(SenderInputError::Psbt)?;
        let builder = payjoin::send::v2::SenderBuilder::new(psbt, Arc::unwrap_or_clone(uri).into());
        Ok(builder.into())
    }

    /// Disable output substitution even if the receiver didn't.
    pub fn always_disable_output_substitution(&self) -> Self {
        self.0.clone().always_disable_output_substitution().into()
    }

    /// Build with recommended fee contribution. Pushes a `Created` event into
    /// `buf` and returns a [`ProvisionalWithReplyKey`] guarding the sender.
    /// The session is not externally observable (cannot post to the directory)
    /// until the buffer's `Created` entry is durably persisted and the
    /// provisional is confirmed.
    pub fn build_recommended(
        &self,
        min_fee_rate_sat_per_kwu: u64,
        buf: &SenderEventBuffer,
    ) -> Result<Arc<ProvisionalWithReplyKey>, SenderInputError> {
        let fee_rate = validate_fee_rate_sat_per_kwu(min_fee_rate_sat_per_kwu)?;
        let mut g = buf.inner.lock().expect("poisoned");
        self.0
            .clone()
            .build_recommended(fee_rate, &mut g)
            .map(|p| Arc::new(ProvisionalWithReplyKey { inner: Mutex::new(Some(p)) }))
            .map_err(|e: payjoin::send::BuildSenderError| {
                SenderInputError::Build(Arc::new(e.into()))
            })
    }

    /// Offer the receiver contribution to pay for his input. Pushes a `Created`
    /// event into `buf` and returns a [`ProvisionalWithReplyKey`]; see
    /// [`Self::build_recommended`] for the persist-before-expose semantics.
    pub fn build_with_additional_fee(
        &self,
        max_fee_contribution_sats: u64,
        change_index: Option<u8>,
        min_fee_rate_sat_per_kwu: u64,
        clamp_fee_contribution: bool,
        buf: &SenderEventBuffer,
    ) -> Result<Arc<ProvisionalWithReplyKey>, SenderInputError> {
        let max_fee_contribution = validate_amount_sat(max_fee_contribution_sats)?;
        let fee_rate = validate_fee_rate_sat_per_kwu(min_fee_rate_sat_per_kwu)?;
        let mut g = buf.inner.lock().expect("poisoned");
        self.0
            .clone()
            .build_with_additional_fee(
                max_fee_contribution,
                change_index.map(|x| x as usize),
                fee_rate,
                clamp_fee_contribution,
                &mut g,
            )
            .map(|p| Arc::new(ProvisionalWithReplyKey { inner: Mutex::new(Some(p)) }))
            .map_err(|e: payjoin::send::BuildSenderError| {
                SenderInputError::Build(Arc::new(e.into()))
            })
    }

    /// Perform Payjoin without incentivizing the payee. Pushes a `Created`
    /// event into `buf` and returns a [`ProvisionalWithReplyKey`]; see
    /// [`Self::build_recommended`] for the persist-before-expose semantics.
    pub fn build_non_incentivizing(
        &self,
        min_fee_rate_sat_per_kwu: u64,
        buf: &SenderEventBuffer,
    ) -> Result<Arc<ProvisionalWithReplyKey>, SenderInputError> {
        let fee_rate = validate_fee_rate_sat_per_kwu(min_fee_rate_sat_per_kwu)?;
        let mut g = buf.inner.lock().expect("poisoned");
        self.0
            .clone()
            .build_non_incentivizing(fee_rate, &mut g)
            .map(|p| Arc::new(ProvisionalWithReplyKey { inner: Mutex::new(Some(p)) }))
            .map_err(|e: payjoin::send::BuildSenderError| {
                SenderInputError::Build(Arc::new(e.into()))
            })
    }
}

// -----------------------------------------------------------------------------
// Provisional<Sender<WithReplyKey>>
// -----------------------------------------------------------------------------

/// A sender staged for persistence by [`SenderBuilder::build_recommended`] (or
/// the other `build_*` methods).
///
/// The directory cannot be polled until the producing `Created` event has been
/// durably persisted in the same [`SenderEventBuffer`] this provisional was
/// minted against. Drain the buffer, then call [`Self::confirm`].
#[derive(uniffi::Object)]
pub struct ProvisionalWithReplyKey {
    inner: Mutex<
        Option<
            payjoin::persist::Provisional<
                payjoin::send::v2::Sender<payjoin::send::v2::WithReplyKey>,
            >,
        >,
    >,
}

#[uniffi::export]
impl ProvisionalWithReplyKey {
    /// Confirm against `buf`. See [`crate::receive::ProvisionalInitialized::confirm`]
    /// for the shared failure-mode semantics (NotYetPersisted, WrongBuffer,
    /// AlreadyConsumed).
    pub fn confirm(
        &self,
        buf: &SenderEventBuffer,
    ) -> Result<Arc<WithReplyKey>, ProvisionalConfirmError> {
        let mut slot = self.inner.lock().expect("poisoned");
        let p = slot.take().ok_or(ProvisionalConfirmError::AlreadyConsumed)?;
        let buf_g = buf.inner.lock().expect("poisoned");
        match p.confirm(&*buf_g) {
            Ok(sender) => Ok(Arc::new(sender.into())),
            Err(failure) => {
                let kind = match failure.kind {
                    payjoin::persist::ConfirmFailureKind::NotYetPersisted =>
                        ProvisionalConfirmError::NotYetPersisted,
                    payjoin::persist::ConfirmFailureKind::WrongBuffer =>
                        ProvisionalConfirmError::WrongBuffer,
                };
                *slot = Some(failure.provisional);
                Err(kind)
            }
        }
    }
}

// -----------------------------------------------------------------------------
// WithReplyKey
// -----------------------------------------------------------------------------

#[derive(Clone, uniffi::Object)]
pub struct WithReplyKey(payjoin::send::v2::Sender<payjoin::send::v2::WithReplyKey>);

impl From<payjoin::send::v2::Sender<payjoin::send::v2::WithReplyKey>> for WithReplyKey {
    fn from(value: payjoin::send::v2::Sender<payjoin::send::v2::WithReplyKey>) -> Self {
        Self(value)
    }
}

impl From<WithReplyKey> for payjoin::send::v2::Sender<payjoin::send::v2::WithReplyKey> {
    fn from(value: WithReplyKey) -> Self { value.0 }
}

impl_cancel_for_sender!(WithReplyKey);

#[uniffi::export]
impl WithReplyKey {
    /// Construct serialized Request and Context from a Payjoin Proposal.
    ///
    /// Important: This request must not be retried or reused on failure.
    /// Retransmitting the same ciphertext breaks OHTTP privacy properties.
    pub fn create_v2_post_request(
        &self,
        ohttp_relay: String,
    ) -> Result<RequestOhttpContext, CreateRequestError> {
        match self.0.create_v2_post_request(ohttp_relay) {
            Ok((req, ctx)) =>
                Ok(RequestOhttpContext { request: req.into(), ohttp_ctx: Arc::new(ctx.into()) }),
            Err(e) => Err(e.into()),
        }
    }

    /// Decodes and validates the response, then transitions to
    /// [`PollingForProposal`] on success.
    pub fn process_response(
        &self,
        response: &[u8],
        post_ctx: &ClientResponse,
        buf: &SenderEventBuffer,
    ) -> Result<Arc<PollingForProposal>, SenderApiError> {
        let mut g = buf.inner.lock().expect("poisoned");
        self.0
            .clone()
            .process_response(response, post_ctx.into(), &mut g)
            .map(|s| Arc::new(s.into()))
            .map_err(SenderApiError::from_api_error_encapsulation)
    }
}

// -----------------------------------------------------------------------------
// V1Context (BIP78)
// -----------------------------------------------------------------------------

#[derive(uniffi::Record)]
pub struct RequestV1Context {
    pub request: Request,
    pub context: Arc<V1Context>,
}

#[derive(Clone, uniffi::Object)]
pub struct V1Context(Arc<payjoin::send::v1::V1Context>);

impl From<payjoin::send::v1::V1Context> for V1Context {
    fn from(value: payjoin::send::v1::V1Context) -> Self { Self(Arc::new(value)) }
}

#[uniffi::export]
impl V1Context {
    pub fn process_response(&self, response: &[u8]) -> Result<String, ResponseError> {
        <payjoin::send::v1::V1Context as Clone>::clone(&self.0.clone())
            .process_response(response)
            .map(|e| e.to_string())
            .map_err(Into::into)
    }
}

#[derive(uniffi::Record)]
pub struct RequestOhttpContext {
    pub request: crate::Request,
    pub ohttp_ctx: Arc<crate::ClientResponse>,
}

// -----------------------------------------------------------------------------
// PollingForProposal
// -----------------------------------------------------------------------------

#[derive(uniffi::Object)]
pub struct PollingForProposal(payjoin::send::v2::Sender<payjoin::send::v2::PollingForProposal>);

impl From<payjoin::send::v2::Sender<payjoin::send::v2::PollingForProposal>> for PollingForProposal {
    fn from(value: payjoin::send::v2::Sender<payjoin::send::v2::PollingForProposal>) -> Self {
        Self(value)
    }
}

impl_cancel_for_sender!(PollingForProposal);

#[derive(uniffi::Enum)]
pub enum PollingForProposalTransitionOutcome {
    /// Got the receiver's signed PSBT. Sign and broadcast it.
    Progress { psbt_base64: String },
    /// No response yet; resume polling from the current state.
    Stasis { inner: Arc<PollingForProposal> },
}

impl
    From<
        payjoin::persist::OptionalTransitionOutcome<
            payjoin::bitcoin::Psbt,
            payjoin::send::v2::Sender<payjoin::send::v2::PollingForProposal>,
        >,
    > for PollingForProposalTransitionOutcome
{
    fn from(
        value: payjoin::persist::OptionalTransitionOutcome<
            payjoin::bitcoin::Psbt,
            payjoin::send::v2::Sender<payjoin::send::v2::PollingForProposal>,
        >,
    ) -> Self {
        match value {
            payjoin::persist::OptionalTransitionOutcome::Progress(psbt) =>
                Self::Progress { psbt_base64: psbt.to_string() },
            payjoin::persist::OptionalTransitionOutcome::Stasis(state) =>
                Self::Stasis { inner: Arc::new(state.into()) },
        }
    }
}

#[uniffi::export]
impl PollingForProposal {
    pub fn create_poll_request(
        &self,
        ohttp_relay: String,
    ) -> Result<RequestOhttpContext, CreateRequestError> {
        self.0
            .create_poll_request(ohttp_relay)
            .map(|(req, ctx)| RequestOhttpContext {
                request: req.into(),
                ohttp_ctx: Arc::new(ctx.into()),
            })
            .map_err(|e| e.into())
    }

    /// Decodes and validates the response. May progress to a final PSBT or
    /// remain in stasis if the response indicates no PSBT is available yet.
    pub fn process_response(
        &self,
        response: &[u8],
        ohttp_ctx: &ClientResponse,
        buf: &SenderEventBuffer,
    ) -> Result<PollingForProposalTransitionOutcome, SenderApiError> {
        let mut g = buf.inner.lock().expect("poisoned");
        self.0
            .clone()
            .process_response(response, ohttp_ctx.into(), &mut g)
            .map(Into::into)
            .map_err(SenderApiError::from_api_error_response)
    }
}
