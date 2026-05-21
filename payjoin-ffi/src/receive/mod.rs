use std::str::FromStr;
use std::sync::{Arc, Mutex};

pub use error::{
    AddressParseError, InputContributionError, InputPairError, JsonReply, OutputSubstitutionError,
    ProtocolError, PsbtInputError, ReceiverApiError, ReceiverBuilderError, ReceiverError,
    SelectionError, SessionError,
};
use payjoin::bitcoin::consensus::Decodable;
use payjoin::bitcoin::psbt::Psbt;
use payjoin::bitcoin::FeeRate;

use crate::error::ForeignError;
pub use crate::error::{
    FfiValidationError, ImplementationError, ProvisionalConfirmError, SerdeJsonError,
};
use crate::ohttp::OhttpKeys;
use crate::receive::error::ReceiverReplayError;
use crate::uri::error::FeeRateError;
use crate::validation::{
    validate_amount_sat, validate_expiration_secs, validate_fee_rate_sat_per_kwu_opt,
    validate_fee_rate_sat_per_vb_opt, validate_optional_script, validate_script_bytes,
    validate_script_vec, validate_weight_units, validate_witness_stack,
};
use crate::{ClientResponse, OutputSubstitution, Request};

pub mod error;

// =============================================================================
// EventBuffer for receiver session events
// =============================================================================

/// Caller-owned, sans-IO event buffer for receiver session events.
///
/// Action methods push events into this buffer; the caller drains it through
/// their storage (sync or async). The buffer carries a process-unique id so
/// [`ProvisionalInitialized`] can refuse to confirm against an unrelated
/// buffer.
#[derive(uniffi::Object)]
pub struct ReceiverEventBuffer {
    pub(crate) inner: Mutex<payjoin::persist::EventBuffer<payjoin::receive::v2::SessionEvent>>,
}

#[uniffi::export]
impl ReceiverEventBuffer {
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

    /// Borrow pending events as JSON strings. The buffer is not mutated; call
    /// `commit(n)` after writing the first `n` of these to storage.
    pub fn peek(&self) -> Result<Vec<String>, SerdeJsonError> {
        let g = self.inner.lock().expect("poisoned");
        g.peek().map(|e| serde_json::to_string(e).map_err(SerdeJsonError::from)).collect()
    }

    /// Drop the first `n` events. Call only after storage commits.
    pub fn commit(&self, n: u64) { self.inner.lock().expect("poisoned").commit(n as usize); }
}

// =============================================================================
// Provisional<Receiver<Initialized>>
// =============================================================================

/// A receiver staged for persistence by [`ReceiverBuilder::build`].
///
/// The Payjoin URI cannot be observed until the producing event has been
/// durably persisted in the same [`ReceiverEventBuffer`] this provisional was
/// minted against. Drain the buffer, then call [`Self::confirm`].
#[derive(uniffi::Object)]
pub struct ProvisionalInitialized {
    inner: Mutex<
        Option<
            payjoin::persist::Provisional<
                payjoin::receive::v2::Receiver<payjoin::receive::v2::Initialized>,
            >,
        >,
    >,
}

#[uniffi::export]
impl ProvisionalInitialized {
    /// Confirm against `buf`. Returns the [`Initialized`] receiver if the
    /// producing event is durable in `buf`, otherwise returns
    /// [`ProvisionalConfirmError::NotYetPersisted`] and leaves the provisional
    /// reusable for retry.
    pub fn confirm(
        &self,
        buf: &ReceiverEventBuffer,
    ) -> Result<Arc<Initialized>, ProvisionalConfirmError> {
        let mut slot = self.inner.lock().expect("poisoned");
        let p = slot.take().ok_or(ProvisionalConfirmError::AlreadyConsumed)?;
        let buf_g = buf.inner.lock().expect("poisoned");
        match p.confirm(&*buf_g) {
            Ok(receiver) => Ok(Arc::new(receiver.into())),
            Err(returned) => {
                *slot = Some(returned);
                Err(ProvisionalConfirmError::NotYetPersisted)
            }
        }
    }
}

// =============================================================================
// Session events
// =============================================================================

#[derive(Debug, Clone, uniffi::Object)]
pub struct ReceiverSessionEvent(payjoin::receive::v2::SessionEvent);

impl From<payjoin::receive::v2::SessionEvent> for ReceiverSessionEvent {
    fn from(event: payjoin::receive::v2::SessionEvent) -> Self { Self(event) }
}

impl From<ReceiverSessionEvent> for payjoin::receive::v2::SessionEvent {
    fn from(event: ReceiverSessionEvent) -> Self { event.0 }
}

#[uniffi::export]
impl ReceiverSessionEvent {
    pub fn to_json(&self) -> Result<String, SerdeJsonError> {
        serde_json::to_string(&self.0).map_err(Into::into)
    }

    #[uniffi::constructor]
    pub fn from_json(json: String) -> Result<Self, SerdeJsonError> {
        let event: payjoin::receive::v2::SessionEvent = serde_json::from_str(&json)?;
        Ok(ReceiverSessionEvent(event))
    }
}

// =============================================================================
// SessionOutcome / ReceiveSession enum
// =============================================================================

#[derive(Clone, uniffi::Object)]
pub struct ReceiverSessionOutcome {
    inner: payjoin::receive::v2::SessionOutcome,
}

impl From<payjoin::receive::v2::SessionOutcome> for ReceiverSessionOutcome {
    fn from(value: payjoin::receive::v2::SessionOutcome) -> Self { Self { inner: value } }
}

impl From<ReceiverSessionOutcome> for payjoin::receive::v2::SessionOutcome {
    fn from(value: ReceiverSessionOutcome) -> Self { value.inner }
}

#[derive(Clone, uniffi::Enum)]
pub enum ReceiveSession {
    Initialized { inner: Arc<Initialized> },
    UncheckedOriginalPayload { inner: Arc<UncheckedOriginalPayload> },
    MaybeInputsOwned { inner: Arc<MaybeInputsOwned> },
    MaybeInputsSeen { inner: Arc<MaybeInputsSeen> },
    OutputsUnknown { inner: Arc<OutputsUnknown> },
    WantsOutputs { inner: Arc<WantsOutputs> },
    WantsInputs { inner: Arc<WantsInputs> },
    WantsFeeRange { inner: Arc<WantsFeeRange> },
    ProvisionalProposal { inner: Arc<ProvisionalProposal> },
    PayjoinProposal { inner: Arc<PayjoinProposal> },
    HasReplyableError { inner: Arc<HasReplyableError> },
    Monitor { inner: Arc<Monitor> },
    Closed { inner: Arc<ReceiverSessionOutcome> },
}

impl From<payjoin::receive::v2::ReceiveSession> for ReceiveSession {
    fn from(value: payjoin::receive::v2::ReceiveSession) -> Self {
        use payjoin::receive::v2::ReceiveSession;
        match value {
            ReceiveSession::Initialized(inner) =>
                Self::Initialized { inner: Arc::new(inner.into()) },
            ReceiveSession::UncheckedOriginalPayload(inner) =>
                Self::UncheckedOriginalPayload { inner: Arc::new(inner.into()) },
            ReceiveSession::MaybeInputsOwned(inner) =>
                Self::MaybeInputsOwned { inner: Arc::new(inner.into()) },
            ReceiveSession::MaybeInputsSeen(inner) =>
                Self::MaybeInputsSeen { inner: Arc::new(inner.into()) },
            ReceiveSession::OutputsUnknown(inner) =>
                Self::OutputsUnknown { inner: Arc::new(inner.into()) },
            ReceiveSession::WantsOutputs(inner) =>
                Self::WantsOutputs { inner: Arc::new(inner.into()) },
            ReceiveSession::WantsInputs(inner) =>
                Self::WantsInputs { inner: Arc::new(inner.into()) },
            ReceiveSession::WantsFeeRange(inner) =>
                Self::WantsFeeRange { inner: Arc::new(inner.into()) },
            ReceiveSession::ProvisionalProposal(inner) =>
                Self::ProvisionalProposal { inner: Arc::new(inner.into()) },
            ReceiveSession::PayjoinProposal(inner) =>
                Self::PayjoinProposal { inner: Arc::new(inner.into()) },
            ReceiveSession::HasReplyableError(inner) =>
                Self::HasReplyableError { inner: Arc::new(inner.into()) },
            ReceiveSession::Monitor(inner) => Self::Monitor { inner: Arc::new(inner.into()) },
            ReceiveSession::Closed(session_outcome) =>
                Self::Closed { inner: Arc::new(session_outcome.into()) },
        }
    }
}

// =============================================================================
// Replay
// =============================================================================

#[derive(uniffi::Object)]
pub struct ReplayResult {
    state: ReceiveSession,
    session_history: ReceiverSessionHistory,
    event_count: u64,
}

#[uniffi::export]
impl ReplayResult {
    pub fn state(&self) -> ReceiveSession { self.state.clone() }

    pub fn session_history(&self) -> ReceiverSessionHistory { self.session_history.clone() }

    /// Number of events that were replayed. Pass this to
    /// [`ReceiverEventBuffer::after_replay`] to construct a buffer whose
    /// committed_count reflects the durable log.
    pub fn event_count(&self) -> u64 { self.event_count }
}

/// Replay the persisted event log into a starting [`ReceiveSession`] and
/// [`ReceiverSessionHistory`]. The caller loads its events from storage
/// however it likes (sync or async, native code) and passes them in as
/// JSON-encoded strings; the library is sans-IO.
#[uniffi::export]
pub fn replay_receiver_event_log(events: Vec<String>) -> Result<ReplayResult, ReceiverReplayError> {
    let mut parsed = Vec::with_capacity(events.len());
    for raw in events {
        let event: payjoin::receive::v2::SessionEvent =
            serde_json::from_str(&raw).map_err(ReceiverReplayError::storage_serde)?;
        parsed.push(event);
    }
    let event_count = parsed.len() as u64;
    let (state, session_history) = payjoin::receive::v2::replay_event_log(parsed)?;
    Ok(ReplayResult { state: state.into(), session_history: session_history.into(), event_count })
}

// =============================================================================
// SessionStatus + SessionHistory
// =============================================================================

#[derive(uniffi::Object)]
pub struct ReceiverSessionStatus(payjoin::receive::v2::SessionStatus);

impl From<payjoin::receive::v2::SessionStatus> for ReceiverSessionStatus {
    fn from(value: payjoin::receive::v2::SessionStatus) -> Self { Self(value) }
}

impl From<ReceiverSessionStatus> for payjoin::receive::v2::SessionStatus {
    fn from(value: ReceiverSessionStatus) -> Self { value.0 }
}

#[derive(Clone, uniffi::Object)]
pub struct ReceiverSessionHistory(pub payjoin::receive::v2::SessionHistory);

impl From<payjoin::receive::v2::SessionHistory> for ReceiverSessionHistory {
    fn from(value: payjoin::receive::v2::SessionHistory) -> Self { Self(value) }
}

impl From<ReceiverSessionHistory> for payjoin::receive::v2::SessionHistory {
    fn from(value: ReceiverSessionHistory) -> Self { value.0 }
}

#[uniffi::export]
impl ReceiverSessionHistory {
    /// Receiver session Payjoin URI
    pub fn pj_uri(&self) -> Arc<crate::PjUri> { Arc::new(self.0.pj_uri().into()) }

    /// Fallback transaction from the session if present
    pub fn fallback_tx(&self) -> Option<Vec<u8>> {
        self.0.fallback_tx().map(|tx| payjoin::bitcoin::consensus::encode::serialize(&tx))
    }

    /// Helper method to query the current status of the session.
    pub fn status(&self) -> ReceiverSessionStatus { self.0.status().into() }
}

// =============================================================================
// Receiver typestates
// =============================================================================

/// Helper macro to add a `cancel` method to each typestate. Each invocation
/// pushes a `Closed(Cancel)` event into `buf` and returns the fallback
/// transaction (or `None` for early states that haven't seen one yet).
macro_rules! impl_cancel_for_receiver {
    ($ty:ident) => {
        #[uniffi::export]
        impl $ty {
            /// Cancel the Payjoin session immediately.
            ///
            /// Pushes a `Closed(Cancel)` event into `buf` and returns the
            /// fallback transaction as consensus-encoded raw bytes if
            /// available, or `None` if the session was cancelled before the
            /// sender's original proposal arrived.
            ///
            /// This is a terminal action — the session cannot be used after
            /// cancellation. The caller is expected to drain `buf` and treat
            /// the `Closed` event as the session boundary.
            pub fn cancel(&self, buf: &ReceiverEventBuffer) -> Option<Vec<u8>> {
                let mut g = buf.inner.lock().expect("poisoned");
                self.0.clone().cancel(&mut *g).map(|tx| payjoin::bitcoin::consensus::serialize(&tx))
            }
        }
    };
}

// -----------------------------------------------------------------------------
// ReceiverBuilder
// -----------------------------------------------------------------------------

#[derive(Clone, Debug, uniffi::Object)]
pub struct ReceiverBuilder(payjoin::receive::v2::ReceiverBuilder);

#[uniffi::export]
impl ReceiverBuilder {
    /// Creates a new builder for an [`Initialized`] receiver.
    ///
    /// # Parameters
    /// - `address`: The Bitcoin address for the payjoin session.
    /// - `directory`: The URL of the store-and-forward payjoin directory.
    /// - `ohttp_keys`: The OHTTP keys used for encrypting and decrypting HTTP requests and responses.
    ///
    /// # References
    /// - [BIP 77: Payjoin Version 2: Serverless Payjoin](https://github.com/bitcoin/bips/blob/master/bip-0077.md)
    #[uniffi::constructor]
    pub fn new(
        address: String,
        directory: String,
        ohttp_keys: Arc<OhttpKeys>,
    ) -> Result<Self, ReceiverBuilderError> {
        let parsed_address = payjoin::bitcoin::Address::from_str(address.as_str())
            .map_err(ReceiverBuilderError::from)?
            .assume_checked();
        Ok(Self(
            payjoin::receive::v2::ReceiverBuilder::new(
                parsed_address,
                directory,
                Arc::unwrap_or_clone(ohttp_keys).into(),
            )
            .map_err(ReceiverBuilderError::from)?,
        ))
    }

    pub fn with_amount(&self, amount_sats: u64) -> Result<Self, FfiValidationError> {
        let amount = validate_amount_sat(amount_sats)?;
        Ok(Self(self.0.clone().with_amount(amount)))
    }

    pub fn with_expiration(&self, expiration_secs: u64) -> Result<Self, FfiValidationError> {
        let expiration = validate_expiration_secs(expiration_secs)?;
        Ok(Self(self.0.clone().with_expiration(expiration)))
    }

    /// Set the maximum effective fee rate the receiver is willing to pay for their own
    /// input/output contributions.
    pub fn with_max_fee_rate(
        &self,
        max_effective_fee_rate_sat_per_vb: u64,
    ) -> Result<Self, FeeRateError> {
        let fee_rate = FeeRate::from_sat_per_vb(max_effective_fee_rate_sat_per_vb)
            .ok_or_else(|| FeeRateError::overflow(max_effective_fee_rate_sat_per_vb))?;
        Ok(Self(self.0.clone().with_max_fee_rate(fee_rate)))
    }

    /// Stage the session for persistence. Pushes a `Created` event into `buf`
    /// and returns a [`ProvisionalInitialized`] guarding [`Initialized`]: the
    /// payjoin URI cannot be observed until the buffer's `Created` entry is
    /// durably persisted and the provisional is confirmed.
    pub fn build(&self, buf: &ReceiverEventBuffer) -> Arc<ProvisionalInitialized> {
        let mut g = buf.inner.lock().expect("poisoned");
        let p = self.0.clone().build(&mut g);
        Arc::new(ProvisionalInitialized { inner: Mutex::new(Some(p)) })
    }
}

impl From<payjoin::receive::v2::ReceiverBuilder> for ReceiverBuilder {
    fn from(value: payjoin::receive::v2::ReceiverBuilder) -> Self { Self(value) }
}

impl From<ReceiverBuilder> for payjoin::receive::v2::ReceiverBuilder {
    fn from(value: ReceiverBuilder) -> Self { value.0 }
}

// -----------------------------------------------------------------------------
// Initialized
// -----------------------------------------------------------------------------

#[derive(Clone, Debug, uniffi::Object)]
pub struct Initialized(payjoin::receive::v2::Receiver<payjoin::receive::v2::Initialized>);

impl From<Initialized> for payjoin::receive::v2::Receiver<payjoin::receive::v2::Initialized> {
    fn from(value: Initialized) -> Self { value.0 }
}

impl From<payjoin::receive::v2::Receiver<payjoin::receive::v2::Initialized>> for Initialized {
    fn from(value: payjoin::receive::v2::Receiver<payjoin::receive::v2::Initialized>) -> Self {
        Self(value)
    }
}

impl_cancel_for_receiver!(Initialized);

#[derive(uniffi::Enum)]
pub enum InitializedTransitionOutcome {
    /// Progressed to the next typestate.
    Progress { inner: Arc<UncheckedOriginalPayload> },
    /// No new payload yet; resume from the current state.
    Stasis { inner: Arc<Initialized> },
}

impl
    From<
        payjoin::persist::OptionalTransitionOutcome<
            payjoin::receive::v2::Receiver<payjoin::receive::v2::UncheckedOriginalPayload>,
            payjoin::receive::v2::Receiver<payjoin::receive::v2::Initialized>,
        >,
    > for InitializedTransitionOutcome
{
    fn from(
        value: payjoin::persist::OptionalTransitionOutcome<
            payjoin::receive::v2::Receiver<payjoin::receive::v2::UncheckedOriginalPayload>,
            payjoin::receive::v2::Receiver<payjoin::receive::v2::Initialized>,
        >,
    ) -> Self {
        match value {
            payjoin::persist::OptionalTransitionOutcome::Progress(payload) =>
                Self::Progress { inner: Arc::new(payload.into()) },
            payjoin::persist::OptionalTransitionOutcome::Stasis(state) =>
                Self::Stasis { inner: Arc::new(state.into()) },
        }
    }
}

#[derive(uniffi::Record)]
pub struct RequestResponse {
    pub request: Request,
    pub client_response: Arc<ClientResponse>,
}

#[uniffi::export]
impl Initialized {
    /// Construct an OHTTP encapsulated GET request, polling the mailbox for the Original PSBT.
    pub fn create_poll_request(
        &self,
        ohttp_relay: String,
    ) -> Result<RequestResponse, ReceiverError> {
        self.0
            .create_poll_request(ohttp_relay)
            .map(|(req, ctx)| RequestResponse {
                request: req.into(),
                client_response: Arc::new(ctx.into()),
            })
            .map_err(Into::into)
    }

    /// Process the response from the directory.
    ///
    /// May progress to [`UncheckedOriginalPayload`] or remain in stasis if no
    /// payload is available yet. Pushes a `RetrievedOriginalPayload` (or
    /// `Closed(Failure)` on fatal error) into `buf` as appropriate.
    pub fn process_response(
        &self,
        body: &[u8],
        ctx: &ClientResponse,
        buf: &ReceiverEventBuffer,
    ) -> Result<InitializedTransitionOutcome, ReceiverApiError> {
        let mut g = buf.inner.lock().expect("poisoned");
        self.0
            .clone()
            .process_response(body, ctx.into(), &mut g)
            .map(Into::into)
            .map_err(ReceiverApiError::from_api_error)
    }

    /// Build a V2 Payjoin URI from the receiver's context.
    pub fn pj_uri(&self) -> crate::PjUri {
        <Self as Into<payjoin::receive::v2::Receiver<payjoin::receive::v2::Initialized>>>::into(
            self.clone(),
        )
        .pj_uri()
        .into()
    }
}

// -----------------------------------------------------------------------------
// UncheckedOriginalPayload
// -----------------------------------------------------------------------------

#[derive(Clone, uniffi::Object)]
pub struct UncheckedOriginalPayload(
    payjoin::receive::v2::Receiver<payjoin::receive::v2::UncheckedOriginalPayload>,
);

impl From<payjoin::receive::v2::Receiver<payjoin::receive::v2::UncheckedOriginalPayload>>
    for UncheckedOriginalPayload
{
    fn from(
        value: payjoin::receive::v2::Receiver<payjoin::receive::v2::UncheckedOriginalPayload>,
    ) -> Self {
        Self(value)
    }
}

impl From<UncheckedOriginalPayload>
    for payjoin::receive::v2::Receiver<payjoin::receive::v2::UncheckedOriginalPayload>
{
    fn from(value: UncheckedOriginalPayload) -> Self { value.0 }
}

impl_cancel_for_receiver!(UncheckedOriginalPayload);

#[uniffi::export(with_foreign)]
pub trait CanBroadcast: Send + Sync {
    fn callback(&self, tx: Vec<u8>) -> Result<bool, ForeignError>;
}

#[uniffi::export]
impl UncheckedOriginalPayload {
    pub fn check_broadcast_suitability(
        &self,
        min_fee_rate_sat_per_kwu: Option<u64>,
        can_broadcast: Arc<dyn CanBroadcast>,
        buf: &ReceiverEventBuffer,
    ) -> Result<Arc<MaybeInputsOwned>, ReceiverApiError> {
        let min_fee_rate = validate_fee_rate_sat_per_kwu_opt(min_fee_rate_sat_per_kwu)?;
        let mut g = buf.inner.lock().expect("poisoned");
        self.0
            .clone()
            .check_broadcast_suitability(
                min_fee_rate,
                |transaction| {
                    can_broadcast
                        .callback(payjoin::bitcoin::consensus::encode::serialize(transaction))
                        .map_err(|e| ImplementationError::new(e).into())
                },
                &mut g,
            )
            .map(|r| Arc::new(r.into()))
            .map_err(ReceiverApiError::from_api_error_with_replyable_state)
    }

    pub fn extract_tx_to_check_broadcast_suitability(&self) -> Vec<u8> {
        payjoin::bitcoin::consensus::encode::serialize(
            &self.0.clone().extract_tx_to_check_broadcast_suitability(),
        )
    }

    pub fn apply_broadcast_suitability(
        &self,
        min_fee_rate_sat_per_kwu: Option<u64>,
        can_broadcast: bool,
        buf: &ReceiverEventBuffer,
    ) -> Result<Arc<MaybeInputsOwned>, ReceiverApiError> {
        let min_fee_rate = validate_fee_rate_sat_per_kwu_opt(min_fee_rate_sat_per_kwu)?;
        let mut g = buf.inner.lock().expect("poisoned");
        self.0
            .clone()
            .apply_broadcast_suitability(min_fee_rate, can_broadcast, &mut g)
            .map(|r| Arc::new(r.into()))
            .map_err(ReceiverApiError::from_api_error_with_replyable_state)
    }

    /// Call this method if the only way to initiate a Payjoin with this receiver
    /// requires manual intervention, as in most consumer wallets.
    pub fn assume_interactive_receiver(&self, buf: &ReceiverEventBuffer) -> Arc<MaybeInputsOwned> {
        let mut g = buf.inner.lock().expect("poisoned");
        Arc::new(self.0.clone().assume_interactive_receiver(&mut g).into())
    }
}

// -----------------------------------------------------------------------------
// InputOwnedReference / TaggedReference helpers
// -----------------------------------------------------------------------------

#[derive(Debug, uniffi::Object)]
pub struct InputOwnedReference(
    payjoin::receive::Reference<payjoin::bitcoin::ScriptBuf, payjoin::receive::InputOwnedTag>,
);

#[uniffi::export]
impl InputOwnedReference {
    pub fn get_value(&self) -> Vec<u8> { self.0.get_value().to_bytes() }

    pub fn mark(&self, result: bool) -> Arc<InputOwnedTaggedReference> {
        Arc::new(InputOwnedTaggedReference(self.0.mark(result)))
    }
}

#[derive(Debug, uniffi::Object)]
pub struct InputOwnedTaggedReference(
    payjoin::receive::TaggedReference<payjoin::bitcoin::ScriptBuf, payjoin::receive::InputOwnedTag>,
);

#[uniffi::export]
impl InputOwnedTaggedReference {
    pub fn get_value(&self) -> Vec<u8> { self.0.get_value().to_bytes() }

    pub fn get_result(&self) -> bool { self.0.get_result() }

    pub fn get_index(&self) -> u64 { self.0.get_index() as u64 }
}

// -----------------------------------------------------------------------------
// MaybeInputsOwned
// -----------------------------------------------------------------------------

#[derive(Clone, uniffi::Object)]
pub struct MaybeInputsOwned(payjoin::receive::v2::Receiver<payjoin::receive::v2::MaybeInputsOwned>);

impl From<payjoin::receive::v2::Receiver<payjoin::receive::v2::MaybeInputsOwned>>
    for MaybeInputsOwned
{
    fn from(value: payjoin::receive::v2::Receiver<payjoin::receive::v2::MaybeInputsOwned>) -> Self {
        Self(value)
    }
}

impl_cancel_for_receiver!(MaybeInputsOwned);

#[uniffi::export(with_foreign)]
pub trait IsScriptOwned: Send + Sync {
    fn callback(&self, script: Vec<u8>) -> Result<bool, ForeignError>;
}

#[uniffi::export]
impl MaybeInputsOwned {
    /// The Sender's Original PSBT
    pub fn extract_tx_to_schedule_broadcast(&self) -> Vec<u8> {
        payjoin::bitcoin::consensus::encode::serialize(
            &self.0.clone().extract_tx_to_schedule_broadcast(),
        )
    }

    pub fn check_inputs_not_owned(
        &self,
        is_owned: Arc<dyn IsScriptOwned>,
        buf: &ReceiverEventBuffer,
    ) -> Result<Arc<MaybeInputsSeen>, ReceiverApiError> {
        let mut g = buf.inner.lock().expect("poisoned");
        self.0
            .clone()
            .check_inputs_not_owned(
                &mut |input| {
                    is_owned
                        .callback(input.to_bytes())
                        .map_err(|e| ImplementationError::new(e).into())
                },
                &mut g,
            )
            .map(|r| Arc::new(r.into()))
            .map_err(ReceiverApiError::from_api_error_with_replyable_state)
    }

    pub fn get_input_script_refs(&self) -> Result<Vec<Arc<InputOwnedReference>>, ReceiverError> {
        self.0
            .clone()
            .get_input_script_refs()
            .map(|iter| {
                iter.map(|input_script_ref| Arc::new(InputOwnedReference(input_script_ref)))
                    .collect::<Vec<_>>()
            })
            .map_err(ReceiverError::from)
    }

    pub fn apply_input_owned_checks(
        &self,
        checked_input_scripts: Vec<Arc<InputOwnedTaggedReference>>,
        buf: &ReceiverEventBuffer,
    ) -> Result<Arc<MaybeInputsSeen>, ReceiverApiError> {
        let mut g = buf.inner.lock().expect("poisoned");
        self.0
            .clone()
            .apply_input_owned_checks(
                checked_input_scripts.into_iter().map(|r| {
                    Arc::try_unwrap(r)
                        .expect("InputOwnedTaggedReference Arc should have a single owner")
                        .0
                }),
                &mut g,
            )
            .map(|r| Arc::new(r.into()))
            .map_err(ReceiverApiError::from_api_error_with_replyable_state)
    }
}

// -----------------------------------------------------------------------------
// MaybeInputsSeen
// -----------------------------------------------------------------------------

#[derive(Debug, uniffi::Object)]
pub struct InputSeenReference(
    payjoin::receive::Reference<payjoin::bitcoin::OutPoint, payjoin::receive::InputSeenTag>,
);

#[uniffi::export]
impl InputSeenReference {
    pub fn get_value(&self) -> OutPoint { self.0.get_value().into() }

    pub fn mark(&self, result: bool) -> Arc<InputSeenTaggedReference> {
        Arc::new(InputSeenTaggedReference(self.0.mark(result)))
    }
}

#[derive(Debug, uniffi::Object)]
pub struct InputSeenTaggedReference(
    payjoin::receive::TaggedReference<payjoin::bitcoin::OutPoint, payjoin::receive::InputSeenTag>,
);

#[uniffi::export]
impl InputSeenTaggedReference {
    pub fn get_value(&self) -> OutPoint { self.0.get_value().into() }

    pub fn get_result(&self) -> bool { self.0.get_result() }

    pub fn get_index(&self) -> u64 { self.0.get_index() as u64 }
}

#[derive(Clone, uniffi::Object)]
pub struct MaybeInputsSeen(payjoin::receive::v2::Receiver<payjoin::receive::v2::MaybeInputsSeen>);

impl From<payjoin::receive::v2::Receiver<payjoin::receive::v2::MaybeInputsSeen>>
    for MaybeInputsSeen
{
    fn from(value: payjoin::receive::v2::Receiver<payjoin::receive::v2::MaybeInputsSeen>) -> Self {
        Self(value)
    }
}

impl_cancel_for_receiver!(MaybeInputsSeen);

#[uniffi::export(with_foreign)]
pub trait IsOutputKnown: Send + Sync {
    fn callback(&self, outpoint: OutPoint) -> Result<bool, ForeignError>;
}

#[uniffi::export]
impl MaybeInputsSeen {
    pub fn check_no_inputs_seen_before(
        &self,
        is_known: Arc<dyn IsOutputKnown>,
        buf: &ReceiverEventBuffer,
    ) -> Result<Arc<OutputsUnknown>, ReceiverApiError> {
        let mut g = buf.inner.lock().expect("poisoned");
        self.0
            .clone()
            .check_no_inputs_seen_before(
                &mut |outpoint| {
                    is_known
                        .callback(OutPoint::from(*outpoint))
                        .map_err(|e| ImplementationError::new(e).into())
                },
                &mut g,
            )
            .map(|r| Arc::new(r.into()))
            .map_err(ReceiverApiError::from_api_error_with_replyable_state)
    }

    pub fn get_input_outpoint_refs(&self) -> Vec<Arc<InputSeenReference>> {
        self.0
            .clone()
            .get_input_outpoint_refs()
            .map(|input_outpoint_ref| Arc::new(InputSeenReference(input_outpoint_ref)))
            .collect::<Vec<_>>()
    }

    pub fn apply_input_seen_checks(
        &self,
        checked_input_outpoints: Vec<Arc<InputSeenTaggedReference>>,
        buf: &ReceiverEventBuffer,
    ) -> Result<Arc<OutputsUnknown>, ReceiverApiError> {
        let mut g = buf.inner.lock().expect("poisoned");
        self.0
            .clone()
            .apply_input_seen_checks(
                checked_input_outpoints.into_iter().map(|r| {
                    Arc::try_unwrap(r)
                        .expect("InputSeenTaggedReference Arc should have a single owner")
                        .0
                }),
                &mut g,
            )
            .map(|r| Arc::new(r.into()))
            .map_err(ReceiverApiError::from_api_error_with_replyable_state)
    }
}

// -----------------------------------------------------------------------------
// OutputsUnknown
// -----------------------------------------------------------------------------

#[derive(Debug, uniffi::Object)]
pub struct OutputOwnedReference(
    payjoin::receive::Reference<payjoin::bitcoin::ScriptBuf, payjoin::receive::OutputOwnedTag>,
);

#[uniffi::export]
impl OutputOwnedReference {
    pub fn get_value(&self) -> Vec<u8> { self.0.get_value().to_bytes() }

    pub fn mark(&self, result: bool) -> Arc<OutputOwnedTaggedReference> {
        Arc::new(OutputOwnedTaggedReference(self.0.mark(result)))
    }
}

#[derive(Debug, uniffi::Object)]
pub struct OutputOwnedTaggedReference(
    payjoin::receive::TaggedReference<
        payjoin::bitcoin::ScriptBuf,
        payjoin::receive::OutputOwnedTag,
    >,
);

#[uniffi::export]
impl OutputOwnedTaggedReference {
    pub fn get_value(&self) -> Vec<u8> { self.0.get_value().to_bytes() }

    pub fn get_result(&self) -> bool { self.0.get_result() }

    pub fn get_index(&self) -> u64 { self.0.get_index() as u64 }
}

/// The receiver has not yet identified which outputs belong to the receiver.
///
/// Only accept PSBTs that send us money. Identify those outputs with
/// `identify_receiver_outputs()` to proceed.
#[derive(Clone, uniffi::Object)]
pub struct OutputsUnknown(payjoin::receive::v2::Receiver<payjoin::receive::v2::OutputsUnknown>);

impl From<payjoin::receive::v2::Receiver<payjoin::receive::v2::OutputsUnknown>> for OutputsUnknown {
    fn from(value: payjoin::receive::v2::Receiver<payjoin::receive::v2::OutputsUnknown>) -> Self {
        Self(value)
    }
}

impl_cancel_for_receiver!(OutputsUnknown);

#[uniffi::export]
impl OutputsUnknown {
    /// Find which outputs belong to the receiver.
    pub fn identify_receiver_outputs(
        &self,
        is_receiver_output: Arc<dyn IsScriptOwned>,
        buf: &ReceiverEventBuffer,
    ) -> Result<Arc<WantsOutputs>, ReceiverApiError> {
        let mut g = buf.inner.lock().expect("poisoned");
        self.0
            .clone()
            .identify_receiver_outputs(
                &mut |input| {
                    is_receiver_output
                        .callback(input.to_bytes())
                        .map_err(|e| ImplementationError::new(e).into())
                },
                &mut g,
            )
            .map(|r| Arc::new(r.into()))
            .map_err(ReceiverApiError::from_api_error_with_replyable_state)
    }

    pub fn get_output_script_refs(&self) -> Vec<Arc<OutputOwnedReference>> {
        self.0
            .clone()
            .get_output_script_refs()
            .map(|output_script_ref| Arc::new(OutputOwnedReference(output_script_ref)))
            .collect::<Vec<_>>()
    }

    pub fn apply_output_owned_checks(
        &self,
        checked_output_scripts: Vec<Arc<OutputOwnedTaggedReference>>,
        buf: &ReceiverEventBuffer,
    ) -> Result<Arc<WantsOutputs>, ReceiverApiError> {
        let mut g = buf.inner.lock().expect("poisoned");
        self.0
            .clone()
            .apply_output_owned_checks(
                checked_output_scripts.into_iter().map(|r| {
                    Arc::try_unwrap(r)
                        .expect("OutputOwnedTaggedReference Arc should have a single owner")
                        .0
                }),
                &mut g,
            )
            .map(|r| Arc::new(r.into()))
            .map_err(ReceiverApiError::from_api_error_with_replyable_state)
    }
}

// -----------------------------------------------------------------------------
// WantsOutputs
// -----------------------------------------------------------------------------

#[derive(uniffi::Object)]
pub struct WantsOutputs(payjoin::receive::v2::Receiver<payjoin::receive::v2::WantsOutputs>);

impl From<payjoin::receive::v2::Receiver<payjoin::receive::v2::WantsOutputs>> for WantsOutputs {
    fn from(value: payjoin::receive::v2::Receiver<payjoin::receive::v2::WantsOutputs>) -> Self {
        Self(value)
    }
}

impl_cancel_for_receiver!(WantsOutputs);

#[uniffi::export]
impl WantsOutputs {
    pub fn output_substitution(&self) -> OutputSubstitution { self.0.output_substitution() }

    pub fn replace_receiver_outputs(
        &self,
        replacement_outputs: Vec<TxOut>,
        drain_script_pubkey: Vec<u8>,
    ) -> Result<WantsOutputs, OutputSubstitutionError> {
        let replacement_outputs = replacement_outputs
            .into_iter()
            .map(|output| output.into_core())
            .collect::<Result<Vec<_>, _>>()?;
        let drain_script = validate_script_vec("drain_script_pubkey", drain_script_pubkey, false)?;
        self.0
            .clone()
            .replace_receiver_outputs(replacement_outputs, &drain_script)
            .map(Into::into)
            .map_err(Into::into)
    }

    pub fn substitute_receiver_script(
        &self,
        output_script_pubkey: Vec<u8>,
    ) -> Result<WantsOutputs, OutputSubstitutionError> {
        let output_script =
            validate_script_vec("output_script_pubkey", output_script_pubkey, false)?;
        self.0
            .clone()
            .substitute_receiver_script(&output_script)
            .map(Into::into)
            .map_err(Into::into)
    }

    pub fn commit_outputs(&self, buf: &ReceiverEventBuffer) -> Arc<WantsInputs> {
        let mut g = buf.inner.lock().expect("poisoned");
        Arc::new(self.0.clone().commit_outputs(&mut g).into())
    }
}

// -----------------------------------------------------------------------------
// WantsInputs
// -----------------------------------------------------------------------------

#[derive(uniffi::Object)]
pub struct WantsInputs(payjoin::receive::v2::Receiver<payjoin::receive::v2::WantsInputs>);

impl From<payjoin::receive::v2::Receiver<payjoin::receive::v2::WantsInputs>> for WantsInputs {
    fn from(value: payjoin::receive::v2::Receiver<payjoin::receive::v2::WantsInputs>) -> Self {
        Self(value)
    }
}

impl_cancel_for_receiver!(WantsInputs);

#[uniffi::export]
impl WantsInputs {
    /// Select receiver input such that the payjoin avoids surveillance.
    pub fn try_preserving_privacy(
        &self,
        candidate_inputs: Vec<Arc<InputPair>>,
    ) -> Result<Arc<InputPair>, SelectionError> {
        let candidate_inputs: Vec<payjoin::receive::InputPair> =
            candidate_inputs.into_iter().map(|pair| Arc::unwrap_or_clone(pair).into()).collect();
        match self.0.clone().try_preserving_privacy(candidate_inputs) {
            Ok(t) => Ok(Arc::new(t.into())),
            Err(e) => Err(e.into()),
        }
    }

    pub fn contribute_inputs(
        &self,
        replacement_inputs: Vec<Arc<InputPair>>,
    ) -> Result<Arc<WantsInputs>, InputContributionError> {
        let replacement_inputs: Vec<payjoin::receive::InputPair> =
            replacement_inputs.into_iter().map(|pair| Arc::unwrap_or_clone(pair).into()).collect();
        self.0
            .clone()
            .contribute_inputs(replacement_inputs)
            .map(|t| Arc::new(t.into()))
            .map_err(Into::into)
    }

    pub fn commit_inputs(&self, buf: &ReceiverEventBuffer) -> Arc<WantsFeeRange> {
        let mut g = buf.inner.lock().expect("poisoned");
        Arc::new(self.0.clone().commit_inputs(&mut g).into())
    }
}

// -----------------------------------------------------------------------------
// InputPair (helper)
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, uniffi::Object)]
pub struct InputPair(payjoin::receive::InputPair);

#[uniffi::export]
impl InputPair {
    #[uniffi::constructor]
    pub fn new(
        txin: TxIn,
        psbtin: PsbtInput,
        expected_weight: Option<Weight>,
    ) -> Result<Self, InputPairError> {
        let txin = txin.into_core()?;
        let psbtin = psbtin.into_core()?;
        let expected_weight = expected_weight.map(|weight| weight.into_core()).transpose()?;
        payjoin::receive::InputPair::new(txin, psbtin, expected_weight)
            .map(Self)
            .map_err(|err| InputPairError::InvalidPsbtInput(Arc::new(err.into())))
    }
}

impl From<InputPair> for payjoin::receive::InputPair {
    fn from(value: InputPair) -> Self { value.0 }
}

impl From<payjoin::receive::InputPair> for InputPair {
    fn from(value: payjoin::receive::InputPair) -> Self { Self(value) }
}

// -----------------------------------------------------------------------------
// WantsFeeRange
// -----------------------------------------------------------------------------

#[derive(uniffi::Object)]
pub struct WantsFeeRange(payjoin::receive::v2::Receiver<payjoin::receive::v2::WantsFeeRange>);

impl From<payjoin::receive::v2::Receiver<payjoin::receive::v2::WantsFeeRange>> for WantsFeeRange {
    fn from(value: payjoin::receive::v2::Receiver<payjoin::receive::v2::WantsFeeRange>) -> Self {
        Self(value)
    }
}

impl_cancel_for_receiver!(WantsFeeRange);

#[uniffi::export]
impl WantsFeeRange {
    /// Applies additional fee contribution now that the receiver has contributed inputs
    /// and may have added new outputs.
    pub fn apply_fee_range(
        &self,
        min_fee_rate_sat_per_vb: Option<u64>,
        max_effective_fee_rate_sat_per_vb: Option<u64>,
        buf: &ReceiverEventBuffer,
    ) -> Result<Arc<ProvisionalProposal>, ReceiverApiError> {
        let min_fee_rate_sat_per_vb = validate_fee_rate_sat_per_vb_opt(min_fee_rate_sat_per_vb)?;
        let max_effective_fee_rate_sat_per_vb =
            validate_fee_rate_sat_per_vb_opt(max_effective_fee_rate_sat_per_vb)?;
        let mut g = buf.inner.lock().expect("poisoned");
        self.0
            .clone()
            .apply_fee_range(min_fee_rate_sat_per_vb, max_effective_fee_rate_sat_per_vb, &mut g)
            .map(|r| Arc::new(r.into()))
            .map_err(ReceiverApiError::from_api_error)
    }
}

// -----------------------------------------------------------------------------
// ProvisionalProposal
// -----------------------------------------------------------------------------

#[derive(uniffi::Object)]
pub struct ProvisionalProposal(
    pub payjoin::receive::v2::Receiver<payjoin::receive::v2::ProvisionalProposal>,
);

impl From<payjoin::receive::v2::Receiver<payjoin::receive::v2::ProvisionalProposal>>
    for ProvisionalProposal
{
    fn from(
        value: payjoin::receive::v2::Receiver<payjoin::receive::v2::ProvisionalProposal>,
    ) -> Self {
        Self(value)
    }
}

impl_cancel_for_receiver!(ProvisionalProposal);

#[uniffi::export(with_foreign)]
pub trait ProcessPsbt: Send + Sync {
    fn callback(&self, psbt: String) -> Result<String, ForeignError>;
}

#[uniffi::export]
impl ProvisionalProposal {
    /// Finalize the proposal. Returns a [`ProvisionalPayjoinProposal`] guarding
    /// the [`PayjoinProposal`] — the proposal cannot be posted to the directory
    /// until the `FinalizedProposal` event is durable and the provisional is
    /// confirmed.
    pub fn finalize_proposal(
        &self,
        process_psbt: Arc<dyn ProcessPsbt>,
        buf: &ReceiverEventBuffer,
    ) -> Result<Arc<ProvisionalPayjoinProposal>, ReceiverApiError> {
        let mut g = buf.inner.lock().expect("poisoned");
        self.0
            .clone()
            .finalize_proposal(
                |pre_processed| {
                    let psbt = process_psbt
                        .callback(pre_processed.to_string())
                        .map_err(ImplementationError::new)?;
                    Ok(Psbt::from_str(&psbt).map_err(ImplementationError::new)?)
                },
                &mut g,
            )
            .map(|p| Arc::new(ProvisionalPayjoinProposal { inner: Mutex::new(Some(p)) }))
            .map_err(ReceiverApiError::from_api_error)
    }

    pub fn psbt_to_sign(&self) -> String { self.0.clone().psbt_to_sign().to_string() }

    pub fn finalize_signed_proposal(
        &self,
        signed_psbt: String,
        buf: &ReceiverEventBuffer,
    ) -> Result<Arc<ProvisionalPayjoinProposal>, ReceiverApiError> {
        let mut g = buf.inner.lock().expect("poisoned");
        self.0
            .clone()
            .finalize_proposal(
                |_| Ok(Psbt::from_str(&signed_psbt).map_err(ImplementationError::new)?),
                &mut g,
            )
            .map(|p| Arc::new(ProvisionalPayjoinProposal { inner: Mutex::new(Some(p)) }))
            .map_err(ReceiverApiError::from_api_error)
    }
}

// -----------------------------------------------------------------------------
// Provisional<Receiver<PayjoinProposal>>
// -----------------------------------------------------------------------------

/// A payjoin proposal staged for persistence by
/// [`ProvisionalProposal::finalize_proposal`] (or `finalize_signed_proposal`).
///
/// The proposal cannot be posted to the directory until the producing
/// `FinalizedProposal` event has been durably persisted in the same
/// [`ReceiverEventBuffer`] this provisional was minted against. Drain the
/// buffer, then call [`Self::confirm`].
#[derive(uniffi::Object)]
pub struct ProvisionalPayjoinProposal {
    inner: Mutex<
        Option<
            payjoin::persist::Provisional<
                payjoin::receive::v2::Receiver<payjoin::receive::v2::PayjoinProposal>,
            >,
        >,
    >,
}

#[uniffi::export]
impl ProvisionalPayjoinProposal {
    /// Confirm against `buf`. Returns the [`PayjoinProposal`] if the producing
    /// event is durable in `buf`, otherwise returns
    /// [`ProvisionalConfirmError::NotYetPersisted`] and leaves the provisional
    /// reusable for retry.
    pub fn confirm(
        &self,
        buf: &ReceiverEventBuffer,
    ) -> Result<Arc<PayjoinProposal>, ProvisionalConfirmError> {
        let mut slot = self.inner.lock().expect("poisoned");
        let p = slot.take().ok_or(ProvisionalConfirmError::AlreadyConsumed)?;
        let buf_g = buf.inner.lock().expect("poisoned");
        match p.confirm(&*buf_g) {
            Ok(proposal) => Ok(Arc::new(proposal.into())),
            Err(returned) => {
                *slot = Some(returned);
                Err(ProvisionalConfirmError::NotYetPersisted)
            }
        }
    }
}

// -----------------------------------------------------------------------------
// PayjoinProposal
// -----------------------------------------------------------------------------

#[derive(Clone, uniffi::Object)]
pub struct PayjoinProposal(
    pub payjoin::receive::v2::Receiver<payjoin::receive::v2::PayjoinProposal>,
);

impl From<PayjoinProposal>
    for payjoin::receive::v2::Receiver<payjoin::receive::v2::PayjoinProposal>
{
    fn from(value: PayjoinProposal) -> Self { value.0 }
}

impl From<payjoin::receive::v2::Receiver<payjoin::receive::v2::PayjoinProposal>>
    for PayjoinProposal
{
    fn from(value: payjoin::receive::v2::Receiver<payjoin::receive::v2::PayjoinProposal>) -> Self {
        Self(value)
    }
}

impl_cancel_for_receiver!(PayjoinProposal);

#[uniffi::export]
impl PayjoinProposal {
    pub fn utxos_to_be_locked(&self) -> Vec<OutPoint> {
        let mut outpoints: Vec<OutPoint> = Vec::new();
        for o in <PayjoinProposal as Into<
            payjoin::receive::v2::Receiver<payjoin::receive::v2::PayjoinProposal>,
        >>::into(self.clone())
        .utxos_to_be_locked()
        {
            outpoints.push(OutPoint::from(*o));
        }
        outpoints
    }

    pub fn psbt(&self) -> String {
        <PayjoinProposal as Into<
            payjoin::receive::v2::Receiver<payjoin::receive::v2::PayjoinProposal>,
        >>::into(self.clone())
        .psbt()
        .clone()
        .to_string()
    }

    /// Construct an OHTTP Encapsulated HTTP POST request for the Proposal PSBT.
    pub fn create_post_request(
        &self,
        ohttp_relay: String,
    ) -> Result<RequestResponse, ReceiverError> {
        self.0.clone().create_post_request(ohttp_relay).map_err(Into::into).map(|(req, ctx)| {
            RequestResponse { request: req.into(), client_response: Arc::new(ctx.into()) }
        })
    }

    /// Processes the response for the final POST message from the receiver client in the v2 Payjoin
    /// protocol.
    pub fn process_response(
        &self,
        body: &[u8],
        ohttp_context: &ClientResponse,
        buf: &ReceiverEventBuffer,
    ) -> Result<Arc<Monitor>, ReceiverApiError> {
        let mut g = buf.inner.lock().expect("poisoned");
        self.0
            .clone()
            .process_response(body, ohttp_context.into(), &mut g)
            .map(|r| Arc::new(r.into()))
            .map_err(ReceiverApiError::from_api_error)
    }
}

// -----------------------------------------------------------------------------
// HasReplyableError
// -----------------------------------------------------------------------------

#[derive(Clone, Debug, uniffi::Object)]
pub struct HasReplyableError(
    pub payjoin::receive::v2::Receiver<payjoin::receive::v2::HasReplyableError>,
);

impl From<HasReplyableError>
    for payjoin::receive::v2::Receiver<payjoin::receive::v2::HasReplyableError>
{
    fn from(value: HasReplyableError) -> Self { value.0 }
}

impl From<payjoin::receive::v2::Receiver<payjoin::receive::v2::HasReplyableError>>
    for HasReplyableError
{
    fn from(
        value: payjoin::receive::v2::Receiver<payjoin::receive::v2::HasReplyableError>,
    ) -> Self {
        Self(value)
    }
}

impl_cancel_for_receiver!(HasReplyableError);

#[uniffi::export]
impl HasReplyableError {
    pub fn create_error_request(
        &self,
        ohttp_relay: String,
    ) -> Result<RequestResponse, SessionError> {
        self.0.clone().create_error_request(ohttp_relay).map_err(Into::into).map(|(req, ctx)| {
            RequestResponse { request: req.into(), client_response: Arc::new(ctx.into()) }
        })
    }

    pub fn process_error_response(
        &self,
        body: &[u8],
        ohttp_context: &ClientResponse,
        buf: &ReceiverEventBuffer,
    ) -> Result<(), ReceiverApiError> {
        let mut g = buf.inner.lock().expect("poisoned");
        self.0
            .clone()
            .process_error_response(body, ohttp_context.into(), &mut g)
            .map_err(ReceiverApiError::from_api_error)
    }
}

// -----------------------------------------------------------------------------
// Monitor
// -----------------------------------------------------------------------------

#[uniffi::export(with_foreign)]
pub trait TransactionExists: Send + Sync {
    fn callback(&self, txid: String) -> Result<Option<Vec<u8>>, ForeignError>;
}

#[derive(uniffi::Enum)]
pub enum MonitorTransitionOutcome {
    /// Progressed: session has concluded (success / fallback / proposal sent).
    Progress,
    /// Stasis: no transaction observed yet; resume from the current state.
    Stasis { inner: Arc<Monitor> },
}

impl
    From<
        payjoin::persist::OptionalTransitionOutcome<
            (),
            payjoin::receive::v2::Receiver<payjoin::receive::v2::Monitor>,
        >,
    > for MonitorTransitionOutcome
{
    fn from(
        value: payjoin::persist::OptionalTransitionOutcome<
            (),
            payjoin::receive::v2::Receiver<payjoin::receive::v2::Monitor>,
        >,
    ) -> Self {
        match value {
            payjoin::persist::OptionalTransitionOutcome::Progress(()) => Self::Progress,
            payjoin::persist::OptionalTransitionOutcome::Stasis(state) =>
                Self::Stasis { inner: Arc::new(state.into()) },
        }
    }
}

#[derive(uniffi::Object)]
pub struct Monitor(pub payjoin::receive::v2::Receiver<payjoin::receive::v2::Monitor>);

impl From<payjoin::receive::v2::Receiver<payjoin::receive::v2::Monitor>> for Monitor {
    fn from(value: payjoin::receive::v2::Receiver<payjoin::receive::v2::Monitor>) -> Self {
        Self(value)
    }
}

impl_cancel_for_receiver!(Monitor);

fn try_deserialize_tx(
    buf: Vec<u8>,
) -> Result<payjoin::bitcoin::transaction::Transaction, ForeignError> {
    payjoin::bitcoin::transaction::Transaction::consensus_decode(&mut buf.as_slice())
        .map_err(|e| ForeignError::InternalError(e.to_string()))
}

#[uniffi::export]
impl Monitor {
    pub fn check_payment(
        &self,
        transaction_exists: Arc<dyn TransactionExists>,
        buf: &ReceiverEventBuffer,
    ) -> Result<MonitorTransitionOutcome, ReceiverApiError> {
        let mut g = buf.inner.lock().expect("poisoned");
        self.0
            .check_payment(
                |txid| {
                    transaction_exists
                        .callback(txid.to_string())
                        .and_then(|buf| buf.map(try_deserialize_tx).transpose())
                        .map_err(|e| ImplementationError::new(e).into())
                },
                &mut g,
            )
            .map(Into::into)
            .map_err(ReceiverApiError::from_api_error)
    }

    pub fn extract_fallback_txid(&self) -> String {
        self.0.clone().extract_fallback_txid().to_string()
    }

    pub fn extract_payjoin_proposal_txid(&self) -> String {
        self.0.clone().extract_payjoin_proposal_txid().to_string()
    }

    pub fn check_fallback_monitorable(
        &self,
        buf: &ReceiverEventBuffer,
    ) -> Result<MonitorTransitionOutcome, ReceiverApiError> {
        let mut g = buf.inner.lock().expect("poisoned");
        self.0
            .check_fallback_monitorable(&mut g)
            .map(Into::into)
            .map_err(ReceiverApiError::from_api_error)
    }

    pub fn fallback_tx_exists(
        &self,
        buf: &ReceiverEventBuffer,
    ) -> Result<MonitorTransitionOutcome, ReceiverApiError> {
        let mut g = buf.inner.lock().expect("poisoned");
        self.0.fallback_tx_exists(&mut g).map(Into::into).map_err(ReceiverApiError::from_api_error)
    }

    pub fn payjoin_tx_exists(
        &self,
        payjoin_tx: Vec<u8>,
        buf: &ReceiverEventBuffer,
    ) -> Result<MonitorTransitionOutcome, ReceiverApiError> {
        let tx = try_deserialize_tx(payjoin_tx)?;
        let mut g = buf.inner.lock().expect("poisoned");
        self.0
            .payjoin_tx_exists(tx, &mut g)
            .map(Into::into)
            .map_err(ReceiverApiError::from_api_error)
    }
}

// =============================================================================
// Primitive value types (TxOut, TxIn, OutPoint, PsbtInput, Weight)
// =============================================================================

/// Primitive representation of a transaction output for the FFI boundary.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, uniffi::Record)]
pub struct TxOut {
    /// Amount in satoshis.
    pub value_sat: u64,
    /// Raw scriptPubKey bytes.
    pub script_pubkey: Vec<u8>,
}

impl TxOut {
    fn into_core(self) -> Result<payjoin::bitcoin::TxOut, FfiValidationError> {
        let value = validate_amount_sat(self.value_sat)?;
        let script_pubkey = validate_script_vec("script_pubkey", self.script_pubkey, false)?;
        Ok(payjoin::bitcoin::TxOut { value, script_pubkey })
    }
}

impl From<payjoin::bitcoin::TxOut> for TxOut {
    fn from(value: payjoin::bitcoin::TxOut) -> Self {
        TxOut { value_sat: value.value.to_sat(), script_pubkey: value.script_pubkey.into_bytes() }
    }
}

/// Primitive representation of a transaction input for the FFI boundary.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, uniffi::Record)]
pub struct TxIn {
    pub previous_output: OutPoint,
    pub script_sig: Vec<u8>,
    pub sequence: u32,
    pub witness: Vec<Vec<u8>>,
}

impl TxIn {
    fn into_core(self) -> Result<payjoin::bitcoin::TxIn, InputPairError> {
        validate_script_bytes("script_sig", &self.script_sig, true)?;
        validate_witness_stack(&self.witness)?;
        let previous_output = self.previous_output.into_core()?;
        Ok(payjoin::bitcoin::TxIn {
            previous_output,
            script_sig: payjoin::bitcoin::ScriptBuf::from_bytes(self.script_sig),
            sequence: payjoin::bitcoin::Sequence(self.sequence),
            witness: self.witness.into(),
        })
    }
}

/// Primitive representation of an outpoint for the FFI boundary.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, uniffi::Record)]
pub struct OutPoint {
    /// Hex-encoded txid (big-endian).
    pub txid: String,
    /// Output index.
    pub vout: u32,
}

impl From<payjoin::bitcoin::OutPoint> for OutPoint {
    fn from(value: payjoin::bitcoin::OutPoint) -> Self {
        OutPoint { txid: value.txid.to_string(), vout: value.vout }
    }
}

impl OutPoint {
    fn into_core(self) -> Result<payjoin::bitcoin::OutPoint, InputPairError> {
        let txid = payjoin::bitcoin::Txid::from_str(&self.txid)
            .map_err(|_| InputPairError::invalid_outpoint(self.txid, self.vout))?;
        Ok(payjoin::bitcoin::OutPoint { txid, vout: self.vout })
    }
}

/// Primitive representation of a PSBT input for the FFI boundary.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, uniffi::Record)]
pub struct PsbtInput {
    pub witness_utxo: Option<TxOut>,
    pub redeem_script: Option<Vec<u8>>,
    pub witness_script: Option<Vec<u8>>,
}

impl PsbtInput {
    fn into_core(self) -> Result<payjoin::bitcoin::psbt::Input, InputPairError> {
        let witness_utxo = self
            .witness_utxo
            .map(|utxo| utxo.into_core())
            .transpose()
            .map_err(InputPairError::from)?;
        let redeem_script = validate_optional_script("redeem_script", self.redeem_script)?;
        let witness_script = validate_optional_script("witness_script", self.witness_script)?;
        Ok(payjoin::bitcoin::psbt::Input {
            witness_utxo,
            redeem_script,
            witness_script,
            ..Default::default()
        })
    }
}

/// Primitive representation of a weight measurement.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, uniffi::Record)]
pub struct Weight {
    pub weight_units: u64,
}

impl Weight {
    fn into_core(self) -> Result<payjoin::bitcoin::Weight, FfiValidationError> {
        validate_weight_units(self.weight_units)
    }
}

impl From<payjoin::bitcoin::Weight> for Weight {
    fn from(value: payjoin::bitcoin::Weight) -> Self { Weight { weight_units: value.to_wu() } }
}
