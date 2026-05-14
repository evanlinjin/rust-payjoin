//! Sender-side sans-IO state machine.
//!
//! The sender's only wallet decision — signing the proposal PSBT once the
//! receiver returns one — is surfaced as [`SenderStep::SignAndFinalize`].
//! The runtime never blocks on a wallet call and never goes through a
//! [`SessionPersister`](payjoin::persist::SessionPersister) callback — every
//! transition is consumed via
//! [`deconstruct`](payjoin::persist::MaybeFatalTransition::deconstruct).

use bitcoin::{Amount, FeeRate, Psbt, Transaction};
use payjoin::persist::{OptionalTransitionOutcome, PersistAction};
use payjoin::send::v2::{
    replay_event_log, PollingForProposal, SendSession, Sender, SenderBuilder, SessionEvent,
    SessionOutcome, WithReplyKey,
};
use payjoin::{PjUri, Request};

use crate::persister::Replay;
use crate::Error;

/// What the caller should do next to advance a [`SenderSession`].
///
/// Returned by [`SenderSession::poll`]. Each variant has a matching `feed_*`
/// method that delivers the answer back to the runtime.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum SenderStep {
    /// Persist these session events atomically before continuing.
    Save(Vec<SessionEvent>),
    /// Send this HTTP request, then feed the response body back via
    /// [`SenderSession::feed_response`].
    SendRequest(Request),
    /// The directory had no payload yet. Sleep, then call `poll` again.
    Backoff,
    /// Sign and finalize the proposal PSBT, then feed it back via
    /// [`SenderSession::feed_signed_psbt`]. By the time this step is emitted
    /// the proposal has already passed payjoin's BIP-78 anti-scam checks; the
    /// caller only needs to add signatures and finalize.
    SignAndFinalize(Psbt),
    /// The session reached its terminal success state. The broadcastable
    /// transaction is now available via [`SenderSession::final_tx`].
    Done,
    /// The session failed terminally.
    Failed(Error),
}

/// Sans-IO state machine for the payjoin v2 sender role.
///
/// **Persistence.** Every internal payjoin state advance produces one
/// `SessionEvent`. The runtime buffers them and surfaces them via
/// [`SenderStep::Save`]; the caller decides where/when/how to persist. To
/// resume after a crash, replay the saved log via
/// [`Self::resume_from_events`].
pub struct SenderSession {
    ohttp_relay: String,
    state: Option<State>,
    result: Option<(Transaction, Amount)>,
    events: Vec<SessionEvent>,
}

#[allow(clippy::large_enum_variant)]
enum State {
    /// Need to POST the original PSBT to the directory.
    PostingOriginal(Sender<WithReplyKey>),
    /// POST sent; awaiting acknowledgement.
    AwaitingPostAck {
        session: Sender<WithReplyKey>,
        ctx: ohttp::ClientResponse,
    },
    /// Need to GET-poll the directory for the receiver's proposal.
    PollingProposal {
        session: Sender<PollingForProposal>,
        pending_backoff: bool,
    },
    /// GET sent; awaiting the directory's response.
    AwaitingProposalPoll {
        session: Sender<PollingForProposal>,
        ctx: ohttp::ClientResponse,
    },
    /// Proposal received and validated; awaiting the signed PSBT.
    NeedSignedPsbt { proposal: Psbt },
    /// Terminal success.
    Done,
    /// Terminal failure.
    Failed(Option<Error>),
}

impl SenderSession {
    /// Build a new sender session.
    ///
    /// `psbt` is the sender's original (signed and finalized) PSBT paying `uri`.
    /// `min_fee_rate` is the lowest feerate the sender will accept in the
    /// counterparty's proposal.
    pub fn new(
        psbt: Psbt,
        uri: PjUri,
        ohttp_relay: impl Into<String>,
        min_fee_rate: FeeRate,
    ) -> Result<Self, Error> {
        let mut events = Vec::new();
        let (action, session) = SenderBuilder::new(psbt, uri)
            .build_recommended(min_fee_rate)
            .map_err(Error::payjoin)?
            .deconstruct();
        record(&mut events, action);
        Ok(Self {
            ohttp_relay: ohttp_relay.into(),
            state: Some(State::PostingOriginal(session)),
            result: None,
            events,
        })
    }

    /// Resume a session from a previously-persisted event log.
    ///
    /// `events` should be the complete sequence of session events as they were
    /// recorded from [`SenderStep::Save`], in order. The runtime replays them
    /// through payjoin's state machine to reconstruct the sender's current
    /// state, then continues from there.
    ///
    /// If the saved log ends in a successful [`SessionOutcome::Success`], the
    /// returned session is parked in [`State::NeedSignedPsbt`] and the next
    /// `poll` will emit [`SenderStep::SignAndFinalize`] again — the caller
    /// must produce a signed PSBT to drive the session to `Done`.
    pub fn resume_from_events(
        events: Vec<SessionEvent>,
        ohttp_relay: impl Into<String>,
        _min_fee_rate: FeeRate,
    ) -> Result<Self, Error> {
        let persister = Replay::new(events);
        let (session, _history) =
            replay_event_log(&persister).map_err(|e| Error::payjoin(format!("{e:?}")))?;

        let state = match session {
            SendSession::WithReplyKey(s) => State::PostingOriginal(s),
            SendSession::PollingForProposal(s) => State::PollingProposal {
                session: s,
                pending_backoff: false,
            },
            SendSession::Closed(SessionOutcome::Success(psbt)) => {
                State::NeedSignedPsbt { proposal: psbt }
            }
            SendSession::Closed(outcome) => State::Failed(Some(Error::Payjoin(format!(
                "session previously closed: {outcome:?}"
            )))),
        };
        Ok(Self {
            ohttp_relay: ohttp_relay.into(),
            state: Some(state),
            result: None,
            events: Vec::new(),
        })
    }

    /// The broadcastable transaction, once the session has reached `Done`.
    pub fn final_tx(&self) -> Option<&Transaction> {
        self.result.as_ref().map(|(t, _)| t)
    }

    /// The network fee, once the session has reached `Done`.
    pub fn fee(&self) -> Option<Amount> {
        self.result.as_ref().map(|(_, f)| *f)
    }

    /// Advance the state machine and report what the caller should do next.
    pub fn poll(&mut self) -> SenderStep {
        if !self.events.is_empty() {
            return SenderStep::Save(std::mem::take(&mut self.events));
        }
        let state = match self.state.take() {
            Some(s) => s,
            None => return SenderStep::Failed(Error::Terminated),
        };
        let (next, step) = self.step(state);
        self.state = Some(next);
        step
    }

    /// Feed back the body of the directory response from the most recent
    /// `SenderStep::SendRequest`.
    pub fn feed_response(&mut self, bytes: Vec<u8>) -> Result<(), Error> {
        let state = self.state.take().ok_or(Error::Terminated)?;
        let next = self.consume_response(state, bytes);
        self.state = Some(next);
        Ok(())
    }

    /// Feed back the signed-and-finalized PSBT from
    /// `SenderStep::SignAndFinalize`. Every input must be signed and
    /// finalized — the runtime calls `extract_tx()` on it next.
    pub fn feed_signed_psbt(&mut self, signed: Psbt) -> Result<(), Error> {
        let state = self.state.take().ok_or(Error::Terminated)?;
        let next = match state {
            State::NeedSignedPsbt { .. } => match self.extract(signed) {
                Ok((tx, fee)) => {
                    self.result = Some((tx, fee));
                    State::Done
                }
                Err(e) => State::Failed(Some(e)),
            },
            other => {
                self.state = Some(other);
                return Err(Error::Payjoin(
                    "feed_signed_psbt called in an unexpected state".into(),
                ));
            }
        };
        self.state = Some(next);
        Ok(())
    }

    fn step(&self, state: State) -> (State, SenderStep) {
        match state {
            State::PostingOriginal(session) => {
                match session.create_v2_post_request(self.ohttp_relay.as_str()) {
                    Ok((req, ctx)) => (
                        State::AwaitingPostAck { session, ctx },
                        SenderStep::SendRequest(req),
                    ),
                    Err(e) => (State::Failed(None), SenderStep::Failed(Error::payjoin(e))),
                }
            }
            State::PollingProposal { session, pending_backoff: true } => (
                State::PollingProposal { session, pending_backoff: false },
                SenderStep::Backoff,
            ),
            State::PollingProposal { session, pending_backoff: false } => {
                match session.create_poll_request(self.ohttp_relay.as_str()) {
                    Ok((req, ctx)) => (
                        State::AwaitingProposalPoll { session, ctx },
                        SenderStep::SendRequest(req),
                    ),
                    Err(e) => (State::Failed(None), SenderStep::Failed(Error::payjoin(e))),
                }
            }
            State::NeedSignedPsbt { proposal } => (
                State::NeedSignedPsbt { proposal: proposal.clone() },
                SenderStep::SignAndFinalize(proposal),
            ),
            State::AwaitingPostAck { .. } | State::AwaitingProposalPoll { .. } => (
                state,
                SenderStep::Failed(Error::Payjoin(
                    "called poll() while awaiting a response".into(),
                )),
            ),
            State::Done => (State::Done, SenderStep::Done),
            State::Failed(opt) => {
                let err = opt.unwrap_or(Error::Terminated);
                (State::Failed(None), SenderStep::Failed(err))
            }
        }
    }

    fn consume_response(&mut self, state: State, bytes: Vec<u8>) -> State {
        match state {
            State::AwaitingPostAck { session, ctx } => {
                let (action, outcome) = session.process_response(&bytes, ctx).deconstruct();
                record(&mut self.events, action);
                match outcome {
                    Ok(next) => State::PollingProposal {
                        session: next,
                        pending_backoff: false,
                    },
                    Err(api) => State::Failed(Some(Error::from_api(api))),
                }
            }
            State::AwaitingProposalPoll { session, ctx } => {
                let (action, outcome) = session.process_response(&bytes, ctx).deconstruct();
                record(&mut self.events, action);
                let outcome = match outcome {
                    Ok(o) => o,
                    Err(api) => return State::Failed(Some(Error::from_api(api))),
                };
                match outcome {
                    OptionalTransitionOutcome::Stasis(session) => State::PollingProposal {
                        session,
                        pending_backoff: true,
                    },
                    OptionalTransitionOutcome::Progress(psbt) => {
                        State::NeedSignedPsbt { proposal: psbt }
                    }
                }
            }
            _ => State::Failed(Some(Error::Payjoin(
                "feed_response called in an unexpected state".into(),
            ))),
        }
    }

    fn extract(&self, signed: Psbt) -> Result<(Transaction, Amount), Error> {
        let fee = signed.fee().map_err(Error::payjoin)?;
        let tx = signed
            .extract_tx()
            .map_err(|e| Error::Wallet(format!("extract_tx after signing: {e}")))?;
        Ok((tx, fee))
    }
}

/// Append `action`'s event (if any) to the session's buffered event log.
fn record(events: &mut Vec<SessionEvent>, action: PersistAction<SessionEvent>) {
    match action {
        PersistAction::Save(e) | PersistAction::SaveAndClose(e) => events.push(e),
        PersistAction::NoOp => {}
    }
}
