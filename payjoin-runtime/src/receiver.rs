//! Receiver-side sans-IO state machine.
//!
//! Every wallet decision the receiver protocol needs (broadcast suitability,
//! SPK ownership, contributed inputs, PSBT signing) is surfaced as a
//! [`ReceiverStep`] variant and answered through the matching `feed_*` method.
//! The runtime itself never blocks on a wallet call and never goes through
//! payjoin's [`SessionPersister`](payjoin::persist::SessionPersister)
//! callback — every transition is consumed via
//! [`deconstruct`](payjoin::persist::MaybeFatalTransition::deconstruct), which
//! hands the event back as plain data.

use bitcoin::{OutPoint, Psbt, ScriptBuf, Transaction};
use payjoin::persist::{OptionalTransitionOutcome, PersistAction};
use payjoin::receive::v2::{
    replay_event_log, Initialized, MaybeInputsOwned, PayjoinProposal, ProvisionalProposal,
    ReceiveSession, Receiver, ReceiverBuilder, SessionEvent, SessionOutcome,
    UncheckedOriginalPayload, WantsInputs,
};
use payjoin::receive::InputPair;
use payjoin::Request;

use crate::persister::Replay;
use crate::{Error, FeeRange};

/// What the caller should do next to advance a [`ReceiverSession`].
///
/// Returned by [`ReceiverSession::poll`]. Each variant has a matching `feed_*`
/// method that delivers the answer back to the runtime.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum ReceiverStep {
    /// Persist these session events atomically before continuing.
    ///
    /// The events represent one logical state advance (e.g. running the full
    /// post-broadcast check ceremony emits four events in a row). Save them in
    /// order, then call `poll` again. If the session crashes after `Save` is
    /// emitted but before the caller persists, recovery is via
    /// [`ReceiverSession::resume_from_events`].
    Save(Vec<SessionEvent>),
    /// Send this HTTP request, then feed the response body back via
    /// [`ReceiverSession::feed_response`].
    SendRequest(Request),
    /// The directory had no payload yet. Sleep, then call `poll` again. The
    /// runtime never enforces a specific delay — pick what's appropriate for
    /// your context (a few seconds is conventional).
    Backoff,
    /// Decide whether the sender's original transaction would be accepted into
    /// the mempool (the `testmempoolaccept` semantic). Answer via
    /// [`ReceiverSession::feed_broadcast_check`].
    CheckBroadcast(Transaction),
    /// Decide which of these `script_pubkey`s the receiver owns. The Vec is
    /// the concatenation of (1) the sender's input prevout `script_pubkey`s
    /// and (2) the sender's output `script_pubkey`s, in their PSBT order.
    /// Answer via [`ReceiverSession::feed_owned`] with a Vec of the same
    /// length and order — `true` means "yes, this is one of mine".
    ResolveOwned(Vec<ScriptBuf>),
    /// Provide the inputs the receiver wishes to contribute to the payjoin.
    /// Answer via [`ReceiverSession::feed_contribute`].
    Contribute,
    /// Sign and finalize the receiver's contributed inputs in the proposal
    /// PSBT. The sender's inputs stay unsigned by design — they will be
    /// signed by the sender on the round trip. Answer via
    /// [`ReceiverSession::feed_signed_psbt`].
    SignAndFinalize(Psbt),
    /// The session reached its terminal success state — the proposal has been
    /// posted and the directory acknowledged it.
    Done,
    /// The session failed terminally. Subsequent `poll` calls return
    /// [`Error::Terminated`].
    Failed(Error),
}

/// Sans-IO state machine for the payjoin v2 receiver role.
///
/// **Persistence.** Every internal payjoin state advance produces one
/// `SessionEvent`. The runtime buffers them and surfaces them via
/// [`ReceiverStep::Save`]; the caller decides where/when/how to persist. To
/// resume after a crash, replay the saved log via
/// [`Self::resume_from_events`].
pub struct ReceiverSession {
    fee_range: FeeRange,
    ohttp_relay: String,
    pj_uri: String,
    state: Option<State>,
    events: Vec<SessionEvent>,
    /// Outpoints of the sender's inputs, captured from the original
    /// transaction as soon as it lands. Used at `NeedSignedPsbt` time to
    /// identify those inputs in the proposal and clear their stale finalized
    /// fields before handing the PSBT to the caller for signing (payjoin
    /// otherwise clears them inside `finalize_proposal`'s callback, which we
    /// can't piggyback on in the sans-IO flow).
    sender_input_outpoints: Vec<OutPoint>,
}

// Variants have naturally different sizes; at most one variant is ever stored
// at a time so the box-each-variant suggestion buys nothing.
#[allow(clippy::large_enum_variant)]
enum State {
    /// GET-poll the directory for the sender's original PSBT.
    Polling {
        session: Receiver<Initialized>,
        pending_backoff: bool,
    },
    /// GET sent; awaiting the directory's response body.
    AwaitingPoll {
        session: Receiver<Initialized>,
        ctx: ohttp::ClientResponse,
    },
    /// Original PSBT received; ask the caller about broadcast suitability.
    /// The `tx` is cached to avoid re-extracting on every `poll`.
    NeedBroadcastCheck {
        session: Receiver<UncheckedOriginalPayload>,
        tx: Transaction,
    },
    /// Broadcast suitability OK; ask the caller about SPK ownership for the
    /// sender's inputs and outputs in one batch.
    NeedOwnership {
        session: Receiver<MaybeInputsOwned>,
        input_spks: Vec<ScriptBuf>,
        output_spks: Vec<ScriptBuf>,
    },
    /// Outputs identified; ask the caller for inputs to contribute.
    NeedContribute {
        session: Receiver<WantsInputs>,
    },
    /// Inputs committed and fee-range applied; ask the caller to sign the
    /// proposal PSBT.
    NeedSignedPsbt {
        session: Receiver<ProvisionalProposal>,
        psbt: Psbt,
    },
    /// Proposal finalized; POST it back to the directory.
    Posting(Receiver<PayjoinProposal>),
    /// POST sent; awaiting acknowledgement.
    AwaitingPostAck,
    /// Terminal success.
    Done,
    /// Terminal failure.
    Failed(Option<Error>),
}

impl ReceiverSession {
    /// Build a new session.
    pub fn new(
        builder: ReceiverBuilder,
        ohttp_relay: impl Into<String>,
        fee_range: FeeRange,
    ) -> Result<Self, Error> {
        let mut events = Vec::new();
        let (action, session) = builder.build().deconstruct();
        record(&mut events, action);
        let pj_uri = session.pj_uri().to_string();
        Ok(Self {
            fee_range,
            ohttp_relay: ohttp_relay.into(),
            pj_uri,
            state: Some(State::Polling {
                session,
                pending_backoff: false,
            }),
            events,
            sender_input_outpoints: Vec::new(),
        })
    }

    /// Resume a session from a previously-persisted event log.
    ///
    /// `events` should be the complete sequence of session events as they were
    /// recorded from [`ReceiverStep::Save`], in order. The runtime replays
    /// them through payjoin's state machine to reconstruct the receiver's
    /// current state, then continues from there.
    ///
    /// # Atomic-persistence requirement
    ///
    /// Each `ReceiverStep::Save` may carry multiple events that represent one
    /// logical state advance — for example, `feed_owned` produces four events
    /// (`CheckedInputsNotOwned`, `CheckedNoInputsSeenBefore`,
    /// `IdentifiedReceiverOutputs`, `CommittedOutputs`). They must be
    /// persisted **atomically as a batch**. If the saved log ends partway
    /// through such a batch this function returns an error, since the state
    /// machine has no clean re-entry point in the middle.
    pub fn resume_from_events(
        events: Vec<SessionEvent>,
        ohttp_relay: impl Into<String>,
        fee_range: FeeRange,
    ) -> Result<Self, Error> {
        let persister = Replay::new(events);
        let (session, history) =
            replay_event_log(&persister).map_err(|e| Error::payjoin(format!("{e:?}")))?;
        let pj_uri = history.pj_uri().to_string();

        let state = match session {
            ReceiveSession::Initialized(s) => State::Polling {
                session: s,
                pending_backoff: false,
            },
            ReceiveSession::UncheckedOriginalPayload(s) => {
                let tx = s.extract_original_tx();
                State::NeedBroadcastCheck { session: s, tx }
            }
            ReceiveSession::MaybeInputsOwned(s) => need_ownership_state(s)?,
            ReceiveSession::WantsInputs(s) => State::NeedContribute { session: s },
            // Resuming directly into ProvisionalProposal requires us to know
            // which inputs are the receiver's so we can clear the sender's
            // stale finalized fields before surfacing the PSBT for signing —
            // but with only a public typestate view we cannot reconstruct that
            // partition from this typestate. Refuse this resume target until
            // upstream exposes either the original tx or the sender-input
            // outpoints on `Receiver<ProvisionalProposal>`.
            ReceiveSession::ProvisionalProposal(_) => {
                return Err(Error::Payjoin(
                    "cannot resume directly into ProvisionalProposal yet; \
                     re-run from an earlier event-log checkpoint"
                        .into(),
                ));
            }
            ReceiveSession::PayjoinProposal(s) => State::Posting(s),
            ReceiveSession::Closed(SessionOutcome::Success(_)) => State::Done,
            ReceiveSession::Closed(outcome) => State::Failed(Some(Error::Payjoin(format!(
                "session previously closed: {outcome:?}"
            )))),
            // Any other variant means the event log ended mid-batch.
            other => {
                return Err(Error::Payjoin(format!(
                    "cannot resume from intermediate state {other:?}; event log was not \
                     persisted atomically per `ReceiverStep::Save` batch"
                )));
            }
        };

        Ok(Self {
            fee_range,
            ohttp_relay: ohttp_relay.into(),
            pj_uri,
            state: Some(state),
            events: Vec::new(),
            sender_input_outpoints: Vec::new(),
        })
    }

    /// The BIP-21 / BIP-77 URI the receiver should share with the sender out of band.
    pub fn pj_uri(&self) -> &str {
        &self.pj_uri
    }

    /// Advance the state machine and report what the caller should do next.
    pub fn poll(&mut self) -> ReceiverStep {
        // Drain any buffered events first — the caller must persist them
        // before we issue any further side-effecting requests.
        if !self.events.is_empty() {
            return ReceiverStep::Save(std::mem::take(&mut self.events));
        }

        let state = match self.state.take() {
            Some(s) => s,
            None => return ReceiverStep::Failed(Error::Terminated),
        };
        let (next, step) = self.step(state);
        self.state = Some(next);
        step
    }

    /// Feed back the body of the directory response from the most recent
    /// `ReceiverStep::SendRequest`.
    pub fn feed_response(&mut self, bytes: Vec<u8>) -> Result<(), Error> {
        let state = self.state.take().ok_or(Error::Terminated)?;
        let next = self.consume_response(state, bytes);
        self.state = Some(next);
        Ok(())
    }

    /// Feed back the broadcast-suitability decision from
    /// `ReceiverStep::CheckBroadcast`.
    pub fn feed_broadcast_check(&mut self, ok: bool) -> Result<(), Error> {
        let state = self.state.take().ok_or(Error::Terminated)?;
        let next = match state {
            State::NeedBroadcastCheck { session, tx } => {
                // Capture the sender's input outpoints from the original tx —
                // we need them later to clear stale finalized fields on the
                // proposal PSBT before surfacing it for signing.
                self.sender_input_outpoints =
                    tx.input.iter().map(|i| i.previous_output).collect();
                self.advance_broadcast(session, ok)
            }
            other => {
                self.state = Some(other);
                return Err(Error::Payjoin(
                    "feed_broadcast_check called in an unexpected state".into(),
                ));
            }
        };
        self.state = Some(next);
        Ok(())
    }

    /// Feed back the ownership decisions for the `ReceiverStep::ResolveOwned`
    /// SPK list. `answers` must have the same length as the SPK list and align
    /// index-for-index.
    pub fn feed_owned(&mut self, answers: Vec<bool>) -> Result<(), Error> {
        let state = self.state.take().ok_or(Error::Terminated)?;
        let next = match state {
            State::NeedOwnership { session, input_spks, output_spks } => {
                let expected = input_spks.len() + output_spks.len();
                if answers.len() != expected {
                    self.state = Some(State::NeedOwnership { session, input_spks, output_spks });
                    return Err(Error::Wallet(format!(
                        "feed_owned: expected {expected} answers, got {}",
                        answers.len()
                    )));
                }
                let (input_answers, output_answers) = answers.split_at(input_spks.len());
                self.advance_ownership(session, input_answers, output_answers)
            }
            other => {
                self.state = Some(other);
                return Err(Error::Payjoin(
                    "feed_owned called in an unexpected state".into(),
                ));
            }
        };
        self.state = Some(next);
        Ok(())
    }

    /// Feed back the inputs to contribute from `ReceiverStep::Contribute`.
    pub fn feed_contribute(&mut self, inputs: Vec<InputPair>) -> Result<(), Error> {
        let state = self.state.take().ok_or(Error::Terminated)?;
        let next = match state {
            State::NeedContribute { session } => self.advance_contribute(session, inputs),
            other => {
                self.state = Some(other);
                return Err(Error::Payjoin(
                    "feed_contribute called in an unexpected state".into(),
                ));
            }
        };
        self.state = Some(next);
        Ok(())
    }

    /// Feed back the signed-and-finalized PSBT from
    /// `ReceiverStep::SignAndFinalize`. The receiver's own inputs must be
    /// signed and finalized; the sender's inputs stay unsigned by design.
    pub fn feed_signed_psbt(&mut self, psbt: Psbt) -> Result<(), Error> {
        let state = self.state.take().ok_or(Error::Terminated)?;
        let next = match state {
            State::NeedSignedPsbt { session, .. } => self.advance_finalize(session, psbt),
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

    fn step(&self, state: State) -> (State, ReceiverStep) {
        match state {
            State::Polling { session, pending_backoff: true } => (
                State::Polling { session, pending_backoff: false },
                ReceiverStep::Backoff,
            ),
            State::Polling { session, pending_backoff: false } => {
                match session.create_poll_request(self.ohttp_relay.as_str()) {
                    Ok((req, ctx)) => (
                        State::AwaitingPoll { session, ctx },
                        ReceiverStep::SendRequest(req),
                    ),
                    Err(e) => (State::Failed(None), ReceiverStep::Failed(Error::payjoin(e))),
                }
            }
            State::NeedBroadcastCheck { session, tx } => (
                State::NeedBroadcastCheck { session, tx: tx.clone() },
                ReceiverStep::CheckBroadcast(tx),
            ),
            State::NeedOwnership { session, input_spks, output_spks } => {
                let combined: Vec<ScriptBuf> =
                    input_spks.iter().chain(output_spks.iter()).cloned().collect();
                (
                    State::NeedOwnership { session, input_spks, output_spks },
                    ReceiverStep::ResolveOwned(combined),
                )
            }
            State::NeedContribute { session } => (
                State::NeedContribute { session },
                ReceiverStep::Contribute,
            ),
            State::NeedSignedPsbt { session, psbt } => (
                State::NeedSignedPsbt { session, psbt: psbt.clone() },
                ReceiverStep::SignAndFinalize(psbt),
            ),
            State::Posting(proposal) => {
                match proposal.create_post_request(self.ohttp_relay.as_str()) {
                    Ok((req, _ctx)) => (State::AwaitingPostAck, ReceiverStep::SendRequest(req)),
                    Err(e) => (State::Failed(None), ReceiverStep::Failed(Error::payjoin(e))),
                }
            }
            State::AwaitingPoll { .. } | State::AwaitingPostAck => (
                state,
                ReceiverStep::Failed(Error::Payjoin(
                    "called poll() while awaiting a response".into(),
                )),
            ),
            State::Done => (State::Done, ReceiverStep::Done),
            State::Failed(opt) => {
                let err = opt.unwrap_or(Error::Terminated);
                (State::Failed(None), ReceiverStep::Failed(err))
            }
        }
    }

    fn consume_response(&mut self, state: State, bytes: Vec<u8>) -> State {
        match state {
            State::AwaitingPoll { session, ctx } => {
                let (action, outcome) = session.process_response(&bytes, ctx).deconstruct();
                record(&mut self.events, action);
                let outcome = match outcome {
                    Ok(o) => o,
                    Err(api) => return State::Failed(Some(Error::from_api(api))),
                };
                match outcome {
                    OptionalTransitionOutcome::Stasis(session) => State::Polling {
                        session,
                        pending_backoff: true,
                    },
                    OptionalTransitionOutcome::Progress(unchecked) => {
                        let tx = unchecked.extract_original_tx();
                        State::NeedBroadcastCheck { session: unchecked, tx }
                    }
                }
            }
            State::AwaitingPostAck => State::Done,
            _ => State::Failed(Some(Error::Payjoin(
                "feed_response called in an unexpected state".into(),
            ))),
        }
    }

    fn advance_broadcast(
        &mut self,
        session: Receiver<UncheckedOriginalPayload>,
        ok: bool,
    ) -> State {
        let (action, outcome) = session
            .check_broadcast_suitability(self.fee_range.min, |_| Ok(ok))
            .deconstruct();
        record(&mut self.events, action);
        let maybe_inputs_owned = match outcome {
            Ok(s) => s,
            Err(api) => return State::Failed(Some(Error::from_api(api))),
        };
        match need_ownership_state(maybe_inputs_owned) {
            Ok(state) => state,
            Err(e) => State::Failed(Some(e)),
        }
    }

    fn advance_ownership(
        &mut self,
        session: Receiver<MaybeInputsOwned>,
        input_answers: &[bool],
        output_answers: &[bool],
    ) -> State {
        let input_idx = std::cell::Cell::new(0usize);
        let (action, outcome) = session
            .check_inputs_not_owned(&mut |_spk| {
                let i = input_idx.get();
                input_idx.set(i + 1);
                Ok(input_answers[i])
            })
            .deconstruct();
        record(&mut self.events, action);
        let inputs_seen = match outcome {
            Ok(s) => s,
            Err(api) => return State::Failed(Some(Error::from_api(api))),
        };

        let (action, outcome) = inputs_seen
            .check_no_inputs_seen_before(&mut |_| Ok(false))
            .deconstruct();
        record(&mut self.events, action);
        let outputs_unknown = match outcome {
            Ok(s) => s,
            Err(api) => return State::Failed(Some(Error::from_api(api))),
        };

        let output_idx = std::cell::Cell::new(0usize);
        let (action, outcome) = outputs_unknown
            .identify_receiver_outputs(&mut |_spk| {
                let i = output_idx.get();
                output_idx.set(i + 1);
                Ok(output_answers[i])
            })
            .deconstruct();
        record(&mut self.events, action);
        let wants_outputs = match outcome {
            Ok(s) => s,
            Err(api) => return State::Failed(Some(Error::from_api(api))),
        };

        let (action, wants_inputs) = wants_outputs.commit_outputs().deconstruct();
        record(&mut self.events, action);

        State::NeedContribute { session: wants_inputs }
    }

    fn advance_contribute(
        &mut self,
        session: Receiver<WantsInputs>,
        inputs: Vec<InputPair>,
    ) -> State {
        if inputs.is_empty() {
            return State::Failed(Some(Error::Wallet(
                "no candidate inputs to contribute".into(),
            )));
        }
        let selected = match session.try_preserving_privacy(inputs) {
            Ok(s) => s,
            Err(e) => {
                return State::Failed(Some(Error::Payjoin(format!(
                    "privacy-preserving selection: {e:?}"
                ))))
            }
        };
        let session = match session.contribute_inputs(vec![selected]) {
            Ok(s) => s,
            Err(e) => {
                return State::Failed(Some(Error::Payjoin(format!("contribute_inputs: {e:?}"))))
            }
        };
        let (action, wants_fee_range) = session.commit_inputs().deconstruct();
        record(&mut self.events, action);
        let (action, outcome) = wants_fee_range
            .apply_fee_range(self.fee_range.min, self.fee_range.max)
            .deconstruct();
        record(&mut self.events, action);
        let provisional = match outcome {
            Ok(s) => s,
            Err(api) => return State::Failed(Some(Error::from_api(api))),
        };
        let mut psbt = provisional.psbt_to_sign();
        // Sender inputs in the proposal still carry their pre-payjoin
        // finalization (the original PSBT arrived already signed). Payjoin
        // itself clears these inside `finalize_proposal`'s callback, but in
        // the sans-IO flow the caller signs *outside* that callback — so we
        // must clear them ourselves before surfacing the PSBT for signing.
        // Otherwise the caller signs a PSBT that still claims the sender's
        // inputs are finalized, and the sender rejects the proposal as
        // `SenderTxinContainsFinalScriptSig`.
        clear_sender_finalization(&mut psbt, &self.sender_input_outpoints);
        State::NeedSignedPsbt { session: provisional, psbt }
    }

    fn advance_finalize(
        &mut self,
        session: Receiver<ProvisionalProposal>,
        signed: Psbt,
    ) -> State {
        let (action, outcome) = session
            .finalize_proposal(|_psbt| Ok(signed.clone()))
            .deconstruct();
        record(&mut self.events, action);
        let finalized = match outcome {
            Ok(s) => s,
            Err(api) => return State::Failed(Some(Error::from_api(api))),
        };
        State::Posting(finalized)
    }
}

/// Append `action`'s event (if any) to the session's buffered event log.
fn record(events: &mut Vec<SessionEvent>, action: PersistAction<SessionEvent>) {
    match action {
        PersistAction::Save(e) | PersistAction::SaveAndClose(e) => events.push(e),
        PersistAction::NoOp => {}
    }
}

/// Mirror payjoin's internal sender-signature clearing: any PSBT input whose
/// `previous_output` is in `sender_outpoints` is part of the sender's original
/// (signed) transaction, and its stale `final_script_sig` / `final_script_witness`
/// / `tap_key_sig` must be removed before the receiver signs and finalizes its
/// own contributed inputs.
fn clear_sender_finalization(psbt: &mut Psbt, sender_outpoints: &[OutPoint]) {
    for (txin, psbtin) in psbt.unsigned_tx.input.iter().zip(psbt.inputs.iter_mut()) {
        if sender_outpoints.contains(&txin.previous_output) {
            psbtin.final_script_sig = None;
            psbtin.final_script_witness = None;
            psbtin.tap_key_sig = None;
        }
    }
}

/// Compute the [`State::NeedOwnership`] payload for a given
/// `Receiver<MaybeInputsOwned>` — pre-enumerate the input and output SPKs the
/// caller will be asked about so a single round-trip covers both.
fn need_ownership_state(session: Receiver<MaybeInputsOwned>) -> Result<State, Error> {
    let input_spks = session
        .sender_input_script_pubkeys()
        .map_err(Error::payjoin)?;
    let tx = session.extract_tx_to_schedule_broadcast();
    let output_spks: Vec<ScriptBuf> = tx
        .output
        .iter()
        .map(|txout| txout.script_pubkey.clone())
        .collect();
    Ok(State::NeedOwnership {
        session,
        input_spks,
        output_spks,
    })
}
