//! Tests for `*Session::resume_from_events`.
//!
//! These tests use the closed-session shortcut: a fresh sender event log that
//! already ends in `Closed(_)`. Constructing a useful intermediate event log
//! requires a real OHTTP keypair (gated behind `#[cfg(test)]` in payjoin), so
//! the live-protocol resumption path is exercised by the e2e example instead.

use bitcoin::FeeRate;
use payjoin_runtime::{Error, SenderSession, SenderSessionEvent, SenderStep};

#[test]
fn sender_resume_from_empty_log_errors() {
    let outcome = SenderSession::resume_from_events(
        Vec::<SenderSessionEvent>::new(),
        "https://relay.example/",
        FeeRate::BROADCAST_MIN,
    );
    assert!(
        matches!(outcome, Err(Error::Payjoin(_))),
        "expected Payjoin error for empty log, got {:?}",
        outcome.as_ref().err()
    );
}

#[test]
fn sender_resume_from_closed_failure_lands_in_failed() {
    // A single-event log that says "we already closed with Failure" — payjoin
    // accepts this as a terminal state.
    let events = vec![SenderSessionEvent::Closed(
        payjoin::send::v2::SessionOutcome::Failure,
    )];

    // Either this resumes to Failed, or it's rejected as an invalid log
    // (no `Created` event first). Both are acceptable — what matters is
    // that resume doesn't panic.
    let outcome = SenderSession::resume_from_events(
        events,
        "https://relay.example/",
        FeeRate::BROADCAST_MIN,
    );

    match outcome {
        Ok(mut session) => {
            assert!(session.final_tx().is_none());
            match session.poll() {
                SenderStep::Failed(_) => {}
                other => panic!("expected Failed after closed-failure resume, got {other:?}"),
            }
        }
        Err(Error::Payjoin(_)) => {
            // payjoin's `replay_events` rejected the log (no `Created` first).
        }
        Err(other) => panic!("unexpected error: {other:?}"),
    }
}
