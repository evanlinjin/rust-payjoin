use std::sync::Arc;

use payjoin::bitcoin::psbt::PsbtParseError as CorePsbtParseError;
use payjoin::send;

use crate::error::FfiValidationError;

/// Error building a Sender from a SenderBuilder.
///
/// This error is unrecoverable.
#[derive(Debug, PartialEq, Eq, thiserror::Error, uniffi::Object)]
#[uniffi::export(Debug, Display, Eq)]
#[error("Error initializing the sender: {msg}")]
pub struct BuildSenderError {
    msg: String,
}

impl From<PsbtParseError> for BuildSenderError {
    fn from(value: PsbtParseError) -> Self { BuildSenderError { msg: value.to_string() } }
}

impl From<send::BuildSenderError> for BuildSenderError {
    fn from(value: send::BuildSenderError) -> Self { BuildSenderError { msg: value.to_string() } }
}

/// FFI-visible PSBT parsing error surfaced at the sender boundary.
#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum PsbtParseError {
    /// The provided PSBT string could not be parsed.
    #[error("Invalid PSBT: {0}")]
    InvalidPsbt(String),
}

impl From<CorePsbtParseError> for PsbtParseError {
    fn from(value: CorePsbtParseError) -> Self { PsbtParseError::InvalidPsbt(value.to_string()) }
}

/// Raised when inputs provided to the sender are malformed or sender build
/// fails.
#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum SenderInputError {
    #[error(transparent)]
    Psbt(PsbtParseError),
    #[error(transparent)]
    Build(Arc<BuildSenderError>),
    #[error(transparent)]
    FfiValidation(FfiValidationError),
}

impl From<FfiValidationError> for SenderInputError {
    fn from(value: FfiValidationError) -> Self { SenderInputError::FfiValidation(value) }
}

/// Error returned when request could not be created.
#[derive(Debug, thiserror::Error, uniffi::Object)]
#[uniffi::export(Debug, Display)]
#[error(transparent)]
pub struct CreateRequestError(#[from] send::v2::CreateRequestError);

/// Error returned for v2-specific payload encapsulation errors.
#[derive(Debug, thiserror::Error, uniffi::Object)]
#[uniffi::export(Debug, Display)]
#[error(transparent)]
pub struct EncapsulationError(#[from] send::v2::EncapsulationError);

/// Error that may occur when the response from receiver is malformed.
#[derive(Debug, thiserror::Error, uniffi::Object)]
#[uniffi::export(Debug, Display)]
#[error(transparent)]
pub struct ValidationError(#[from] send::ValidationError);

/// Represent an error returned by Payjoin receiver.
#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum ResponseError {
    /// `WellKnown` Errors are defined in the BIP78 spec.
    #[error("A receiver error occurred: {0}")]
    WellKnown(Arc<WellKnownError>),

    /// Errors caused by malformed responses.
    #[error("An error occurred due to a malformed response: {0}")]
    Validation(Arc<ValidationError>),

    /// `Unrecognized` Errors are NOT defined in the BIP78 spec.
    ///
    /// It is NOT safe to display `Unrecognized` errors to end users as they
    /// could be used maliciously to phish a non technical user. Only display
    /// them in debug logs.
    #[error("An unrecognized error occurred")]
    Unrecognized { error_code: String, msg: String },
}

impl From<send::ResponseError> for ResponseError {
    fn from(value: send::ResponseError) -> Self {
        match value {
            send::ResponseError::WellKnown(e) => ResponseError::WellKnown(Arc::new(e.into())),
            send::ResponseError::Validation(e) => ResponseError::Validation(Arc::new(e.into())),
            send::ResponseError::Unrecognized { error_code, message } =>
                ResponseError::Unrecognized { error_code, msg: message },
        }
    }
}

/// A well-known error that can be safely displayed to end users.
#[derive(Debug, thiserror::Error, uniffi::Object)]
#[uniffi::export(Debug, Display)]
#[error(transparent)]
pub struct WellKnownError(#[from] send::WellKnownError);

/// Discriminator for the underlying protocol-level error inside
/// [`SenderApiError`]. Foreign code can match on the kind without parsing
/// the `msg` string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum SenderErrorKind {
    /// HPKE / OHTTP encapsulation error processing the directory response.
    Encapsulation,
    /// Structured error returned by the receiver (BIP78 well-known,
    /// validation error, or unrecognized error code). The `msg` field
    /// carries the human-readable details. For typed inspection of
    /// well-known error codes, [`ResponseError`] remains accessible as a
    /// standalone uniffi type for callers that need it.
    Response,
}

/// Surface-level error returned by sender action methods that operate on a
/// [`crate::send::SenderEventBuffer`]. Storage errors are not represented here
/// — those surface from the caller's drain loop.
///
/// Mirrors the shape of [`crate::receive::ReceiverApiError`]: a `kind`
/// discriminator paired with a human-readable `msg`. Foreign code matches on
/// (severity, kind) for control flow.
#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum SenderApiError {
    /// Retry the action from the same state.
    #[error("Transient {kind:?} error: {msg}")]
    Transient { kind: SenderErrorKind, msg: String },
    /// Session is terminally closed.
    #[error("Fatal {kind:?} error: {msg}")]
    Fatal { kind: SenderErrorKind, msg: String },
}

impl SenderApiError {
    /// Convert from `ApiError<EncapsulationError>`.
    pub(crate) fn from_api_error_encapsulation(
        err: payjoin::persist::ApiError<send::v2::EncapsulationError>,
    ) -> Self {
        match err {
            payjoin::persist::ApiError::Transient(e) => SenderApiError::Transient {
                kind: SenderErrorKind::Encapsulation,
                msg: e.to_string(),
            },
            payjoin::persist::ApiError::Fatal(e) =>
                SenderApiError::Fatal { kind: SenderErrorKind::Encapsulation, msg: e.to_string() },
            payjoin::persist::ApiError::FatalWithState(e, _) =>
                SenderApiError::Fatal { kind: SenderErrorKind::Encapsulation, msg: e.to_string() },
        }
    }

    /// Convert from `ApiError<ResponseError>`.
    pub(crate) fn from_api_error_response(
        err: payjoin::persist::ApiError<send::ResponseError>,
    ) -> Self {
        match err {
            payjoin::persist::ApiError::Transient(e) =>
                SenderApiError::Transient { kind: SenderErrorKind::Response, msg: e.to_string() },
            payjoin::persist::ApiError::Fatal(e) =>
                SenderApiError::Fatal { kind: SenderErrorKind::Response, msg: e.to_string() },
            payjoin::persist::ApiError::FatalWithState(e, _) =>
                SenderApiError::Fatal { kind: SenderErrorKind::Response, msg: e.to_string() },
        }
    }
}

/// Error that may occur when the sender session event log is replayed.
#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum SenderReplayError {
    /// Replay-time error from the library (invalid event sequence, etc.).
    #[error("Replay error: {0}")]
    Replay(String),
}

impl From<payjoin::error::ReplayError<send::v2::SendSession, send::v2::SessionEvent>>
    for SenderReplayError
{
    fn from(
        value: payjoin::error::ReplayError<send::v2::SendSession, send::v2::SessionEvent>,
    ) -> Self {
        SenderReplayError::Replay(value.to_string())
    }
}
