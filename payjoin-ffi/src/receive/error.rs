use std::sync::Arc;

use payjoin::receive;

use crate::error::{FfiValidationError, ForeignError, ImplementationError};
use crate::receive::HasReplyableError;
use crate::uri::error::IntoUrlError;

/// The top-level error type for the payjoin receiver.
#[derive(Debug, thiserror::Error, uniffi::Error)]
#[non_exhaustive]
pub enum ReceiverError {
    /// Error in underlying protocol function.
    #[error("Protocol error: {0}")]
    Protocol(Arc<ProtocolError>),
    /// Error arising due to the specific receiver implementation
    /// (e.g. database errors, network failures, wallet errors).
    #[error("Implementation error: {0}")]
    Implementation(Arc<ImplementationError>),
    /// Error that may occur when converting a value into a URL.
    #[error("IntoUrl error: {0}")]
    IntoUrl(Arc<IntoUrlError>),
    /// Catch-all for unhandled error variants.
    #[error("An unexpected error occurred")]
    Unexpected,
}

impl From<receive::Error> for ReceiverError {
    fn from(value: receive::Error) -> Self {
        use ReceiverError::*;

        match value {
            receive::Error::Protocol(e) => Protocol(Arc::new(ProtocolError(e))),
            receive::Error::Implementation(e) =>
                Implementation(Arc::new(ImplementationError::from(e))),
            _ => Unexpected,
        }
    }
}

impl From<receive::ProtocolError> for ReceiverError {
    fn from(value: receive::ProtocolError) -> Self {
        ReceiverError::Protocol(Arc::new(ProtocolError(value)))
    }
}

impl From<payjoin::ImplementationError> for ReceiverError {
    fn from(value: payjoin::ImplementationError) -> Self {
        ReceiverError::Implementation(Arc::new(ImplementationError::from(value)))
    }
}

impl From<payjoin::IntoUrlError> for ReceiverError {
    fn from(value: payjoin::IntoUrlError) -> Self { ReceiverError::IntoUrl(Arc::new(value.into())) }
}

/// Discriminator for the underlying protocol-level error inside
/// [`ReceiverApiError`]. Mirrors the variants of [`ReceiverError`] so foreign
/// code can match on the kind without parsing the message string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum ReceiverErrorKind {
    /// Error in underlying protocol function (BIP 77 / BIP 78).
    Protocol,
    /// Error arising due to the specific receiver implementation
    /// (e.g. database, network, wallet).
    Implementation,
    /// Error converting a value into a URL.
    IntoUrl,
    /// Catch-all for unhandled error variants.
    Unexpected,
}

impl ReceiverErrorKind {
    fn from_receiver_error(err: &ReceiverError) -> Self {
        match err {
            ReceiverError::Protocol(_) => ReceiverErrorKind::Protocol,
            ReceiverError::Implementation(_) => ReceiverErrorKind::Implementation,
            ReceiverError::IntoUrl(_) => ReceiverErrorKind::IntoUrl,
            ReceiverError::Unexpected => ReceiverErrorKind::Unexpected,
        }
    }
}

/// Surface-level error returned by receiver action methods that operate on an
/// [`crate::receive::ReceiverEventBuffer`]. Storage errors are not represented
/// here — those surface from the caller's drain loop.
///
/// Each protocol-level variant carries a [`ReceiverErrorKind`] discriminator
/// so foreign code can match on the kind (Protocol / Implementation / IntoUrl
/// / Unexpected) without parsing the `msg` string.
#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum ReceiverApiError {
    /// Retry the action from the same state.
    #[error("Transient {kind:?} error: {msg}")]
    Transient { kind: ReceiverErrorKind, msg: String },
    /// Session is terminally closed.
    #[error("Fatal {kind:?} error: {msg}")]
    Fatal { kind: ReceiverErrorKind, msg: String },
    /// Fatal error that also produced a transition to [`HasReplyableError`].
    /// The caller can use the returned state to reply to the sender.
    #[error("Fatal {kind:?} error with replyable state: {msg}")]
    FatalWithReplyableState { kind: ReceiverErrorKind, msg: String, state: Arc<HasReplyableError> },
    /// FFI-layer validation failure (e.g. bad fee rate / amount input).
    #[error("Input validation error: {0}")]
    InputValidation(FfiValidationError),
    /// FFI-layer deserialization failure (e.g. malformed transaction bytes).
    #[error("Input deserialization error: {msg}")]
    InputDeserialization { msg: String },
}

impl ReceiverApiError {
    /// Convert from `ApiError<E>` (no error-state variant).
    pub(crate) fn from_api_error<E>(err: payjoin::persist::ApiError<E>) -> Self
    where
        ReceiverError: From<E>,
    {
        match err {
            payjoin::persist::ApiError::Transient(e) => {
                let wrapped: ReceiverError = e.into();
                ReceiverApiError::Transient {
                    kind: ReceiverErrorKind::from_receiver_error(&wrapped),
                    msg: wrapped.to_string(),
                }
            }
            payjoin::persist::ApiError::Fatal(e) => {
                let wrapped: ReceiverError = e.into();
                ReceiverApiError::Fatal {
                    kind: ReceiverErrorKind::from_receiver_error(&wrapped),
                    msg: wrapped.to_string(),
                }
            }
            payjoin::persist::ApiError::FatalWithState(e, _) => {
                let wrapped: ReceiverError = e.into();
                ReceiverApiError::Fatal {
                    kind: ReceiverErrorKind::from_receiver_error(&wrapped),
                    msg: wrapped.to_string(),
                }
            }
        }
    }

    /// Convert from `ApiError<E, Receiver<HasReplyableError>>` (with error-state).
    pub(crate) fn from_api_error_with_replyable_state<E>(
        err: payjoin::persist::ApiError<
            E,
            payjoin::receive::v2::Receiver<payjoin::receive::v2::HasReplyableError>,
        >,
    ) -> Self
    where
        ReceiverError: From<E>,
    {
        match err {
            payjoin::persist::ApiError::Transient(e) => {
                let wrapped: ReceiverError = e.into();
                ReceiverApiError::Transient {
                    kind: ReceiverErrorKind::from_receiver_error(&wrapped),
                    msg: wrapped.to_string(),
                }
            }
            payjoin::persist::ApiError::Fatal(e) => {
                let wrapped: ReceiverError = e.into();
                ReceiverApiError::Fatal {
                    kind: ReceiverErrorKind::from_receiver_error(&wrapped),
                    msg: wrapped.to_string(),
                }
            }
            payjoin::persist::ApiError::FatalWithState(e, state) => {
                let wrapped: ReceiverError = e.into();
                ReceiverApiError::FatalWithReplyableState {
                    kind: ReceiverErrorKind::from_receiver_error(&wrapped),
                    msg: wrapped.to_string(),
                    state: Arc::new(state.into()),
                }
            }
        }
    }
}

impl From<FfiValidationError> for ReceiverApiError {
    fn from(value: FfiValidationError) -> Self { ReceiverApiError::InputValidation(value) }
}

impl From<ForeignError> for ReceiverApiError {
    fn from(value: ForeignError) -> Self {
        ReceiverApiError::InputDeserialization { msg: value.to_string() }
    }
}

/// Error that may occur when building a receiver session.
#[derive(Debug, thiserror::Error, uniffi::Error)]
#[non_exhaustive]
pub enum ReceiverBuilderError {
    /// The provided Bitcoin address is invalid.
    #[error("Invalid Bitcoin address: {0}")]
    InvalidAddress(Arc<AddressParseError>),
    /// Error that may occur when converting a value into a URL.
    #[error("Invalid directory URL: {0}")]
    IntoUrl(Arc<IntoUrlError>),
}

impl From<payjoin::IntoUrlError> for ReceiverBuilderError {
    fn from(value: payjoin::IntoUrlError) -> Self {
        ReceiverBuilderError::IntoUrl(Arc::new(value.into()))
    }
}

impl From<payjoin::bitcoin::address::ParseError> for ReceiverBuilderError {
    fn from(value: payjoin::bitcoin::address::ParseError) -> Self {
        ReceiverBuilderError::InvalidAddress(Arc::new(value.into()))
    }
}

/// Error parsing a Bitcoin address.
#[derive(Debug, thiserror::Error, uniffi::Object)]
#[uniffi::export(Debug, Display)]
#[error("Invalid Bitcoin address: {msg}")]
pub struct AddressParseError {
    msg: String,
}

impl From<payjoin::bitcoin::address::ParseError> for AddressParseError {
    fn from(value: payjoin::bitcoin::address::ParseError) -> Self {
        AddressParseError { msg: value.to_string() }
    }
}

/// The replyable error type for the payjoin receiver.
#[derive(Debug, thiserror::Error, uniffi::Object)]
#[uniffi::export(Debug, Display)]
#[error(transparent)]
pub struct ProtocolError(#[from] receive::ProtocolError);

/// The standard format for errors that can be replied as JSON.
#[derive(Debug, Clone, PartialEq, Eq, uniffi::Object)]
#[uniffi::export(Debug, Eq)]
pub struct JsonReply(receive::JsonReply);

impl From<JsonReply> for receive::JsonReply {
    fn from(value: JsonReply) -> Self { value.0 }
}

impl From<receive::JsonReply> for JsonReply {
    fn from(value: receive::JsonReply) -> Self { Self(value) }
}

impl From<ProtocolError> for JsonReply {
    fn from(value: ProtocolError) -> Self { Self((&value.0).into()) }
}

/// Error that may occur during a v2 session typestate change.
#[derive(Debug, thiserror::Error, uniffi::Object)]
#[uniffi::export(Debug, Display)]
#[error(transparent)]
pub struct SessionError(#[from] receive::v2::SessionError);

/// Protocol error raised during output substitution.
#[derive(Debug, thiserror::Error, uniffi::Object)]
#[uniffi::export(Debug, Display)]
#[error(transparent)]
pub struct OutputSubstitutionProtocolError(#[from] receive::OutputSubstitutionError);

/// Error that may occur when output substitution fails.
#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum OutputSubstitutionError {
    #[error(transparent)]
    Protocol(Arc<OutputSubstitutionProtocolError>),
    #[error(transparent)]
    FfiValidation(FfiValidationError),
}

impl From<receive::OutputSubstitutionError> for OutputSubstitutionError {
    fn from(value: receive::OutputSubstitutionError) -> Self {
        OutputSubstitutionError::Protocol(Arc::new(value.into()))
    }
}

impl From<FfiValidationError> for OutputSubstitutionError {
    fn from(value: FfiValidationError) -> Self { OutputSubstitutionError::FfiValidation(value) }
}

/// Error that may occur when coin selection fails.
#[derive(Debug, thiserror::Error, uniffi::Object)]
#[uniffi::export(Debug, Display)]
#[error(transparent)]
pub struct SelectionError(#[from] receive::SelectionError);

/// Error that may occur when input contribution fails.
#[derive(Debug, thiserror::Error, uniffi::Object)]
#[uniffi::export(Debug, Display)]
#[error(transparent)]
pub struct InputContributionError(#[from] receive::InputContributionError);

/// Error validating a PSBT Input.
#[derive(Debug, thiserror::Error, uniffi::Object)]
#[uniffi::export(Debug, Display)]
#[error(transparent)]
pub struct PsbtInputError(#[from] receive::PsbtInputError);

/// Error constructing an [`InputPair`](crate::InputPair).
#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum InputPairError {
    /// Provided outpoint could not be parsed.
    #[error("Invalid outpoint (txid={txid}, vout={vout})")]
    InvalidOutPoint { txid: String, vout: u32 },
    /// PSBT input failed validation in the core library.
    #[error("Invalid PSBT input: {0}")]
    InvalidPsbtInput(Arc<PsbtInputError>),
    /// Input failed validation in the FFI layer.
    #[error("Invalid input: {0}")]
    FfiValidation(FfiValidationError),
}

impl InputPairError {
    pub fn invalid_outpoint(txid: String, vout: u32) -> Self {
        InputPairError::InvalidOutPoint { txid, vout }
    }
}

impl From<FfiValidationError> for InputPairError {
    fn from(value: FfiValidationError) -> Self { InputPairError::FfiValidation(value) }
}

/// Error that may occur when a receiver event log is replayed.
#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum ReceiverReplayError {
    /// Replay-time error from the library (invalid event sequence, etc.).
    #[error("Replay error: {0}")]
    Replay(String),
    /// Stored event could not be deserialized as JSON.
    #[error("Stored event JSON deserialization error: {0}")]
    StorageSerde(String),
}

impl ReceiverReplayError {
    pub(crate) fn storage_serde(e: serde_json::Error) -> Self {
        ReceiverReplayError::StorageSerde(e.to_string())
    }
}

impl From<payjoin::error::ReplayError<receive::v2::ReceiveSession, receive::v2::SessionEvent>>
    for ReceiverReplayError
{
    fn from(
        value: payjoin::error::ReplayError<receive::v2::ReceiveSession, receive::v2::SessionEvent>,
    ) -> Self {
        ReceiverReplayError::Replay(value.to_string())
    }
}
