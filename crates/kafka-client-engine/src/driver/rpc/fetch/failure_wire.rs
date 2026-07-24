//! Nested generated wire failure classification for Fetch driver terminals.

use kafka_client_core::FetchFailure;
use kafka_wire_core::{DecodeError, EncodeError};

pub(super) fn classify_wire_encode_error(failure: &EncodeError) -> FetchFailure {
    #[allow(
        clippy::match_same_arms,
        reason = "named current variants remain audited separately from the non-exhaustive fallback"
    )]
    match failure {
        EncodeError::UnsupportedVersion { .. }
        | EncodeError::FieldNotRepresentable { .. }
        | EncodeError::NullNotAllowed { .. }
        | EncodeError::TaggedFieldsNotRepresentable { .. } => FetchFailure::Compatibility,
        EncodeError::LengthOverflow { .. }
        | EncodeError::KnownTagConflict { .. }
        | EncodeError::UnclaimedKnownTag { .. }
        | EncodeError::KnownTagCapacityExceeded { .. }
        | EncodeError::TaggedFieldsInvalid(_)
        | EncodeError::SizeMismatch { .. }
        | EncodeError::FrameTooLarge { .. }
        | EncodeError::FrameLimitExceeded { .. } => FetchFailure::DriverRejected,
        _ => FetchFailure::DriverRejected,
    }
}

pub(super) fn classify_wire_decode_error(failure: &DecodeError) -> FetchFailure {
    #[allow(
        clippy::match_same_arms,
        reason = "named current variants remain audited separately from the non-exhaustive fallback"
    )]
    match failure {
        DecodeError::UnsupportedVersion { .. } => FetchFailure::Compatibility,
        DecodeError::LimitExceeded { .. } | DecodeError::LengthOverflow { .. } => {
            FetchFailure::ResponseTooLarge
        }
        DecodeError::UnexpectedEnd { .. }
        | DecodeError::InvalidBoolean { .. }
        | DecodeError::NegativeLength { .. }
        | DecodeError::NullNotAllowed { .. }
        | DecodeError::CountExceedsFrame { .. }
        | DecodeError::InvalidUtf8 { .. }
        | DecodeError::MalformedVarint { .. }
        | DecodeError::TaggedFieldOrder { .. }
        | DecodeError::TaggedFieldSize { .. }
        | DecodeError::TrailingBytes { .. } => FetchFailure::InvalidResponse,
        _ => FetchFailure::InvalidResponse,
    }
}
