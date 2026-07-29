//! Exhaustive normalized-protocol translation into deterministic core input.

use core::num::NonZeroI16;

use kafka_client_core::{
    AlterUserScramCredentialBrokerError, AlterUserScramCredentialOutcome,
    AlterUserScramCredentialsBatch, AlterUserScramCredentialsInput, DeliveryStatus,
};

use crate::{
    driver::{
        AlterUserScramCredentialsDriverFailureKind, AlterUserScramCredentialsRawTerminal,
        AlterUserScramCredentialsTerminalFact,
    },
    protocol::admin::alter_user_scram_credentials::{
        AlterUserScramCredentialsCorrelationRef, AlterUserScramCredentialsResponseFailure,
        NormalizedAlterUserScramCredentialOutcome, normalize_alter_user_scram_credentials_response,
    },
};

pub(super) fn terminal_input(
    raw: &AlterUserScramCredentialsRawTerminal,
    retained_limit: usize,
) -> (AlterUserScramCredentialsInput, usize) {
    match raw.fact() {
        AlterUserScramCredentialsTerminalFact::Response {
            selected_version: Some(selected_version),
            response,
        } => {
            let correlation =
                AlterUserScramCredentialsCorrelationRef::new(raw.plan().affected_users());
            match normalize_alter_user_scram_credentials_response(
                selected_version,
                correlation,
                response,
                retained_limit,
            ) {
                Ok(normalized) => {
                    let (throttle_time_ms, outcomes, retained_bytes) = normalized.into_parts();
                    (
                        AlterUserScramCredentialsInput::BrokerResponded {
                            batch: AlterUserScramCredentialsBatch::new(
                                throttle_time_ms,
                                outcomes.into_iter().map(core_outcome).collect(),
                            ),
                        },
                        retained_bytes,
                    )
                }
                Err(error) => (protocol_failure(error), 0),
            }
        }
        AlterUserScramCredentialsTerminalFact::Response {
            selected_version: None,
            ..
        } => (
            AlterUserScramCredentialsInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            },
            0,
        ),
        AlterUserScramCredentialsTerminalFact::Failed { kind, delivery } => {
            (driver_failure(kind, delivery), 0)
        }
    }
}

fn core_outcome(
    outcome: NormalizedAlterUserScramCredentialOutcome,
) -> AlterUserScramCredentialOutcome {
    let (user, error_code, error_message, error_message_truncated) = outcome.into_parts();
    match NonZeroI16::new(error_code) {
        Some(code) => AlterUserScramCredentialOutcome::failed(
            user,
            AlterUserScramCredentialBrokerError::new(code, error_message, error_message_truncated),
        ),
        None => AlterUserScramCredentialOutcome::altered(user),
    }
}

pub(super) const fn protocol_failure(
    error: AlterUserScramCredentialsResponseFailure,
) -> AlterUserScramCredentialsInput {
    match error {
        AlterUserScramCredentialsResponseFailure::UnsupportedApiVersion { .. } => {
            AlterUserScramCredentialsInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            }
        }
        AlterUserScramCredentialsResponseFailure::RetainedBytes { .. } => {
            AlterUserScramCredentialsInput::ResponseTooLarge
        }
        AlterUserScramCredentialsResponseFailure::NegativeThrottleTime { .. }
        | AlterUserScramCredentialsResponseFailure::TooManyResults { .. }
        | AlterUserScramCredentialsResponseFailure::ResultCount { .. }
        | AlterUserScramCredentialsResponseFailure::EmptyUser
        | AlterUserScramCredentialsResponseFailure::UserTooLong { .. }
        | AlterUserScramCredentialsResponseFailure::EmptyAffectedUsers
        | AlterUserScramCredentialsResponseFailure::TooManyAffectedUsers { .. }
        | AlterUserScramCredentialsResponseFailure::EmptyAffectedUser
        | AlterUserScramCredentialsResponseFailure::AffectedUserTooLong { .. }
        | AlterUserScramCredentialsResponseFailure::DuplicateAffectedUser
        | AlterUserScramCredentialsResponseFailure::DuplicateUser
        | AlterUserScramCredentialsResponseFailure::MissingUser
        | AlterUserScramCredentialsResponseFailure::UnexpectedUser => {
            AlterUserScramCredentialsInput::InvalidResponse
        }
    }
}

const fn driver_failure(
    kind: AlterUserScramCredentialsDriverFailureKind,
    delivery: DeliveryStatus,
) -> AlterUserScramCredentialsInput {
    match kind {
        AlterUserScramCredentialsDriverFailureKind::DeadlineElapsed => {
            AlterUserScramCredentialsInput::DriverDeadlineElapsed { delivery }
        }
        AlterUserScramCredentialsDriverFailureKind::Compatibility => {
            AlterUserScramCredentialsInput::ProtocolIncompatible { delivery }
        }
        AlterUserScramCredentialsDriverFailureKind::InvalidResponse => {
            AlterUserScramCredentialsInput::InvalidResponse
        }
        AlterUserScramCredentialsDriverFailureKind::Transport => {
            AlterUserScramCredentialsInput::TransportFailed { delivery }
        }
    }
}
