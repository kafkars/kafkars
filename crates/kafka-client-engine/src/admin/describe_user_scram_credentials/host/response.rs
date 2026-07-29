//! Exhaustive normalized-protocol translation into deterministic core input.

use core::num::NonZeroI16;

use kafka_client_core::{
    DeliveryStatus, DescribeUserScramCredentialsBatch, DescribeUserScramCredentialsBrokerError,
    DescribeUserScramCredentialsInput, DescribeUserScramCredentialsUserOutcome,
    ScramCredentialInfo,
};

use crate::{
    driver::{
        DescribeUserScramCredentialsDriverFailureKind, DescribeUserScramCredentialsRawTerminal,
        DescribeUserScramCredentialsTerminalFact,
    },
    protocol::admin::describe_user_scram_credentials::{
        DescribeUserScramCredentialsRequestRef, DescribeUserScramCredentialsResponseFailure,
        NormalizedScramCredentialInfo, NormalizedUserScramCredentials,
        normalize_describe_user_scram_credentials_response,
    },
};

pub(super) fn terminal_input(
    raw: &DescribeUserScramCredentialsRawTerminal,
) -> (DescribeUserScramCredentialsInput, usize) {
    match raw.fact() {
        DescribeUserScramCredentialsTerminalFact::Response {
            selected_version: Some(selected_version),
            response,
        } => {
            let request = request_ref(raw.plan());
            match normalize_describe_user_scram_credentials_response(
                selected_version,
                request,
                response,
                raw.result_limit(),
            ) {
                Ok(normalized) => {
                    let (
                        throttle_time_ms,
                        error_code,
                        error_message,
                        error_message_truncated,
                        results,
                        retained_bytes,
                    ) = normalized.into_parts();
                    (
                        normalized_input(
                            throttle_time_ms,
                            error_code,
                            error_message,
                            error_message_truncated,
                            results,
                        ),
                        retained_bytes,
                    )
                }
                Err(error) => (protocol_failure(error), 0),
            }
        }
        DescribeUserScramCredentialsTerminalFact::Response {
            selected_version: None,
            ..
        } => (
            DescribeUserScramCredentialsInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            },
            0,
        ),
        DescribeUserScramCredentialsTerminalFact::Failed { kind, delivery } => {
            (driver_failure(kind, delivery), 0)
        }
    }
}

fn request_ref(
    plan: &kafka_client_core::DescribeUserScramCredentialsPlan,
) -> DescribeUserScramCredentialsRequestRef<'_> {
    match plan.users() {
        Some(users) => DescribeUserScramCredentialsRequestRef::selected(users),
        None => DescribeUserScramCredentialsRequestRef::all(),
    }
}

fn normalized_input(
    throttle_time_ms: u32,
    error_code: i16,
    error_message: Option<String>,
    error_message_truncated: bool,
    results: Vec<NormalizedUserScramCredentials>,
) -> DescribeUserScramCredentialsInput {
    match NonZeroI16::new(error_code) {
        Some(code) => DescribeUserScramCredentialsInput::BrokerRejected {
            error: DescribeUserScramCredentialsBrokerError::new(
                code,
                error_message,
                error_message_truncated,
            ),
        },
        None => DescribeUserScramCredentialsInput::BrokerResponded {
            batch: DescribeUserScramCredentialsBatch::new(
                throttle_time_ms,
                results.into_iter().map(core_user).collect(),
            ),
        },
    }
}

fn core_user(result: NormalizedUserScramCredentials) -> DescribeUserScramCredentialsUserOutcome {
    let (user, error_code, error_message, error_message_truncated, infos) = result.into_parts();
    match NonZeroI16::new(error_code) {
        Some(code) => DescribeUserScramCredentialsUserOutcome::broker_failed(
            user,
            DescribeUserScramCredentialsBrokerError::new(
                code,
                error_message,
                error_message_truncated,
            ),
        ),
        None => DescribeUserScramCredentialsUserOutcome::described(
            user,
            infos.into_iter().map(core_info).collect(),
        ),
    }
}

fn core_info(info: NormalizedScramCredentialInfo) -> ScramCredentialInfo {
    let (mechanism, iterations) = info.into_parts();
    ScramCredentialInfo::new(mechanism, iterations)
}

pub(super) const fn protocol_failure(
    error: DescribeUserScramCredentialsResponseFailure,
) -> DescribeUserScramCredentialsInput {
    match error {
        DescribeUserScramCredentialsResponseFailure::UnsupportedApiVersion { .. } => {
            DescribeUserScramCredentialsInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            }
        }
        DescribeUserScramCredentialsResponseFailure::RetainedBytes { .. } => {
            DescribeUserScramCredentialsInput::ResponseTooLarge
        }
        DescribeUserScramCredentialsResponseFailure::NegativeThrottleTime { .. }
        | DescribeUserScramCredentialsResponseFailure::ResultsWithTopLevelError { .. }
        | DescribeUserScramCredentialsResponseFailure::TooManyResults { .. }
        | DescribeUserScramCredentialsResponseFailure::TooManyCredentialInfos { .. }
        | DescribeUserScramCredentialsResponseFailure::EmptyUser
        | DescribeUserScramCredentialsResponseFailure::UserTooLong { .. }
        | DescribeUserScramCredentialsResponseFailure::TooManyCredentialsForUser { .. }
        | DescribeUserScramCredentialsResponseFailure::EmptyCredentialsOnSuccess
        | DescribeUserScramCredentialsResponseFailure::CredentialsWithUserError { .. }
        | DescribeUserScramCredentialsResponseFailure::InvalidMechanism { .. }
        | DescribeUserScramCredentialsResponseFailure::NonPositiveIterations { .. }
        | DescribeUserScramCredentialsResponseFailure::DuplicateMechanism { .. }
        | DescribeUserScramCredentialsResponseFailure::EmptyUserFilter
        | DescribeUserScramCredentialsResponseFailure::TooManyRequestedUsers { .. }
        | DescribeUserScramCredentialsResponseFailure::EmptyRequestedUser
        | DescribeUserScramCredentialsResponseFailure::RequestedUserTooLong { .. }
        | DescribeUserScramCredentialsResponseFailure::DuplicateRequestedUser
        | DescribeUserScramCredentialsResponseFailure::DuplicateUser
        | DescribeUserScramCredentialsResponseFailure::MissingUser
        | DescribeUserScramCredentialsResponseFailure::UnexpectedUser => {
            DescribeUserScramCredentialsInput::InvalidResponse
        }
    }
}

const fn driver_failure(
    kind: DescribeUserScramCredentialsDriverFailureKind,
    delivery: DeliveryStatus,
) -> DescribeUserScramCredentialsInput {
    match kind {
        DescribeUserScramCredentialsDriverFailureKind::DeadlineElapsed => {
            DescribeUserScramCredentialsInput::DriverDeadlineElapsed { delivery }
        }
        DescribeUserScramCredentialsDriverFailureKind::Compatibility => {
            DescribeUserScramCredentialsInput::ProtocolIncompatible { delivery }
        }
        DescribeUserScramCredentialsDriverFailureKind::InvalidResponse => {
            DescribeUserScramCredentialsInput::InvalidResponse
        }
        DescribeUserScramCredentialsDriverFailureKind::Transport => {
            DescribeUserScramCredentialsInput::TransportFailed { delivery }
        }
    }
}
