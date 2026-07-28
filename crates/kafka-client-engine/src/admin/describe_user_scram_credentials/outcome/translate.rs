//! Exhaustive core-to-engine SCRAM credential-description terminal translation.

use kafka_client_core::{
    DeliveryStatus as CoreDeliveryStatus,
    DescribeUserScramCredentialsBrokerError as CoreBrokerError,
    DescribeUserScramCredentialsFailureKind as CoreFailureKind,
    DescribeUserScramCredentialsTerminal as CoreTerminal,
    DescribeUserScramCredentialsUserOutcome as CoreUserOutcome,
    DescribeUserScramCredentialsUserResult as CoreUserResult, ScramCredentialInfo as CoreInfo,
};

use super::{
    DescribeUserScramCredentialInfo, DescribeUserScramCredentialOutcome,
    DescribeUserScramCredentialsBatch, DescribeUserScramCredentialsBrokerError,
    DescribeUserScramCredentialsDeliveryStatus, DescribeUserScramCredentialsFailure,
    DescribeUserScramCredentialsFailureKind, DescribeUserScramCredentialsOutcome,
    DescribeUserScramCredentialsUserResult,
};

pub(crate) fn translate_terminal(terminal: CoreTerminal) -> DescribeUserScramCredentialsOutcome {
    match terminal {
        CoreTerminal::Described(batch) => {
            let (throttle_time_ms, outcomes) = batch.into_parts();
            DescribeUserScramCredentialsOutcome::Described(DescribeUserScramCredentialsBatch {
                throttle_time_ms,
                outcomes: outcomes.into_iter().map(translate_user).collect(),
            })
        }
        CoreTerminal::Failed(failure) => {
            DescribeUserScramCredentialsOutcome::Failed(DescribeUserScramCredentialsFailure {
                kind: translate_failure_kind(failure.kind().clone()),
                delivery: translate_delivery(failure.delivery()),
            })
        }
    }
}

fn translate_user(outcome: CoreUserOutcome) -> DescribeUserScramCredentialOutcome {
    let (user, result) = outcome.into_parts();
    let result = match result {
        CoreUserResult::Described(infos) => DescribeUserScramCredentialsUserResult::Described(
            infos.into_iter().map(translate_info).collect(),
        ),
        CoreUserResult::BrokerFailed(error) => {
            DescribeUserScramCredentialsUserResult::BrokerFailed(translate_broker_error(error))
        }
    };
    DescribeUserScramCredentialOutcome { user, result }
}

fn translate_info(info: CoreInfo) -> DescribeUserScramCredentialInfo {
    let (mechanism, iterations) = info.into_parts();
    DescribeUserScramCredentialInfo {
        mechanism,
        iterations,
    }
}

fn translate_failure_kind(kind: CoreFailureKind) -> DescribeUserScramCredentialsFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => {
            DescribeUserScramCredentialsFailureKind::DeadlineElapsed
        }
        CoreFailureKind::DriverRejected => DescribeUserScramCredentialsFailureKind::DriverRejected,
        CoreFailureKind::Transport => DescribeUserScramCredentialsFailureKind::Transport,
        CoreFailureKind::Broker(error) => {
            DescribeUserScramCredentialsFailureKind::Broker(translate_broker_error(error))
        }
        CoreFailureKind::ResponseTooLarge => {
            DescribeUserScramCredentialsFailureKind::ResponseTooLarge
        }
        CoreFailureKind::Compatibility => DescribeUserScramCredentialsFailureKind::Compatibility,
        CoreFailureKind::InvalidResponse => {
            DescribeUserScramCredentialsFailureKind::InvalidResponse
        }
    }
}

fn translate_broker_error(error: CoreBrokerError) -> DescribeUserScramCredentialsBrokerError {
    let (code, message, message_truncated) = error.into_parts();
    DescribeUserScramCredentialsBrokerError {
        code,
        message,
        message_truncated,
    }
}

const fn translate_delivery(
    status: CoreDeliveryStatus,
) -> DescribeUserScramCredentialsDeliveryStatus {
    match status {
        CoreDeliveryStatus::NotSent => DescribeUserScramCredentialsDeliveryStatus::NotSent,
        CoreDeliveryStatus::PossiblySent => {
            DescribeUserScramCredentialsDeliveryStatus::PossiblySent
        }
    }
}
