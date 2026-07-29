//! Exhaustive deterministic-core terminal translation.

use kafka_client_core::{
    AlterUserScramCredentialResult as CoreResult,
    AlterUserScramCredentialsFailureKind as CoreFailureKind,
    AlterUserScramCredentialsTerminal as CoreTerminal, DeliveryStatus,
};

use super::{
    AlterUserScramCredentialBrokerError, AlterUserScramCredentialOutcome,
    AlterUserScramCredentialsBatch, AlterUserScramCredentialsDeliveryStatus,
    AlterUserScramCredentialsFailure, AlterUserScramCredentialsFailureKind,
    AlterUserScramCredentialsOutcome,
};

pub(crate) fn translate_terminal(terminal: CoreTerminal) -> AlterUserScramCredentialsOutcome {
    match terminal {
        CoreTerminal::Altered(batch) => {
            let (throttle_time_ms, outcomes) = batch.into_parts();
            AlterUserScramCredentialsOutcome::Altered(AlterUserScramCredentialsBatch {
                throttle_time_ms,
                outcomes: outcomes
                    .into_iter()
                    .map(|outcome| {
                        let (user, result) = outcome.into_parts();
                        AlterUserScramCredentialOutcome {
                            user,
                            result: match result {
                                CoreResult::Altered => Ok(()),
                                CoreResult::Failed(error) => {
                                    let (code, message, message_truncated) = error.into_parts();
                                    Err(AlterUserScramCredentialBrokerError {
                                        code,
                                        message,
                                        message_truncated,
                                    })
                                }
                            },
                        }
                    })
                    .collect(),
            })
        }
        CoreTerminal::Failed(failure) => {
            AlterUserScramCredentialsOutcome::Failed(AlterUserScramCredentialsFailure {
                kind: failure_kind(failure.kind()),
                delivery: delivery(failure.delivery()),
            })
        }
    }
}

const fn failure_kind(kind: CoreFailureKind) -> AlterUserScramCredentialsFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => AlterUserScramCredentialsFailureKind::DeadlineElapsed,
        CoreFailureKind::DriverRejected => AlterUserScramCredentialsFailureKind::DriverRejected,
        CoreFailureKind::Transport => AlterUserScramCredentialsFailureKind::Transport,
        CoreFailureKind::ResponseTooLarge => AlterUserScramCredentialsFailureKind::ResponseTooLarge,
        CoreFailureKind::Compatibility => AlterUserScramCredentialsFailureKind::Compatibility,
        CoreFailureKind::InvalidResponse => AlterUserScramCredentialsFailureKind::InvalidResponse,
    }
}

const fn delivery(status: DeliveryStatus) -> AlterUserScramCredentialsDeliveryStatus {
    match status {
        DeliveryStatus::NotSent => AlterUserScramCredentialsDeliveryStatus::NotSent,
        DeliveryStatus::PossiblySent => AlterUserScramCredentialsDeliveryStatus::PossiblySent,
    }
}
