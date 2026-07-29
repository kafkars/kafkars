//! Exhaustive core-to-engine translation for Admin `DescribeTransactions`.

use kafka_client_core::{
    AdminDescribeTransactionResult as CoreResult,
    AdminDescribeTransactionsFailureKind as CoreFailureKind,
    AdminDescribeTransactionsTerminal as CoreTerminal, DeliveryStatus as CoreDeliveryStatus,
};

use super::super::AdminDescribeTransactionTopic;
use super::{
    AdminDescribeTransactionDescription, AdminDescribeTransactionEngineBrokerError,
    AdminDescribeTransactionEngineResult, AdminDescribeTransactionsDeliveryStatus,
    AdminDescribeTransactionsEngineBatch, AdminDescribeTransactionsFailure,
    AdminDescribeTransactionsFailureKind, AdminDescribeTransactionsOutcome,
};

pub(crate) fn translate_terminal(terminal: CoreTerminal) -> AdminDescribeTransactionsOutcome {
    match terminal {
        CoreTerminal::Described(batch) => {
            let (throttle_time_ms, outcomes) = batch.into_parts();
            AdminDescribeTransactionsOutcome::Described(AdminDescribeTransactionsEngineBatch {
                throttle_time_ms,
                results: outcomes
                    .into_iter()
                    .map(|outcome| {
                        let (transactional_id, result) = outcome.into_parts();
                        let result = match result {
                            CoreResult::Described(description) => {
                                let (
                                    state,
                                    timeout_ms,
                                    start_time_ms,
                                    producer_id,
                                    producer_epoch,
                                    topics,
                                ) = description.into_parts();
                                Ok(AdminDescribeTransactionDescription::new(
                                    state,
                                    timeout_ms,
                                    start_time_ms,
                                    producer_id,
                                    producer_epoch,
                                    topics
                                        .into_iter()
                                        .map(|topic| {
                                            let (topic, partitions) = topic.into_parts();
                                            AdminDescribeTransactionTopic::new(topic, partitions)
                                        })
                                        .collect(),
                                ))
                            }
                            CoreResult::BrokerFailed(error) => {
                                Err(AdminDescribeTransactionEngineBrokerError {
                                    code: error.code(),
                                })
                            }
                        };
                        AdminDescribeTransactionEngineResult {
                            transactional_id,
                            result,
                        }
                    })
                    .collect(),
            })
        }
        CoreTerminal::Failed(failure) => {
            AdminDescribeTransactionsOutcome::Failed(AdminDescribeTransactionsFailure {
                kind: failure_kind(failure.kind()),
                delivery: delivery(failure.delivery()),
            })
        }
    }
}

const fn failure_kind(kind: CoreFailureKind) -> AdminDescribeTransactionsFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => AdminDescribeTransactionsFailureKind::DeadlineElapsed,
        CoreFailureKind::DriverRejected => AdminDescribeTransactionsFailureKind::DriverRejected,
        CoreFailureKind::Transport => AdminDescribeTransactionsFailureKind::Transport,
        CoreFailureKind::ResponseTooLarge => AdminDescribeTransactionsFailureKind::ResponseTooLarge,
        CoreFailureKind::Compatibility => AdminDescribeTransactionsFailureKind::Compatibility,
        CoreFailureKind::InvalidResponse => AdminDescribeTransactionsFailureKind::InvalidResponse,
    }
}

const fn delivery(status: CoreDeliveryStatus) -> AdminDescribeTransactionsDeliveryStatus {
    match status {
        CoreDeliveryStatus::NotSent => AdminDescribeTransactionsDeliveryStatus::NotSent,
        CoreDeliveryStatus::PossiblySent => AdminDescribeTransactionsDeliveryStatus::PossiblySent,
    }
}
