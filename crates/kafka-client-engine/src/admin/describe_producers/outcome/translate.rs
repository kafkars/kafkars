//! Exhaustive core-to-engine translation for Admin `DescribeProducers`.

use kafka_client_core::{
    AdminDescribeProducerResult as CoreResult,
    AdminDescribeProducersFailureKind as CoreFailureKind,
    AdminDescribeProducersTerminal as CoreTerminal, DeliveryStatus as CoreDeliveryStatus,
};

use super::{
    AdminDescribeProducerEngineBrokerError, AdminDescribeProducerEngineResult,
    AdminDescribeProducerState, AdminDescribeProducersDeliveryStatus,
    AdminDescribeProducersEngineBatch, AdminDescribeProducersFailure,
    AdminDescribeProducersFailureKind, AdminDescribeProducersOutcome,
};

pub(crate) fn translate_terminal(terminal: CoreTerminal) -> AdminDescribeProducersOutcome {
    match terminal {
        CoreTerminal::Described(batch) => {
            let (throttle_time_ms, outcomes) = batch.into_parts();
            AdminDescribeProducersOutcome::Described(AdminDescribeProducersEngineBatch {
                throttle_time_ms,
                results: outcomes
                    .into_iter()
                    .map(|outcome| {
                        let (topic, partition, result) = outcome.into_parts();
                        let result = match result {
                            CoreResult::Described(producers) => Ok(producers
                                .into_iter()
                                .map(|producer| {
                                    let (
                                        producer_id,
                                        producer_epoch,
                                        last_sequence,
                                        last_timestamp,
                                        coordinator_epoch,
                                        transaction_offset,
                                    ) = producer.into_parts();
                                    AdminDescribeProducerState::new(
                                        producer_id,
                                        producer_epoch,
                                        last_sequence,
                                        last_timestamp,
                                        coordinator_epoch,
                                        transaction_offset,
                                    )
                                })
                                .collect()),
                            CoreResult::BrokerFailed(error) => {
                                let (code, message, message_truncated) = error.into_parts();
                                Err(AdminDescribeProducerEngineBrokerError {
                                    code,
                                    message,
                                    message_truncated,
                                })
                            }
                        };
                        AdminDescribeProducerEngineResult {
                            topic,
                            partition,
                            result,
                        }
                    })
                    .collect(),
            })
        }
        CoreTerminal::Failed(failure) => {
            AdminDescribeProducersOutcome::Failed(AdminDescribeProducersFailure {
                kind: failure_kind(failure.kind()),
                delivery: delivery(failure.delivery()),
            })
        }
    }
}

const fn failure_kind(kind: CoreFailureKind) -> AdminDescribeProducersFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => AdminDescribeProducersFailureKind::DeadlineElapsed,
        CoreFailureKind::DriverRejected => AdminDescribeProducersFailureKind::DriverRejected,
        CoreFailureKind::Transport => AdminDescribeProducersFailureKind::Transport,
        CoreFailureKind::ResponseTooLarge => AdminDescribeProducersFailureKind::ResponseTooLarge,
        CoreFailureKind::Compatibility => AdminDescribeProducersFailureKind::Compatibility,
        CoreFailureKind::InvalidResponse => AdminDescribeProducersFailureKind::InvalidResponse,
    }
}

const fn delivery(status: CoreDeliveryStatus) -> AdminDescribeProducersDeliveryStatus {
    match status {
        CoreDeliveryStatus::NotSent => AdminDescribeProducersDeliveryStatus::NotSent,
        CoreDeliveryStatus::PossiblySent => AdminDescribeProducersDeliveryStatus::PossiblySent,
    }
}
