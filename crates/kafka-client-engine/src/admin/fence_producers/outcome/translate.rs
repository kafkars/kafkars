//! Exhaustive core-to-engine translation for Admin `FenceProducers`.

use kafka_client_core::{
    AdminFenceProducerResult as CoreResult, AdminFenceProducersFailureKind as CoreFailureKind,
    AdminFenceProducersTerminal as CoreTerminal, DeliveryStatus as CoreDeliveryStatus,
};

use super::{
    AdminFenceProducerEngineBrokerError, AdminFenceProducerEngineResult,
    AdminFenceProducersDeliveryStatus, AdminFenceProducersEngineBatch, AdminFenceProducersFailure,
    AdminFenceProducersFailureKind, AdminFenceProducersOutcome, AdminFencedProducerEngineIdentity,
};

pub(crate) fn translate_terminal(terminal: CoreTerminal) -> AdminFenceProducersOutcome {
    match terminal {
        CoreTerminal::Fenced(batch) => {
            let (throttle_time_ms, outcomes) = batch.into_parts();
            AdminFenceProducersOutcome::Fenced(AdminFenceProducersEngineBatch {
                throttle_time_ms,
                results: outcomes
                    .into_iter()
                    .map(|outcome| {
                        let (transactional_id, result) = outcome.into_parts();
                        let result = match result {
                            CoreResult::Fenced(identity) => {
                                let (producer_id, producer_epoch) = identity.into_parts();
                                Ok(AdminFencedProducerEngineIdentity {
                                    producer_id,
                                    producer_epoch,
                                })
                            }
                            CoreResult::BrokerFailed(error) => {
                                Err(AdminFenceProducerEngineBrokerError { code: error.code() })
                            }
                        };
                        AdminFenceProducerEngineResult {
                            transactional_id,
                            result,
                        }
                    })
                    .collect(),
            })
        }
        CoreTerminal::Failed(failure) => {
            AdminFenceProducersOutcome::Failed(AdminFenceProducersFailure {
                kind: failure_kind(failure.kind()),
                delivery: delivery(failure.delivery()),
            })
        }
    }
}

const fn failure_kind(kind: CoreFailureKind) -> AdminFenceProducersFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => AdminFenceProducersFailureKind::DeadlineElapsed,
        CoreFailureKind::DriverRejected => AdminFenceProducersFailureKind::DriverRejected,
        CoreFailureKind::Transport => AdminFenceProducersFailureKind::Transport,
        CoreFailureKind::ResponseTooLarge => AdminFenceProducersFailureKind::ResponseTooLarge,
        CoreFailureKind::Compatibility => AdminFenceProducersFailureKind::Compatibility,
        CoreFailureKind::InvalidResponse => AdminFenceProducersFailureKind::InvalidResponse,
    }
}

const fn delivery(status: CoreDeliveryStatus) -> AdminFenceProducersDeliveryStatus {
    match status {
        CoreDeliveryStatus::NotSent => AdminFenceProducersDeliveryStatus::NotSent,
        CoreDeliveryStatus::PossiblySent => AdminFenceProducersDeliveryStatus::PossiblySent,
    }
}
