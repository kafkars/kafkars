//! Exhaustive core-to-engine Admin `AlterClientQuotas` terminal translation.

use kafka_client_core::{
    AlterClientQuotaBrokerError as CoreBrokerError, AlterClientQuotaEntity as CoreEntity,
    AlterClientQuotaEntityComponent as CoreEntityComponent, AlterClientQuotaOutcome as CoreOutcome,
    AlterClientQuotaResult as CoreResult, AlterClientQuotasFailureKind as CoreFailureKind,
    AlterClientQuotasTerminal as CoreTerminal, DeliveryStatus as CoreDeliveryStatus,
};

use super::{
    AlterClientQuotaBrokerError, AlterClientQuotaOutcome, AlterClientQuotasBatch,
    AlterClientQuotasDeliveryStatus, AlterClientQuotasFailure, AlterClientQuotasFailureKind,
    AlterClientQuotasOutcome,
};
use crate::admin::{AlterClientQuotaEntity, AlterClientQuotaEntityComponent};

pub(crate) fn translate_terminal(terminal: CoreTerminal) -> AlterClientQuotasOutcome {
    match terminal {
        CoreTerminal::Altered(batch) => {
            let (throttle_time_ms, outcomes) = batch.into_parts();
            AlterClientQuotasOutcome::Altered(AlterClientQuotasBatch {
                throttle_time_ms,
                outcomes: outcomes.into_iter().map(translate_outcome).collect(),
            })
        }
        CoreTerminal::Failed(failure) => {
            AlterClientQuotasOutcome::Failed(AlterClientQuotasFailure {
                kind: translate_failure_kind(failure.kind()),
                delivery: translate_delivery(failure.delivery()),
            })
        }
    }
}

fn translate_outcome(outcome: CoreOutcome) -> AlterClientQuotaOutcome {
    let (entity, result) = outcome.into_parts();
    AlterClientQuotaOutcome {
        entity: translate_entity(entity),
        result: match result {
            CoreResult::Altered => Ok(()),
            CoreResult::Failed(error) => Err(translate_broker_error(error)),
        },
    }
}

fn translate_entity(entity: CoreEntity) -> AlterClientQuotaEntity {
    AlterClientQuotaEntity::new(
        entity
            .into_components()
            .into_iter()
            .map(translate_entity_component)
            .collect(),
    )
}

fn translate_entity_component(component: CoreEntityComponent) -> AlterClientQuotaEntityComponent {
    let (entity_type, entity_name) = component.into_parts();
    AlterClientQuotaEntityComponent::new(entity_type, entity_name)
}

const fn translate_failure_kind(kind: CoreFailureKind) -> AlterClientQuotasFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => AlterClientQuotasFailureKind::DeadlineElapsed,
        CoreFailureKind::DriverRejected => AlterClientQuotasFailureKind::DriverRejected,
        CoreFailureKind::Transport => AlterClientQuotasFailureKind::Transport,
        CoreFailureKind::ResponseTooLarge => AlterClientQuotasFailureKind::ResponseTooLarge,
        CoreFailureKind::Compatibility => AlterClientQuotasFailureKind::Compatibility,
        CoreFailureKind::InvalidResponse => AlterClientQuotasFailureKind::InvalidResponse,
    }
}

fn translate_broker_error(error: CoreBrokerError) -> AlterClientQuotaBrokerError {
    let (code, message, message_truncated) = error.into_parts();
    AlterClientQuotaBrokerError {
        code,
        message,
        message_truncated,
    }
}

const fn translate_delivery(status: CoreDeliveryStatus) -> AlterClientQuotasDeliveryStatus {
    match status {
        CoreDeliveryStatus::NotSent => AlterClientQuotasDeliveryStatus::NotSent,
        CoreDeliveryStatus::PossiblySent => AlterClientQuotasDeliveryStatus::PossiblySent,
    }
}
