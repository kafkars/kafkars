//! Exhaustive core-to-engine Admin `DescribeClientQuotas` terminal translation.

use kafka_client_core::{
    DeliveryStatus as CoreDeliveryStatus, DescribeClientQuotaEntity as CoreEntity,
    DescribeClientQuotaEntityComponent as CoreEntityComponent,
    DescribeClientQuotaValue as CoreValue, DescribeClientQuotasBrokerError as CoreBrokerError,
    DescribeClientQuotasFailureKind as CoreFailureKind,
    DescribeClientQuotasTerminal as CoreTerminal,
};

use super::{
    DescribeClientQuotaEntity, DescribeClientQuotaEntityComponent, DescribeClientQuotaValue,
    DescribeClientQuotasBatch, DescribeClientQuotasBrokerError, DescribeClientQuotasDeliveryStatus,
    DescribeClientQuotasFailure, DescribeClientQuotasFailureKind, DescribeClientQuotasOutcome,
};

pub(crate) fn translate_terminal(terminal: CoreTerminal) -> DescribeClientQuotasOutcome {
    match terminal {
        CoreTerminal::Described(batch) => {
            let (throttle_time_ms, entities) = batch.into_parts();
            DescribeClientQuotasOutcome::Described(DescribeClientQuotasBatch {
                throttle_time_ms,
                entities: entities.into_iter().map(translate_entity).collect(),
            })
        }
        CoreTerminal::Failed(failure) => {
            DescribeClientQuotasOutcome::Failed(DescribeClientQuotasFailure {
                kind: translate_failure_kind(failure.kind().clone()),
                delivery: translate_delivery(failure.delivery()),
            })
        }
    }
}

fn translate_entity(entity: CoreEntity) -> DescribeClientQuotaEntity {
    let (components, values) = entity.into_parts();
    DescribeClientQuotaEntity {
        components: components
            .into_iter()
            .map(translate_entity_component)
            .collect(),
        values: values.into_iter().map(translate_value).collect(),
    }
}

fn translate_entity_component(
    component: CoreEntityComponent,
) -> DescribeClientQuotaEntityComponent {
    let (entity_type, entity_name) = component.into_parts();
    DescribeClientQuotaEntityComponent {
        entity_type,
        entity_name,
    }
}

fn translate_value(value: CoreValue) -> DescribeClientQuotaValue {
    let (key, value) = value.into_parts();
    DescribeClientQuotaValue { key, value }
}

fn translate_failure_kind(kind: CoreFailureKind) -> DescribeClientQuotasFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => DescribeClientQuotasFailureKind::DeadlineElapsed,
        CoreFailureKind::DriverRejected => DescribeClientQuotasFailureKind::DriverRejected,
        CoreFailureKind::Transport => DescribeClientQuotasFailureKind::Transport,
        CoreFailureKind::Broker(error) => {
            DescribeClientQuotasFailureKind::Broker(translate_broker_error(error))
        }
        CoreFailureKind::ResponseTooLarge => DescribeClientQuotasFailureKind::ResponseTooLarge,
        CoreFailureKind::Compatibility => DescribeClientQuotasFailureKind::Compatibility,
        CoreFailureKind::InvalidResponse => DescribeClientQuotasFailureKind::InvalidResponse,
    }
}

fn translate_broker_error(error: CoreBrokerError) -> DescribeClientQuotasBrokerError {
    let (code, message, message_truncated) = error.into_parts();
    DescribeClientQuotasBrokerError {
        code,
        message,
        message_truncated,
    }
}

const fn translate_delivery(status: CoreDeliveryStatus) -> DescribeClientQuotasDeliveryStatus {
    match status {
        CoreDeliveryStatus::NotSent => DescribeClientQuotasDeliveryStatus::NotSent,
        CoreDeliveryStatus::PossiblySent => DescribeClientQuotasDeliveryStatus::PossiblySent,
    }
}
