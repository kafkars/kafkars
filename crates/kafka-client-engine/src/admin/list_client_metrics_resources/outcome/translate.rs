//! Exhaustive core-to-engine client-metrics resource terminal translation.

use kafka_client_core::{
    DeliveryStatus as CoreDeliveryStatus, ListClientMetricsResourcesFailureKind as CoreFailureKind,
    ListClientMetricsResourcesTerminal as CoreTerminal,
};

use super::{
    ListClientMetricsResourcesBrokerError, ListClientMetricsResourcesDeliveryStatus,
    ListClientMetricsResourcesFailure, ListClientMetricsResourcesFailureKind,
    ListClientMetricsResourcesListing, ListClientMetricsResourcesOutcome,
};

pub(crate) fn translate_terminal(terminal: CoreTerminal) -> ListClientMetricsResourcesOutcome {
    match terminal {
        CoreTerminal::Listed(listing) => {
            let (throttle_time_ms, resource_names) = listing.into_parts();
            ListClientMetricsResourcesOutcome::Listed(ListClientMetricsResourcesListing {
                throttle_time_ms,
                resource_names,
            })
        }
        CoreTerminal::BrokerRejected(error) => {
            let (throttle_time_ms, code) = error.into_parts();
            ListClientMetricsResourcesOutcome::BrokerRejected(
                ListClientMetricsResourcesBrokerError {
                    throttle_time_ms,
                    code,
                },
            )
        }
        CoreTerminal::Failed(failure) => {
            ListClientMetricsResourcesOutcome::Failed(ListClientMetricsResourcesFailure {
                kind: failure_kind(failure.kind()),
                delivery: delivery(failure.delivery()),
            })
        }
    }
}

const fn failure_kind(kind: CoreFailureKind) -> ListClientMetricsResourcesFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => ListClientMetricsResourcesFailureKind::DeadlineElapsed,
        CoreFailureKind::DriverRejected => ListClientMetricsResourcesFailureKind::DriverRejected,
        CoreFailureKind::Transport => ListClientMetricsResourcesFailureKind::Transport,
        CoreFailureKind::ResponseTooLarge => {
            ListClientMetricsResourcesFailureKind::ResponseTooLarge
        }
        CoreFailureKind::Compatibility => ListClientMetricsResourcesFailureKind::Compatibility,
        CoreFailureKind::InvalidResponse => ListClientMetricsResourcesFailureKind::InvalidResponse,
    }
}

const fn delivery(status: CoreDeliveryStatus) -> ListClientMetricsResourcesDeliveryStatus {
    match status {
        CoreDeliveryStatus::NotSent => ListClientMetricsResourcesDeliveryStatus::NotSent,
        CoreDeliveryStatus::PossiblySent => ListClientMetricsResourcesDeliveryStatus::PossiblySent,
    }
}
