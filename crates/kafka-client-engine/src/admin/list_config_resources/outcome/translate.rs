//! Exhaustive core-to-engine configuration-resource terminal translation.

use kafka_client_core::{
    DeliveryStatus as CoreDeliveryStatus, ListConfigResourcesFailureKind as CoreFailureKind,
    ListConfigResourcesTerminal as CoreTerminal, ListedConfigResource as CoreResource,
};

use super::{
    ListConfigResourcesBrokerError, ListConfigResourcesDeliveryStatus, ListConfigResourcesFailure,
    ListConfigResourcesFailureKind, ListConfigResourcesOutcome,
};
use crate::admin::list_config_resources::{ListConfigResource, ListConfigResourcesListing};

pub(crate) fn translate_terminal(terminal: CoreTerminal) -> ListConfigResourcesOutcome {
    match terminal {
        CoreTerminal::Listed(listing) => {
            let (throttle_time_ms, resources) = listing.into_parts();
            ListConfigResourcesOutcome::Listed(ListConfigResourcesListing {
                throttle_time_ms,
                resources: resources.into_iter().map(translate_resource).collect(),
            })
        }
        CoreTerminal::BrokerRejected(error) => {
            let (throttle_time_ms, code) = error.into_parts();
            ListConfigResourcesOutcome::BrokerRejected(ListConfigResourcesBrokerError {
                throttle_time_ms,
                code,
            })
        }
        CoreTerminal::Failed(failure) => {
            ListConfigResourcesOutcome::Failed(ListConfigResourcesFailure {
                kind: failure_kind(failure.kind()),
                delivery: delivery(failure.delivery()),
            })
        }
    }
}

fn translate_resource(resource: CoreResource) -> ListConfigResource {
    let (resource_type, name) = resource.into_parts();
    ListConfigResource {
        resource_type: resource_type.code(),
        name,
    }
}

const fn failure_kind(kind: CoreFailureKind) -> ListConfigResourcesFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => ListConfigResourcesFailureKind::DeadlineElapsed,
        CoreFailureKind::DriverRejected => ListConfigResourcesFailureKind::DriverRejected,
        CoreFailureKind::Transport => ListConfigResourcesFailureKind::Transport,
        CoreFailureKind::ResponseTooLarge => ListConfigResourcesFailureKind::ResponseTooLarge,
        CoreFailureKind::Compatibility => ListConfigResourcesFailureKind::Compatibility,
        CoreFailureKind::InvalidResponse => ListConfigResourcesFailureKind::InvalidResponse,
    }
}

const fn delivery(status: CoreDeliveryStatus) -> ListConfigResourcesDeliveryStatus {
    match status {
        CoreDeliveryStatus::NotSent => ListConfigResourcesDeliveryStatus::NotSent,
        CoreDeliveryStatus::PossiblySent => ListConfigResourcesDeliveryStatus::PossiblySent,
    }
}
