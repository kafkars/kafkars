//! Exhaustive generated-free response and driver-failure translation.

use core::num::NonZeroI16;

use kafka_client_core::{
    ConfigResourceType as CoreResourceType, DeliveryStatus,
    ListConfigResourcesBrokerError as CoreBrokerError, ListConfigResourcesInput,
    ListedConfigResource as CoreResource,
};

use crate::{
    driver::{
        ListConfigResourcesDriverFailureKind, ListConfigResourcesRawTerminal,
        ListConfigResourcesTerminalFact,
    },
    protocol::admin::list_config_resources::{
        ListConfigResource as ProtocolResource, ListConfigResourcesProtocolFailure,
        normalize_list_config_resources_response,
    },
};

pub(super) fn terminal_input(
    raw: &ListConfigResourcesRawTerminal,
    retained_limit: usize,
) -> (ListConfigResourcesInput, usize) {
    match raw.fact() {
        ListConfigResourcesTerminalFact::Response {
            selected_version,
            response,
        } => match normalize_list_config_resources_response(
            selected_version,
            response,
            retained_limit,
        ) {
            Ok(normalized) => {
                let (throttle_time_ms, error_code, resources, retained_bytes) =
                    normalized.into_parts();
                (
                    normalized_protocol_input(throttle_time_ms, error_code, resources),
                    retained_bytes,
                )
            }
            Err(error) => (protocol_failure(error), 0),
        },
        ListConfigResourcesTerminalFact::Failed { kind, delivery } => {
            (driver_failure(kind, delivery), 0)
        }
    }
}

fn normalized_protocol_input(
    throttle_time_ms: u32,
    error_code: i16,
    resources: Vec<ProtocolResource>,
) -> ListConfigResourcesInput {
    normalized_iter_input(
        throttle_time_ms,
        error_code,
        resources.into_iter().map(ProtocolResource::into_parts),
    )
}

pub(super) fn normalized_iter_input<I>(
    throttle_time_ms: u32,
    error_code: i16,
    resources: I,
) -> ListConfigResourcesInput
where
    I: IntoIterator<Item = (i8, String)>,
{
    let mut resources = resources.into_iter();
    if let Some(code) = NonZeroI16::new(error_code) {
        return if resources.next().is_none() {
            ListConfigResourcesInput::BrokerRejected {
                error: CoreBrokerError::new(throttle_time_ms, code),
            }
        } else {
            ListConfigResourcesInput::InvalidResponse
        };
    }

    let (minimum, _) = resources.size_hint();
    let mut core_resources = Vec::new();
    if core_resources.try_reserve_exact(minimum).is_err() {
        return ListConfigResourcesInput::ResponseTooLarge;
    }
    for (resource_type, name) in resources {
        let Ok(resource_type) = CoreResourceType::new(resource_type) else {
            return ListConfigResourcesInput::InvalidResponse;
        };
        core_resources.push(CoreResource::new(resource_type, name));
    }
    ListConfigResourcesInput::BrokerResponded {
        throttle_time_ms,
        resources: core_resources,
    }
}

const fn protocol_failure(error: ListConfigResourcesProtocolFailure) -> ListConfigResourcesInput {
    match error {
        ListConfigResourcesProtocolFailure::MissingSelectedVersion
        | ListConfigResourcesProtocolFailure::UnsupportedApiVersion { .. } => {
            ListConfigResourcesInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            }
        }
        ListConfigResourcesProtocolFailure::NormalizedBytesExceeded { .. }
        | ListConfigResourcesProtocolFailure::RetainedBytes { .. }
        | ListConfigResourcesProtocolFailure::Allocation { .. } => {
            ListConfigResourcesInput::ResponseTooLarge
        }
        ListConfigResourcesProtocolFailure::NegativeThrottleTime { .. }
        | ListConfigResourcesProtocolFailure::TooManyResources { .. }
        | ListConfigResourcesProtocolFailure::NonPositiveResourceType { .. }
        | ListConfigResourcesProtocolFailure::EmptyResourceName
        | ListConfigResourcesProtocolFailure::ResourceNameTooLong { .. }
        | ListConfigResourcesProtocolFailure::ResponseTextBytesExceeded { .. }
        | ListConfigResourcesProtocolFailure::DuplicateResource { .. } => {
            ListConfigResourcesInput::InvalidResponse
        }
    }
}

const fn driver_failure(
    kind: ListConfigResourcesDriverFailureKind,
    delivery: DeliveryStatus,
) -> ListConfigResourcesInput {
    match kind {
        ListConfigResourcesDriverFailureKind::DeadlineElapsed => {
            ListConfigResourcesInput::DriverDeadlineElapsed { delivery }
        }
        ListConfigResourcesDriverFailureKind::Compatibility => {
            ListConfigResourcesInput::ProtocolIncompatible { delivery }
        }
        ListConfigResourcesDriverFailureKind::InvalidResponse => {
            ListConfigResourcesInput::InvalidResponse
        }
        ListConfigResourcesDriverFailureKind::Transport => {
            ListConfigResourcesInput::TransportFailed { delivery }
        }
    }
}
