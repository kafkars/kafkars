//! Exhaustive generated-free response and driver-failure translation.

use core::num::NonZeroI16;

use kafka_client_core::{
    DeliveryStatus, ListClientMetricsResourcesBrokerError, ListClientMetricsResourcesInput,
};

use crate::{
    driver::{
        ListClientMetricsResourcesDriverFailureKind, ListClientMetricsResourcesRawTerminal,
        ListClientMetricsResourcesTerminalFact,
    },
    protocol::admin::list_client_metrics_resources::{
        ListClientMetricsResourcesProtocolFailure, normalize_list_client_metrics_resources_response,
    },
};

pub(super) fn terminal_input(
    raw: &ListClientMetricsResourcesRawTerminal,
    retained_limit: usize,
) -> (ListClientMetricsResourcesInput, usize) {
    match raw.fact() {
        ListClientMetricsResourcesTerminalFact::Response {
            selected_version,
            response,
        } => match normalize_list_client_metrics_resources_response(
            selected_version,
            response,
            retained_limit,
        ) {
            Ok(normalized) => {
                let (throttle_time_ms, error_code, resource_names, retained_bytes) =
                    normalized.into_parts();
                (
                    normalized_input(throttle_time_ms, error_code, resource_names),
                    retained_bytes,
                )
            }
            Err(error) => (protocol_failure(error), 0),
        },
        ListClientMetricsResourcesTerminalFact::Failed { kind, delivery } => {
            (driver_failure(kind, delivery), 0)
        }
    }
}

pub(super) fn normalized_input(
    throttle_time_ms: u32,
    error_code: i16,
    resource_names: Vec<String>,
) -> ListClientMetricsResourcesInput {
    match NonZeroI16::new(error_code) {
        Some(code) if resource_names.is_empty() => {
            ListClientMetricsResourcesInput::BrokerRejected {
                error: ListClientMetricsResourcesBrokerError::new(throttle_time_ms, code),
            }
        }
        Some(_) => ListClientMetricsResourcesInput::InvalidResponse,
        None => ListClientMetricsResourcesInput::BrokerResponded {
            throttle_time_ms,
            resource_names,
        },
    }
}

pub(super) const fn protocol_failure(
    error: ListClientMetricsResourcesProtocolFailure,
) -> ListClientMetricsResourcesInput {
    match error {
        ListClientMetricsResourcesProtocolFailure::MissingSelectedVersion
        | ListClientMetricsResourcesProtocolFailure::UnsupportedApiVersion { .. } => {
            ListClientMetricsResourcesInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            }
        }
        ListClientMetricsResourcesProtocolFailure::RetainedBytes { .. }
        | ListClientMetricsResourcesProtocolFailure::Allocation { .. } => {
            ListClientMetricsResourcesInput::ResponseTooLarge
        }
        ListClientMetricsResourcesProtocolFailure::NegativeThrottleTime { .. }
        | ListClientMetricsResourcesProtocolFailure::SuccessPayloadWithBrokerError
        | ListClientMetricsResourcesProtocolFailure::TooManyResources { .. }
        | ListClientMetricsResourcesProtocolFailure::UnexpectedResourceType { .. }
        | ListClientMetricsResourcesProtocolFailure::EmptyResourceName
        | ListClientMetricsResourcesProtocolFailure::ResourceNameTooLong { .. }
        | ListClientMetricsResourcesProtocolFailure::ResponseTextBytesExceeded { .. }
        | ListClientMetricsResourcesProtocolFailure::DuplicateResourceName => {
            ListClientMetricsResourcesInput::InvalidResponse
        }
    }
}

const fn driver_failure(
    kind: ListClientMetricsResourcesDriverFailureKind,
    delivery: DeliveryStatus,
) -> ListClientMetricsResourcesInput {
    match kind {
        ListClientMetricsResourcesDriverFailureKind::DeadlineElapsed => {
            ListClientMetricsResourcesInput::DriverDeadlineElapsed { delivery }
        }
        ListClientMetricsResourcesDriverFailureKind::Compatibility => {
            ListClientMetricsResourcesInput::ProtocolIncompatible { delivery }
        }
        ListClientMetricsResourcesDriverFailureKind::InvalidResponse => {
            ListClientMetricsResourcesInput::InvalidResponse
        }
        ListClientMetricsResourcesDriverFailureKind::Transport => {
            ListClientMetricsResourcesInput::TransportFailed { delivery }
        }
    }
}
