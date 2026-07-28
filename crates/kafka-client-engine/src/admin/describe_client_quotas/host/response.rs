//! Exhaustive normalized-protocol translation into deterministic core input.

use core::num::NonZeroI16;

use kafka_client_core::{
    DeliveryStatus, DescribeClientQuotaEntity, DescribeClientQuotaEntityComponent,
    DescribeClientQuotaValue, DescribeClientQuotasBatch, DescribeClientQuotasBrokerError,
    DescribeClientQuotasInput,
};

use crate::{
    driver::{
        DescribeClientQuotasDriverFailureKind, DescribeClientQuotasRawTerminal,
        DescribeClientQuotasTerminalFact,
    },
    protocol::admin::describe_client_quotas::{
        DescribeClientQuotasResponseFailure, NormalizedClientQuotaEntry,
        normalize_describe_client_quotas_response,
    },
};

pub(super) fn terminal_input(
    raw: &DescribeClientQuotasRawTerminal,
    retained_limit: usize,
) -> (DescribeClientQuotasInput, usize) {
    match raw.fact() {
        DescribeClientQuotasTerminalFact::Response {
            selected_version: Some(selected_version),
            response,
        } => {
            match normalize_describe_client_quotas_response(
                selected_version,
                response,
                retained_limit,
            ) {
                Ok(normalized) => {
                    let (
                        throttle_time_ms,
                        error_code,
                        error_message,
                        error_message_truncated,
                        entries,
                        retained_bytes,
                    ) = normalized.into_parts();
                    (
                        normalized_input(
                            throttle_time_ms,
                            error_code,
                            error_message,
                            error_message_truncated,
                            entries,
                        ),
                        retained_bytes,
                    )
                }
                Err(error) => (protocol_failure(error), 0),
            }
        }
        DescribeClientQuotasTerminalFact::Response {
            selected_version: None,
            ..
        } => (
            DescribeClientQuotasInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            },
            0,
        ),
        DescribeClientQuotasTerminalFact::Failed { kind, delivery } => {
            (driver_failure(kind, delivery), 0)
        }
    }
}

fn normalized_input(
    throttle_time_ms: u32,
    error_code: i16,
    error_message: Option<String>,
    error_message_truncated: bool,
    entries: Vec<NormalizedClientQuotaEntry>,
) -> DescribeClientQuotasInput {
    match NonZeroI16::new(error_code) {
        Some(code) => DescribeClientQuotasInput::BrokerRejected {
            error: DescribeClientQuotasBrokerError::new(
                code,
                error_message,
                error_message_truncated,
            ),
        },
        None => DescribeClientQuotasInput::BrokerResponded {
            batch: DescribeClientQuotasBatch::new(
                throttle_time_ms,
                entries.into_iter().map(core_entry).collect(),
            ),
        },
    }
}

fn core_entry(entry: NormalizedClientQuotaEntry) -> DescribeClientQuotaEntity {
    let (components, values) = entry.into_parts();
    DescribeClientQuotaEntity::new(
        components
            .into_iter()
            .map(|component| {
                let (entity_type, entity_name) = component.into_parts();
                DescribeClientQuotaEntityComponent::new(entity_type, entity_name)
            })
            .collect(),
        values
            .into_iter()
            .map(|value| {
                let (key, value) = value.into_parts();
                DescribeClientQuotaValue::new(key, value)
            })
            .collect(),
    )
}

pub(super) const fn protocol_failure(
    error: DescribeClientQuotasResponseFailure,
) -> DescribeClientQuotasInput {
    match error {
        DescribeClientQuotasResponseFailure::UnsupportedApiVersion { .. } => {
            DescribeClientQuotasInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            }
        }
        DescribeClientQuotasResponseFailure::RetainedBytes { .. } => {
            DescribeClientQuotasInput::ResponseTooLarge
        }
        DescribeClientQuotasResponseFailure::NegativeThrottleTime { .. }
        | DescribeClientQuotasResponseFailure::MissingEntriesOnSuccess
        | DescribeClientQuotasResponseFailure::EntriesWithTopLevelError { .. }
        | DescribeClientQuotasResponseFailure::TooManyEntries { .. }
        | DescribeClientQuotasResponseFailure::EmptyEntity
        | DescribeClientQuotasResponseFailure::TooManyEntityComponents { .. }
        | DescribeClientQuotasResponseFailure::EmptyEntityType
        | DescribeClientQuotasResponseFailure::EntityTypeTooLong { .. }
        | DescribeClientQuotasResponseFailure::EmptyEntityName
        | DescribeClientQuotasResponseFailure::EntityNameTooLong { .. }
        | DescribeClientQuotasResponseFailure::EmptyValues
        | DescribeClientQuotasResponseFailure::TooManyQuotaValues { .. }
        | DescribeClientQuotasResponseFailure::EmptyQuotaKey
        | DescribeClientQuotasResponseFailure::QuotaKeyTooLong { .. }
        | DescribeClientQuotasResponseFailure::NonFiniteQuotaValue
        | DescribeClientQuotasResponseFailure::DuplicateEntityType
        | DescribeClientQuotasResponseFailure::DuplicateQuotaKey
        | DescribeClientQuotasResponseFailure::DuplicateEntity => {
            DescribeClientQuotasInput::InvalidResponse
        }
    }
}

const fn driver_failure(
    kind: DescribeClientQuotasDriverFailureKind,
    delivery: DeliveryStatus,
) -> DescribeClientQuotasInput {
    match kind {
        DescribeClientQuotasDriverFailureKind::DeadlineElapsed => {
            DescribeClientQuotasInput::DriverDeadlineElapsed { delivery }
        }
        DescribeClientQuotasDriverFailureKind::Compatibility => {
            DescribeClientQuotasInput::ProtocolIncompatible { delivery }
        }
        DescribeClientQuotasDriverFailureKind::InvalidResponse => {
            DescribeClientQuotasInput::InvalidResponse
        }
        DescribeClientQuotasDriverFailureKind::Transport => {
            DescribeClientQuotasInput::TransportFailed { delivery }
        }
    }
}
