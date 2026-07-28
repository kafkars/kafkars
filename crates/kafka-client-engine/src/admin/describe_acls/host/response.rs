//! Exhaustive normalized-protocol translation into deterministic core input.

use core::num::NonZeroI16;

use kafka_client_core::{
    DeliveryStatus, DescribeAclBinding, DescribeAclsBatch, DescribeAclsBrokerError,
    DescribeAclsInput,
};

use crate::{
    driver::{DescribeAclsDriverFailureKind, DescribeAclsRawTerminal, DescribeAclsTerminalFact},
    protocol::admin::describe_acls::{
        DescribeAclsResponseFailure, NormalizedAclBinding, NormalizedDescribeAclsResponse,
        normalize_describe_acls_response,
    },
};

pub(super) fn terminal_input(raw: &DescribeAclsRawTerminal) -> (DescribeAclsInput, usize) {
    match raw.fact() {
        DescribeAclsTerminalFact::Response {
            selected_version: Some(selected_version),
            response,
        } => match normalize_describe_acls_response(selected_version, response, raw.result_limit())
        {
            Ok(normalized) => normalized_input(normalized),
            Err(error) => (protocol_failure(error), 0),
        },
        DescribeAclsTerminalFact::Response {
            selected_version: None,
            ..
        } => (
            DescribeAclsInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            },
            0,
        ),
        DescribeAclsTerminalFact::Failed { kind, delivery } => (driver_failure(kind, delivery), 0),
    }
}

fn normalized_input(normalized: NormalizedDescribeAclsResponse) -> (DescribeAclsInput, usize) {
    let (
        throttle_time_ms,
        error_code,
        error_message,
        error_message_truncated,
        bindings,
        retained_bytes,
    ) = normalized.into_parts();
    let input = match NonZeroI16::new(error_code) {
        Some(code) => DescribeAclsInput::BrokerRejected {
            error: DescribeAclsBrokerError::new(code, error_message, error_message_truncated),
        },
        None => DescribeAclsInput::BrokerResponded {
            batch: DescribeAclsBatch::new(
                throttle_time_ms,
                bindings.into_iter().map(core_binding).collect(),
            ),
        },
    };
    (input, retained_bytes)
}

fn core_binding(binding: NormalizedAclBinding) -> DescribeAclBinding {
    let (resource_type, resource_name, pattern_type, principal, host, operation, permission_type) =
        binding.into_parts();
    DescribeAclBinding::new(
        resource_type,
        resource_name,
        pattern_type,
        principal,
        host,
        operation,
        permission_type,
    )
}

pub(super) const fn protocol_failure(error: DescribeAclsResponseFailure) -> DescribeAclsInput {
    match error {
        DescribeAclsResponseFailure::UnsupportedApiVersion { .. } => {
            DescribeAclsInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            }
        }
        DescribeAclsResponseFailure::RetainedBytes { .. } => DescribeAclsInput::ResponseTooLarge,
        DescribeAclsResponseFailure::NegativeThrottleTime { .. }
        | DescribeAclsResponseFailure::ResourcesWithTopLevelError { .. }
        | DescribeAclsResponseFailure::TooManyResources { .. }
        | DescribeAclsResponseFailure::EmptyResourceName
        | DescribeAclsResponseFailure::ResourceNameTooLong { .. }
        | DescribeAclsResponseFailure::EmptyResourceAcls
        | DescribeAclsResponseFailure::TooManyAcls { .. }
        | DescribeAclsResponseFailure::EmptyPrincipal
        | DescribeAclsResponseFailure::PrincipalTooLong { .. }
        | DescribeAclsResponseFailure::EmptyHost
        | DescribeAclsResponseFailure::HostTooLong { .. }
        | DescribeAclsResponseFailure::DuplicateResource
        | DescribeAclsResponseFailure::DuplicateAcl => DescribeAclsInput::InvalidResponse,
    }
}

const fn driver_failure(
    kind: DescribeAclsDriverFailureKind,
    delivery: DeliveryStatus,
) -> DescribeAclsInput {
    match kind {
        DescribeAclsDriverFailureKind::DeadlineElapsed => {
            DescribeAclsInput::DriverDeadlineElapsed { delivery }
        }
        DescribeAclsDriverFailureKind::Compatibility => {
            DescribeAclsInput::ProtocolIncompatible { delivery }
        }
        DescribeAclsDriverFailureKind::InvalidResponse => DescribeAclsInput::InvalidResponse,
        DescribeAclsDriverFailureKind::Transport => DescribeAclsInput::TransportFailed { delivery },
    }
}
