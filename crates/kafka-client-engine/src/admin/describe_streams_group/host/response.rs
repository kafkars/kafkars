//! Exhaustive generated-free response and driver-failure translation.

use kafka_client_core::{DeliveryStatus, DescribeStreamsGroupInput, DescribeStreamsGroupPlan};

use crate::{
    driver::{
        DescribeStreamsGroupDriverFailureKind, DescribeStreamsGroupTerminal as DriverTerminal,
        DescribeStreamsGroupTerminalFact,
    },
    protocol::admin::describe_streams_group::{
        DescribeStreamsGroupProtocolFailure, NormalizedDescribeStreamsGroupResult,
        normalize_describe_streams_group_response_with_charge,
    },
};

pub(super) fn terminal_input(
    raw: &DriverTerminal,
    plan: &DescribeStreamsGroupPlan,
    retained_limit: usize,
) -> (DescribeStreamsGroupInput, usize) {
    match raw.fact() {
        DescribeStreamsGroupTerminalFact::Response {
            selected_version,
            response,
        } => match normalize_describe_streams_group_response_with_charge(
            plan.group_id(),
            plan.include_authorized_operations(),
            plan.include_topology_description(),
            selected_version,
            response,
            retained_limit,
        ) {
            Ok((NormalizedDescribeStreamsGroupResult::Described(result), retained_bytes)) => (
                DescribeStreamsGroupInput::BrokerResponded { result },
                retained_bytes,
            ),
            Ok((NormalizedDescribeStreamsGroupResult::Failed(error), retained_bytes)) => (
                DescribeStreamsGroupInput::BrokerRejected { error },
                retained_bytes,
            ),
            Err(error) => (protocol_failure(error), 0),
        },
        DescribeStreamsGroupTerminalFact::Failed { kind, delivery } => {
            (driver_failure(kind, delivery), 0)
        }
    }
}

pub(super) const fn protocol_failure(
    error: DescribeStreamsGroupProtocolFailure,
) -> DescribeStreamsGroupInput {
    match error {
        DescribeStreamsGroupProtocolFailure::MissingSelectedVersion
        | DescribeStreamsGroupProtocolFailure::UnsupportedApiVersion { .. }
        | DescribeStreamsGroupProtocolFailure::TopologyDescriptionRequiresV1 => {
            DescribeStreamsGroupInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            }
        }
        DescribeStreamsGroupProtocolFailure::TooManyItems
        | DescribeStreamsGroupProtocolFailure::ScalarTooLarge
        | DescribeStreamsGroupProtocolFailure::ResponseTextBytesExceeded
        | DescribeStreamsGroupProtocolFailure::GroupDiagnosticTooLarge
        | DescribeStreamsGroupProtocolFailure::RetainedBytesOverflow
        | DescribeStreamsGroupProtocolFailure::RetainedBytes { .. }
        | DescribeStreamsGroupProtocolFailure::Allocation => {
            DescribeStreamsGroupInput::ResponseTooLarge
        }
        DescribeStreamsGroupProtocolFailure::NegativeThrottleTime { .. }
        | DescribeStreamsGroupProtocolFailure::MissingGroup
        | DescribeStreamsGroupProtocolFailure::DuplicateGroup
        | DescribeStreamsGroupProtocolFailure::UnexpectedGroup
        | DescribeStreamsGroupProtocolFailure::DiagnosticOnSuccess
        | DescribeStreamsGroupProtocolFailure::PayloadOnGroupError
        | DescribeStreamsGroupProtocolFailure::EmptyRequiredScalar
        | DescribeStreamsGroupProtocolFailure::InvalidEpoch
        | DescribeStreamsGroupProtocolFailure::InvalidNumericValue
        | DescribeStreamsGroupProtocolFailure::TopologyDescriptionStatusMismatch
        | DescribeStreamsGroupProtocolFailure::UnexpectedAuthorizedOperations
        | DescribeStreamsGroupProtocolFailure::DuplicateIdentity => {
            DescribeStreamsGroupInput::InvalidResponse
        }
    }
}

const fn driver_failure(
    kind: DescribeStreamsGroupDriverFailureKind,
    delivery: DeliveryStatus,
) -> DescribeStreamsGroupInput {
    match kind {
        DescribeStreamsGroupDriverFailureKind::DeadlineElapsed => {
            DescribeStreamsGroupInput::DriverDeadlineElapsed { delivery }
        }
        DescribeStreamsGroupDriverFailureKind::Compatibility => {
            DescribeStreamsGroupInput::ProtocolIncompatible { delivery }
        }
        DescribeStreamsGroupDriverFailureKind::InvalidResponse => {
            DescribeStreamsGroupInput::InvalidResponse
        }
        DescribeStreamsGroupDriverFailureKind::Transport => {
            DescribeStreamsGroupInput::TransportFailed { delivery }
        }
    }
}
