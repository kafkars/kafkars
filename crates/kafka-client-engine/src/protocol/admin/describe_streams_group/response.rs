//! Strict singleton API-89 v0-v1 correlation and normalization.

use kafka_wire::{StreamsGroupDescribeResponse, streams_group_describe_response::DescribedGroup};

use super::{
    NormalizedDescribeStreamsGroupResult,
    materialize::{materialize_error, materialize_success},
    retention::{MAX_RESPONSE_TEXT_BYTES, response_required_bytes},
    validation::validate_success_group,
};

/// Generated response facts unsafe to bind to the accepted API-89 operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeStreamsGroupProtocolFailure {
    MissingSelectedVersion,
    UnsupportedApiVersion { actual: i16 },
    TopologyDescriptionRequiresV1,
    NegativeThrottleTime { actual: i32 },
    MissingGroup,
    DuplicateGroup,
    UnexpectedGroup,
    DiagnosticOnSuccess,
    PayloadOnGroupError,
    EmptyRequiredScalar,
    InvalidEpoch,
    InvalidNumericValue,
    TopologyDescriptionStatusMismatch,
    UnexpectedAuthorizedOperations,
    TooManyItems,
    DuplicateIdentity,
    ScalarTooLarge,
    ResponseTextBytesExceeded,
    GroupDiagnosticTooLarge,
    RetainedBytesOverflow,
    RetainedBytes { required: usize, limit: usize },
    Allocation,
}

/// Validates and copies exactly one coordinator-correlated streams group.
pub(crate) fn normalize_describe_streams_group_response(
    expected_group_id: &str,
    include_authorized_operations: bool,
    include_topology_description: bool,
    selected_version: Option<i16>,
    response: &StreamsGroupDescribeResponse,
    retained_limit: usize,
) -> Result<NormalizedDescribeStreamsGroupResult, DescribeStreamsGroupProtocolFailure> {
    let selected_version = validate_version(selected_version, include_topology_description)?;
    let throttle_time_ms = u32::try_from(response.throttle_time_ms).map_err(|_| {
        DescribeStreamsGroupProtocolFailure::NegativeThrottleTime {
            actual: response.throttle_time_ms,
        }
    })?;
    let group = matching_group(expected_group_id, &response.groups)?;
    if group.error_code != 0 {
        if !group.members.is_empty()
            || group.topology.is_some()
            || group.topology_description.is_some()
        {
            return Err(DescribeStreamsGroupProtocolFailure::PayloadOnGroupError);
        }
        if group
            .error_message
            .as_ref()
            .is_some_and(|message| message.len() > MAX_RESPONSE_TEXT_BYTES)
        {
            return Err(DescribeStreamsGroupProtocolFailure::GroupDiagnosticTooLarge);
        }
        return materialize_error(throttle_time_ms, group);
    }
    validate_success_group(
        group,
        include_authorized_operations,
        include_topology_description,
        selected_version,
    )?;
    let required = response_required_bytes(group)?;
    if required > retained_limit {
        return Err(DescribeStreamsGroupProtocolFailure::RetainedBytes {
            required,
            limit: retained_limit,
        });
    }
    materialize_success(
        throttle_time_ms,
        group,
        include_authorized_operations,
        selected_version,
    )
}

/// Validates, charges, and copies one coordinator-correlated streams group.
///
/// The first pass discovers the exact success size without allocating the
/// terminal. Broker errors have already materialized their bounded diagnostic,
/// so their terminal charge is derived directly from that stable value.
pub(crate) fn normalize_describe_streams_group_response_with_charge(
    expected_group_id: &str,
    include_authorized_operations: bool,
    include_topology_description: bool,
    selected_version: Option<i16>,
    response: &StreamsGroupDescribeResponse,
    retained_limit: usize,
) -> Result<(NormalizedDescribeStreamsGroupResult, usize), DescribeStreamsGroupProtocolFailure> {
    match normalize_describe_streams_group_response(
        expected_group_id,
        include_authorized_operations,
        include_topology_description,
        selected_version,
        response,
        0,
    ) {
        Ok(NormalizedDescribeStreamsGroupResult::Failed(error)) => {
            let required = expected_group_id
                .len()
                .checked_add(error.message().map_or(0, str::len))
                .ok_or(DescribeStreamsGroupProtocolFailure::RetainedBytesOverflow)?;
            if required > retained_limit {
                return Err(DescribeStreamsGroupProtocolFailure::RetainedBytes {
                    required,
                    limit: retained_limit,
                });
            }
            Ok((
                NormalizedDescribeStreamsGroupResult::Failed(error),
                required,
            ))
        }
        Ok(NormalizedDescribeStreamsGroupResult::Described(result)) => {
            Ok((NormalizedDescribeStreamsGroupResult::Described(result), 0))
        }
        Err(DescribeStreamsGroupProtocolFailure::RetainedBytes { required, limit: 0 }) => {
            if required > retained_limit {
                return Err(DescribeStreamsGroupProtocolFailure::RetainedBytes {
                    required,
                    limit: retained_limit,
                });
            }
            let normalized = normalize_describe_streams_group_response(
                expected_group_id,
                include_authorized_operations,
                include_topology_description,
                selected_version,
                response,
                required,
            )?;
            Ok((normalized, required))
        }
        Err(error) => Err(error),
    }
}

fn validate_version(
    selected_version: Option<i16>,
    include_topology_description: bool,
) -> Result<i16, DescribeStreamsGroupProtocolFailure> {
    match selected_version {
        None => Err(DescribeStreamsGroupProtocolFailure::MissingSelectedVersion),
        Some(0) if include_topology_description => {
            Err(DescribeStreamsGroupProtocolFailure::TopologyDescriptionRequiresV1)
        }
        Some(version @ (0 | 1)) => Ok(version),
        Some(actual) => Err(DescribeStreamsGroupProtocolFailure::UnsupportedApiVersion { actual }),
    }
}

fn matching_group<'a>(
    expected: &str,
    groups: &'a [DescribedGroup],
) -> Result<&'a DescribedGroup, DescribeStreamsGroupProtocolFailure> {
    let mut matching = None;
    for group in groups {
        if group.group_id.as_str() != expected {
            return Err(DescribeStreamsGroupProtocolFailure::UnexpectedGroup);
        }
        if matching.replace(group).is_some() {
            return Err(DescribeStreamsGroupProtocolFailure::DuplicateGroup);
        }
    }
    matching.ok_or(DescribeStreamsGroupProtocolFailure::MissingGroup)
}
