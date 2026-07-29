//! Validate-first normalization of generated metadata-quorum responses.

use kafka_wire::DescribeQuorumResponse;

use super::{
    NormalizedDescribeMetadataQuorumResponse, NormalizedMetadataQuorumOutcome,
    NormalizedQuorumError,
    materialize::materialize_success,
    retention::{bounded_diagnostic, ensure_limit, error_charge, success_charge},
    validation::{MAX_VERSION, MIN_VERSION, successful_partition, validate_success_payload},
};

/// Compatibility, malformed-shape, allocation, or retained-capacity failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeMetadataQuorumProtocolFailure {
    UnsupportedApiVersion {
        actual: i16,
    },
    UnexpectedTopicCount {
        actual: usize,
    },
    UnexpectedTopicName,
    UnexpectedPartitionCount {
        actual: usize,
    },
    UnexpectedPartition {
        actual: i32,
    },
    FieldNotRepresentable {
        field: &'static str,
    },
    TooMany {
        field: &'static str,
        actual: usize,
        max: usize,
    },
    EmptyVoterSet,
    NegativeId {
        field: &'static str,
        actual: i32,
    },
    InvalidSentinel {
        field: &'static str,
        actual: i64,
    },
    LeaderNotVoter {
        actual: i32,
    },
    EmptyString {
        field: &'static str,
    },
    StringTooLong {
        field: &'static str,
        actual: usize,
        max: usize,
    },
    ZeroListenerPort,
    DuplicateReplicaId {
        actual: i32,
    },
    ReplicaInBothRoles {
        actual: i32,
    },
    DuplicateNodeId {
        actual: i32,
    },
    DuplicateListenerName {
        node_id: i32,
    },
    RetainedBytes {
        required: usize,
        limit: usize,
    },
}

/// Normalizes one exact selected v0-v2 response without exposing generated DTOs.
pub(crate) fn normalize_describe_metadata_quorum_response(
    selected_version: i16,
    response: &DescribeQuorumResponse,
    retained_limit: usize,
) -> Result<NormalizedDescribeMetadataQuorumResponse, DescribeMetadataQuorumProtocolFailure> {
    if !(MIN_VERSION..=MAX_VERSION).contains(&selected_version) {
        return Err(
            DescribeMetadataQuorumProtocolFailure::UnsupportedApiVersion {
                actual: selected_version,
            },
        );
    }
    if response.error_code != 0 {
        return normalized_error(
            response.error_code,
            (selected_version >= 2)
                .then_some(response.error_message.as_deref())
                .flatten(),
            true,
            retained_limit,
        );
    }
    let partition = successful_partition(selected_version, response)?;
    if partition.error_code != 0 {
        return normalized_error(
            partition.error_code,
            (selected_version >= 2)
                .then_some(partition.error_message.as_deref())
                .flatten(),
            false,
            retained_limit,
        );
    }
    validate_success_payload(selected_version, partition, &response.nodes)?;
    let required = success_charge(response).unwrap_or(usize::MAX);
    ensure_limit(required, retained_limit)?;
    let quorum = materialize_success(
        selected_version,
        partition,
        &response.nodes,
        required,
        retained_limit,
    )?;
    Ok(NormalizedDescribeMetadataQuorumResponse::new(
        NormalizedMetadataQuorumOutcome::Quorum(quorum),
        required,
    ))
}

fn normalized_error(
    code: i16,
    source: Option<&str>,
    top_level: bool,
    limit: usize,
) -> Result<NormalizedDescribeMetadataQuorumResponse, DescribeMetadataQuorumProtocolFailure> {
    debug_assert_ne!(code, 0);
    let (bounded, truncated) = bounded_diagnostic(source);
    let required = error_charge(bounded.map_or(0, str::len)).unwrap_or(usize::MAX);
    ensure_limit(required, limit)?;
    let message = bounded
        .map(|value| {
            let mut message = String::new();
            message.try_reserve_exact(value.len()).map_err(|_| {
                DescribeMetadataQuorumProtocolFailure::RetainedBytes { required, limit }
            })?;
            message.push_str(value);
            Ok(message)
        })
        .transpose()?;
    let error = NormalizedQuorumError::new(code, message, truncated);
    let outcome = if top_level {
        NormalizedMetadataQuorumOutcome::TopLevelError(error)
    } else {
        NormalizedMetadataQuorumOutcome::PartitionError(error)
    };
    Ok(NormalizedDescribeMetadataQuorumResponse::new(
        outcome, required,
    ))
}
