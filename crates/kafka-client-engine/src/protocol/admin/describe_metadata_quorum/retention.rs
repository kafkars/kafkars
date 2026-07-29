//! Checked retained-capacity accounting and bounded diagnostic copying.

use core::mem::size_of;

use kafka_wire::DescribeQuorumResponse;

use super::{
    NormalizedDescribeMetadataQuorumResponse, NormalizedMetadataQuorum,
    NormalizedMetadataQuorumOutcome, NormalizedQuorumListener, NormalizedQuorumNode,
    NormalizedQuorumReplica, response::DescribeMetadataQuorumProtocolFailure,
};

pub(super) const DIAGNOSTIC_BYTES: usize = 1024;

pub(super) fn error_charge(message_bytes: usize) -> Option<usize> {
    size_of::<NormalizedDescribeMetadataQuorumResponse>()
        .checked_add(size_of::<NormalizedMetadataQuorumOutcome>())?
        .checked_add(message_bytes)
}

pub(super) fn success_charge(response: &DescribeQuorumResponse) -> Option<usize> {
    let partition = response.topics.first()?.partitions.first()?;
    let replicas = partition
        .current_voters
        .len()
        .checked_add(partition.observers.len())?;
    let listener_count = response.nodes.iter().try_fold(0usize, |count, node| {
        count.checked_add(node.listeners.len())
    })?;
    let text_bytes = response.nodes.iter().try_fold(0usize, |bytes, node| {
        node.listeners.iter().try_fold(bytes, |bytes, listener| {
            bytes
                .checked_add(listener.name.len())?
                .checked_add(listener.host.len())
        })
    })?;
    size_of::<NormalizedDescribeMetadataQuorumResponse>()
        .checked_add(size_of::<NormalizedMetadataQuorumOutcome>())?
        .checked_add(size_of::<NormalizedMetadataQuorum>())?
        .checked_add(replicas.checked_mul(size_of::<NormalizedQuorumReplica>())?)?
        .checked_add(
            response
                .nodes
                .len()
                .checked_mul(size_of::<NormalizedQuorumNode>())?,
        )?
        .checked_add(listener_count.checked_mul(size_of::<NormalizedQuorumListener>())?)?
        .checked_add(text_bytes)
}

pub(super) fn bounded_diagnostic(source: Option<&str>) -> (Option<&str>, bool) {
    let Some(source) = source else {
        return (None, false);
    };
    if source.len() <= DIAGNOSTIC_BYTES {
        return (Some(source), false);
    }
    let mut end = DIAGNOSTIC_BYTES;
    while !source.is_char_boundary(end) {
        end -= 1;
    }
    (Some(&source[..end]), true)
}

pub(super) fn ensure_limit(
    required: usize,
    limit: usize,
) -> Result<(), DescribeMetadataQuorumProtocolFailure> {
    (required <= limit)
        .then_some(())
        .ok_or(DescribeMetadataQuorumProtocolFailure::RetainedBytes { required, limit })
}
