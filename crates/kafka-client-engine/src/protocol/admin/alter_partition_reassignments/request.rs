//! Charged generated reassignment construction from caller-order changes.

use std::{error::Error, fmt};

use kafka_wire::{
    AlterPartitionReassignmentsRequest,
    alter_partition_reassignments_request::{ReassignablePartition, ReassignableTopic},
};

use super::{AlterPartitionReassignmentRef, retention::generated_request_peak_charge};

/// Request construction failure before generated or driver ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AlterPartitionReassignmentsRequestFailure {
    NegativeTimeout,
    RetainedBytes,
}

impl fmt::Display for AlterPartitionReassignmentsRequestFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::NegativeTimeout => "reassignment request timeout is negative",
            Self::RetainedBytes => "generated reassignment request exceeds its proven budget",
        })
    }
}

impl Error for AlterPartitionReassignmentsRequestFailure {}

/// Builds one generated API-key 45 request without routing or retry policy.
pub(crate) fn alter_partition_reassignments_request(
    changes: &[AlterPartitionReassignmentRef<'_>],
    allow_replication_factor_change: bool,
    timeout_ms: i32,
    scratch_limit: usize,
) -> Result<AlterPartitionReassignmentsRequest, AlterPartitionReassignmentsRequestFailure> {
    if timeout_ms < 0 {
        return Err(AlterPartitionReassignmentsRequestFailure::NegativeTimeout);
    }
    let charge = generated_request_peak_charge(changes.iter().copied())
        .ok_or(AlterPartitionReassignmentsRequestFailure::RetainedBytes)?;
    if charge > scratch_limit {
        return Err(AlterPartitionReassignmentsRequestFailure::RetainedBytes);
    }
    let order = grouped_order(changes)?;
    let mut request = AlterPartitionReassignmentsRequest::default();
    request.timeout_ms = timeout_ms;
    request.allow_replication_factor_change = allow_replication_factor_change;
    append_topics(&mut request, changes, &order);
    Ok(request)
}

fn grouped_order(
    changes: &[AlterPartitionReassignmentRef<'_>],
) -> Result<Vec<usize>, AlterPartitionReassignmentsRequestFailure> {
    let mut order = Vec::new();
    order
        .try_reserve_exact(changes.len())
        .map_err(|_| AlterPartitionReassignmentsRequestFailure::RetainedBytes)?;
    order.extend(0..changes.len());
    order.sort_unstable_by(|left, right| {
        changes[*left]
            .topic()
            .as_bytes()
            .cmp(changes[*right].topic().as_bytes())
            .then_with(|| left.cmp(right))
    });
    Ok(order)
}

fn append_topics(
    request: &mut AlterPartitionReassignmentsRequest,
    changes: &[AlterPartitionReassignmentRef<'_>],
    order: &[usize],
) {
    let mut cursor = 0usize;
    while cursor < order.len() {
        let topic_name = changes[order[cursor]].topic();
        let mut topic = ReassignableTopic::default();
        topic.name = topic_name.into();
        while cursor < order.len() && changes[order[cursor]].topic() == topic_name {
            let change = changes[order[cursor]];
            let mut partition = ReassignablePartition::default();
            partition.partition_index = change.partition();
            partition.replicas = change.replicas().map(<[i32]>::to_vec);
            topic.partitions.push(partition);
            cursor += 1;
        }
        request.topics.push(topic);
    }
}
