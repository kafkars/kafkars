//! Fallible allocation and canonical ordering of validated quorum facts.

use kafka_wire::describe_quorum_response::{Listener, Node, PartitionData, ReplicaState};

use super::{
    NormalizedMetadataQuorum, NormalizedQuorumListener, NormalizedQuorumNode,
    NormalizedQuorumReplica,
    response::DescribeMetadataQuorumProtocolFailure,
    validation::{nonnegative_i32, nonnegative_i64, optional_i32, optional_i64},
};

pub(super) fn materialize_success(
    version: i16,
    partition: &PartitionData,
    source_nodes: &[Node],
    required: usize,
    limit: usize,
) -> Result<NormalizedMetadataQuorum, DescribeMetadataQuorumProtocolFailure> {
    let voters = replicas(version, &partition.current_voters, required, limit)?;
    let observers = replicas(version, &partition.observers, required, limit)?;
    reject_role_duplicates(&voters, &observers)?;
    let nodes = (version >= 2)
        .then(|| normalized_nodes(source_nodes, required, limit))
        .transpose()?;
    Ok(NormalizedMetadataQuorum::new(
        optional_i32("leader_id", partition.leader_id)?,
        nonnegative_i32("leader_epoch", partition.leader_epoch)?,
        nonnegative_i64("high_watermark", partition.high_watermark)?,
        voters,
        observers,
        nodes,
    ))
}

fn replicas(
    version: i16,
    source: &[ReplicaState],
    required: usize,
    limit: usize,
) -> Result<Vec<NormalizedQuorumReplica>, DescribeMetadataQuorumProtocolFailure> {
    let mut values = reserved(source.len(), required, limit)?;
    for replica in source {
        values.push(NormalizedQuorumReplica::new(
            replica.replica_id,
            (version >= 2 && !replica.replica_directory_id.is_zero())
                .then(|| replica.replica_directory_id.to_bytes()),
            optional_i64("log_end_offset", replica.log_end_offset)?,
            (version >= 1)
                .then(|| optional_i64("last_fetch_timestamp", replica.last_fetch_timestamp))
                .transpose()?
                .flatten(),
            (version >= 1)
                .then(|| optional_i64("last_caught_up_timestamp", replica.last_caught_up_timestamp))
                .transpose()?
                .flatten(),
        ));
    }
    values.sort_unstable_by_key(NormalizedQuorumReplica::replica_id);
    if let Some(pair) = values
        .windows(2)
        .find(|pair| pair[0].replica_id() == pair[1].replica_id())
    {
        return Err(DescribeMetadataQuorumProtocolFailure::DuplicateReplicaId {
            actual: pair[0].replica_id(),
        });
    }
    Ok(values)
}

fn reject_role_duplicates(
    voters: &[NormalizedQuorumReplica],
    observers: &[NormalizedQuorumReplica],
) -> Result<(), DescribeMetadataQuorumProtocolFailure> {
    let mut voter = 0;
    let mut observer = 0;
    while voter < voters.len() && observer < observers.len() {
        match voters[voter]
            .replica_id()
            .cmp(&observers[observer].replica_id())
        {
            core::cmp::Ordering::Less => voter += 1,
            core::cmp::Ordering::Greater => observer += 1,
            core::cmp::Ordering::Equal => {
                return Err(DescribeMetadataQuorumProtocolFailure::ReplicaInBothRoles {
                    actual: voters[voter].replica_id(),
                });
            }
        }
    }
    Ok(())
}

fn normalized_nodes(
    source: &[Node],
    required: usize,
    limit: usize,
) -> Result<Vec<NormalizedQuorumNode>, DescribeMetadataQuorumProtocolFailure> {
    let mut nodes = reserved(source.len(), required, limit)?;
    for node in source {
        let mut listeners = reserved(node.listeners.len(), required, limit)?;
        for listener in &node.listeners {
            listeners.push(normalized_listener(listener, required, limit)?);
        }
        listeners
            .sort_unstable_by(|left, right| left.name().as_bytes().cmp(right.name().as_bytes()));
        if listeners
            .windows(2)
            .any(|pair| pair[0].name() == pair[1].name())
        {
            return Err(
                DescribeMetadataQuorumProtocolFailure::DuplicateListenerName {
                    node_id: node.node_id,
                },
            );
        }
        nodes.push(NormalizedQuorumNode::new(node.node_id, listeners));
    }
    nodes.sort_unstable_by_key(NormalizedQuorumNode::node_id);
    if let Some(pair) = nodes
        .windows(2)
        .find(|pair| pair[0].node_id() == pair[1].node_id())
    {
        return Err(DescribeMetadataQuorumProtocolFailure::DuplicateNodeId {
            actual: pair[0].node_id(),
        });
    }
    Ok(nodes)
}

fn normalized_listener(
    source: &Listener,
    required: usize,
    limit: usize,
) -> Result<NormalizedQuorumListener, DescribeMetadataQuorumProtocolFailure> {
    Ok(NormalizedQuorumListener::new(
        copy(source.name.as_str(), required, limit)?,
        copy(source.host.as_str(), required, limit)?,
        source.port,
    ))
}

fn reserved<T>(
    capacity: usize,
    required: usize,
    limit: usize,
) -> Result<Vec<T>, DescribeMetadataQuorumProtocolFailure> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(capacity)
        .map_err(|_| DescribeMetadataQuorumProtocolFailure::RetainedBytes { required, limit })?;
    Ok(values)
}

fn copy(
    source: &str,
    required: usize,
    limit: usize,
) -> Result<String, DescribeMetadataQuorumProtocolFailure> {
    let mut value = String::new();
    value
        .try_reserve_exact(source.len())
        .map_err(|_| DescribeMetadataQuorumProtocolFailure::RetainedBytes { required, limit })?;
    value.push_str(source);
    Ok(value)
}
