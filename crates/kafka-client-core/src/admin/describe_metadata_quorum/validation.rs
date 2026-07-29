//! Linear validation for bounded protocol-normalized quorum values.

use super::{
    DESCRIBE_METADATA_QUORUM_MAX_LISTENERS_PER_NODE, DESCRIBE_METADATA_QUORUM_MAX_NODES,
    DESCRIBE_METADATA_QUORUM_MAX_REPLICAS, DescribeMetadataQuorumListener,
    DescribeMetadataQuorumNode, DescribeMetadataQuorumReplica, DescribeMetadataQuorumValueError,
};

const MAX_LISTENER_STRING_BYTES: usize = i16::MAX as usize;

pub(super) fn validate_description(
    leader_id: Option<i32>,
    leader_epoch: i32,
    high_watermark: i64,
    voters: &[DescribeMetadataQuorumReplica],
    observers: &[DescribeMetadataQuorumReplica],
    nodes: Option<&[DescribeMetadataQuorumNode]>,
) -> Result<(), DescribeMetadataQuorumValueError> {
    if leader_id.is_some_and(|id| id < 0) {
        return Err(DescribeMetadataQuorumValueError::NegativeLeaderId);
    }
    if leader_epoch < 0 {
        return Err(DescribeMetadataQuorumValueError::NegativeLeaderEpoch);
    }
    if high_watermark < 0 {
        return Err(DescribeMetadataQuorumValueError::NegativeHighWatermark);
    }
    validate_replicas(voters, true)?;
    validate_replicas(observers, false)?;
    validate_roles(voters, observers)?;
    if let Some(nodes) = nodes {
        validate_nodes(nodes)?;
    }
    Ok(())
}

fn validate_replicas(
    replicas: &[DescribeMetadataQuorumReplica],
    voters: bool,
) -> Result<(), DescribeMetadataQuorumValueError> {
    let too_many = if voters {
        DescribeMetadataQuorumValueError::TooManyVoters
    } else {
        DescribeMetadataQuorumValueError::TooManyObservers
    };
    if replicas.len() > DESCRIBE_METADATA_QUORUM_MAX_REPLICAS {
        return Err(too_many);
    }
    let noncanonical = if voters {
        DescribeMetadataQuorumValueError::NonCanonicalVoterOrder
    } else {
        DescribeMetadataQuorumValueError::NonCanonicalObserverOrder
    };
    for (index, replica) in replicas.iter().enumerate() {
        validate_replica(replica)?;
        if index > 0 && replicas[index - 1].replica_id() >= replica.replica_id() {
            return Err(noncanonical);
        }
    }
    Ok(())
}

fn validate_replica(
    replica: &DescribeMetadataQuorumReplica,
) -> Result<(), DescribeMetadataQuorumValueError> {
    if replica.replica_id() < 0 {
        return Err(DescribeMetadataQuorumValueError::NegativeReplicaId);
    }
    if replica.log_end_offset().is_some_and(|value| value < 0) {
        return Err(DescribeMetadataQuorumValueError::NegativeReplicaOffset);
    }
    if replica
        .last_fetch_timestamp_ms()
        .is_some_and(|value| value < 0)
        || replica
            .last_caught_up_timestamp_ms()
            .is_some_and(|value| value < 0)
    {
        return Err(DescribeMetadataQuorumValueError::NegativeReplicaTimestamp);
    }
    if replica.replica_directory_id() == Some([0; 16]) {
        return Err(DescribeMetadataQuorumValueError::ZeroReplicaDirectoryId);
    }
    Ok(())
}

fn validate_roles(
    voters: &[DescribeMetadataQuorumReplica],
    observers: &[DescribeMetadataQuorumReplica],
) -> Result<(), DescribeMetadataQuorumValueError> {
    let mut observer = 0;
    for voter in voters {
        while observer < observers.len() && observers[observer].replica_id() < voter.replica_id() {
            observer += 1;
        }
        if observers.get(observer).map(|value| value.replica_id()) == Some(voter.replica_id()) {
            return Err(DescribeMetadataQuorumValueError::ReplicaRoleOverlap);
        }
    }
    Ok(())
}

fn validate_nodes(
    nodes: &[DescribeMetadataQuorumNode],
) -> Result<(), DescribeMetadataQuorumValueError> {
    if nodes.len() > DESCRIBE_METADATA_QUORUM_MAX_NODES {
        return Err(DescribeMetadataQuorumValueError::TooManyNodes);
    }
    for (index, node) in nodes.iter().enumerate() {
        if node.node_id() < 0 {
            return Err(DescribeMetadataQuorumValueError::NegativeNodeId);
        }
        if index > 0 && nodes[index - 1].node_id() >= node.node_id() {
            return Err(DescribeMetadataQuorumValueError::NonCanonicalNodeOrder);
        }
        validate_listeners(node.listeners())?;
    }
    Ok(())
}

fn validate_listeners(
    listeners: &[DescribeMetadataQuorumListener],
) -> Result<(), DescribeMetadataQuorumValueError> {
    if listeners.len() > DESCRIBE_METADATA_QUORUM_MAX_LISTENERS_PER_NODE {
        return Err(DescribeMetadataQuorumValueError::TooManyListeners);
    }
    for (index, listener) in listeners.iter().enumerate() {
        validate_listener(listener)?;
        if index > 0 && listeners[index - 1].name().as_bytes() >= listener.name().as_bytes() {
            return Err(DescribeMetadataQuorumValueError::NonCanonicalListenerOrder);
        }
    }
    Ok(())
}

fn validate_listener(
    listener: &DescribeMetadataQuorumListener,
) -> Result<(), DescribeMetadataQuorumValueError> {
    if listener.name().is_empty() {
        return Err(DescribeMetadataQuorumValueError::EmptyListenerName);
    }
    if listener.host().is_empty() {
        return Err(DescribeMetadataQuorumValueError::EmptyListenerHost);
    }
    if listener.name().len() > MAX_LISTENER_STRING_BYTES {
        return Err(DescribeMetadataQuorumValueError::ListenerNameTooLong);
    }
    if listener.host().len() > MAX_LISTENER_STRING_BYTES {
        return Err(DescribeMetadataQuorumValueError::ListenerHostTooLong);
    }
    if listener.port() == 0 {
        return Err(DescribeMetadataQuorumValueError::ZeroListenerPort);
    }
    Ok(())
}
