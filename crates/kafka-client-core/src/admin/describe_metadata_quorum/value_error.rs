//! Rejections for malformed or unbounded normalized quorum values.

use core::fmt;

/// Invalid protocol-normalized metadata-quorum description.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeMetadataQuorumValueError {
    /// Kafka reported a negative leader identity other than normalized absence.
    NegativeLeaderId,
    /// Kafka reported a negative leader epoch.
    NegativeLeaderEpoch,
    /// Kafka reported a negative high watermark.
    NegativeHighWatermark,
    /// One voter or observer identity is negative.
    NegativeReplicaId,
    /// One present replica offset is negative.
    NegativeReplicaOffset,
    /// One present replica timestamp is negative.
    NegativeReplicaTimestamp,
    /// A present replica directory identity is the normalized zero sentinel.
    ZeroReplicaDirectoryId,
    /// The voter collection exceeds the fixed operation limit.
    TooManyVoters,
    /// The observer collection exceeds the fixed operation limit.
    TooManyObservers,
    /// Voters are not in strict replica-ID order.
    NonCanonicalVoterOrder,
    /// Observers are not in strict replica-ID order.
    NonCanonicalObserverOrder,
    /// One identity appears as both voter and observer.
    ReplicaRoleOverlap,
    /// The represented node collection exceeds the fixed operation limit.
    TooManyNodes,
    /// Nodes are not in strict node-ID order.
    NonCanonicalNodeOrder,
    /// One node identity is negative.
    NegativeNodeId,
    /// One node has too many represented listeners.
    TooManyListeners,
    /// Listeners are not in strict listener-name byte order.
    NonCanonicalListenerOrder,
    /// A listener name is empty.
    EmptyListenerName,
    /// A listener hostname is empty.
    EmptyListenerHost,
    /// A listener name exceeds Kafka's signed-short string limit.
    ListenerNameTooLong,
    /// A listener hostname exceeds Kafka's signed-short string limit.
    ListenerHostTooLong,
    /// A listener reports the reserved zero port.
    ZeroListenerPort,
}

impl fmt::Display for DescribeMetadataQuorumValueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid DescribeMetadataQuorum value: {self:?}")
    }
}

impl std::error::Error for DescribeMetadataQuorumValueError {}
