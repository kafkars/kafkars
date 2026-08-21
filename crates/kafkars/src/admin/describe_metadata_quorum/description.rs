//! Stable successful metadata-quorum description.

use super::{MetadataQuorumNode, MetadataQuorumReplica};

/// Successful bounded description of Kafka's fixed metadata quorum.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataQuorumDescription {
    leader_id: Option<i32>,
    leader_epoch: i32,
    high_watermark: i64,
    voters: Vec<MetadataQuorumReplica>,
    observers: Vec<MetadataQuorumReplica>,
    nodes: Option<Vec<MetadataQuorumNode>>,
}

impl MetadataQuorumDescription {
    pub(crate) const fn new(
        leader_id: Option<i32>,
        leader_epoch: i32,
        high_watermark: i64,
        voters: Vec<MetadataQuorumReplica>,
        observers: Vec<MetadataQuorumReplica>,
        nodes: Option<Vec<MetadataQuorumNode>>,
    ) -> Self {
        Self {
            leader_id,
            leader_epoch,
            high_watermark,
            voters,
            observers,
            nodes,
        }
    }

    /// Returns the leader identity, or absence for Kafka's unknown sentinel.
    pub const fn leader_id(&self) -> Option<i32> {
        self.leader_id
    }

    /// Returns the nonnegative leader epoch.
    pub const fn leader_epoch(&self) -> i32 {
        self.leader_epoch
    }

    /// Returns the nonnegative quorum high watermark.
    pub const fn high_watermark(&self) -> i64 {
        self.high_watermark
    }

    /// Returns voters in strict replica-ID order.
    pub fn voters(&self) -> &[MetadataQuorumReplica] {
        &self.voters
    }

    /// Returns observers in strict replica-ID order.
    pub fn observers(&self) -> &[MetadataQuorumReplica] {
        &self.observers
    }

    /// Returns v2 node facts, or absence when the negotiated version omits them.
    pub fn nodes(&self) -> Option<&[MetadataQuorumNode]> {
        self.nodes.as_deref()
    }

    /// Consumes this description into stable generated-free parts.
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        Option<i32>,
        i32,
        i64,
        Vec<MetadataQuorumReplica>,
        Vec<MetadataQuorumReplica>,
        Option<Vec<MetadataQuorumNode>>,
    ) {
        (
            self.leader_id,
            self.leader_epoch,
            self.high_watermark,
            self.voters,
            self.observers,
            self.nodes,
        )
    }
}
