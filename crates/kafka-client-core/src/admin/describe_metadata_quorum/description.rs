//! Bounded normalized description of Kafka's fixed metadata quorum.

use super::{
    DescribeMetadataQuorumNode, DescribeMetadataQuorumReplica, DescribeMetadataQuorumValueError,
};

/// Maximum voters or observers retained by one fixed query.
pub const DESCRIBE_METADATA_QUORUM_MAX_REPLICAS: usize = 16 * 1024;
/// Maximum represented quorum nodes retained by one fixed query.
pub const DESCRIBE_METADATA_QUORUM_MAX_NODES: usize = 16 * 1024;
/// Maximum listener endpoints retained for one represented node.
pub const DESCRIBE_METADATA_QUORUM_MAX_LISTENERS_PER_NODE: usize = 128;

/// Successful bounded description of Kafka's fixed metadata quorum.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeMetadataQuorumDescription {
    leader_id: Option<i32>,
    leader_epoch: i32,
    high_watermark: i64,
    voters: Vec<DescribeMetadataQuorumReplica>,
    observers: Vec<DescribeMetadataQuorumReplica>,
    nodes: Option<Vec<DescribeMetadataQuorumNode>>,
}

impl DescribeMetadataQuorumDescription {
    /// Validates one already protocol-normalized fixed-quorum result.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        leader_id: Option<i32>,
        leader_epoch: i32,
        high_watermark: i64,
        voters: Vec<DescribeMetadataQuorumReplica>,
        observers: Vec<DescribeMetadataQuorumReplica>,
        nodes: Option<Vec<DescribeMetadataQuorumNode>>,
    ) -> Result<Self, DescribeMetadataQuorumValueError> {
        super::validation::validate_description(
            leader_id,
            leader_epoch,
            high_watermark,
            &voters,
            &observers,
            nodes.as_deref(),
        )?;
        Ok(Self {
            leader_id,
            leader_epoch,
            high_watermark,
            voters,
            observers,
            nodes,
        })
    }

    /// Returns the leader identity, or absence for Kafka's unknown sentinel.
    pub const fn leader_id(&self) -> Option<i32> {
        self.leader_id
    }

    /// Returns the latest nonnegative leader epoch.
    pub const fn leader_epoch(&self) -> i32 {
        self.leader_epoch
    }

    /// Returns the nonnegative quorum high watermark.
    pub const fn high_watermark(&self) -> i64 {
        self.high_watermark
    }

    /// Returns voters in strict replica-ID order.
    pub fn voters(&self) -> &[DescribeMetadataQuorumReplica] {
        &self.voters
    }

    /// Returns observers in strict replica-ID order.
    pub fn observers(&self) -> &[DescribeMetadataQuorumReplica] {
        &self.observers
    }

    /// Returns v2 node facts, or `None` when not represented by the response.
    pub fn nodes(&self) -> Option<&[DescribeMetadataQuorumNode]> {
        self.nodes.as_deref()
    }

    /// Consumes the description into adapter-owned stable values.
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        Option<i32>,
        i32,
        i64,
        Vec<DescribeMetadataQuorumReplica>,
        Vec<DescribeMetadataQuorumReplica>,
        Option<Vec<DescribeMetadataQuorumNode>>,
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
