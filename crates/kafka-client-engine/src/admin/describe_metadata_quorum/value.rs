//! Stable generated-free metadata-quorum values.

/// One voter or observer in the fixed metadata quorum.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeMetadataQuorumReplica {
    pub(super) replica_id: i32,
    pub(super) replica_directory_id: Option<[u8; 16]>,
    pub(super) log_end_offset: Option<i64>,
    pub(super) last_fetch_timestamp_ms: Option<i64>,
    pub(super) last_caught_up_timestamp_ms: Option<i64>,
}

impl DescribeMetadataQuorumReplica {
    /// Returns the nonnegative replica identity.
    pub const fn replica_id(&self) -> i32 {
        self.replica_id
    }

    /// Returns the v2 directory identity after zero-sentinel normalization.
    pub const fn replica_directory_id(&self) -> Option<[u8; 16]> {
        self.replica_directory_id
    }

    /// Returns the log-end offset, or absence for Kafka's unknown sentinel.
    pub const fn log_end_offset(&self) -> Option<i64> {
        self.log_end_offset
    }

    /// Returns the last fetch timestamp, or absence when unknown or unrepresented.
    pub const fn last_fetch_timestamp_ms(&self) -> Option<i64> {
        self.last_fetch_timestamp_ms
    }

    /// Returns the last caught-up timestamp, or absence when unknown or unrepresented.
    pub const fn last_caught_up_timestamp_ms(&self) -> Option<i64> {
        self.last_caught_up_timestamp_ms
    }

    /// Consumes this replica into stable scalar parts.
    #[allow(clippy::type_complexity)]
    pub fn into_parts(self) -> (i32, Option<[u8; 16]>, Option<i64>, Option<i64>, Option<i64>) {
        (
            self.replica_id,
            self.replica_directory_id,
            self.log_end_offset,
            self.last_fetch_timestamp_ms,
            self.last_caught_up_timestamp_ms,
        )
    }
}

/// One named listener endpoint advertised by a quorum node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeMetadataQuorumListener {
    pub(super) name: String,
    pub(super) host: String,
    pub(super) port: u16,
}

impl DescribeMetadataQuorumListener {
    /// Returns the listener name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the advertised hostname.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Returns the advertised nonzero port.
    pub const fn port(&self) -> u16 {
        self.port
    }

    /// Consumes this endpoint into stable scalar parts.
    pub fn into_parts(self) -> (String, String, u16) {
        (self.name, self.host, self.port)
    }
}

/// One quorum node and its canonically ordered listeners.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeMetadataQuorumNode {
    pub(super) node_id: i32,
    pub(super) listeners: Vec<DescribeMetadataQuorumListener>,
}

impl DescribeMetadataQuorumNode {
    /// Returns the nonnegative quorum node identity.
    pub const fn node_id(&self) -> i32 {
        self.node_id
    }

    /// Returns listeners in strict UTF-8 byte order by name.
    pub fn listeners(&self) -> &[DescribeMetadataQuorumListener] {
        &self.listeners
    }

    /// Consumes this node into stable parts.
    pub fn into_parts(self) -> (i32, Vec<DescribeMetadataQuorumListener>) {
        (self.node_id, self.listeners)
    }
}

/// Successful bounded description of Kafka's fixed metadata quorum.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeMetadataQuorumDescription {
    pub(super) leader_id: Option<i32>,
    pub(super) leader_epoch: i32,
    pub(super) high_watermark: i64,
    pub(super) voters: Vec<DescribeMetadataQuorumReplica>,
    pub(super) observers: Vec<DescribeMetadataQuorumReplica>,
    pub(super) nodes: Option<Vec<DescribeMetadataQuorumNode>>,
}

impl DescribeMetadataQuorumDescription {
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
    pub fn voters(&self) -> &[DescribeMetadataQuorumReplica] {
        &self.voters
    }

    /// Returns observers in strict replica-ID order.
    pub fn observers(&self) -> &[DescribeMetadataQuorumReplica] {
        &self.observers
    }

    /// Returns v2 node facts, or absence when not represented.
    pub fn nodes(&self) -> Option<&[DescribeMetadataQuorumNode]> {
        self.nodes.as_deref()
    }

    /// Consumes this description into stable parts.
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
