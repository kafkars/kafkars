//! Generated-free metadata-quorum facts retained above the protocol seam.

/// One exact signed Kafka error with a bounded optional diagnostic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedQuorumError {
    code: i16,
    message: Option<String>,
    message_truncated: bool,
}

impl NormalizedQuorumError {
    pub(super) const fn new(code: i16, message: Option<String>, message_truncated: bool) -> Self {
        Self {
            code,
            message,
            message_truncated,
        }
    }

    pub(crate) fn into_parts(self) -> (i16, Option<String>, bool) {
        (self.code, self.message, self.message_truncated)
    }
}

/// One voter or observer, ordered by nonnegative replica ID.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedQuorumReplica {
    replica_id: i32,
    directory_id: Option<[u8; 16]>,
    log_end_offset: Option<i64>,
    last_fetch_timestamp: Option<i64>,
    last_caught_up_timestamp: Option<i64>,
}

impl NormalizedQuorumReplica {
    pub(super) const fn new(
        replica_id: i32,
        directory_id: Option<[u8; 16]>,
        log_end_offset: Option<i64>,
        last_fetch_timestamp: Option<i64>,
        last_caught_up_timestamp: Option<i64>,
    ) -> Self {
        Self {
            replica_id,
            directory_id,
            log_end_offset,
            last_fetch_timestamp,
            last_caught_up_timestamp,
        }
    }

    pub(crate) const fn into_parts(
        self,
    ) -> (i32, Option<[u8; 16]>, Option<i64>, Option<i64>, Option<i64>) {
        (
            self.replica_id,
            self.directory_id,
            self.log_end_offset,
            self.last_fetch_timestamp,
            self.last_caught_up_timestamp,
        )
    }

    pub(super) const fn replica_id(&self) -> i32 {
        self.replica_id
    }
}

/// One controller listener, ordered by listener-name UTF-8 bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedQuorumListener {
    name: String,
    host: String,
    port: u16,
}

impl NormalizedQuorumListener {
    pub(super) const fn new(name: String, host: String, port: u16) -> Self {
        Self { name, host, port }
    }

    pub(crate) fn into_parts(self) -> (String, String, u16) {
        (self.name, self.host, self.port)
    }

    pub(super) fn name(&self) -> &str {
        &self.name
    }
}

/// One controller node, ordered by nonnegative node ID.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedQuorumNode {
    node_id: i32,
    listeners: Vec<NormalizedQuorumListener>,
}

impl NormalizedQuorumNode {
    pub(super) const fn new(node_id: i32, listeners: Vec<NormalizedQuorumListener>) -> Self {
        Self { node_id, listeners }
    }

    pub(crate) fn into_parts(self) -> (i32, Vec<NormalizedQuorumListener>) {
        (self.node_id, self.listeners)
    }

    pub(super) const fn node_id(&self) -> i32 {
        self.node_id
    }
}

/// One successful `__cluster_metadata` partition-zero description.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedMetadataQuorum {
    leader_id: Option<i32>,
    leader_epoch: i32,
    high_watermark: i64,
    voters: Vec<NormalizedQuorumReplica>,
    observers: Vec<NormalizedQuorumReplica>,
    nodes: Option<Vec<NormalizedQuorumNode>>,
}

impl NormalizedMetadataQuorum {
    pub(super) const fn new(
        leader_id: Option<i32>,
        leader_epoch: i32,
        high_watermark: i64,
        voters: Vec<NormalizedQuorumReplica>,
        observers: Vec<NormalizedQuorumReplica>,
        nodes: Option<Vec<NormalizedQuorumNode>>,
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

    pub(crate) fn into_parts(
        self,
    ) -> (
        Option<i32>,
        i32,
        i64,
        Vec<NormalizedQuorumReplica>,
        Vec<NormalizedQuorumReplica>,
        Option<Vec<NormalizedQuorumNode>>,
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

/// Distinguishes API-level, partition-level, and successful quorum terminals.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum NormalizedMetadataQuorumOutcome {
    TopLevelError(NormalizedQuorumError),
    PartitionError(NormalizedQuorumError),
    Quorum(NormalizedMetadataQuorum),
}

/// One bounded normalized API-key 55 response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedDescribeMetadataQuorumResponse {
    outcome: NormalizedMetadataQuorumOutcome,
    retained_bytes: usize,
}

impl NormalizedDescribeMetadataQuorumResponse {
    pub(super) const fn new(
        outcome: NormalizedMetadataQuorumOutcome,
        retained_bytes: usize,
    ) -> Self {
        Self {
            outcome,
            retained_bytes,
        }
    }

    pub(crate) fn into_parts(self) -> (NormalizedMetadataQuorumOutcome, usize) {
        (self.outcome, self.retained_bytes)
    }
}
