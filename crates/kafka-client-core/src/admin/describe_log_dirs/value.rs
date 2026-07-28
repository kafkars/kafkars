//! Wire-free log-directory and replica facts returned by one broker.

use core::num::NonZeroI16;

/// Exact broker-declared failure for a broker or one of its log directories.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdminDescribeLogDirsBrokerError {
    code: NonZeroI16,
}

impl AdminDescribeLogDirsBrokerError {
    /// Creates one exact signed Kafka error.
    pub const fn new(code: NonZeroI16) -> Self {
        Self { code }
    }

    /// Returns Kafka's exact signed error code.
    pub const fn code(self) -> i16 {
        self.code.get()
    }
}

/// One replica stored in a broker log directory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminLogDirReplicaInfo {
    topic: String,
    partition: i32,
    size_bytes: i64,
    offset_lag: i64,
    future: bool,
}

impl AdminLogDirReplicaInfo {
    /// Creates one protocol-normalized replica fact.
    pub const fn new(
        topic: String,
        partition: i32,
        size_bytes: i64,
        offset_lag: i64,
        future: bool,
    ) -> Self {
        Self {
            topic,
            partition,
            size_bytes,
            offset_lag,
            future,
        }
    }

    /// Returns the exact topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the nonnegative partition index.
    pub const fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns Kafka's reported replica size in bytes.
    pub const fn size_bytes(&self) -> i64 {
        self.size_bytes
    }

    /// Returns Kafka's reported replica offset lag.
    pub const fn offset_lag(&self) -> i64 {
        self.offset_lag
    }

    /// Returns whether this is the future replica.
    pub const fn is_future(&self) -> bool {
        self.future
    }

    /// Consumes the replica fact into adapter-owned scalar parts.
    pub fn into_parts(self) -> (String, i32, i64, i64, bool) {
        (
            self.topic,
            self.partition,
            self.size_bytes,
            self.offset_lag,
            self.future,
        )
    }
}

/// Successful description of one broker log directory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminLogDirDescription {
    replicas: Vec<AdminLogDirReplicaInfo>,
    total_bytes: Option<i64>,
    usable_bytes: Option<i64>,
    cordoned: Option<bool>,
}

impl AdminLogDirDescription {
    /// Creates one bounded, protocol-normalized directory description.
    pub const fn new(
        replicas: Vec<AdminLogDirReplicaInfo>,
        total_bytes: Option<i64>,
        usable_bytes: Option<i64>,
        cordoned: Option<bool>,
    ) -> Self {
        Self {
            replicas,
            total_bytes,
            usable_bytes,
            cordoned,
        }
    }

    /// Returns replicas in deterministic topic-partition order.
    pub fn replicas(&self) -> &[AdminLogDirReplicaInfo] {
        &self.replicas
    }

    /// Returns volume capacity when represented by the negotiated version.
    pub const fn total_bytes(&self) -> Option<i64> {
        self.total_bytes
    }

    /// Returns usable volume capacity when represented by the negotiated version.
    pub const fn usable_bytes(&self) -> Option<i64> {
        self.usable_bytes
    }

    /// Returns cordon status when represented by the negotiated version.
    pub const fn cordoned(&self) -> Option<bool> {
        self.cordoned
    }

    /// Consumes the description into adapter-owned parts.
    pub fn into_parts(
        self,
    ) -> (
        Vec<AdminLogDirReplicaInfo>,
        Option<i64>,
        Option<i64>,
        Option<bool>,
    ) {
        (
            self.replicas,
            self.total_bytes,
            self.usable_bytes,
            self.cordoned,
        )
    }
}

/// Exact result for one broker log-directory path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminLogDirResult {
    /// Kafka described the directory and its replicas.
    Described(AdminLogDirDescription),
    /// Kafka rejected or could not read this directory.
    BrokerFailed(AdminDescribeLogDirsBrokerError),
}

/// One log-directory result retained with its path identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminLogDirOutcome {
    path: String,
    result: AdminLogDirResult,
}

impl AdminLogDirOutcome {
    /// Creates one successful log-directory outcome.
    pub const fn described(path: String, description: AdminLogDirDescription) -> Self {
        Self {
            path,
            result: AdminLogDirResult::Described(description),
        }
    }

    /// Creates one exact directory-level broker failure.
    pub const fn broker_failed(path: String, error: AdminDescribeLogDirsBrokerError) -> Self {
        Self {
            path,
            result: AdminLogDirResult::BrokerFailed(error),
        }
    }

    /// Returns the absolute broker log-directory path.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Returns the directory's exact normalized result.
    pub const fn result(&self) -> &AdminLogDirResult {
        &self.result
    }

    /// Consumes the directory outcome into adapter-owned parts.
    pub fn into_parts(self) -> (String, AdminLogDirResult) {
        (self.path, self.result)
    }
}
