//! Bounded protocol-normalized values for one `DescribeCluster` terminal.

use core::num::NonZeroI16;

use crate::DeliveryStatus;

/// One broker endpoint in a normalized cluster description.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterBroker {
    id: i32,
    host: String,
    port: u16,
    rack: Option<String>,
    fenced: bool,
}

impl ClusterBroker {
    /// Creates one validated endpoint fact.
    pub const fn new(id: i32, host: String, port: u16, rack: Option<String>, fenced: bool) -> Self {
        Self {
            id,
            host,
            port,
            rack,
            fenced,
        }
    }

    /// Returns the broker identifier.
    pub const fn id(&self) -> i32 {
        self.id
    }

    /// Returns the broker host.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Returns the nonzero broker port.
    pub const fn port(&self) -> u16 {
        self.port
    }

    /// Returns the optional rack name.
    pub fn rack(&self) -> Option<&str> {
        self.rack.as_deref()
    }

    /// Returns whether Kafka reports this broker as fenced.
    pub const fn is_fenced(&self) -> bool {
        self.fenced
    }

    /// Consumes the broker into adapter-owned parts.
    pub fn into_parts(self) -> (i32, String, u16, Option<String>, bool) {
        (self.id, self.host, self.port, self.rack, self.fenced)
    }
}

/// One bounded successful cluster description.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterDescription {
    cluster_id: String,
    controller_id: Option<i32>,
    brokers: Vec<ClusterBroker>,
    authorized_operations: Option<i32>,
}

impl ClusterDescription {
    /// Creates a normalized description ordered by broker identifier.
    pub const fn new(
        cluster_id: String,
        controller_id: Option<i32>,
        brokers: Vec<ClusterBroker>,
    ) -> Self {
        Self {
            cluster_id,
            controller_id,
            brokers,
            authorized_operations: None,
        }
    }

    /// Creates a normalized description with explicitly requested authorization bits.
    pub const fn new_with_authorized_operations(
        cluster_id: String,
        controller_id: Option<i32>,
        brokers: Vec<ClusterBroker>,
        authorized_operations: Option<i32>,
    ) -> Self {
        Self {
            cluster_id,
            controller_id,
            brokers,
            authorized_operations,
        }
    }

    /// Returns Kafka's cluster identifier.
    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    /// Returns the nullable broker-reported controller or fallback broker identifier.
    pub const fn controller_id(&self) -> Option<i32> {
        self.controller_id
    }

    /// Returns brokers in deterministic identifier order.
    pub fn brokers(&self) -> &[ClusterBroker] {
        &self.brokers
    }

    /// Returns the raw cluster authorization bitfield when Kafka supplied it.
    pub const fn authorized_operations(&self) -> Option<i32> {
        self.authorized_operations
    }

    /// Consumes the description into adapter-owned parts.
    pub fn into_parts(self) -> (String, Option<i32>, Vec<ClusterBroker>) {
        (self.cluster_id, self.controller_id, self.brokers)
    }

    /// Consumes the description into adapter-owned parts including authorization bits.
    pub fn into_parts_with_authorized_operations(
        self,
    ) -> (String, Option<i32>, Vec<ClusterBroker>, Option<i32>) {
        (
            self.cluster_id,
            self.controller_id,
            self.brokers,
            self.authorized_operations,
        )
    }
}

/// Exact top-level broker rejection with a bounded nullable diagnostic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeClusterBrokerError {
    code: NonZeroI16,
    message: Option<String>,
    message_truncated: bool,
}

impl DescribeClusterBrokerError {
    /// Creates a lossless signed code with bounded diagnostic storage.
    pub const fn new(code: NonZeroI16, message: Option<String>, message_truncated: bool) -> Self {
        Self {
            code,
            message,
            message_truncated,
        }
    }

    /// Consumes the error into adapter-owned parts.
    pub fn into_parts(self) -> (i16, Option<String>, bool) {
        (self.code.get(), self.message, self.message_truncated)
    }
}

/// Whole-operation failure outside a top-level broker rejection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DescribeClusterFailureKind {
    /// The public absolute deadline elapsed before driver ownership.
    DeadlineElapsed,
    /// The driver rejected the request before transport ownership.
    DriverRejected,
    /// Transport failed after the request entered driver ownership.
    Transport,
    /// The broker cannot represent the explicitly requested cluster view.
    Compatibility,
    /// A broker response was malformed or exceeded the retained-result budget.
    InvalidResponse,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DescribeClusterFailure {
    kind: DescribeClusterFailureKind,
    delivery: DeliveryStatus,
}

impl DescribeClusterFailure {
    pub(crate) const fn new(kind: DescribeClusterFailureKind, delivery: DeliveryStatus) -> Self {
        Self { kind, delivery }
    }

    /// Returns the core-owned failure category.
    pub const fn kind(self) -> DescribeClusterFailureKind {
        self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(self) -> DeliveryStatus {
        self.delivery
    }
}

/// Exactly one terminal decision for a `DescribeCluster` operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DescribeClusterTerminal {
    /// One bounded successful cluster description.
    Cluster(ClusterDescription),
    /// An exact top-level broker rejection.
    BrokerRejected(DescribeClusterBrokerError),
    /// A local or transport whole-operation failure.
    Failed(DescribeClusterFailure),
}
