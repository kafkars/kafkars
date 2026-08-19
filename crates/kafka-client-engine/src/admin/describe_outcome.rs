//! Engine-owned terminal representation for one `DescribeCluster` call.

use core::fmt;

use kafka_client_core::{
    ClusterBroker as CoreBroker, DeliveryStatus as CoreDeliveryStatus,
    DescribeClusterFailureKind as CoreFailureKind, DescribeClusterTerminal,
};

/// Stable delivery certainty independent of deterministic core types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeClusterDeliveryStatus {
    /// The request definitely did not reach Kafka.
    NotSent,
    /// The request may have reached Kafka.
    PossiblySent,
}

/// One broker endpoint in a cluster description.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterBroker {
    id: i32,
    host: String,
    port: u16,
    rack: Option<String>,
    fenced: bool,
}

impl ClusterBroker {
    /// Returns the broker identifier.
    pub const fn id(&self) -> i32 {
        self.id
    }

    /// Returns the broker host.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Returns the broker port.
    pub const fn port(&self) -> u16 {
        self.port
    }

    /// Returns the nullable rack.
    pub fn rack(&self) -> Option<&str> {
        self.rack.as_deref()
    }

    /// Returns whether Kafka reports this broker as fenced.
    pub const fn is_fenced(&self) -> bool {
        self.fenced
    }

    /// Consumes the broker into stable parts.
    pub fn into_parts(self) -> (i32, String, u16, Option<String>, bool) {
        (self.id, self.host, self.port, self.rack, self.fenced)
    }
}

/// One bounded successful cluster description.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterDescription {
    cluster_id: String,
    controller_id: Option<i32>,
    brokers: Vec<ClusterBroker>,
    authorized_operations: Option<i32>,
}

impl ClusterDescription {
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

    /// Consumes the description into stable parts.
    pub fn into_parts(self) -> (String, Option<i32>, Vec<ClusterBroker>) {
        (self.cluster_id, self.controller_id, self.brokers)
    }

    /// Consumes the description into stable parts including authorization bits.
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

/// Exact top-level broker rejection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeClusterBrokerError {
    code: i16,
    message: Option<String>,
    message_truncated: bool,
}

impl DescribeClusterBrokerError {
    /// Consumes the error into stable parts.
    pub fn into_parts(self) -> (i16, Option<String>, bool) {
        (self.code, self.message, self.message_truncated)
    }
}

/// Stable whole-operation failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeClusterFailureKind {
    /// Original deadline elapsed before driver ownership.
    DeadlineElapsed,
    /// Driver rejected the call before transport ownership.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// SASL authentication rejected the connection before ordinary call admission.
    Authentication,
    /// The broker cannot represent the explicitly requested protocol semantics.
    Compatibility,
    /// Broker response was malformed or exceeded its retained-result budget.
    InvalidResponse,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DescribeClusterFailure {
    kind: DescribeClusterFailureKind,
    delivery: DescribeClusterDeliveryStatus,
}

impl DescribeClusterFailure {
    /// Returns the stable category.
    pub const fn kind(self) -> DescribeClusterFailureKind {
        self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(self) -> DescribeClusterDeliveryStatus {
        self.delivery
    }
}

/// Exactly one engine-owned terminal decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeClusterOutcome {
    /// One successful cluster description.
    Cluster(ClusterDescription),
    /// One exact top-level broker rejection.
    BrokerRejected(DescribeClusterBrokerError),
    /// One local or transport failure.
    Failed(DescribeClusterFailure),
}

/// Failure to observe a named completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeClusterObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The observer generation is no longer live.
    Stale,
}

impl fmt::Display for DescribeClusterObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "DescribeCluster result was already observed",
            Self::Stale => "DescribeCluster observer is stale",
        })
    }
}

impl std::error::Error for DescribeClusterObserverError {}

pub(crate) fn translate_terminal(terminal: DescribeClusterTerminal) -> DescribeClusterOutcome {
    match terminal {
        DescribeClusterTerminal::Cluster(description) => {
            let (cluster_id, controller_id, brokers, authorized_operations) =
                description.into_parts_with_authorized_operations();
            DescribeClusterOutcome::Cluster(ClusterDescription {
                cluster_id,
                controller_id,
                brokers: brokers.into_iter().map(translate_broker).collect(),
                authorized_operations,
            })
        }
        DescribeClusterTerminal::BrokerRejected(error) => {
            let (code, message, message_truncated) = error.into_parts();
            DescribeClusterOutcome::BrokerRejected(DescribeClusterBrokerError {
                code,
                message,
                message_truncated,
            })
        }
        DescribeClusterTerminal::Failed(failure) => {
            DescribeClusterOutcome::Failed(DescribeClusterFailure {
                kind: match failure.kind() {
                    CoreFailureKind::DeadlineElapsed => DescribeClusterFailureKind::DeadlineElapsed,
                    CoreFailureKind::DriverRejected => DescribeClusterFailureKind::DriverRejected,
                    CoreFailureKind::Transport => DescribeClusterFailureKind::Transport,
                    CoreFailureKind::Authentication => DescribeClusterFailureKind::Authentication,
                    CoreFailureKind::Compatibility => DescribeClusterFailureKind::Compatibility,
                    CoreFailureKind::InvalidResponse => DescribeClusterFailureKind::InvalidResponse,
                },
                delivery: translate_delivery(failure.delivery()),
            })
        }
    }
}

fn translate_broker(broker: CoreBroker) -> ClusterBroker {
    let (id, host, port, rack, fenced) = broker.into_parts();
    ClusterBroker {
        id,
        host,
        port,
        rack,
        fenced,
    }
}

const fn translate_delivery(delivery: CoreDeliveryStatus) -> DescribeClusterDeliveryStatus {
    match delivery {
        CoreDeliveryStatus::NotSent => DescribeClusterDeliveryStatus::NotSent,
        CoreDeliveryStatus::PossiblySent => DescribeClusterDeliveryStatus::PossiblySent,
    }
}
