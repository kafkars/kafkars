//! Stable Rust cluster and broker representations.

/// One broker endpoint reported by Kafka.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterBroker {
    id: i32,
    host: String,
    port: u16,
    rack: Option<String>,
    fenced: bool,
}

impl ClusterBroker {
    pub(crate) const fn new(
        id: i32,
        host: String,
        port: u16,
        rack: Option<String>,
        fenced: bool,
    ) -> Self {
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

    /// Returns the broker port.
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
}

/// One bounded broker-endpoint cluster description.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterDescription {
    cluster_id: String,
    controller_id: Option<i32>,
    brokers: Vec<ClusterBroker>,
    authorized_operations: Option<i32>,
}

impl ClusterDescription {
    #[cfg(test)]
    pub(crate) const fn new(
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

    pub(crate) const fn new_with_authorized_operations(
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

    /// Returns Kafka's raw cluster authorization bitfield when requested and supplied.
    pub const fn authorized_operations(&self) -> Option<i32> {
        self.authorized_operations
    }
}
