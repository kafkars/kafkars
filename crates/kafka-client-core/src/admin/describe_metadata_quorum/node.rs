//! Stable controller-node and listener endpoint facts.

/// One named listener endpoint advertised by a quorum node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeMetadataQuorumListener {
    name: String,
    host: String,
    port: u16,
}

impl DescribeMetadataQuorumListener {
    /// Creates one protocol-normalized listener endpoint.
    pub const fn new(name: String, host: String, port: u16) -> Self {
        Self { name, host, port }
    }

    /// Returns the listener name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the advertised hostname.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Returns the advertised port.
    pub const fn port(&self) -> u16 {
        self.port
    }

    /// Consumes the listener into adapter-owned scalar parts.
    pub fn into_parts(self) -> (String, String, u16) {
        (self.name, self.host, self.port)
    }
}

/// One quorum node and its canonically ordered listeners.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeMetadataQuorumNode {
    node_id: i32,
    listeners: Vec<DescribeMetadataQuorumListener>,
}

impl DescribeMetadataQuorumNode {
    /// Creates one protocol-normalized node fact.
    pub const fn new(node_id: i32, listeners: Vec<DescribeMetadataQuorumListener>) -> Self {
        Self { node_id, listeners }
    }

    /// Returns the nonnegative quorum node identity.
    pub const fn node_id(&self) -> i32 {
        self.node_id
    }

    /// Returns listeners in strict UTF-8 byte order by listener name.
    pub fn listeners(&self) -> &[DescribeMetadataQuorumListener] {
        &self.listeners
    }

    /// Consumes the node into adapter-owned scalar parts.
    pub fn into_parts(self) -> (i32, Vec<DescribeMetadataQuorumListener>) {
        (self.node_id, self.listeners)
    }
}
