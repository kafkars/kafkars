//! Stable metadata-quorum node and listener facts.

use super::MetadataQuorumListener;

/// One quorum node and its canonically ordered listeners.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataQuorumNode {
    node_id: i32,
    listeners: Vec<MetadataQuorumListener>,
}

impl MetadataQuorumNode {
    pub(crate) const fn new(node_id: i32, listeners: Vec<MetadataQuorumListener>) -> Self {
        Self { node_id, listeners }
    }

    /// Returns the nonnegative quorum node identity.
    pub const fn node_id(&self) -> i32 {
        self.node_id
    }

    /// Returns listeners in strict UTF-8 byte order by name.
    pub fn listeners(&self) -> &[MetadataQuorumListener] {
        &self.listeners
    }

    /// Consumes this node into stable generated-free parts.
    pub fn into_parts(self) -> (i32, Vec<MetadataQuorumListener>) {
        (self.node_id, self.listeners)
    }
}
