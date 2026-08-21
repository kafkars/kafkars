//! Stable wire-free values for all Kafka group types.

/// One Kafka group visible across the cluster.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GroupListing {
    group_id: String,
    protocol_type: String,
    group_state: Option<String>,
    group_type: Option<String>,
}

impl GroupListing {
    pub(crate) const fn new(
        group_id: String,
        protocol_type: String,
        group_state: Option<String>,
        group_type: Option<String>,
    ) -> Self {
        Self {
            group_id,
            protocol_type,
            group_state,
            group_type,
        }
    }

    /// Returns the stable group identifier.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns the group protocol type without narrowing to consumer groups.
    pub fn protocol_type(&self) -> &str {
        &self.protocol_type
    }

    /// Returns the state when represented by the selected broker version.
    pub fn group_state(&self) -> Option<&str> {
        self.group_state.as_deref()
    }

    /// Returns the group type when represented by the selected broker version.
    pub fn group_type(&self) -> Option<&str> {
        self.group_type.as_deref()
    }
}

/// Exact top-level `ListGroups` rejection from one discovered broker.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ListGroupsBrokerError {
    broker_id: i32,
    code: i16,
}

impl ListGroupsBrokerError {
    pub(crate) const fn new(broker_id: i32, code: i16) -> Self {
        Self { broker_id, code }
    }

    /// Returns the exact broker identity.
    pub const fn broker_id(self) -> i32 {
        self.broker_id
    }

    /// Returns Kafka's exact signed error code.
    pub const fn code(self) -> i16 {
        self.code
    }
}
