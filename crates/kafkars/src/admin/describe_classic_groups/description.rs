//! Stable public classic group and member facts.

/// One member of a classic group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClassicGroupMember {
    member_id: String,
    group_instance_id: Option<String>,
    client_id: String,
    client_host: String,
    metadata: Vec<u8>,
    assignment: Vec<u8>,
}

impl ClassicGroupMember {
    pub(crate) const fn new(
        member_id: String,
        group_instance_id: Option<String>,
        client_id: String,
        client_host: String,
        metadata: Vec<u8>,
        assignment: Vec<u8>,
    ) -> Self {
        Self {
            member_id,
            group_instance_id,
            client_id,
            client_host,
            metadata,
            assignment,
        }
    }

    /// Returns the stable member ID.
    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    /// Returns the optional static member instance ID.
    pub fn group_instance_id(&self) -> Option<&str> {
        self.group_instance_id.as_deref()
    }

    /// Returns the member's client ID.
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Returns the member's client host.
    pub fn client_host(&self) -> &str {
        &self.client_host
    }

    /// Returns raw metadata for the active classic group protocol.
    pub fn metadata(&self) -> &[u8] {
        &self.metadata
    }

    /// Returns the raw assignment supplied by the classic group leader.
    pub fn assignment(&self) -> &[u8] {
        &self.assignment
    }
}

/// Successful description of one classic group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClassicGroupDescription {
    state: String,
    protocol_type: String,
    protocol_data: String,
    members: Vec<ClassicGroupMember>,
    authorized_operations: Option<i32>,
}

impl ClassicGroupDescription {
    pub(crate) const fn new(
        state: String,
        protocol_type: String,
        protocol_data: String,
        members: Vec<ClassicGroupMember>,
        authorized_operations: Option<i32>,
    ) -> Self {
        Self {
            state,
            protocol_type,
            protocol_data,
            members,
            authorized_operations,
        }
    }

    /// Returns Kafka's exact classic group state string.
    pub fn state(&self) -> &str {
        &self.state
    }

    /// Returns Kafka's classic group protocol type.
    pub fn protocol_type(&self) -> &str {
        &self.protocol_type
    }

    /// Returns Kafka's selected classic protocol data.
    pub fn protocol_data(&self) -> &str {
        &self.protocol_data
    }

    /// Returns members ordered by member ID bytes.
    pub fn members(&self) -> &[ClassicGroupMember] {
        &self.members
    }

    /// Returns the raw authorization bitfield when explicitly requested and represented.
    pub const fn authorized_operations(&self) -> Option<i32> {
        self.authorized_operations
    }
}
