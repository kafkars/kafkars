//! Engine-owned inert intent for one metadata-quorum voter addition.

use kafka_client_core::{
    AddRaftVoterEndpoint as CoreEndpoint, AddRaftVoterPlan as CorePlan,
    AddRaftVoterPlanError as CorePlanError,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AddRaftVoterPlanFailure {
    Invalid,
    RetainedBytes,
}

/// One named voter endpoint validated only after deadline capture.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddRaftVoterEndpoint {
    name: String,
    host: String,
    port: u16,
}

impl AddRaftVoterEndpoint {
    /// Creates inert endpoint data.
    pub const fn new(name: String, host: String, port: u16) -> Self {
        Self { name, host, port }
    }

    /// Returns the listener name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the listener host.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Returns the listener port.
    pub const fn port(&self) -> u16 {
        self.port
    }

    /// Consumes the endpoint into exact scalar parts.
    pub fn into_parts(self) -> (String, String, u16) {
        (self.name, self.host, self.port)
    }

    fn into_core(self) -> CoreEndpoint {
        let (name, host, port) = self.into_parts();
        CoreEndpoint::new(canonical_string(name), canonical_string(host), port)
    }
}

/// One inert voter-addition request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddRaftVoterRequest {
    cluster_id: Option<String>,
    voter_id: i32,
    voter_directory_id: [u8; 16],
    listeners: Vec<AddRaftVoterEndpoint>,
    ack_when_committed: bool,
}

impl AddRaftVoterRequest {
    /// Creates inert intent. Validation remains deferred until submission.
    pub const fn new(
        cluster_id: Option<String>,
        voter_id: i32,
        voter_directory_id: [u8; 16],
        listeners: Vec<AddRaftVoterEndpoint>,
    ) -> Self {
        Self {
            cluster_id,
            voter_id,
            voter_directory_id,
            listeners,
            ack_when_committed: true,
        }
    }

    /// Replaces whether success waits for the new voter set to be committed.
    pub const fn ack_when_committed(mut self, ack_when_committed: bool) -> Self {
        self.ack_when_committed = ack_when_committed;
        self
    }

    /// Returns the optional cluster identity.
    pub fn cluster_id(&self) -> Option<&str> {
        self.cluster_id.as_deref()
    }

    /// Returns the requested voter identity.
    pub const fn voter_id(&self) -> i32 {
        self.voter_id
    }

    /// Returns the voter directory UUID bytes.
    pub const fn voter_directory_id(&self) -> [u8; 16] {
        self.voter_directory_id
    }

    /// Returns endpoints in caller order.
    pub fn listeners(&self) -> &[AddRaftVoterEndpoint] {
        &self.listeners
    }

    /// Consumes the request into identity and listener parts.
    pub fn into_parts(self) -> (Option<String>, i32, [u8; 16], Vec<AddRaftVoterEndpoint>) {
        (
            self.cluster_id,
            self.voter_id,
            self.voter_directory_id,
            self.listeners,
        )
    }

    pub(crate) fn into_plan(self) -> Result<CorePlan, AddRaftVoterPlanFailure> {
        let ack_when_committed = self.ack_when_committed;
        let (cluster_id, voter_id, voter_directory_id, source) = self.into_parts();
        let mut listeners = Vec::new();
        listeners
            .try_reserve_exact(source.len())
            .map_err(|_| AddRaftVoterPlanFailure::RetainedBytes)?;
        listeners.extend(source.into_iter().map(AddRaftVoterEndpoint::into_core));
        CorePlan::new(
            cluster_id.map(canonical_string),
            voter_id,
            voter_directory_id,
            listeners,
        )
        .map(|plan| plan.with_ack_when_committed(ack_when_committed))
        .map_err(|_error: CorePlanError| AddRaftVoterPlanFailure::Invalid)
    }
}

fn canonical_string(value: String) -> String {
    value.into_boxed_str().into_string()
}
