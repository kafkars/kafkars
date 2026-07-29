//! Bounded identity, listener, and acknowledgment intent for one `AddRaftVoter` request.

use core::fmt;
use std::collections::BTreeSet;

/// Maximum number of named endpoints accepted for one voter.
pub const ADD_RAFT_VOTER_MAX_LISTENERS: usize = 128;
/// Maximum UTF-8 bytes accepted in any cluster, listener-name, or host scalar.
pub const ADD_RAFT_VOTER_MAX_TEXT_BYTES: usize = i16::MAX as usize;
/// Maximum aggregate UTF-8 bytes retained by one voter-addition plan.
pub const ADD_RAFT_VOTER_MAX_REQUEST_TEXT_BYTES: usize = 256 * 1024;

/// One validated named endpoint for a voter joining the metadata quorum.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddRaftVoterEndpoint {
    name: String,
    host: String,
    port: u16,
}

impl AddRaftVoterEndpoint {
    /// Creates one endpoint for validation by [`AddRaftVoterPlan`].
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

    /// Consumes the endpoint into adapter-owned scalar parts.
    pub fn into_parts(self) -> (String, String, u16) {
        (self.name, self.host, self.port)
    }
}

/// Validated intent for one controller `AddRaftVoter` RPC.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddRaftVoterPlan {
    cluster_id: Option<String>,
    voter_id: i32,
    voter_directory_id: [u8; 16],
    listeners: Vec<AddRaftVoterEndpoint>,
    ack_when_committed: bool,
}

impl AddRaftVoterPlan {
    /// Validates one voter identity and its bounded, uniquely named endpoints.
    ///
    /// By default Kafka acknowledges only after the new voter set is committed.
    pub fn new(
        cluster_id: Option<String>,
        voter_id: i32,
        voter_directory_id: [u8; 16],
        listeners: Vec<AddRaftVoterEndpoint>,
    ) -> Result<Self, AddRaftVoterPlanError> {
        validate_cluster_id(cluster_id.as_deref())?;
        if voter_id < 0 {
            return Err(AddRaftVoterPlanError::NegativeVoterId);
        }
        if voter_directory_id == [0; 16] {
            return Err(AddRaftVoterPlanError::ZeroVoterDirectoryId);
        }
        if listeners.is_empty() {
            return Err(AddRaftVoterPlanError::EmptyListeners);
        }
        if listeners.len() > ADD_RAFT_VOTER_MAX_LISTENERS {
            return Err(AddRaftVoterPlanError::TooManyListeners);
        }

        let mut listener_names = BTreeSet::new();
        let mut aggregate = cluster_id.as_ref().map_or(0, String::len);
        for listener in &listeners {
            validate_listener(listener)?;
            if !listener_names.insert(listener.name()) {
                return Err(AddRaftVoterPlanError::DuplicateListenerName);
            }
            aggregate = aggregate
                .checked_add(listener.name().len())
                .and_then(|bytes| bytes.checked_add(listener.host().len()))
                .ok_or(AddRaftVoterPlanError::RequestTextBytesExceeded)?;
            if aggregate > ADD_RAFT_VOTER_MAX_REQUEST_TEXT_BYTES {
                return Err(AddRaftVoterPlanError::RequestTextBytesExceeded);
            }
        }

        Ok(Self {
            cluster_id,
            voter_id,
            voter_directory_id,
            listeners,
            ack_when_committed: true,
        })
    }

    /// Replaces whether Kafka waits for the new voter set to be committed.
    pub const fn with_ack_when_committed(mut self, ack_when_committed: bool) -> Self {
        self.ack_when_committed = ack_when_committed;
        self
    }

    /// Returns the optional cluster identity.
    pub fn cluster_id(&self) -> Option<&str> {
        self.cluster_id.as_deref()
    }

    /// Returns the nonnegative voter identity.
    pub const fn voter_id(&self) -> i32 {
        self.voter_id
    }

    /// Returns the nonzero voter directory UUID bytes.
    pub const fn voter_directory_id(&self) -> [u8; 16] {
        self.voter_directory_id
    }

    /// Returns listener endpoints in caller order.
    pub fn listeners(&self) -> &[AddRaftVoterEndpoint] {
        &self.listeners
    }

    /// Returns whether Kafka waits for the new voter set to be committed.
    pub const fn ack_when_committed(&self) -> bool {
        self.ack_when_committed
    }

    /// Returns the earliest API-key 80 version representing this acknowledgment policy.
    pub const fn minimum_api_version(&self) -> i16 {
        if self.ack_when_committed { 0 } else { 1 }
    }

    /// Consumes this plan into adapter-owned scalar parts.
    pub fn into_parts(
        self,
    ) -> (
        Option<String>,
        i32,
        [u8; 16],
        Vec<AddRaftVoterEndpoint>,
        bool,
    ) {
        (
            self.cluster_id,
            self.voter_id,
            self.voter_directory_id,
            self.listeners,
            self.ack_when_committed,
        )
    }
}

fn validate_cluster_id(cluster_id: Option<&str>) -> Result<(), AddRaftVoterPlanError> {
    if cluster_id.is_some_and(str::is_empty) {
        return Err(AddRaftVoterPlanError::EmptyClusterId);
    }
    if cluster_id.is_some_and(|value| value.len() > ADD_RAFT_VOTER_MAX_TEXT_BYTES) {
        return Err(AddRaftVoterPlanError::ClusterIdTooLong);
    }
    Ok(())
}

fn validate_listener(listener: &AddRaftVoterEndpoint) -> Result<(), AddRaftVoterPlanError> {
    if listener.name().is_empty() {
        return Err(AddRaftVoterPlanError::EmptyListenerName);
    }
    if listener.name().len() > ADD_RAFT_VOTER_MAX_TEXT_BYTES {
        return Err(AddRaftVoterPlanError::ListenerNameTooLong);
    }
    if listener.host().is_empty() {
        return Err(AddRaftVoterPlanError::EmptyListenerHost);
    }
    if listener.host().len() > ADD_RAFT_VOTER_MAX_TEXT_BYTES {
        return Err(AddRaftVoterPlanError::ListenerHostTooLong);
    }
    if listener.port() == 0 {
        return Err(AddRaftVoterPlanError::ZeroListenerPort);
    }
    Ok(())
}

/// Invalid deterministic voter-addition intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AddRaftVoterPlanError {
    /// A present cluster identity was empty.
    EmptyClusterId,
    /// A cluster identity exceeded the per-scalar text bound.
    ClusterIdTooLong,
    /// Kafka voter identities cannot be negative.
    NegativeVoterId,
    /// The all-zero UUID cannot identify a voter directory.
    ZeroVoterDirectoryId,
    /// At least one listener endpoint is required.
    EmptyListeners,
    /// The listener count exceeded the deterministic request bound.
    TooManyListeners,
    /// A listener name was empty.
    EmptyListenerName,
    /// A listener name exceeded the per-scalar text bound.
    ListenerNameTooLong,
    /// A listener host was empty.
    EmptyListenerHost,
    /// A listener host exceeded the per-scalar text bound.
    ListenerHostTooLong,
    /// Port zero cannot identify a reachable endpoint.
    ZeroListenerPort,
    /// Listener names must uniquely identify endpoints.
    DuplicateListenerName,
    /// Aggregate request text exceeded the admitted request bound.
    RequestTextBytesExceeded,
}

impl fmt::Display for AddRaftVoterPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid AddRaftVoter plan: {self:?}")
    }
}

impl std::error::Error for AddRaftVoterPlanError {}
