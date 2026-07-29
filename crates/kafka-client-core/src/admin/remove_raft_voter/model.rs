//! Bounded voter identity for one destructive `RemoveRaftVoter` request.

use core::fmt;

/// Maximum UTF-8 bytes accepted in a present cluster identity.
pub const REMOVE_RAFT_VOTER_MAX_CLUSTER_ID_BYTES: usize = i16::MAX as usize;

/// Validated intent for one destructive controller `RemoveRaftVoter` RPC.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoveRaftVoterPlan {
    cluster_id: Option<String>,
    voter_id: i32,
    voter_directory_id: [u8; 16],
}

impl RemoveRaftVoterPlan {
    /// Validates one optional cluster and nonzero voter-directory identity.
    pub fn new(
        cluster_id: Option<String>,
        voter_id: i32,
        voter_directory_id: [u8; 16],
    ) -> Result<Self, RemoveRaftVoterPlanError> {
        if cluster_id.as_deref().is_some_and(str::is_empty) {
            return Err(RemoveRaftVoterPlanError::EmptyClusterId);
        }
        if cluster_id
            .as_ref()
            .is_some_and(|value| value.len() > REMOVE_RAFT_VOTER_MAX_CLUSTER_ID_BYTES)
        {
            return Err(RemoveRaftVoterPlanError::ClusterIdTooLong);
        }
        if voter_id < 0 {
            return Err(RemoveRaftVoterPlanError::NegativeVoterId);
        }
        if voter_directory_id == [0; 16] {
            return Err(RemoveRaftVoterPlanError::ZeroVoterDirectoryId);
        }
        Ok(Self {
            cluster_id,
            voter_id,
            voter_directory_id,
        })
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

    /// Consumes this plan into adapter-owned scalar parts.
    pub fn into_parts(self) -> (Option<String>, i32, [u8; 16]) {
        (self.cluster_id, self.voter_id, self.voter_directory_id)
    }
}

/// Invalid deterministic voter-removal intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemoveRaftVoterPlanError {
    /// A present cluster identity was empty.
    EmptyClusterId,
    /// A cluster identity exceeded the deterministic scalar bound.
    ClusterIdTooLong,
    /// Kafka voter identities cannot be negative.
    NegativeVoterId,
    /// The all-zero UUID cannot identify a voter directory.
    ZeroVoterDirectoryId,
}

impl fmt::Display for RemoveRaftVoterPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid RemoveRaftVoter plan: {self:?}")
    }
}

impl std::error::Error for RemoveRaftVoterPlanError {}
