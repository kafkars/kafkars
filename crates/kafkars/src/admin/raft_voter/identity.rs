//! Stable metadata-quorum voter identity.

/// One Kafka metadata-quorum voter and its storage-directory identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RaftVoterIdentity {
    voter_id: i32,
    directory_id: [u8; 16],
}

impl RaftVoterIdentity {
    /// Creates inert voter identity validated when an operation is submitted.
    pub const fn new(voter_id: i32, directory_id: [u8; 16]) -> Self {
        Self {
            voter_id,
            directory_id,
        }
    }

    /// Returns Kafka's signed voter ID.
    pub const fn voter_id(&self) -> i32 {
        self.voter_id
    }

    /// Returns the exact Kafka storage-directory UUID bytes.
    pub const fn directory_id(&self) -> [u8; 16] {
        self.directory_id
    }

    pub(crate) const fn into_parts(self) -> (i32, [u8; 16]) {
        (self.voter_id, self.directory_id)
    }
}
