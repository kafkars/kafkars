//! Stable identities and endpoints shared by Kafka quorum-voter operations.

mod endpoint;
mod identity;

pub use endpoint::RaftVoterEndpoint;
pub use identity::RaftVoterIdentity;

#[cfg(test)]
mod endpoint_test;
#[cfg(test)]
mod identity_test;
