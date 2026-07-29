//! Declarative facade for removing one Kafka metadata-quorum voter.

mod builder;
mod operation;
mod result;

pub use builder::RemoveRaftVoterBuilder;
pub use operation::RemoveRaftVoter;
pub use result::RemoveRaftVoterResult;

#[cfg(test)]
mod builder_test;
#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod result_test;
