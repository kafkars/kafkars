//! Assignment-fenced group checkpoint values and catalog identities.

mod checkpoint;
mod identity;

pub use checkpoint::{
    GroupCheckpoint, GroupCheckpointEntry, GroupCheckpointEntryError, GroupCheckpointError,
};
pub use identity::{AssignmentGeneration, GroupId, MemberId};

#[cfg(test)]
mod checkpoint_test;
