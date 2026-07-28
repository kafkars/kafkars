//! Declarative facade for current classic-group state observation.

mod error;
mod immediate;
mod model;

pub use error::{GroupConsumerStateError, GroupConsumerStateErrorKind};
pub use model::{
    GroupConsumerAssignment, GroupConsumerAssignmentPartition, GroupConsumerMetadata,
    GroupConsumerState,
};
