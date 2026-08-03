//! Declarative facade for current classic-group state observation.

mod error;
#[cfg(test)]
mod error_test;
mod immediate;
mod model;
mod operation;

pub use error::{
    GroupConsumerNextEventError, GroupConsumerNextEventErrorKind,
    GroupConsumerRevocationAcknowledgeError, GroupConsumerRevocationAcknowledgeErrorKind,
    GroupConsumerStateError, GroupConsumerStateErrorKind, GroupConsumerTryTakeEventError,
    GroupConsumerTryTakeEventErrorKind,
};
pub use immediate::GroupConsumerRevocationControl;
pub use model::{
    GroupConsumerAssignment, GroupConsumerAssignmentPartition, GroupConsumerEvent,
    GroupConsumerMembershipEpoch, GroupConsumerMetadata, GroupConsumerState,
};
pub use operation::GroupConsumerNextEvent;

#[cfg(test)]
mod immediate_test;
