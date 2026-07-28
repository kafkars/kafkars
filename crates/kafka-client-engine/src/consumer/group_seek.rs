//! Declarative terminal-observable seek boundary for one classic-group consumer.

mod admission;
mod completion;
mod error;
mod operation;
mod position;

#[cfg(test)]
mod admission_test;
#[cfg(test)]
mod completion_test;
#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod position_test;

pub use admission::{
    GroupConsumerSeekAdmissionError, GroupConsumerSeekAdmissionErrorKind, GroupConsumerSeekCapture,
};
pub(crate) use completion::{
    GroupConsumerSeekCompletion, GroupConsumerSeekCompletionObservation, GroupConsumerSeekTerminal,
    GroupConsumerSeekTerminalFailure, GroupConsumerSeekTerminalFailureKind,
};
pub use error::{GroupConsumerSeekError, GroupConsumerSeekErrorKind};
pub use operation::GroupConsumerSeek;
pub use position::GroupConsumerSeekPosition;
