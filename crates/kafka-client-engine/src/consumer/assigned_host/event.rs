//! Declarative public boundary for immediate assigned-consumer failure events.

mod error;
mod model;
mod translate;

#[cfg(test)]
mod handle_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod translate_test;

pub use error::{AssignedConsumerTryTakeEventError, AssignedConsumerTryTakeEventErrorKind};
pub use model::{
    AssignedConsumerEvent, AssignedConsumerFetchFailure, AssignedConsumerFetchFailureKind,
    AssignedConsumerFetchFence, AssignedConsumerFetchThrottleFailure,
    AssignedConsumerFetchThrottleFailureKind, AssignedConsumerPositionFence,
    AssignedConsumerPositionResolutionFailure, AssignedConsumerPositionResolutionFailureKind,
};
pub(super) use translate::translate_retained_event;
