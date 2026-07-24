//! Declarative facade for concrete direct-consumer position execution.

mod owner;

pub(super) use owner::{
    PositionExecutionError, PositionResolutionExecutor, PositionSubmission,
    PreparedPositionResolution,
};

#[cfg(test)]
mod close_test;
#[cfg(test)]
mod fence_test;
#[cfg(test)]
mod owner_test;
#[cfg(test)]
mod ownership_test;

#[cfg(test)]
pub(super) use owner_test::{assignment, resolve_fence};
