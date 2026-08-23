//! Closed tracked-call adapter for `ShareGroupHeartbeat` v1.

mod adapter;
mod submission;
#[cfg(test)]
mod submission_test;

#[expect(
    unused_imports,
    reason = "the hosted share membership owner lands in the next checkpoint"
)]
pub(crate) use adapter::{
    ShareGroupHeartbeatCall, ShareGroupHeartbeatCompletionError, ShareGroupHeartbeatResolution,
    ShareGroupHeartbeatRoute,
};
#[expect(
    unused_imports,
    reason = "the hosted share membership owner lands in the next checkpoint"
)]
pub(crate) use submission::ShareGroupHeartbeatSubmitErrorKind;
