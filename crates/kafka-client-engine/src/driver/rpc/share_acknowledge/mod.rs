//! Exact-broker tracked-call boundary for `ShareAcknowledge` v1.
#![allow(
    dead_code,
    reason = "the tracked-call checkpoint precedes hosted acknowledgement ownership"
)]

mod call;
#[cfg(test)]
mod call_test;
mod failure;
#[cfg(test)]
mod failure_test;
mod route;
mod submission;
#[cfg(test)]
mod submission_test;
mod terminal;
#[cfg(test)]
mod terminal_test;

#[expect(
    unused_imports,
    reason = "hosted ShareAcknowledge execution lands in the next checkpoint"
)]
pub(crate) use {
    call::{ShareAcknowledgeCall, ShareAcknowledgeCompletionErrorKind},
    failure::ShareAcknowledgeDriverFailureKind,
    route::ShareAcknowledgeRoute,
    submission::ShareAcknowledgeDriverSubmitErrorKind,
    terminal::ShareAcknowledgeResolution,
};
