//! Exact-broker tracked-call boundary for `ShareAcknowledge` v1.

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

pub(crate) use {
    call::{ShareAcknowledgeCall, ShareAcknowledgeCompletionErrorKind},
    failure::ShareAcknowledgeDriverFailureKind,
    route::ShareAcknowledgeRoute,
    submission::ShareAcknowledgeDriverSubmitErrorKind,
    terminal::ShareAcknowledgeResolution,
};
