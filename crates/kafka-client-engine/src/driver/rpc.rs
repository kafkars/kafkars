//! Declarative boundary for concrete generated RPC ownership.

mod calls;
#[cfg(test)]
mod calls_test;
mod submission;
#[cfg(test)]
mod submission_test;

pub(crate) use calls::{ProduceCompletionFailure, TrackedProduceCalls};
pub(crate) use submission::ProduceSubmitError;
