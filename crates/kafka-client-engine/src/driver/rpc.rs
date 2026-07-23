//! Declarative boundary for concrete generated RPC ownership.

mod calls;
#[cfg(test)]
mod calls_test;
#[allow(
    dead_code,
    reason = "engine-host turns consume tracked create-topics calls next"
)]
mod create_topics_calls;
#[cfg(test)]
mod create_topics_calls_test;
mod create_topics_submission;
#[cfg(test)]
mod create_topics_submission_test;
mod create_topics_terminal;
#[cfg(test)]
mod create_topics_terminal_test;
mod submission;
#[cfg(test)]
mod submission_test;

pub(crate) use calls::{ProduceCompletionFailure, TrackedProduceCalls};
#[allow(
    unused_imports,
    reason = "engine-host turns consume tracked create-topics calls next"
)]
pub(crate) use create_topics_calls::{CreateTopicsCompletionFailure, TrackedCreateTopicsCalls};
pub(crate) use submission::ProduceSubmitError;
