//! Shared plaintext environment, bounded waiting, and topic cleanup for opt-in cluster tests.

mod environment;
mod error;
mod operation;
mod topic;

pub(crate) use environment::client_builder_from_environment;
pub(crate) use error::TestError;
#[allow(
    unused_imports,
    reason = "only the qualification matrix bounds repeated flush admission"
)]
pub(crate) use operation::wait_within_for;
pub(crate) use operation::{OPERATION_TIMEOUT, wait_within};
#[allow(
    unused_imports,
    reason = "only the qualification matrix builds replicated-topic readiness"
)]
pub(crate) use topic::wait_for_topic_metadata;
pub(crate) use topic::{TopicCleanup, create_topics, delete_topics, ready_client, unique_name};
