//! Shared plaintext environment, bounded waiting, and topic cleanup for opt-in cluster tests.

mod environment;
mod error;
mod operation;
mod topic;

pub(crate) use environment::client_builder_from_environment;
pub(crate) use error::TestError;
pub(crate) use operation::{OPERATION_TIMEOUT, wait_within};
pub(crate) use topic::{TopicCleanup, create_topics, delete_topics, ready_client, unique_name};
