//! Shard terminal diagnostic scenarios.

use std::error::Error;

use crate::completion::CompletionRegistryError;

use super::terminal::ProducerShardTerminalError;

#[test]
fn completion_failure_preserves_its_typed_source() {
    let error = ProducerShardTerminalError::from(CompletionRegistryError::UnsettledCompletion);

    assert_eq!(
        error.to_string(),
        CompletionRegistryError::UnsettledCompletion.to_string()
    );
    assert!(error.source().is_some());
}
