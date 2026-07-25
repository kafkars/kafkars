//! Group wake failure retains its operating-system diagnostic.

use std::{error::Error, io};

use super::registry_wake::GroupConsumerShardWakeError;

#[test]
fn wake_error_preserves_message_and_source() {
    let error = GroupConsumerShardWakeError::from_io(io::Error::other("group wake closed"));

    assert_eq!(
        error.to_string(),
        "group-consumer shard wake failed: group wake closed"
    );
    assert_eq!(
        error.source().map(ToString::to_string),
        Some("group wake closed".to_owned())
    );
}
