//! Assigned-consumer wake errors preserve their concrete I/O context.

use std::io;

use super::wake::AssignedConsumerShardWakeError;

#[test]
fn assigned_wake_error_names_the_concrete_domain() {
    let error = AssignedConsumerShardWakeError::from_io(io::Error::new(
        io::ErrorKind::BrokenPipe,
        "test wake closed",
    ));

    assert_eq!(
        error.to_string(),
        "assigned-consumer shard wake failed: test wake closed"
    );
}
