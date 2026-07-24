//! Accepted-call fault retention and exact port-error observations.

use std::io;

use super::{
    result::{AssignedConsumerAccepted, AssignedConsumerAcceptedFaultKind},
    wake::AssignedConsumerShardWakeError,
};

#[test]
fn accepted_call_retains_a_post_commit_wake_failure() {
    let accepted = AssignedConsumerAccepted::new(
        7_u8,
        Err(AssignedConsumerShardWakeError::from_io(io::Error::other(
            "wake unavailable",
        ))),
    );

    assert_eq!(
        accepted.fault(),
        Some(AssignedConsumerAcceptedFaultKind::Wake)
    );
    assert_eq!(accepted.into_value(), 7);
}

#[test]
fn accepted_call_without_a_wake_failure_has_no_fault() {
    let accepted = AssignedConsumerAccepted::new(11_u8, Ok(()));

    assert_eq!(accepted.fault(), None);
    assert_eq!(accepted.into_value(), 11);
}
