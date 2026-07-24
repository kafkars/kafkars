//! Accepted-call fault retention and exact port-error observations.

use std::io;

use super::{
    result::{
        AssignedConsumerAccepted, AssignedConsumerPortAcceptedFaultKind, AssignedConsumerPortError,
        AssignedConsumerTryCloseError, AssignedConsumerTryCloseErrorKind,
    },
    shard::AssignedConsumerShardLockError,
    wake::AssignedConsumerShardWakeError,
};
use crate::completion::CompletionRegistryError;

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
        Some(AssignedConsumerPortAcceptedFaultKind::Wake)
    );
    assert_eq!(accepted.into_value(), 7);
}

#[test]
fn accepted_call_without_a_wake_failure_has_no_fault() {
    let accepted = AssignedConsumerAccepted::new(11_u8, Ok(()));

    assert_eq!(accepted.fault(), None);
    assert_eq!(accepted.into_value(), 11);
}

#[test]
fn close_admission_translates_private_ownership_failures() {
    let cases = [
        (
            AssignedConsumerPortError::Closed,
            AssignedConsumerTryCloseErrorKind::Closed,
        ),
        (
            AssignedConsumerPortError::Lock(AssignedConsumerShardLockError::Contended),
            AssignedConsumerTryCloseErrorKind::Contended,
        ),
        (
            AssignedConsumerPortError::Owner {
                error:
                    super::super::assigned_owner_model::AssignedConsumerOwnerError::EffectsPending,
                wake: None,
            },
            AssignedConsumerTryCloseErrorKind::Pending,
        ),
        (
            AssignedConsumerPortError::Owner {
                error: super::super::assigned_owner_model::AssignedConsumerOwnerError::Completion(
                    CompletionRegistryError::Full,
                ),
                wake: None,
            },
            AssignedConsumerTryCloseErrorKind::CompletionCapacity,
        ),
        (
            AssignedConsumerPortError::Lock(AssignedConsumerShardLockError::OwnerMissing),
            AssignedConsumerTryCloseErrorKind::HostUnavailable,
        ),
    ];

    for (private, expected) in cases {
        assert_eq!(
            AssignedConsumerTryCloseError::from_port(&private).kind(),
            expected
        );
    }
}
