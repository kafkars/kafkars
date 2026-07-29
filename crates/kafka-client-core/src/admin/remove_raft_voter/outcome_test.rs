//! Successful and exact voter-removal rejection value scenarios.

use core::num::NonZeroI16;

use crate::DeliveryStatus;

use super::{
    RemoveRaftVoterBrokerError, RemoveRaftVoterFailure, RemoveRaftVoterFailureKind,
    RemoveRaftVoterSuccess,
};

#[test]
fn success_preserves_nonnegative_throttle_observation() {
    assert_eq!(RemoveRaftVoterSuccess::new(17).throttle_time_ms(), 17);
}

#[test]
fn broker_error_preserves_throttle_signed_code_and_nullable_diagnostic() {
    let error = RemoveRaftVoterBrokerError::new(
        23,
        NonZeroI16::new(-41).unwrap_or_else(|| panic!("nonzero")),
        Some("voter rejected".to_owned()),
        true,
    );

    assert_eq!(error.throttle_time_ms(), 23);
    assert_eq!(error.code(), -41);
    assert_eq!(error.message(), Some("voter rejected"));
    assert!(error.message_truncated());
    assert_eq!(
        error.into_parts(),
        (23, -41, Some("voter rejected".to_owned()), true)
    );
}

#[test]
fn mechanism_failure_preserves_kind_and_authoritative_delivery() {
    let failure = RemoveRaftVoterFailure::new(
        RemoveRaftVoterFailureKind::Transport,
        DeliveryStatus::NotSent,
    );

    assert_eq!(failure.kind(), RemoveRaftVoterFailureKind::Transport);
    assert_eq!(failure.delivery(), DeliveryStatus::NotSent);
}
