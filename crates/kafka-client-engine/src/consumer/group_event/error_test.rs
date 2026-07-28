//! Stable graceful-revocation acknowledgment error categories.

use crate::{
    clock::ClockError,
    consumer::{
        ClassicGroupRevocationAcknowledgeError, GroupConsumerRevocationPortError,
        GroupConsumerShardLockError,
    },
};
use kafka_client_core::ClassicGracefulRevocationError;

use super::{
    GroupConsumerRevocationAcknowledgeError, GroupConsumerRevocationAcknowledgeErrorKind as Kind,
};

#[test]
fn revocation_port_failures_have_exhaustive_public_categories() {
    for (port, kind) in [
        (GroupConsumerRevocationPortError::Closed, Kind::Closed),
        (
            GroupConsumerRevocationPortError::Clock(ClockError::InstantOverflow),
            Kind::Clock,
        ),
        (
            GroupConsumerRevocationPortError::Lock(GroupConsumerShardLockError::Contended),
            Kind::Contended,
        ),
        (
            GroupConsumerRevocationPortError::Lock(GroupConsumerShardLockError::Poisoned),
            Kind::HostUnavailable,
        ),
        (
            GroupConsumerRevocationPortError::UnknownGroup,
            Kind::GroupUnavailable,
        ),
        (
            GroupConsumerRevocationPortError::GroupUnavailable,
            Kind::GroupUnavailable,
        ),
        (
            GroupConsumerRevocationPortError::Acknowledge(
                ClassicGroupRevocationAcknowledgeError::NoActiveLease,
            ),
            Kind::StaleAssignmentEpoch,
        ),
        (
            GroupConsumerRevocationPortError::Acknowledge(
                ClassicGroupRevocationAcknowledgeError::AssignmentEpochMismatch,
            ),
            Kind::StaleAssignmentEpoch,
        ),
        (
            GroupConsumerRevocationPortError::Acknowledge(
                ClassicGroupRevocationAcknowledgeError::DeadlineElapsed,
            ),
            Kind::DeadlineElapsed,
        ),
        (
            GroupConsumerRevocationPortError::Acknowledge(
                ClassicGroupRevocationAcknowledgeError::Core(
                    ClassicGracefulRevocationError::NotActive,
                ),
            ),
            Kind::InternalInvariant,
        ),
        (
            GroupConsumerRevocationPortError::Acknowledge(
                ClassicGroupRevocationAcknowledgeError::UnexpectedEffect,
            ),
            Kind::InternalInvariant,
        ),
    ] {
        assert_eq!(
            GroupConsumerRevocationAcknowledgeError::from_port(port).kind(),
            kind
        );
    }
}
