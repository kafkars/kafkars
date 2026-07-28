//! Exhaustive registration and membership-start result translation.

use kafka_client_engine::{
    GroupConsumerRegistrationError, GroupConsumerRegistrationErrorKind, GroupConsumerStartAccepted,
    GroupConsumerStartError, GroupConsumerStartErrorKind,
};

use crate::{ErrorKind, KafkaError};

pub(super) fn accepted_fault(accepted: GroupConsumerStartAccepted) -> Option<KafkaError> {
    if accepted.entry_faulted() {
        Some(KafkaError::new(
            ErrorKind::Internal,
            "group membership was accepted but retained an engine fault",
        ))
    } else if accepted.wake_failed() {
        Some(KafkaError::new(
            ErrorKind::Internal,
            "group membership was accepted but host wakeup failed",
        ))
    } else {
        None
    }
}

pub(super) fn translate_group_registration(error: &GroupConsumerRegistrationError) -> KafkaError {
    translate_group_registration_kind(error.kind())
}

pub(super) fn translate_group_registration_kind(
    kind: GroupConsumerRegistrationErrorKind,
) -> KafkaError {
    match kind {
        GroupConsumerRegistrationErrorKind::Closed => {
            KafkaError::new(ErrorKind::State, "group-consumer registration is closed")
        }
        GroupConsumerRegistrationErrorKind::Contended => KafkaError::new(
            ErrorKind::Backpressure,
            "group-consumer registration is temporarily contended",
        ),
        GroupConsumerRegistrationErrorKind::Backpressure => KafkaError::new(
            ErrorKind::Backpressure,
            "bounded group-consumer registration capacity is full",
        ),
        GroupConsumerRegistrationErrorKind::InvalidInput => KafkaError::new(
            ErrorKind::Configuration,
            "group id, subscription, or processing timeout is outside the supported bounded domain",
        ),
        GroupConsumerRegistrationErrorKind::Internal => KafkaError::new(
            ErrorKind::Internal,
            "group-consumer registration ownership is unavailable",
        ),
    }
}

pub(crate) fn translate_group_start(error: GroupConsumerStartError) -> KafkaError {
    match error.kind() {
        GroupConsumerStartErrorKind::Closed => {
            KafkaError::new(ErrorKind::State, "group membership admission is closed")
        }
        GroupConsumerStartErrorKind::Contended => KafkaError::new(
            ErrorKind::Backpressure,
            "group membership owner is temporarily contended",
        ),
        GroupConsumerStartErrorKind::AlreadyStarted => {
            KafkaError::new(ErrorKind::State, "group membership has already started")
        }
        GroupConsumerStartErrorKind::GroupUnavailable => KafkaError::new(
            ErrorKind::State,
            "registered group is closing or unavailable",
        ),
        GroupConsumerStartErrorKind::InvalidTimeout => KafkaError::new(
            ErrorKind::Configuration,
            "group membership timeout is outside the supported deadline domain",
        ),
        GroupConsumerStartErrorKind::Internal => KafkaError::new(
            ErrorKind::Internal,
            "group membership ownership is unavailable",
        ),
    }
}
