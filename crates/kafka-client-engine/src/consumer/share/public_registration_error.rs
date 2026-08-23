//! Lossless public failure vocabulary for share-member registration.

use std::{sync::Arc, time::Duration};

use crate::EngineShareConsumerFetchConfig;

use super::{
    port::{
        ShareRegistrationPortFailureSource,
        ShareRegistrationPortFailureSource::{Closed, Lock, Registry},
    },
    public_registration::ShareConsumerRegistration,
    registry_registration::ShareConsumerRegistrationFailureKind,
    shard::ShareConsumerShardLockError,
};

/// Stable reason share registration did not transfer ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShareConsumerRegistrationErrorKind {
    /// Engine-wide share admission has closed.
    Closed,
    /// Another owner temporarily holds the share registry.
    Contended,
    /// A bounded member or retained-name capacity is full.
    Backpressure,
    /// A group, rack, topic, or timeout is invalid.
    InvalidInput,
    /// Engine ownership could not be preserved consistently.
    Internal,
}

/// Rejected registration retaining the exact caller-owned request.
#[derive(Debug)]
#[must_use = "registration rejection retains the exact request"]
pub struct ShareConsumerRegistrationError {
    kind: ShareConsumerRegistrationErrorKind,
    registration: Box<ShareConsumerRegistration>,
}

impl ShareConsumerRegistrationError {
    /// Returns the stable pre-admission rejection category.
    pub const fn kind(&self) -> ShareConsumerRegistrationErrorKind {
        self.kind
    }

    /// Recovers the exact registration whose ownership did not transfer.
    pub fn into_registration(self) -> ShareConsumerRegistration {
        *self.registration
    }
}

impl core::fmt::Display for ShareConsumerRegistrationError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(formatter, "share registration rejected: {:?}", self.kind)
    }
}

impl std::error::Error for ShareConsumerRegistrationError {}

pub(super) fn registration_error(
    kind: ShareConsumerRegistrationErrorKind,
    group: Arc<str>,
    rack: Option<Arc<str>>,
    topics: Vec<Arc<str>>,
    fetch: EngineShareConsumerFetchConfig,
    close_timeout: Duration,
) -> ShareConsumerRegistrationError {
    ShareConsumerRegistrationError {
        kind,
        registration: Box::new(ShareConsumerRegistration {
            group,
            rack,
            topics,
            fetch,
            close_timeout,
        }),
    }
}

pub(super) const fn registration_error_kind(
    source: ShareRegistrationPortFailureSource,
) -> ShareConsumerRegistrationErrorKind {
    match source {
        Closed => ShareConsumerRegistrationErrorKind::Closed,
        Lock(ShareConsumerShardLockError::Contended) => {
            ShareConsumerRegistrationErrorKind::Contended
        }
        Lock(ShareConsumerShardLockError::Poisoned)
        | Registry(
            ShareConsumerRegistrationFailureKind::Closed
            | ShareConsumerRegistrationFailureKind::IdentityExhausted
            | ShareConsumerRegistrationFailureKind::Allocation,
        ) => ShareConsumerRegistrationErrorKind::Internal,
        Registry(
            ShareConsumerRegistrationFailureKind::Capacity
            | ShareConsumerRegistrationFailureKind::RetainedBytes,
        ) => ShareConsumerRegistrationErrorKind::Backpressure,
        Registry(ShareConsumerRegistrationFailureKind::InvalidInput) => {
            ShareConsumerRegistrationErrorKind::InvalidInput
        }
    }
}
