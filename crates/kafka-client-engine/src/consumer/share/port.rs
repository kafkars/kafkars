//! Capture-first share registration and membership-start admission port.

use std::{sync::Arc, time::Duration};

use kafka_client_core::GroupId;

use super::{
    close_state::ShareConsumerCloseCompletion,
    registry_close::ShareConsumerCloseAdmissionError,
    registry_registration::{
        ShareConsumerRegistrationFailure, ShareConsumerRegistrationFailureKind,
        ShareConsumerStartError,
    },
    shard::{ShareConsumerShardLockError, ShareConsumerShardState},
    shard_wake::ShareConsumerShardWakeError,
};
use crate::clock::{ClockError, DeadlineCapture};

#[derive(Clone)]
pub(crate) struct ShareConsumerPort {
    pub(super) shared: Arc<ShareConsumerShardState>,
}

impl ShareConsumerPort {
    pub(crate) fn capture_deadline_after(
        &self,
        timeout: Duration,
    ) -> Result<DeadlineCapture, ClockError> {
        self.shared.clock().capture_deadline_after(timeout)
    }

    pub(crate) fn try_register(
        &self,
        group: Arc<str>,
        rack: Option<Arc<str>>,
        topics: Vec<Arc<str>>,
    ) -> Result<ShareRegistrationAdmission, ShareRegistrationPortFailure> {
        if self.shared.admission_is_closed() {
            return Err(ShareRegistrationPortFailure::closed(group, rack, topics));
        }
        let mut registry = match self.shared.try_registry() {
            Ok(registry) => registry,
            Err(kind) => {
                return Err(ShareRegistrationPortFailure::lock(
                    kind, group, rack, topics,
                ));
            }
        };
        if self.shared.admission_is_closed() {
            return Err(ShareRegistrationPortFailure::closed(group, rack, topics));
        }
        let group_id = registry
            .try_register(group, rack, topics)
            .map_err(ShareRegistrationPortFailure::registry)?;
        drop(registry);
        Ok(ShareRegistrationAdmission {
            group_id,
            wake: self.shared.request_turn().err(),
        })
    }

    pub(crate) fn try_begin(
        &self,
        group_id: GroupId,
        capture: DeadlineCapture,
    ) -> Result<ShareStartAdmission, ShareStartPortError> {
        if self.shared.admission_is_closed() {
            return Err(ShareStartPortError::Closed);
        }
        let mut registry = self
            .shared
            .try_registry()
            .map_err(ShareStartPortError::Lock)?;
        if self.shared.admission_is_closed() {
            return Err(ShareStartPortError::Closed);
        }
        registry
            .try_begin(group_id, capture)
            .map_err(ShareStartPortError::Registry)?;
        drop(registry);
        Ok(ShareStartAdmission {
            wake: self.shared.request_turn().err(),
        })
    }

    pub(crate) fn try_begin_close(
        &self,
        group_id: GroupId,
        timeout: Duration,
    ) -> Result<ShareCloseAdmission, ShareClosePortError> {
        let capture = self
            .capture_deadline_after(timeout)
            .map_err(ShareClosePortError::Clock)?;
        if self.shared.admission_is_closed() {
            return Err(ShareClosePortError::Closed);
        }
        let mut registry = self
            .shared
            .try_registry()
            .map_err(ShareClosePortError::Lock)?;
        if self.shared.admission_is_closed() {
            return Err(ShareClosePortError::Closed);
        }
        let completion = registry
            .begin_explicit_close(group_id, capture)
            .map_err(ShareClosePortError::Registry)?;
        drop(registry);
        Ok(ShareCloseAdmission {
            completion,
            wake: self.shared.request_turn().err(),
        })
    }

    pub(crate) fn request_control_close(&self, timeout: Duration) -> Result<(), ClockError> {
        let capture = self.capture_deadline_after(timeout)?;
        self.shared.close_admission();
        let mut registry = self.shared.control_registry();
        registry.request_control_close(capture);
        drop(registry);
        let _wake = self.shared.request_turn();
        Ok(())
    }
}

#[must_use = "accepted share registration retains any wake failure"]
pub(crate) struct ShareRegistrationAdmission {
    pub(super) group_id: GroupId,
    pub(super) wake: Option<ShareConsumerShardWakeError>,
}

impl ShareRegistrationAdmission {
    pub(crate) const fn group_id(&self) -> GroupId {
        self.group_id
    }

    pub(crate) const fn wake_failed(&self) -> bool {
        self.wake.is_some()
    }
}

#[must_use = "accepted share start retains any wake failure"]
pub(crate) struct ShareStartAdmission {
    wake: Option<ShareConsumerShardWakeError>,
}

impl ShareStartAdmission {
    pub(crate) const fn wake_failed(&self) -> bool {
        self.wake.is_some()
    }
}

#[must_use = "accepted share close retains its exact terminal observer"]
pub(crate) struct ShareCloseAdmission {
    pub(crate) completion: ShareConsumerCloseCompletion,
    wake: Option<ShareConsumerShardWakeError>,
}

impl ShareCloseAdmission {
    pub(crate) const fn wake_failed(&self) -> bool {
        self.wake.is_some()
    }
}

#[must_use = "share registration failure returns the exact caller-owned names"]
pub(crate) struct ShareRegistrationPortFailure {
    pub(crate) source: ShareRegistrationPortFailureSource,
    pub(crate) group: Arc<str>,
    pub(crate) rack: Option<Arc<str>>,
    pub(crate) topics: Vec<Arc<str>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ShareRegistrationPortFailureSource {
    Closed,
    Lock(ShareConsumerShardLockError),
    Registry(ShareConsumerRegistrationFailureKind),
}

impl ShareRegistrationPortFailure {
    pub(super) fn closed(group: Arc<str>, rack: Option<Arc<str>>, topics: Vec<Arc<str>>) -> Self {
        Self {
            source: ShareRegistrationPortFailureSource::Closed,
            group,
            rack,
            topics,
        }
    }

    pub(super) fn lock(
        kind: ShareConsumerShardLockError,
        group: Arc<str>,
        rack: Option<Arc<str>>,
        topics: Vec<Arc<str>>,
    ) -> Self {
        Self {
            source: ShareRegistrationPortFailureSource::Lock(kind),
            group,
            rack,
            topics,
        }
    }

    pub(super) fn registry(failure: ShareConsumerRegistrationFailure) -> Self {
        Self {
            source: ShareRegistrationPortFailureSource::Registry(failure.kind),
            group: failure.group,
            rack: failure.rack,
            topics: failure.topics,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ShareStartPortError {
    Closed,
    Lock(ShareConsumerShardLockError),
    Registry(ShareConsumerStartError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ShareClosePortError {
    Closed,
    Clock(ClockError),
    Lock(ShareConsumerShardLockError),
    Registry(ShareConsumerCloseAdmissionError),
}
