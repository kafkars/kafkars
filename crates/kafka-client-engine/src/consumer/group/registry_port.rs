//! Capture-first private group registration and membership-cycle admission.

use std::{sync::Arc, time::Duration};

use kafka_client_core::{ClassicGroupTiming, ClassicHeartbeatPolicy, GroupId, MembershipCycle};

use crate::clock::{ClockError, DeadlineCapture, MonotonicClock};

use super::{
    registry::GroupConsumerRegistrationFailureKind,
    registry_cycle::GroupConsumerCycleAdmissionError,
    registry_shard::{GroupConsumerShardLockError, GroupConsumerShardState},
    registry_wake::GroupConsumerShardWakeError,
};

#[derive(Clone)]
pub(crate) struct GroupConsumerPort {
    pub(super) shared: Arc<GroupConsumerShardState>,
    pub(super) clock: Arc<MonotonicClock>,
}

impl GroupConsumerPort {
    pub(crate) fn try_register(
        &self,
        group: Arc<str>,
        local_topics: Vec<Arc<str>>,
        timing: ClassicGroupTiming,
        heartbeat_policy: ClassicHeartbeatPolicy,
    ) -> Result<GroupId, GroupConsumerPortRegistrationFailure> {
        if self.shared.admission_is_closed() {
            return Err(registration_failure(
                GroupConsumerPortRegistrationFailureKind::CLOSED,
                group,
                local_topics,
            ));
        }
        let mut registry = match self.shared.try_registry() {
            Ok(registry) => registry,
            Err(error) => {
                return Err(registration_failure(
                    GroupConsumerPortRegistrationFailureKind::lock(error),
                    group,
                    local_topics,
                ));
            }
        };
        if self.shared.admission_is_closed() {
            return Err(registration_failure(
                GroupConsumerPortRegistrationFailureKind::CLOSED,
                group,
                local_topics,
            ));
        }
        registry
            .try_register(group, local_topics, timing, heartbeat_policy)
            .map_err(|failure| GroupConsumerPortRegistrationFailure {
                kind: GroupConsumerPortRegistrationFailureKind::registry(failure.kind),
                group: failure.group,
                local_topics: failure.local_topics,
            })
    }

    pub(crate) fn begin_cycle(
        &self,
        group_id: GroupId,
        timeout: Duration,
    ) -> Result<GroupConsumerCycleAdmission, GroupConsumerCyclePortError> {
        let capture = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(GroupConsumerCyclePortError::clock)?;
        self.admit_captured_cycle(group_id, capture)
    }

    fn admit_captured_cycle(
        &self,
        group_id: GroupId,
        capture: DeadlineCapture,
    ) -> Result<GroupConsumerCycleAdmission, GroupConsumerCyclePortError> {
        if self.shared.admission_is_closed() {
            return Err(GroupConsumerCyclePortError::CLOSED);
        }
        let mut registry = self
            .shared
            .try_registry()
            .map_err(GroupConsumerCyclePortError::lock)?;
        if self.shared.admission_is_closed() {
            return Err(GroupConsumerCyclePortError::CLOSED);
        }
        let cycle = registry
            .try_begin_classic_cycle(group_id, capture)
            .map_err(GroupConsumerCyclePortError::registry)?;
        drop(registry);
        Ok(GroupConsumerCycleAdmission {
            cycle,
            wake: self.shared.request_turn().err(),
        })
    }

    pub(crate) fn close_admission(&self) {
        self.shared.close_admission();
        let _wake_result = self.shared.request_turn();
    }
}

#[must_use = "accepted membership retains any post-commit wake failure"]
pub(crate) struct GroupConsumerCycleAdmission {
    cycle: MembershipCycle,
    wake: Option<GroupConsumerShardWakeError>,
}

impl GroupConsumerCycleAdmission {
    pub(crate) const fn cycle(&self) -> MembershipCycle {
        self.cycle
    }

    pub(crate) const fn wake_failed(&self) -> bool {
        self.wake.is_some()
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) struct GroupConsumerCyclePortError {
    kind: GroupConsumerCyclePortErrorKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GroupConsumerCyclePortErrorKind {
    Clock(ClockError),
    Closed,
    Lock(GroupConsumerShardLockError),
    Registry(GroupConsumerCycleAdmissionError),
}

impl GroupConsumerCyclePortError {
    pub(crate) const CLOSED: Self = Self {
        kind: GroupConsumerCyclePortErrorKind::Closed,
    };

    const fn clock(error: ClockError) -> Self {
        Self {
            kind: GroupConsumerCyclePortErrorKind::Clock(error),
        }
    }

    const fn lock(error: GroupConsumerShardLockError) -> Self {
        Self {
            kind: GroupConsumerCyclePortErrorKind::Lock(error),
        }
    }

    const fn registry(error: GroupConsumerCycleAdmissionError) -> Self {
        Self {
            kind: GroupConsumerCyclePortErrorKind::Registry(error),
        }
    }
}

impl core::fmt::Debug for GroupConsumerCyclePortError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self.kind {
            GroupConsumerCyclePortErrorKind::Clock(error) => {
                formatter.debug_tuple("Clock").field(&error).finish()
            }
            GroupConsumerCyclePortErrorKind::Closed => formatter.write_str("Closed"),
            GroupConsumerCyclePortErrorKind::Lock(error) => {
                formatter.debug_tuple("Lock").field(&error).finish()
            }
            GroupConsumerCyclePortErrorKind::Registry(error) => {
                formatter.debug_tuple("Registry").field(&error).finish()
            }
        }
    }
}

#[must_use = "registration rejection retains the exact caller-owned names"]
pub(crate) struct GroupConsumerPortRegistrationFailure {
    pub(crate) kind: GroupConsumerPortRegistrationFailureKind,
    pub(crate) group: Arc<str>,
    pub(crate) local_topics: Vec<Arc<str>>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) struct GroupConsumerPortRegistrationFailureKind {
    kind: GroupConsumerPortRegistrationFailureReason,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GroupConsumerPortRegistrationFailureReason {
    Closed,
    Lock(GroupConsumerShardLockError),
    Registry(GroupConsumerRegistrationFailureKind),
}

impl GroupConsumerPortRegistrationFailureKind {
    pub(crate) const CLOSED: Self = Self {
        kind: GroupConsumerPortRegistrationFailureReason::Closed,
    };

    const fn lock(error: GroupConsumerShardLockError) -> Self {
        Self {
            kind: GroupConsumerPortRegistrationFailureReason::Lock(error),
        }
    }

    const fn registry(error: GroupConsumerRegistrationFailureKind) -> Self {
        Self {
            kind: GroupConsumerPortRegistrationFailureReason::Registry(error),
        }
    }
}

impl core::fmt::Debug for GroupConsumerPortRegistrationFailureKind {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self.kind {
            GroupConsumerPortRegistrationFailureReason::Closed => formatter.write_str("Closed"),
            GroupConsumerPortRegistrationFailureReason::Lock(error) => {
                formatter.debug_tuple("Lock").field(&error).finish()
            }
            GroupConsumerPortRegistrationFailureReason::Registry(error) => {
                formatter.debug_tuple("Registry").field(&error).finish()
            }
        }
    }
}

fn registration_failure(
    kind: GroupConsumerPortRegistrationFailureKind,
    group: Arc<str>,
    local_topics: Vec<Arc<str>>,
) -> GroupConsumerPortRegistrationFailure {
    GroupConsumerPortRegistrationFailure {
        kind,
        group,
        local_topics,
    }
}
