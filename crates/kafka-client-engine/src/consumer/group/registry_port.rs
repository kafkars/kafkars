//! Capture-first private group registration and membership-cycle admission.

use std::{sync::Arc, time::Duration};

use kafka_client_core::{GroupId, MembershipCycle};

use crate::clock::{ClockError, DeadlineCapture, MonotonicClock};

use super::{
    classic_group_execution::ClassicGroupExecutionError,
    consumer_group_execution::ConsumerGroupExecutionAdmissionError,
    registry_cycle::{GroupConsumerCycleAcceptance, GroupConsumerCycleAdmissionError},
    registry_shard::{GroupConsumerShardLockError, GroupConsumerShardState},
    registry_wake::GroupConsumerShardWakeError,
};

pub(crate) use super::registry_port_registration::GroupConsumerPortRegistrationCategory;
#[cfg(test)]
pub(crate) use super::registry_port_registration::GroupConsumerPortRegistrationFailureKind;

#[derive(Clone)]
pub(crate) struct GroupConsumerPort {
    pub(super) shared: Arc<GroupConsumerShardState>,
    pub(super) clock: Arc<MonotonicClock>,
}

impl GroupConsumerPort {
    pub(in crate::consumer) fn capture_cycle_deadline(
        &self,
        timeout: Duration,
    ) -> Result<DeadlineCapture, ClockError> {
        self.clock.capture_deadline_after(timeout)
    }

    pub(in crate::consumer) fn shares_registry_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.shared, &other.shared)
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

    pub(in crate::consumer) fn admit_captured_cycle(
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
        let acceptance = registry
            .try_begin_cycle(group_id, capture)
            .map_err(GroupConsumerCyclePortError::registry)?;
        let entry_faulted = registry
            .entries
            .iter()
            .find(|entry| entry.group_id() == group_id)
            .is_some_and(|entry| entry.fault.is_some());
        drop(registry);
        Ok(GroupConsumerCycleAdmission {
            classic_cycle: match acceptance {
                GroupConsumerCycleAcceptance::Classic(cycle) => Some(cycle),
                GroupConsumerCycleAcceptance::Consumer => None,
            },
            entry_faulted,
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
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "classic cycle identity remains engine-private")
    )]
    classic_cycle: Option<MembershipCycle>,
    entry_faulted: bool,
    wake: Option<GroupConsumerShardWakeError>,
}

impl GroupConsumerCycleAdmission {
    #[cfg(test)]
    pub(crate) const fn cycle(&self) -> MembershipCycle {
        match self.classic_cycle {
            Some(cycle) => cycle,
            None => panic!("classic cycle requested for a consumer-protocol admission"),
        }
    }

    pub(crate) const fn wake_failed(&self) -> bool {
        self.wake.is_some()
    }

    pub(crate) const fn entry_faulted(&self) -> bool {
        self.entry_faulted
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupConsumerCyclePortErrorCategory {
    InvalidTimeout,
    Closed,
    Contended,
    AlreadyStarted,
    GroupUnavailable,
    InternalInvariant,
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

    pub(crate) const fn public_category(self) -> GroupConsumerCyclePortErrorCategory {
        match self.kind {
            GroupConsumerCyclePortErrorKind::Clock(_) => {
                GroupConsumerCyclePortErrorCategory::InvalidTimeout
            }
            GroupConsumerCyclePortErrorKind::Closed
            | GroupConsumerCyclePortErrorKind::Registry(
                GroupConsumerCycleAdmissionError::RegistryClosed,
            ) => GroupConsumerCyclePortErrorCategory::Closed,
            GroupConsumerCyclePortErrorKind::Lock(GroupConsumerShardLockError::Contended) => {
                GroupConsumerCyclePortErrorCategory::Contended
            }
            GroupConsumerCyclePortErrorKind::Registry(
                GroupConsumerCycleAdmissionError::Execution(ClassicGroupExecutionError::Occupied)
                | GroupConsumerCycleAdmissionError::ConsumerExecution(
                    ConsumerGroupExecutionAdmissionError::Occupied,
                ),
            ) => GroupConsumerCyclePortErrorCategory::AlreadyStarted,
            GroupConsumerCyclePortErrorKind::Registry(
                GroupConsumerCycleAdmissionError::UnknownGroup
                | GroupConsumerCycleAdmissionError::GroupClosing,
            ) => GroupConsumerCyclePortErrorCategory::GroupUnavailable,
            GroupConsumerCyclePortErrorKind::Lock(GroupConsumerShardLockError::Poisoned)
            | GroupConsumerCyclePortErrorKind::Registry(
                GroupConsumerCycleAdmissionError::EntryFault
                | GroupConsumerCycleAdmissionError::Execution(_)
                | GroupConsumerCycleAdmissionError::ConsumerExecution(_)
                | GroupConsumerCycleAdmissionError::ProtocolMismatch,
            ) => GroupConsumerCyclePortErrorCategory::InternalInvariant,
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
