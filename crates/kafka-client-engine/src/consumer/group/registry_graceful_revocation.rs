//! Production staging, scheduling, and completion of graceful assignment release.

use kafka_client_core::{
    ClassicGeneration, ClassicGracefulRevocationLease, Deadline, GroupId, LiveGroupAssignment,
    Moment,
};

use crate::clock::ClockError;

use super::{
    classic_group_fetch::ClassicGroupFetchOwner,
    classic_group_graceful_revocation::{
        ClassicGroupRevocationAcknowledgeError, ClassicGroupRevocationHostError,
        ClassicGroupRevocationOwner, ClassicGroupRevocationStageError, ClassicGroupRevocationTurn,
    },
    registry::GroupConsumerRegistry,
    registry_entry::GroupConsumerEntryState,
    registry_port::GroupConsumerPort,
    registry_shard::GroupConsumerShardLockError,
    session_catalog::GroupSessionCatalog,
};

/// Private observation and completion failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer) enum GroupConsumerRevocationPortError {
    Closed,
    Clock(ClockError),
    Lock(GroupConsumerShardLockError),
    UnknownGroup,
    GroupUnavailable,
    Acknowledge(ClassicGroupRevocationAcknowledgeError),
}

pub(super) fn stage_classic_group_revocation(
    catalog: &mut GroupSessionCatalog,
    fetch: &ClassicGroupFetchOwner,
    revocation: &mut ClassicGroupRevocationOwner,
    assignment: LiveGroupAssignment,
    generation: ClassicGeneration,
    deadline: Deadline,
    now: Moment,
) -> Result<(), (ClassicGroupRevocationStageError, LiveGroupAssignment)> {
    if catalog.live_assignment() != Some(&assignment) {
        return Err((
            ClassicGroupRevocationStageError::AssignmentMismatch,
            assignment,
        ));
    }
    let Some(activation) = fetch.activation() else {
        return Err((
            ClassicGroupRevocationStageError::FetchBindingMissing,
            assignment,
        ));
    };
    let assignment_epoch = activation.binding().assignment_epoch();
    if fetch.machine_assignment_epoch() != Some(assignment_epoch)
        || assignment_epoch.get() != assignment.assignment_generation().get()
    {
        return Err((
            ClassicGroupRevocationStageError::FetchBindingMismatch,
            assignment,
        ));
    }
    let named = catalog.prepare_graceful_revocation_event(&assignment);
    let publishes_event = named.is_some();
    let lease = ClassicGracefulRevocationLease::new(assignment_epoch, deadline);
    revocation
        .begin(assignment, generation, lease, now)
        .map_err(|(error, assignment)| {
            (ClassicGroupRevocationStageError::Owner(error), assignment)
        })?;
    catalog.commit_graceful_revocation_event(named, lease.assignment_epoch().get());
    if !publishes_event {
        revocation
            .lose_owner()
            .map_err(|_error| unreachable!("newly armed revocation can be lost"))?;
    }
    Ok(())
}

impl GroupConsumerRegistry {
    pub(super) fn turn_graceful_revocation(
        &mut self,
        now: Moment,
    ) -> Result<ClassicGroupRevocationTurn, ClassicGroupRevocationHostError> {
        for entry in &mut self.entries {
            if entry.revocation.expire_if_due(now)? {
                return Ok(ClassicGroupRevocationTurn::Progress);
            }
            if entry.revocation.terminal().is_some()
                && entry.revocation.settle_terminal(
                    &entry.classic,
                    &mut entry.catalog,
                    &mut entry.processing_lease,
                    &mut entry.fetch,
                )?
            {
                return Ok(ClassicGroupRevocationTurn::Progress);
            }
        }
        Ok(ClassicGroupRevocationTurn::Idle)
    }

    pub(super) fn graceful_revocation_next_deadline(&self) -> Option<Deadline> {
        self.entries
            .iter()
            .filter_map(|entry| entry.revocation.next_deadline())
            .min()
    }

    pub(super) fn graceful_revocation_unsettled(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| !entry.revocation.is_dormant())
            .count()
    }

    pub(super) fn acknowledge_revocation(
        &mut self,
        group_id: GroupId,
        assignment_epoch: u64,
        now: Moment,
    ) -> Result<(), GroupConsumerRevocationPortError> {
        let entry = self
            .entries
            .iter_mut()
            .find(|entry| entry.group_id() == group_id)
            .ok_or(GroupConsumerRevocationPortError::UnknownGroup)?;
        if entry.state != GroupConsumerEntryState::Active || entry.fault.is_some() {
            return Err(GroupConsumerRevocationPortError::GroupUnavailable);
        }
        let active = entry.revocation.active_assignment_epoch().ok_or(
            GroupConsumerRevocationPortError::Acknowledge(
                ClassicGroupRevocationAcknowledgeError::NoActiveLease,
            ),
        )?;
        if active.get() != assignment_epoch {
            return Err(GroupConsumerRevocationPortError::Acknowledge(
                ClassicGroupRevocationAcknowledgeError::AssignmentEpochMismatch,
            ));
        }
        entry
            .revocation
            .acknowledge(active, now)
            .map_err(GroupConsumerRevocationPortError::Acknowledge)
    }
}

impl GroupConsumerPort {
    pub(in crate::consumer) fn try_acknowledge_revocation(
        &self,
        group_id: GroupId,
        assignment_epoch: u64,
    ) -> Result<(), GroupConsumerRevocationPortError> {
        let now = self
            .clock
            .now()
            .map_err(GroupConsumerRevocationPortError::Clock)?;
        if self.shared.admission_is_closed() {
            return Err(GroupConsumerRevocationPortError::Closed);
        }
        let mut registry = self
            .shared
            .try_registry()
            .map_err(GroupConsumerRevocationPortError::Lock)?;
        if self.shared.admission_is_closed() {
            return Err(GroupConsumerRevocationPortError::Closed);
        }
        let result = registry.acknowledge_revocation(group_id, assignment_epoch, now);
        drop(registry);
        if result.is_ok()
            || matches!(
                result,
                Err(GroupConsumerRevocationPortError::Acknowledge(
                    ClassicGroupRevocationAcknowledgeError::DeadlineElapsed
                ))
            )
        {
            let _wake = self.shared.request_turn();
        }
        result
    }
}
