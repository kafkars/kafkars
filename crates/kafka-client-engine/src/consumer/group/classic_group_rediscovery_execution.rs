//! Bounded submission and terminal installation for core-authorized coordinator rediscovery.

use kafka_client_core::{
    ConsumerGroupHeartbeatFailure, ConsumerGroupHeartbeatRequestKind, GroupId, LiveGroupAssignment,
    Moment,
};

use crate::driver::{
    ConsumerGroupHeartbeatRoute, DriverOwner,
    classic_group::{
        ClassicCoordinatorInvalidationAdmissionFailureKind,
        ClassicCoordinatorInvalidationPermission, ClassicCoordinatorInvalidationPoll,
        ClassicCoordinatorInvalidationTerminalFailure,
    },
};

use super::{
    classic_group_entry_fault::ClassicGroupEntryFault,
    classic_group_execution::ClassicGroupExecutionError,
    consumer_group_assignment_retirement::stage_consumer_group_revocation,
    consumer_group_close::{fail_consumer_group_leave, finish_consumer_group_leave_failure},
    consumer_group_execution::{ConsumerGroupExecutionError, ConsumerGroupRediscoveryState},
    consumer_group_execution_terminal::{
        ConsumerGroupRediscoveryDecision, fail_consumer_group_entry,
    },
    consumer_group_heartbeat_failure::core_close_terminal,
    registry::GroupConsumerRegistry,
    registry_entry::GroupConsumerEntry,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ClassicCoordinatorInvalidationTurn {
    Idle,
    Progress,
    Blocked,
}

impl GroupConsumerRegistry {
    pub(super) fn settle_consumer_group_rediscovery(
        &mut self,
        index: usize,
        now: Moment,
        failure: ConsumerGroupHeartbeatFailure,
        route: ConsumerGroupHeartbeatRoute,
    ) -> Result<(), ConsumerGroupExecutionError> {
        let state = self.entries[index]
            .consumer
            .as_ref()
            .ok_or(ConsumerGroupExecutionError::MissingPrepared)?
            .rediscovery_state();
        if state == ConsumerGroupRediscoveryState::ReplacementAdmitted {
            route.accept();
            let is_leave = consumer_group_rediscovery_is_leave(&self.entries[index])?;
            let decision = self.entries[index]
                .consumer
                .as_mut()
                .ok_or(ConsumerGroupExecutionError::MissingPrepared)?
                .apply_current_rediscovery(now, failure)?;
            let ConsumerGroupRediscoveryDecision::Terminal { revoked, failure } = decision else {
                return Err(ConsumerGroupExecutionError::EffectShape);
            };
            finish_consumer_group_rediscovery_terminal(
                &mut self.entries[index],
                is_leave,
                revoked,
                failure,
            )?;
            return Ok(());
        }
        if state != ConsumerGroupRediscoveryState::Open {
            route.accept();
            return Err(ConsumerGroupExecutionError::EffectShape);
        }

        let group_id = self.entries[index].group_id();
        let (entries, invalidations) = (&mut self.entries, &mut self.coordinator_invalidations);
        let permit = match invalidations
            .as_mut()
            .ok_or(ConsumerGroupExecutionError::EffectShape)?
            .try_reserve(group_id)
        {
            Ok(permit) => permit,
            Err(_error) => {
                route.accept();
                fail_consumer_group_rediscovery_entry(&mut entries[index], failure)?;
                return Ok(());
            }
        };
        let pending = match route.into_coordinator_invalidation(group_id) {
            Ok(pending) => pending,
            Err(route) => {
                drop(permit);
                route.accept();
                fail_consumer_group_rediscovery_entry(&mut entries[index], failure)?;
                return Ok(());
            }
        };
        let is_leave = consumer_group_rediscovery_is_leave(&entries[index])?;
        let decision = entries[index]
            .consumer
            .as_mut()
            .ok_or(ConsumerGroupExecutionError::MissingPrepared)?
            .apply_current_rediscovery(now, failure)?;
        match decision {
            ConsumerGroupRediscoveryDecision::Rediscover => {
                if let Err(failure) = permit.install(pending) {
                    entries[index].fault = Some(
                        ClassicGroupEntryFault::CoordinatorInvalidationInstall(failure),
                    );
                    return Err(ConsumerGroupExecutionError::EffectShape);
                }
            }
            ConsumerGroupRediscoveryDecision::Terminal { revoked, failure } => {
                drop(permit);
                drop(pending);
                finish_consumer_group_rediscovery_terminal(
                    &mut entries[index],
                    is_leave,
                    revoked,
                    failure,
                )?;
            }
        }
        Ok(())
    }

    pub(super) fn drive_one_classic_coordinator_invalidation(
        &mut self,
        driver: &DriverOwner,
    ) -> Result<ClassicCoordinatorInvalidationTurn, ClassicGroupExecutionError> {
        let poll = match self
            .coordinator_invalidations
            .as_mut()
            .ok_or(ClassicGroupExecutionError::CallRegistryUnavailable)?
            .drive_one(driver)
        {
            Ok(poll) => poll,
            Err(failure) => {
                return match failure.kind() {
                    ClassicCoordinatorInvalidationAdmissionFailureKind::Full => {
                        Ok(ClassicCoordinatorInvalidationTurn::Blocked)
                    }
                    ClassicCoordinatorInvalidationAdmissionFailureKind::Closed
                    | ClassicCoordinatorInvalidationAdmissionFailureKind::Wake
                    | ClassicCoordinatorInvalidationAdmissionFailureKind::IdentityExhausted
                    | ClassicCoordinatorInvalidationAdmissionFailureKind::ForeignDriver
                    | ClassicCoordinatorInvalidationAdmissionFailureKind::VersionBoundsInvalid
                    | ClassicCoordinatorInvalidationAdmissionFailureKind::Unrecognized => {
                        Err(ClassicGroupExecutionError::CoordinatorInvalidationAdmission)
                    }
                };
            }
        };
        match poll {
            ClassicCoordinatorInvalidationPoll::Idle => {
                Ok(ClassicCoordinatorInvalidationTurn::Idle)
            }
            ClassicCoordinatorInvalidationPoll::Submitted { group_id } => {
                self.permit_consumer_group_rediscovery_after_invalidation_admission(group_id)?;
                Ok(ClassicCoordinatorInvalidationTurn::Progress)
            }
            ClassicCoordinatorInvalidationPoll::Pending { .. } => {
                Ok(ClassicCoordinatorInvalidationTurn::Blocked)
            }
            ClassicCoordinatorInvalidationPoll::Terminal(terminal) => {
                self.apply_classic_coordinator_invalidation_terminal(
                    terminal.group_id(),
                    terminal.result(),
                )?;
                Ok(ClassicCoordinatorInvalidationTurn::Progress)
            }
        }
    }

    pub(super) fn apply_classic_coordinator_invalidation_terminal(
        &mut self,
        group_id: GroupId,
        result: Result<
            ClassicCoordinatorInvalidationPermission,
            ClassicCoordinatorInvalidationTerminalFailure,
        >,
    ) -> Result<(), ClassicGroupExecutionError> {
        let entry = self
            .entries
            .iter_mut()
            .find(|entry| entry.group_id() == group_id)
            .ok_or(ClassicGroupExecutionError::CallIdentityMismatch)?;
        if entry.leave.owns_coordinator_invalidation() {
            let _completed = entry.leave.complete_coordinator_invalidation(result);
            return Ok(());
        }
        if let Some(consumer) = entry.consumer.as_ref() {
            if consumer.rediscovery_state()
                == ConsumerGroupRediscoveryState::AwaitingInvalidationAdmission
            {
                return Err(ClassicGroupExecutionError::CoordinatorInvalidationGate);
            }
            return Ok(());
        }
        match result {
            Ok(
                ClassicCoordinatorInvalidationPermission::Applied
                | ClassicCoordinatorInvalidationPermission::IgnoredStale,
            ) => entry
                .rediscovery
                .permit_rejoin()
                .map_err(|_error| ClassicGroupExecutionError::CoordinatorInvalidationGate),
            Err(failure) => {
                entry.fault = Some(ClassicGroupEntryFault::CoordinatorInvalidationTerminal(
                    failure,
                ));
                Err(ClassicGroupExecutionError::CoordinatorInvalidationTerminal)
            }
        }
    }

    fn permit_consumer_group_rediscovery_after_invalidation_admission(
        &mut self,
        group_id: GroupId,
    ) -> Result<(), ClassicGroupExecutionError> {
        let entry = self
            .entries
            .iter_mut()
            .find(|entry| entry.group_id() == group_id)
            .ok_or(ClassicGroupExecutionError::CallIdentityMismatch)?;
        if entry.leave.owns_coordinator_invalidation() {
            return Ok(());
        }
        let Some(consumer) = entry.consumer.as_mut() else {
            return Ok(());
        };
        match consumer.rediscovery_state() {
            ConsumerGroupRediscoveryState::AwaitingInvalidationAdmission => consumer
                .permit_rediscovery_replacement()
                .map_err(|_error| ClassicGroupExecutionError::CoordinatorInvalidationGate),
            ConsumerGroupRediscoveryState::Open => Ok(()),
            ConsumerGroupRediscoveryState::ReplacementAdmitted => {
                Err(ClassicGroupExecutionError::CoordinatorInvalidationGate)
            }
        }
    }
}

fn consumer_group_rediscovery_is_leave(
    entry: &GroupConsumerEntry,
) -> Result<bool, ConsumerGroupExecutionError> {
    Ok(entry
        .consumer
        .as_ref()
        .ok_or(ConsumerGroupExecutionError::MissingPrepared)?
        .prepared()
        .ok_or(ConsumerGroupExecutionError::MissingPrepared)?
        .kind()
        == ConsumerGroupHeartbeatRequestKind::Leave)
}

fn fail_consumer_group_rediscovery_entry(
    entry: &mut GroupConsumerEntry,
    failure: ConsumerGroupHeartbeatFailure,
) -> Result<(), ConsumerGroupExecutionError> {
    if consumer_group_rediscovery_is_leave(entry)? {
        fail_consumer_group_leave(entry, failure, core_close_terminal(failure))
    } else {
        fail_consumer_group_entry(entry, failure)
    }
}

pub(super) fn finish_consumer_group_rediscovery_terminal(
    entry: &mut GroupConsumerEntry,
    is_leave: bool,
    revoked: Option<LiveGroupAssignment>,
    failure: ConsumerGroupHeartbeatFailure,
) -> Result<(), ConsumerGroupExecutionError> {
    if is_leave {
        finish_consumer_group_leave_failure(entry, revoked, core_close_terminal(failure))
    } else {
        drop(entry.consumer_reconciliation.take());
        stage_consumer_group_revocation(entry, revoked)
    }
}
