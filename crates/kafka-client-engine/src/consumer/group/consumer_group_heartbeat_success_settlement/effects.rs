//! Atomic installation of one exact core-authorized KIP-848 success effect.

use kafka_client_core::{
    ConsumerGroupHeartbeatEffect, ConsumerGroupMemberEpoch, Deadline, MembershipCycle, Moment,
};

use crate::clock::OperationDeadline;

use super::{
    super::{
        consumer_group_assignment_install::{
            PreparedConsumerGroupAssignmentInstall, install_consumer_group_assignment,
            install_reconciled_consumer_group_assignment,
        },
        consumer_group_execution::ConsumerGroupExecutionError,
        registry_entry::GroupConsumerEntry,
        registry_graceful_revocation::consumer_group::stage_consumer_group_reconciliation,
        session_catalog_consumer::ConsumerGroupMemberCandidate,
    },
    reconciliation::reconciliation_core_matches,
};

pub(super) struct ConsumerGroupSuccessEffectContext {
    pub(super) candidate: ConsumerGroupMemberCandidate,
    pub(super) member_epoch: ConsumerGroupMemberEpoch,
    pub(super) deadline: OperationDeadline,
    pub(super) now: Moment,
    pub(super) replaces_live_assignment: bool,
    pub(super) install_cycle: Option<MembershipCycle>,
    pub(super) current_cycle: Option<MembershipCycle>,
    pub(super) revocation_deadline: Option<Deadline>,
}

#[expect(
    clippy::too_many_lines,
    reason = "one exhaustive core-effect dispatch preserves atomic catalog installation"
)]
pub(super) fn settle_success_effect(
    entry: &mut GroupConsumerEntry,
    effect: ConsumerGroupHeartbeatEffect,
    context: ConsumerGroupSuccessEffectContext,
) -> Result<(), ConsumerGroupExecutionError> {
    let ConsumerGroupSuccessEffectContext {
        candidate,
        member_epoch,
        deadline,
        now,
        replaces_live_assignment,
        install_cycle,
        current_cycle,
        revocation_deadline,
    } = context;
    match effect {
        ConsumerGroupHeartbeatEffect::Reconcile {
            previous,
            assignment,
            member_epoch: installed_epoch,
            schedule,
        } if installed_epoch == member_epoch
            && schedule.assignment_generation()
                == Some(
                    previous
                        .as_ref()
                        .map_or(assignment.assignment_generation(), |previous| {
                            previous.assignment_generation()
                        }),
                ) =>
        {
            if !reconciliation_core_matches(entry, previous.as_ref(), &assignment) {
                return Err(ConsumerGroupExecutionError::EffectShape);
            }
            let cycle = install_cycle.ok_or(ConsumerGroupExecutionError::EffectShape)?;
            match previous {
                None if !replaces_live_assignment => {
                    let install = PreparedConsumerGroupAssignmentInstall::new(
                        candidate,
                        cycle,
                        member_epoch,
                        assignment,
                        deadline,
                        now,
                    );
                    install_consumer_group_assignment(entry, install)?;
                }
                Some(previous)
                    if replaces_live_assignment
                        && entry.catalog.live_assignment() == Some(&previous)
                        && entry.consumer_reconciliation.is_none() =>
                {
                    let install = PreparedConsumerGroupAssignmentInstall::new(
                        candidate,
                        cycle,
                        member_epoch,
                        assignment,
                        deadline,
                        now,
                    );
                    stage_consumer_group_reconciliation(
                        entry,
                        previous,
                        install,
                        revocation_deadline,
                        now,
                    )?;
                }
                _ => return Err(ConsumerGroupExecutionError::EffectShape),
            }
        }
        ConsumerGroupHeartbeatEffect::AwaitAssignment {
            member_id,
            member_epoch: accepted_epoch,
            schedule,
        } if member_id == candidate.member_id()
            && accepted_epoch == member_epoch
            && schedule.assignment_generation().is_none()
            && entry.catalog.live_assignment().is_none()
            && entry.consumer_reconciliation.is_none()
            && entry.consumer_revocation.is_none()
            && entry.consumer.as_ref().is_some_and(|execution| {
                execution.machine().phase()
                    == kafka_client_core::ConsumerGroupHeartbeatPhase::AwaitingAssignment
                    && execution.machine().member_epoch() == Some(member_epoch)
                    && execution.machine().live_assignment().is_none()
                    && execution.machine().pending_assignment().is_none()
                    && execution.machine().schedule() == Some(schedule)
            }) =>
        {
            entry.catalog.commit_consumer_group_awaiting_assignment(
                candidate,
                current_cycle.ok_or(ConsumerGroupExecutionError::EffectShape)?,
                member_epoch,
            );
        }
        ConsumerGroupHeartbeatEffect::ArmHeartbeat { schedule }
            if entry.catalog.current_member_id() == Some(candidate.member_id())
                && entry.consumer.as_ref().is_some_and(|execution| {
                    execution.machine().member_epoch() == Some(member_epoch)
                })
                && entry.consumer.as_ref().is_some_and(|execution| {
                    let reportable = execution.machine().live_assignment();
                    entry.catalog.live_assignment() == reportable
                        && reportable.is_some_and(|assignment| {
                            Some(assignment.assignment_generation())
                                == schedule.assignment_generation()
                        })
                }) =>
        {
            if entry.catalog.consumer_group_member_epoch() != Some(member_epoch) {
                entry
                    .catalog
                    .commit_consumer_group_reconciliation_epoch(&candidate, member_epoch);
            }
        }
        ConsumerGroupHeartbeatEffect::InstallReconciled {
            member_id,
            member_epoch: installed_epoch,
            assignment_generation,
            schedule,
        } if member_id == candidate.member_id()
            && installed_epoch == member_epoch
            && schedule.assignment_generation() == Some(assignment_generation)
            && entry.catalog.current_member_id() == Some(member_id)
            && entry.catalog.consumer_group_member_epoch() == Some(member_epoch)
            && entry.catalog.live_assignment().is_none()
            && entry.consumer_revocation.is_none()
            && entry.consumer.as_ref().is_some_and(|execution| {
                execution.machine().pending_assignment().is_none()
                    && execution
                        .machine()
                        .live_assignment()
                        .is_some_and(|assignment| {
                            assignment.member_id() == member_id
                                && assignment.assignment_generation() == assignment_generation
                        })
            }) =>
        {
            let install = entry
                .consumer_reconciliation
                .take()
                .ok_or(ConsumerGroupExecutionError::EffectShape)?;
            if install.member_id() != member_id
                || install.member_epoch() != member_epoch
                || install.assignment().assignment_generation() != assignment_generation
            {
                entry.consumer_reconciliation = Some(install);
                return Err(ConsumerGroupExecutionError::EffectShape);
            }
            let install = install.refresh_resolution_boundary(deadline, now);
            install_reconciled_consumer_group_assignment(entry, install)?;
        }
        _ => return Err(ConsumerGroupExecutionError::EffectShape),
    }
    Ok(())
}
