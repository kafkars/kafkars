//! Sync-confirmed cooperative fencing, partial revocation, and added-position start.

use kafka_client_core::{
    AssignedTopicPartition, ClassicGroupEffect, ClassicGroupInput, GroupId, GroupPositionFence,
    MemberId, MembershipCycle, Moment,
};

use super::{
    classic_group_execution::ClassicGroupExecutionError,
    classic_group_fetch::ClassicGroupFetchControlError,
    classic_group_join::PreparedClassicGroupJoin,
    classic_group_position::{ClassicGroupPositionExecutionState, ClassicGroupPositionPreparation},
    registry::GroupConsumerRegistry,
    registry_graceful_revocation::stage_classic_group_reconciliation_revocation,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ClassicGroupReconciliationTurn {
    Idle,
    Progress,
    Blocked,
}

impl GroupConsumerRegistry {
    #[expect(
        clippy::too_many_lines,
        reason = "confirmed reconciliation commits its candidate, position, processing lease, catalog, and optional follow-up Join as one ordered ownership transition"
    )]
    pub(super) fn finish_one_classic_group_reconciliation(
        &mut self,
        now: Moment,
        clock: &crate::clock::MonotonicClock,
    ) -> Result<ClassicGroupReconciliationTurn, ClassicGroupExecutionError> {
        let Some(index) = self.entries.iter().position(|entry| {
            entry.is_active()
                && entry.fault.is_none()
                && entry
                    .classic_reconciliation
                    .as_ref()
                    .is_some_and(|pending| {
                        !pending.assignment_loss_is_staged()
                            && pending.sync_is_confirmed()
                            && pending.revocation_is_staged()
                            && pending.revocation_is_settled()
                            && pending.position_was_installed()
                    })
                && entry.position.has_ready_bootstrap_terminal()
        }) else {
            return Ok(ClassicGroupReconciliationTurn::Idle);
        };
        let entry = &mut self.entries[index];
        if !entry.fetch.reconciliation_is_ready() {
            return Ok(ClassicGroupReconciliationTurn::Blocked);
        }
        let requires_followup = entry
            .classic_reconciliation
            .as_ref()
            .is_some_and(|pending| pending.reconciliation().requires_followup());
        let rejoin_is_already_scheduled = entry.classic.machine().pending_rejoin().is_some();
        let start_followup = requires_followup && !rejoin_is_already_scheduled;
        if start_followup && !entry.heartbeat.is_locally_clearable() {
            return Ok(ClassicGroupReconciliationTurn::Blocked);
        }
        let state = entry
            .position
            .replace(ClassicGroupPositionExecutionState::Dormant);
        let ClassicGroupPositionExecutionState::Complete(completed) = state else {
            entry.position.set(state);
            return Err(ClassicGroupExecutionError::Reconciliation);
        };
        let pending = entry
            .classic_reconciliation
            .take()
            .ok_or(ClassicGroupExecutionError::Reconciliation)?;
        let reconciliation = pending.reconciliation();
        let previous = reconciliation.previous_assignment();
        let replacement = reconciliation.replacement_assignment();
        let previous_cycle = reconciliation.previous_cycle();
        let replacement_cycle = reconciliation.replacement_cycle();
        let replacement_generation = reconciliation.replacement_classic_generation();
        let replacement_assignment_generation = replacement.assignment_generation();
        let requires_followup = reconciliation.requires_followup();
        let replacement_member_id = replacement.member_id();
        let catalog_member_id = entry.catalog.current_member_id();
        let previous_fence = GroupPositionFence::new(
            previous.group_id(),
            previous_cycle,
            previous.member_id(),
            previous.assignment_generation(),
        );
        let replacement_fence = GroupPositionFence::new(
            replacement.group_id(),
            replacement_cycle,
            replacement.member_id(),
            replacement_assignment_generation,
        );
        let previous_processing = kafka_client_core::ClassicProcessingLeaseFence::new(
            previous.group_id(),
            previous_cycle,
            previous.assignment_generation(),
        );
        let replacement_processing = kafka_client_core::ClassicProcessingLeaseFence::new(
            replacement.group_id(),
            replacement_cycle,
            replacement_assignment_generation,
        );
        let entry_group_id = entry.group_id();
        let processing = match entry
            .processing_lease
            .prepare_reconciliation(previous_processing, replacement_processing)
        {
            Ok(processing) => processing,
            Err(_error) => {
                entry
                    .position
                    .set(ClassicGroupPositionExecutionState::Complete(completed));
                entry.classic_reconciliation = Some(pending);
                return Err(ClassicGroupExecutionError::Reconciliation);
            }
        };
        let Some(candidate) = entry.classic.pending.take() else {
            drop(processing);
            entry
                .position
                .set(ClassicGroupPositionExecutionState::Complete(completed));
            entry.classic_reconciliation = Some(pending);
            return Err(ClassicGroupExecutionError::Reconciliation);
        };
        if let Err(error) = entry.fetch.reconcile_assignment(
            completed,
            previous_fence,
            replacement_fence,
            reconciliation.delta().retained(),
            reconciliation.delta().removed(),
            reconciliation.delta().added(),
            replacement.partitions(),
        ) {
            let mapped = map_fetch_reconciliation(&error);
            drop(processing);
            entry.classic.pending = Some(candidate);
            if let Some(completed) = error.into_completed() {
                entry
                    .position
                    .set(ClassicGroupPositionExecutionState::Complete(completed));
            } else {
                entry.fault = Some(
                    super::classic_group_entry_fault::ClassicGroupEntryFault::FetchOwner(
                        entry.fetch.fault().map_or_else(
                            || unreachable!("retained reconciliation fault"),
                            super::classic_group_fetch::ClassicGroupFetchOwnerFault::kind,
                        ),
                    ),
                );
            }
            entry.classic_reconciliation = Some(pending);
            return Err(mapped);
        }
        let transition = match entry
            .classic
            .apply(ClassicGroupInput::ReconciliationApplied {
                cycle: replacement_cycle,
                assignment_generation: replacement_assignment_generation,
                now,
            }) {
            Ok(transition) => transition,
            Err(_error) => {
                drop(processing);
                entry.classic.pending = Some(candidate);
                entry.classic_reconciliation = Some(pending);
                return Err(ClassicGroupExecutionError::Reconciliation);
            }
        };
        if start_followup {
            entry.heartbeat.clear_local().unwrap_or_else(|_error| {
                unreachable!("prevalidated local heartbeat owner remains synchronously clearable")
            });
        }
        let mut effects = transition.into_effects();
        let first = effects.next();
        let second = effects.next();
        let followup_matches = followup_join_matches(
            start_followup,
            entry_group_id,
            catalog_member_id,
            replacement_cycle,
            replacement_member_id,
            first.as_ref(),
            second.as_ref(),
        );
        let join = match (start_followup, followup_matches, first, second) {
            (false, true, None, None) => None,
            (
                true,
                true,
                Some(ClassicGroupEffect::Join {
                    group_id,
                    cycle,
                    protocol,
                    member_id: Some(member_id),
                    timing,
                    deadline,
                }),
                None,
            ) => {
                let mapped = match clock.operation_deadline(deadline) {
                    Ok(mapped) => mapped,
                    Err(_error) => {
                        entry.classic.pending = Some(candidate);
                        entry.fault = Some(
                            super::classic_group_entry_fault::ClassicGroupEntryFault::ClassicReconciliationPostCore {
                                requires_followup,
                                first: Some(ClassicGroupEffect::Join {
                                    group_id,
                                    cycle,
                                    protocol,
                                    member_id: Some(member_id),
                                    timing,
                                    deadline,
                                }),
                                second: None,
                            },
                        );
                        drop(processing);
                        entry.classic_reconciliation = Some(pending);
                        return Err(ClassicGroupExecutionError::Reconciliation);
                    }
                };
                Some(prepare_retained_followup_join(
                    group_id, cycle, protocol, member_id, timing, mapped,
                ))
            }
            (start_followup, _matches, first, second) => {
                entry.classic.pending = Some(candidate);
                entry.fault = Some(
                    super::classic_group_entry_fault::ClassicGroupEntryFault::ClassicReconciliationPostCore {
                        requires_followup: start_followup,
                        first,
                        second,
                    },
                );
                drop(processing);
                entry.classic_reconciliation = Some(pending);
                return Err(ClassicGroupExecutionError::Reconciliation);
            }
        };
        let _transition = processing.commit();
        let (_previous, replacement, _delta) = pending.into_reconciliation().into_assignments();
        entry
            .catalog
            .commit_classic_group_install(candidate, replacement, replacement_generation);
        entry.catalog.stage_installed_assignment_event();
        entry.catalog.confirm_sync_event();
        if let Some(join) = join {
            entry
                .execution
                .stage_rejoin_join(join)
                .map_err(|(_error, _join)| ClassicGroupExecutionError::Reconciliation)?;
        }
        Ok(ClassicGroupReconciliationTurn::Progress)
    }

    #[expect(
        clippy::too_many_lines,
        reason = "revocation staging keeps Fetch controls, position transfer, and the exact graceful-revocation owner in one ordered transition"
    )]
    pub(super) fn stage_one_classic_group_reconciliation(
        &mut self,
        now: Moment,
    ) -> Result<ClassicGroupReconciliationTurn, ClassicGroupExecutionError> {
        let Some(index) = self.entries.iter().position(|entry| {
            entry.is_active()
                && entry.fault.is_none()
                && entry
                    .classic_reconciliation
                    .as_ref()
                    .is_some_and(|pending| {
                        !pending.assignment_loss_is_staged()
                            && pending.sync_is_confirmed()
                            && !pending.revocation_is_staged()
                    })
        }) else {
            return Ok(ClassicGroupReconciliationTurn::Idle);
        };
        let entry = &mut self.entries[index];
        if entry.fetch.has_ready_delivery() {
            return Ok(ClassicGroupReconciliationTurn::Blocked);
        }
        let pending = entry
            .classic_reconciliation
            .as_ref()
            .ok_or(ClassicGroupExecutionError::Reconciliation)?;
        let reconciliation = pending.reconciliation();
        let previous = reconciliation.previous_assignment();
        let previous_cycle = reconciliation.previous_cycle();
        let previous_generation = reconciliation.previous_classic_generation();
        let revocation_deadline = pending.revocation_deadline();
        let mut removed = Vec::new();
        removed
            .try_reserve_exact(reconciliation.delta().removed().len())
            .map_err(|_error| ClassicGroupExecutionError::Reconciliation)?;
        removed.extend_from_slice(reconciliation.delta().removed());
        let mut targets = Vec::new();
        targets
            .try_reserve_exact(removed.len())
            .map_err(|_error| ClassicGroupExecutionError::Reconciliation)?;
        targets.extend(removed.iter().map(|partition| {
            AssignedTopicPartition::new(partition.topic_id(), partition.partition())
        }));
        let position_fence = GroupPositionFence::new(
            previous.group_id(),
            previous_cycle,
            previous.member_id(),
            previous.assignment_generation(),
        );
        if !targets.is_empty() {
            let accepted = match entry.fetch.pause_partitions(position_fence, &targets) {
                Ok(accepted) => accepted,
                Err(ClassicGroupFetchControlError::Pending) => {
                    return Ok(ClassicGroupReconciliationTurn::Blocked);
                }
                Err(error) => return Err(map_control(error)),
            };
            if accepted.fault_retained() {
                return Err(ClassicGroupExecutionError::Reconciliation);
            }
        }
        if let Some(position) = entry.classic_reconciliation.as_mut().and_then(
            super::classic_group_reconciliation::PreparedClassicGroupReconciliation::take_position,
        ) {
            entry.position.set(match position {
                ClassicGroupPositionPreparation::Prepared(prepared) => {
                    ClassicGroupPositionExecutionState::Prepared(prepared)
                }
                ClassicGroupPositionPreparation::Complete(completed) => {
                    ClassicGroupPositionExecutionState::Complete(completed)
                }
            });
        }
        if removed.is_empty() {
            entry
                .classic_reconciliation
                .as_mut()
                .ok_or(ClassicGroupExecutionError::Reconciliation)?
                .stage_revocation();
            return Ok(ClassicGroupReconciliationTurn::Progress);
        }
        let assignment = entry
            .classic_reconciliation
            .as_mut()
            .and_then(
                super::classic_group_reconciliation::PreparedClassicGroupReconciliation::take_revocation_assignment,
            )
            .ok_or(ClassicGroupExecutionError::Reconciliation)?;
        match stage_classic_group_reconciliation_revocation(
            &mut entry.catalog,
            &entry.fetch,
            &mut entry.revocation,
            assignment,
            previous_generation,
            &removed,
            revocation_deadline,
            now,
        ) {
            Ok(()) => {
                entry
                    .classic_reconciliation
                    .as_mut()
                    .ok_or(ClassicGroupExecutionError::Reconciliation)?
                    .stage_revocation();
                Ok(ClassicGroupReconciliationTurn::Progress)
            }
            Err((_error, assignment)) => {
                entry
                    .classic_reconciliation
                    .as_mut()
                    .ok_or(ClassicGroupExecutionError::Reconciliation)?
                    .restore_revocation_assignment(assignment);
                Err(ClassicGroupExecutionError::Reconciliation)
            }
        }
    }
}

pub(super) fn followup_join_matches(
    requires_followup: bool,
    entry_group_id: GroupId,
    catalog_member_id: Option<MemberId>,
    replacement_cycle: MembershipCycle,
    replacement_member_id: MemberId,
    first: Option<&ClassicGroupEffect>,
    second: Option<&ClassicGroupEffect>,
) -> bool {
    match (requires_followup, first, second) {
        (false, None, None) => true,
        (
            true,
            Some(ClassicGroupEffect::Join {
                group_id,
                cycle,
                member_id: Some(member_id),
                ..
            }),
            None,
        ) => {
            *group_id == entry_group_id
                && replacement_cycle.checked_next() == Some(*cycle)
                && catalog_member_id == Some(*member_id)
                && replacement_member_id == *member_id
        }
        _ => false,
    }
}

pub(super) const fn prepare_retained_followup_join(
    group_id: GroupId,
    cycle: MembershipCycle,
    protocol: kafka_client_core::ClassicProtocol,
    member_id: MemberId,
    timing: kafka_client_core::ClassicGroupTiming,
    deadline: crate::clock::OperationDeadline,
) -> PreparedClassicGroupJoin {
    PreparedClassicGroupJoin::new_with_member_id(
        group_id,
        cycle,
        protocol,
        Some(member_id),
        timing,
        deadline,
    )
}

fn map_control(_error: ClassicGroupFetchControlError) -> ClassicGroupExecutionError {
    ClassicGroupExecutionError::Reconciliation
}

fn map_fetch_reconciliation(
    error: &super::classic_group_fetch::ClassicGroupFetchReconciliationError,
) -> ClassicGroupExecutionError {
    match error.kind() {
        super::classic_group_fetch::ClassicGroupFetchReconciliationErrorKind::NotReady
        | super::classic_group_fetch::ClassicGroupFetchReconciliationErrorKind::BindingMismatch
        | super::classic_group_fetch::ClassicGroupFetchReconciliationErrorKind::PositionShape
        | super::classic_group_fetch::ClassicGroupFetchReconciliationErrorKind::Allocation
        | super::classic_group_fetch::ClassicGroupFetchReconciliationErrorKind::EffectCapacity
        | super::classic_group_fetch::ClassicGroupFetchReconciliationErrorKind::Event(_)
        | super::classic_group_fetch::ClassicGroupFetchReconciliationErrorKind::Core(_)
        | super::classic_group_fetch::ClassicGroupFetchReconciliationErrorKind::PostCore => {
            ClassicGroupExecutionError::Reconciliation
        }
    }
}
