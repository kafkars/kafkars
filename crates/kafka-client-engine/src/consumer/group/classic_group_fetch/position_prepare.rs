//! Exact preparation and stale reconciliation for seek `ListOffsets` work.

use kafka_client_core::AssignedConsumerEffect;

use crate::consumer::{
    assigned_owner_model::{PendingPosition, position_isolation},
    position_execution::PreparedPositionResolution,
};

use super::{
    super::session_catalog::GroupSessionCatalog,
    model::{
        ClassicGroupFetchEffectFailure, ClassicGroupFetchFront, ClassicGroupFetchOwnerFault,
        minimum_deadline,
    },
    owner::ClassicGroupFetchOwner,
};

impl ClassicGroupFetchOwner {
    pub(super) fn interpret_position_effect(
        &mut self,
        effect: AssignedConsumerEffect,
        catalog: &GroupSessionCatalog,
    ) -> Option<ClassicGroupFetchFront> {
        let result = match effect {
            AssignedConsumerEffect::ResolvePosition { fence, .. } => {
                self.prepare_position(effect, fence, catalog)
            }
            AssignedConsumerEffect::ArmPositionThrottle { fence, deadline } => self
                .timers
                .arm_position(fence, deadline)
                .map(|_disposition| ())
                .map_err(ClassicGroupFetchEffectFailure::Timer),
            AssignedConsumerEffect::PositionResolutionFailed { .. } => {
                match self.events.discard_terminal(effect) {
                    Ok(()) => {
                        self.effects.pop_front();
                        return Some(ClassicGroupFetchFront::Interpreted);
                    }
                    Err(error) => Err(ClassicGroupFetchEffectFailure::Event(error)),
                }
            }
            _ => return None,
        };
        match result {
            Ok(()) => {
                if let Err(error) = self.events.observe_effect(effect) {
                    self.fault = Some(ClassicGroupFetchOwnerFault::Effect {
                        effect,
                        failure: ClassicGroupFetchEffectFailure::Event(error),
                    });
                    self.settle_seek_host_unavailable();
                    return Some(ClassicGroupFetchFront::Idle);
                }
                self.effects.pop_front();
                Some(ClassicGroupFetchFront::Interpreted)
            }
            Err(failure) => {
                self.fault = Some(ClassicGroupFetchOwnerFault::Effect { effect, failure });
                self.settle_seek_host_unavailable();
                Some(ClassicGroupFetchFront::Idle)
            }
        }
    }

    pub(in crate::consumer::group) fn next_deadline(&self) -> Option<kafka_client_core::Deadline> {
        if self.is_faulted() {
            return None;
        }
        let mut next = self.timers.next_deadline();
        for pending in &self.raw_position_deadlines {
            next = minimum_deadline(next, pending.deadline.core());
        }
        for pending in &self.pending_positions {
            next = minimum_deadline(next, pending.deadline.core());
        }
        for pending in &self.pending_fetches {
            next = minimum_deadline(next, pending.deadline());
        }
        if let Some(deadline) = self.fetches.broker_session_close_deadline() {
            next = minimum_deadline(next, deadline);
        }
        if let Some(deadline) = self.fetches.broker_session_maintenance_deadline() {
            next = minimum_deadline(next, deadline);
        }
        next
    }

    pub(super) fn reconcile_raw_position_deadlines(&mut self, effect: AssignedConsumerEffect) {
        match effect {
            AssignedConsumerEffect::Suspend { fence } => {
                self.raw_position_deadlines.retain(|pending| {
                    pending.fence.partition() != fence.partition()
                        || pending.fence.assignment_epoch() > fence.assignment_epoch()
                        || (pending.fence.assignment_epoch() == fence.assignment_epoch()
                            && pending.fence.position_epoch() >= fence.position_epoch())
                });
            }
            AssignedConsumerEffect::Revoke {
                assignment_epoch,
                partition,
            } => {
                self.raw_position_deadlines.retain(|pending| {
                    pending.fence.partition() != partition
                        || pending.fence.assignment_epoch() != assignment_epoch
                });
            }
            _ => {}
        }
    }

    pub(super) fn prepare_position(
        &mut self,
        effect: AssignedConsumerEffect,
        fence: kafka_client_core::PositionFence,
        catalog: &GroupSessionCatalog,
    ) -> Result<(), ClassicGroupFetchEffectFailure> {
        if self.pending_positions.len() >= self.partition_capacity {
            return Err(ClassicGroupFetchEffectFailure::PositionCapacity);
        }
        let Some(retained) = self.raw_position_deadlines.front().copied() else {
            return Err(ClassicGroupFetchEffectFailure::PositionDeadlineMismatch);
        };
        if retained.fence != fence {
            return Err(ClassicGroupFetchEffectFailure::PositionDeadlineMismatch);
        }
        let topic = catalog
            .copy_topic_name(fence.partition().topic_id())
            .map_err(ClassicGroupFetchEffectFailure::PositionCatalog)?;
        let deadline = retained.deadline;
        let prepared = PreparedPositionResolution::new(
            effect,
            topic,
            position_isolation(self.read_isolation),
            deadline,
        )
        .map_err(ClassicGroupFetchEffectFailure::PositionPreparation)?;
        self.raw_position_deadlines.pop_front();
        self.pending_positions
            .push_back(PendingPosition { prepared, deadline });
        Ok(())
    }

    pub(super) fn reconcile_pending_positions(&mut self) -> bool {
        let retained = self.pending_positions.len();
        for _index in 0..retained {
            let Some(pending) = self.pending_positions.pop_front() else {
                return true;
            };
            match pending.prepared.reconcile_ownership(&self.machine) {
                Ok(Some(prepared)) => self.pending_positions.push_back(PendingPosition {
                    prepared,
                    deadline: pending.deadline,
                }),
                Ok(None) => {}
                Err((error, prepared)) => {
                    self.fault = Some(ClassicGroupFetchOwnerFault::PendingPosition {
                        error,
                        _pending: PendingPosition {
                            prepared,
                            deadline: pending.deadline,
                        },
                    });
                    return false;
                }
            }
        }
        true
    }
}
