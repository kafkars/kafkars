//! Exact terminal retirement and partition offset-reset proposal settlement.

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerTransition, FetchOwnership,
    GroupPositionMissingOffsetPolicy, PositionFence, StartPosition,
};

use crate::{
    clock::{DeadlineCapture, MonotonicClock},
    consumer::{
        assigned_owner_model::RawPositionDeadline,
        fetch_execution::{
            FetchExecutionError, FetchTerminalProposal, PartitionOffsetOutOfRangeProposal,
        },
    },
};

use super::{
    model::{
        ClassicGroupFetchOffsetResetFailure, ClassicGroupFetchOwnerFault,
        ClassicGroupFetchTransitionFailure,
    },
    owner::ClassicGroupFetchOwner,
};

impl ClassicGroupFetchOwner {
    pub(super) fn settle_terminal_proposal(
        &mut self,
        clock: &MonotonicClock,
        proposal: FetchTerminalProposal,
    ) -> Result<Option<AssignedConsumerTransition>, FetchExecutionError> {
        let proposal = match proposal.into_partition_offset_out_of_range() {
            Ok(proposal) => proposal,
            Err(proposal) => {
                return self
                    .fetches
                    .apply_terminal_proposal(&mut self.machine, proposal);
            }
        };
        let Some(position) = reset_position(self.missing_offset_policy) else {
            return self.apply_generic_offset_out_of_range(proposal);
        };
        let fence = proposal.fence();
        if self.machine.fetch_ownership(fence) != Ok(FetchOwnership::Active) {
            return self.apply_generic_offset_out_of_range(proposal);
        }
        let capture = match clock.capture_deadline_after(self.fetch_attempt_timeout) {
            Ok(capture) => capture,
            Err(error) => {
                self.retain_reset_proposal_fault(
                    proposal,
                    ClassicGroupFetchOffsetResetFailure::Clock(error),
                );
                return Ok(None);
            }
        };
        if let Err(failure) = self.preflight_reset_capacity() {
            self.retain_reset_proposal_fault(proposal, failure);
            return Ok(None);
        }
        self.apply_offset_out_of_range_reset(proposal, position, capture)
    }

    fn apply_offset_out_of_range_reset(
        &mut self,
        proposal: PartitionOffsetOutOfRangeProposal,
        position: StartPosition,
        capture: DeadlineCapture,
    ) -> Result<Option<AssignedConsumerTransition>, FetchExecutionError> {
        let fetch_fence = proposal.fence();
        let assignment_epoch = fetch_fence.position().assignment_epoch();
        let partition = fetch_fence.position().partition();
        let event_claims = match self.events.prepare_partition(partition) {
            Ok(claims) => claims,
            Err(error) => {
                self.retain_reset_proposal_fault(
                    proposal,
                    ClassicGroupFetchOffsetResetFailure::Event(error),
                );
                return Ok(None);
            }
        };
        let input = reset_input(fetch_fence, position, capture);
        let transition =
            match self
                .fetches
                .apply_offset_out_of_range_reset(&mut self.machine, proposal, input)
            {
                Ok(Some(transition)) => transition,
                Ok(None) => {
                    event_claims.rollback_event_claims();
                    return Ok(None);
                }
                Err(error) => {
                    event_claims.rollback_event_claims();
                    return Err(error);
                }
            };
        let Some(position_fence) =
            reset_transition_fence(&transition, assignment_epoch, partition, position, capture)
        else {
            event_claims.rollback_event_claims();
            self.retain_reset_transition_fault(transition);
            return Ok(None);
        };
        if event_claims
            .commit_event_claims(transition.effects())
            .is_err()
        {
            self.retain_reset_transition_fault(transition);
            return Ok(None);
        }
        self.raw_position_deadlines.push_back(RawPositionDeadline {
            fence: position_fence,
            deadline: capture.operation_deadline(),
        });
        Ok(Some(transition))
    }

    fn apply_generic_offset_out_of_range(
        &mut self,
        proposal: PartitionOffsetOutOfRangeProposal,
    ) -> Result<Option<AssignedConsumerTransition>, FetchExecutionError> {
        self.fetches
            .apply_terminal_proposal(&mut self.machine, proposal.into_proposal())
    }

    fn preflight_reset_capacity(&self) -> Result<(), ClassicGroupFetchOffsetResetFailure> {
        let effect_limit = self.effect_capacity.min(self.effects.capacity());
        let actual_effects = self.effects.len().saturating_add(2);
        if actual_effects > effect_limit {
            return Err(ClassicGroupFetchOffsetResetFailure::EffectCapacity {
                actual: actual_effects,
                limit: effect_limit,
            });
        }
        let raw_limit = self
            .partition_capacity
            .min(self.raw_position_deadlines.capacity());
        let actual_raw = self.raw_position_deadlines.len().saturating_add(1);
        if actual_raw > raw_limit {
            return Err(ClassicGroupFetchOffsetResetFailure::RawDeadlineCapacity {
                actual: actual_raw,
                limit: raw_limit,
            });
        }
        let pending_limit = self
            .partition_capacity
            .min(self.pending_positions.capacity());
        let actual_pending = self.pending_positions.len().saturating_add(1);
        if actual_pending > pending_limit {
            return Err(
                ClassicGroupFetchOffsetResetFailure::PendingPositionCapacity {
                    actual: actual_pending,
                    limit: pending_limit,
                },
            );
        }
        Ok(())
    }

    fn retain_reset_proposal_fault(
        &mut self,
        proposal: PartitionOffsetOutOfRangeProposal,
        failure: ClassicGroupFetchOffsetResetFailure,
    ) {
        self.fault = Some(ClassicGroupFetchOwnerFault::OffsetReset {
            _proposal: proposal,
            failure,
        });
        self.settle_seek_host_unavailable();
    }

    fn retain_reset_transition_fault(&mut self, transition: AssignedConsumerTransition) {
        self.fault = Some(ClassicGroupFetchOwnerFault::Transition {
            transition,
            failure: ClassicGroupFetchTransitionFailure::ControlInvariant,
        });
        self.settle_seek_host_unavailable();
    }
}

const fn reset_position(policy: GroupPositionMissingOffsetPolicy) -> Option<StartPosition> {
    match policy {
        GroupPositionMissingOffsetPolicy::Error => None,
        GroupPositionMissingOffsetPolicy::Earliest => Some(StartPosition::Beginning),
        GroupPositionMissingOffsetPolicy::Latest => Some(StartPosition::End),
    }
}

fn reset_input(
    fence: kafka_client_core::FetchFence,
    position: StartPosition,
    capture: DeadlineCapture,
) -> AssignedConsumerInput {
    AssignedConsumerInput::Seek {
        assignment_epoch: fence.position().assignment_epoch(),
        partition: fence.position().partition(),
        position,
        now: capture.now(),
        resolution_deadline: capture.deadline(),
    }
}

fn reset_transition_fence(
    transition: &AssignedConsumerTransition,
    assignment_epoch: kafka_client_core::AssignmentEpoch,
    partition: kafka_client_core::AssignedTopicPartition,
    position: StartPosition,
    capture: DeadlineCapture,
) -> Option<PositionFence> {
    let [
        AssignedConsumerEffect::Suspend { fence: suspended },
        AssignedConsumerEffect::ResolvePosition {
            fence,
            position: actual_position,
            deadline,
        },
    ] = transition.effects()
    else {
        return None;
    };
    (transition.assignment_epoch() == Some(assignment_epoch)
        && suspended == fence
        && fence.assignment_epoch() == assignment_epoch
        && fence.partition() == partition
        && *actual_position == position
        && *deadline == capture.deadline())
    .then_some(*fence)
}
