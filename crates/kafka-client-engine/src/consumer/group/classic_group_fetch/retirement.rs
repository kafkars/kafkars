//! Exact assignment-loss retirement before classic membership and catalog revocation.

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedTopicPartition, AssignmentEpoch,
    GroupId, GroupPositionFence, LiveGroupAssignment, MemberId,
};

use super::{
    model::{
        ClassicGroupFetchOwnerFault, ClassicGroupFetchOwnerFaultKind,
        ClassicGroupFetchTransitionFailure,
    },
    owner::ClassicGroupFetchOwner,
};

/// Successful Fetch-side disposition for one exact classic assignment loss.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::group) enum ClassicGroupFetchRetirement {
    /// The matching membership assignment had not activated Fetch.
    Inactive,
    /// The exact active Fetch assignment was retired and its controls were queued.
    Retired {
        position_fence: GroupPositionFence,
        assignment_epoch: AssignmentEpoch,
        controls: usize,
    },
}

/// Stable reason Fetch could not retire before catalog revocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::group) enum ClassicGroupFetchRetirementError {
    Faulted,
    InactiveMachineAssignment {
        assignment_epoch: AssignmentEpoch,
    },
    AssignmentIdentityMismatch {
        position_fence: GroupPositionFence,
        group_id: GroupId,
        member_id: MemberId,
        assignment_generation: kafka_client_core::AssignmentGeneration,
    },
    MachineAssignmentMismatch {
        binding: AssignmentEpoch,
        machine: Option<AssignmentEpoch>,
    },
    EffectCapacity {
        actual: usize,
        limit: usize,
    },
    Retained(ClassicGroupFetchOwnerFaultKind),
}

impl ClassicGroupFetchOwner {
    /// Retires the exact Fetch activation before its catalog assignment is revoked.
    ///
    /// Existing accepted calls are not cancelled. The queued `Revoke` controls
    /// first return local request and store reservations, then leave routed
    /// calls to drain through the ordinary stale-confirmation path.
    #[expect(
        clippy::too_many_lines,
        reason = "retirement is one ordered linear transition that must retain and fault the exact owner"
    )]
    pub(in crate::consumer::group) fn retire_for_assignment_loss(
        &mut self,
        assignment: &LiveGroupAssignment,
    ) -> Result<ClassicGroupFetchRetirement, ClassicGroupFetchRetirementError> {
        if self.is_faulted() {
            return Err(ClassicGroupFetchRetirementError::Faulted);
        }
        let Some(activation) = self.activation.as_ref() else {
            return match self.machine.assignment_epoch() {
                None => Ok(ClassicGroupFetchRetirement::Inactive),
                Some(assignment_epoch) => Err(
                    ClassicGroupFetchRetirementError::InactiveMachineAssignment {
                        assignment_epoch,
                    },
                ),
            };
        };
        let binding = activation.binding();
        let position_fence = binding.position_fence();
        if position_fence.group_id() != assignment.group_id()
            || position_fence.member_id() != assignment.member_id()
            || position_fence.assignment_generation() != assignment.assignment_generation()
        {
            return Err(
                ClassicGroupFetchRetirementError::AssignmentIdentityMismatch {
                    position_fence,
                    group_id: assignment.group_id(),
                    member_id: assignment.member_id(),
                    assignment_generation: assignment.assignment_generation(),
                },
            );
        }
        let assignment_epoch = binding.assignment_epoch();
        let machine_epoch = self.machine.assignment_epoch();
        if machine_epoch != Some(assignment_epoch) {
            return Err(
                ClassicGroupFetchRetirementError::MachineAssignmentMismatch {
                    binding: assignment_epoch,
                    machine: machine_epoch,
                },
            );
        }
        let actual = self
            .effects
            .len()
            .saturating_add(assignment.partitions().len());
        if actual > self.effect_capacity {
            return Err(ClassicGroupFetchRetirementError::EffectCapacity {
                actual,
                limit: self.effect_capacity,
            });
        }

        self.settle_seek_assignment_lost();
        let transition = match self.machine.apply(AssignedConsumerInput::RetireAssignment {
            assignment_epoch: Some(assignment_epoch),
        }) {
            Ok(transition) => transition,
            Err(error) => {
                let kind = ClassicGroupFetchOwnerFaultKind::Core(error);
                self.fault = Some(ClassicGroupFetchOwnerFault::Core {
                    _input: AssignedConsumerInput::RetireAssignment {
                        assignment_epoch: Some(assignment_epoch),
                    },
                    error,
                });
                return Err(ClassicGroupFetchRetirementError::Retained(kind));
            }
        };
        let controls = transition.effects().len();
        if !exact_retirement_controls(&transition, assignment, assignment_epoch) {
            let failure = ClassicGroupFetchTransitionFailure::RetirementControls;
            let kind = ClassicGroupFetchOwnerFaultKind::Transition(failure);
            self.fault = Some(ClassicGroupFetchOwnerFault::Transition {
                _transition: transition,
                failure,
            });
            return Err(ClassicGroupFetchRetirementError::Retained(kind));
        }
        let remaining = self.effect_capacity.saturating_sub(self.effects.len());
        if controls > remaining {
            let failure = ClassicGroupFetchTransitionFailure::EffectCapacity {
                actual: controls,
                limit: remaining,
            };
            let kind = ClassicGroupFetchOwnerFaultKind::Transition(failure);
            self.fault = Some(ClassicGroupFetchOwnerFault::Transition {
                _transition: transition,
                failure,
            });
            return Err(ClassicGroupFetchRetirementError::Retained(kind));
        }
        debug_assert_eq!(transition.assignment_epoch(), None);
        debug_assert_eq!(controls, assignment.partitions().len());
        for effect in transition.into_effects() {
            self.effects.push_back(effect);
        }
        self.activation = None;
        match self.fetches.take_ready() {
            Ok(Some(delivery)) => {
                if let Err(failure) = self.fetches.reclaim(delivery) {
                    self.fault = Some(ClassicGroupFetchOwnerFault::Reclaim { _failure: failure });
                    return Err(ClassicGroupFetchRetirementError::Retained(
                        ClassicGroupFetchOwnerFaultKind::Reclaim,
                    ));
                }
            }
            Ok(None) => {}
            Err(error) => {
                self.fault = Some(ClassicGroupFetchOwnerFault::Fetch(error));
                return Err(ClassicGroupFetchRetirementError::Retained(
                    ClassicGroupFetchOwnerFaultKind::Fetch(error),
                ));
            }
        }
        self.fetches.request_broker_session_close();
        Ok(ClassicGroupFetchRetirement::Retired {
            position_fence,
            assignment_epoch,
            controls,
        })
    }
}

fn exact_retirement_controls(
    transition: &kafka_client_core::AssignedConsumerTransition,
    assignment: &LiveGroupAssignment,
    assignment_epoch: AssignmentEpoch,
) -> bool {
    transition.assignment_epoch().is_none()
        && transition.effects().len() == assignment.partitions().len()
        && transition
            .effects()
            .iter()
            .zip(assignment.partitions())
            .all(|(effect, partition)| {
                *effect
                    == AssignedConsumerEffect::Revoke {
                        assignment_epoch,
                        partition: AssignedTopicPartition::new(
                            partition.topic_id(),
                            partition.partition(),
                        ),
                    }
            })
}
