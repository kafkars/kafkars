//! Exact completion binding and terminal retention for one assigned close.

mod publication;
#[cfg(test)]
mod publication_test;

use kafka_client_core::{AssignedConsumerCloseId, AssignedConsumerEffect};

use crate::completion::CompletionId;

use super::assigned_close_error::{
    AssignedCloseEffectKind, AssignedCloseSlotError, AssignedCloseSlotPhase,
};

#[derive(Debug, Eq, PartialEq)]
enum AssignedCloseState {
    Vacant,
    Reserved(CompletionId),
    Accepted {
        completion_id: CompletionId,
        close_id: AssignedConsumerCloseId,
    },
    Ready {
        completion_id: CompletionId,
        close_id: AssignedConsumerCloseId,
    },
    Published,
}

/// One allocation-free terminal slot reserved before core close admission.
#[derive(Debug)]
pub(super) struct AssignedCloseSlot {
    state: AssignedCloseState,
}

impl AssignedCloseSlot {
    /// Creates the sole close slot for a future `AssignedConsumerOwner`.
    pub(super) const fn create_for_assigned_owner() -> Self {
        Self {
            state: AssignedCloseState::Vacant,
        }
    }

    /// Reserves terminal capacity before applying core `BeginClose`.
    pub(super) fn reserve(
        &mut self,
        completion_id: CompletionId,
    ) -> Result<(), AssignedCloseSlotError> {
        if self.phase() != AssignedCloseSlotPhase::Vacant {
            return Err(AssignedCloseSlotError::InvalidReservation {
                phase: self.phase(),
            });
        }
        self.state = AssignedCloseState::Reserved(completion_id);
        Ok(())
    }

    /// Releases capacity when core rejects `BeginClose`.
    pub(super) fn release_rejected(&mut self) -> Result<CompletionId, AssignedCloseSlotError> {
        let AssignedCloseState::Reserved(completion_id) = self.state else {
            return Err(AssignedCloseSlotError::InvalidRelease {
                phase: self.phase(),
            });
        };
        self.state = AssignedCloseState::Vacant;
        Ok(completion_id)
    }

    /// Applies only the ordered close effects emitted by core.
    pub(super) fn observe_close_effect(
        &mut self,
        effect: AssignedConsumerEffect,
    ) -> Result<(), AssignedCloseSlotError> {
        match effect {
            AssignedConsumerEffect::AcceptClose { close_id } => self.accept(close_id),
            AssignedConsumerEffect::CompleteClose { close_id } => self.complete(close_id),
            effect => Err(AssignedCloseSlotError::UnexpectedEffect { effect }),
        }
    }

    /// Returns the accepted identity needed for the exact `CloseDrained` fact.
    pub(super) fn accepted_id(&self) -> Result<AssignedConsumerCloseId, AssignedCloseSlotError> {
        match &self.state {
            AssignedCloseState::Accepted { close_id, .. } => Ok(*close_id),
            _ => Err(AssignedCloseSlotError::AcceptedIdUnavailable {
                phase: self.phase(),
            }),
        }
    }

    /// Reports the scalar lifecycle phase without exposing retained identity.
    pub(super) const fn phase(&self) -> AssignedCloseSlotPhase {
        match &self.state {
            AssignedCloseState::Vacant => AssignedCloseSlotPhase::Vacant,
            AssignedCloseState::Reserved(_) => AssignedCloseSlotPhase::Reserved,
            AssignedCloseState::Accepted { .. } => AssignedCloseSlotPhase::Accepted,
            AssignedCloseState::Ready { .. } => AssignedCloseSlotPhase::Ready,
            AssignedCloseState::Published => AssignedCloseSlotPhase::Published,
        }
    }

    fn accept(&mut self, supplied: AssignedConsumerCloseId) -> Result<(), AssignedCloseSlotError> {
        match &self.state {
            AssignedCloseState::Reserved(completion_id) => {
                self.state = AssignedCloseState::Accepted {
                    completion_id: *completion_id,
                    close_id: supplied,
                };
                Ok(())
            }
            AssignedCloseState::Accepted {
                close_id: active, ..
            }
            | AssignedCloseState::Ready {
                close_id: active, ..
            } if *active == supplied => Err(AssignedCloseSlotError::DuplicateEffect {
                effect: AssignedCloseEffectKind::Accept,
                close_id: supplied,
            }),
            AssignedCloseState::Accepted {
                close_id: active, ..
            }
            | AssignedCloseState::Ready {
                close_id: active, ..
            } => Err(AssignedCloseSlotError::MismatchedCloseId {
                effect: AssignedCloseEffectKind::Accept,
                active: *active,
                supplied,
            }),
            AssignedCloseState::Published => Err(AssignedCloseSlotError::StaleEffect {
                effect: AssignedCloseEffectKind::Accept,
                close_id: supplied,
            }),
            AssignedCloseState::Vacant => Err(AssignedCloseSlotError::EffectOutOfOrder {
                effect: AssignedCloseEffectKind::Accept,
                close_id: supplied,
                phase: AssignedCloseSlotPhase::Vacant,
            }),
        }
    }

    fn complete(
        &mut self,
        supplied: AssignedConsumerCloseId,
    ) -> Result<(), AssignedCloseSlotError> {
        match &self.state {
            AssignedCloseState::Accepted {
                completion_id,
                close_id: active,
            } if *active == supplied => {
                self.state = AssignedCloseState::Ready {
                    completion_id: *completion_id,
                    close_id: supplied,
                };
                Ok(())
            }
            AssignedCloseState::Accepted {
                close_id: active, ..
            } => Err(AssignedCloseSlotError::MismatchedCloseId {
                effect: AssignedCloseEffectKind::Complete,
                active: *active,
                supplied,
            }),
            AssignedCloseState::Ready {
                close_id: active, ..
            } => {
                if *active == supplied {
                    Err(AssignedCloseSlotError::DuplicateEffect {
                        effect: AssignedCloseEffectKind::Complete,
                        close_id: supplied,
                    })
                } else {
                    Err(AssignedCloseSlotError::MismatchedCloseId {
                        effect: AssignedCloseEffectKind::Complete,
                        active: *active,
                        supplied,
                    })
                }
            }
            AssignedCloseState::Published => Err(AssignedCloseSlotError::StaleEffect {
                effect: AssignedCloseEffectKind::Complete,
                close_id: supplied,
            }),
            AssignedCloseState::Vacant | AssignedCloseState::Reserved(_) => {
                Err(AssignedCloseSlotError::EffectOutOfOrder {
                    effect: AssignedCloseEffectKind::Complete,
                    close_id: supplied,
                    phase: self.phase(),
                })
            }
        }
    }
}
