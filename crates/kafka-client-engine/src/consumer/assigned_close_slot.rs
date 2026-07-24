//! Fixed-capacity terminal retention for one future assigned-consumer owner.

use kafka_client_core::{AssignedConsumerCloseId, AssignedConsumerEffect};

use super::assigned_close_error::{
    AssignedCloseEffectKind, AssignedCloseSlotError, AssignedCloseSlotPhase,
};

#[derive(Debug, Eq, PartialEq)]
enum AssignedCloseState {
    Vacant,
    Reserved,
    Accepted(AssignedConsumerCloseId),
    Ready(AssignedConsumerCloseId),
    Reclaimed,
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
    pub(super) fn reserve(&mut self) -> Result<(), AssignedCloseSlotError> {
        if self.phase() != AssignedCloseSlotPhase::Vacant {
            return Err(AssignedCloseSlotError::InvalidReservation {
                phase: self.phase(),
            });
        }
        self.state = AssignedCloseState::Reserved;
        Ok(())
    }

    /// Releases capacity when core rejects `BeginClose`.
    pub(super) fn release_rejected(&mut self) -> Result<(), AssignedCloseSlotError> {
        if self.phase() != AssignedCloseSlotPhase::Reserved {
            return Err(AssignedCloseSlotError::InvalidRelease {
                phase: self.phase(),
            });
        }
        self.state = AssignedCloseState::Vacant;
        Ok(())
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

    /// Takes the retained terminal identity and permanently reclaims the slot.
    pub(super) fn take_ready(&mut self) -> Result<AssignedConsumerCloseId, AssignedCloseSlotError> {
        let AssignedCloseState::Ready(close_id) = &self.state else {
            return Err(AssignedCloseSlotError::TerminalUnavailable {
                phase: self.phase(),
            });
        };
        let close_id = *close_id;
        self.state = AssignedCloseState::Reclaimed;
        Ok(close_id)
    }

    /// Returns the accepted identity needed for the exact `CloseDrained` fact.
    pub(super) fn accepted_id(&self) -> Result<AssignedConsumerCloseId, AssignedCloseSlotError> {
        match &self.state {
            AssignedCloseState::Accepted(close_id) => Ok(*close_id),
            _ => Err(AssignedCloseSlotError::AcceptedIdUnavailable {
                phase: self.phase(),
            }),
        }
    }

    /// Reports the scalar lifecycle phase without exposing retained identity.
    pub(super) const fn phase(&self) -> AssignedCloseSlotPhase {
        match &self.state {
            AssignedCloseState::Vacant => AssignedCloseSlotPhase::Vacant,
            AssignedCloseState::Reserved => AssignedCloseSlotPhase::Reserved,
            AssignedCloseState::Accepted(_) => AssignedCloseSlotPhase::Accepted,
            AssignedCloseState::Ready(_) => AssignedCloseSlotPhase::Ready,
            AssignedCloseState::Reclaimed => AssignedCloseSlotPhase::Reclaimed,
        }
    }

    fn accept(&mut self, supplied: AssignedConsumerCloseId) -> Result<(), AssignedCloseSlotError> {
        match &self.state {
            AssignedCloseState::Reserved => {
                self.state = AssignedCloseState::Accepted(supplied);
                Ok(())
            }
            AssignedCloseState::Accepted(active) | AssignedCloseState::Ready(active)
                if *active == supplied =>
            {
                Err(AssignedCloseSlotError::DuplicateEffect {
                    effect: AssignedCloseEffectKind::Accept,
                    close_id: supplied,
                })
            }
            AssignedCloseState::Accepted(active) | AssignedCloseState::Ready(active) => {
                Err(AssignedCloseSlotError::MismatchedCloseId {
                    effect: AssignedCloseEffectKind::Accept,
                    active: *active,
                    supplied,
                })
            }
            AssignedCloseState::Reclaimed => Err(AssignedCloseSlotError::StaleEffect {
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
            AssignedCloseState::Accepted(active) if *active == supplied => {
                self.state = AssignedCloseState::Ready(supplied);
                Ok(())
            }
            AssignedCloseState::Accepted(active) => {
                Err(AssignedCloseSlotError::MismatchedCloseId {
                    effect: AssignedCloseEffectKind::Complete,
                    active: *active,
                    supplied,
                })
            }
            AssignedCloseState::Ready(active) => {
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
            AssignedCloseState::Reclaimed => Err(AssignedCloseSlotError::StaleEffect {
                effect: AssignedCloseEffectKind::Complete,
                close_id: supplied,
            }),
            AssignedCloseState::Vacant | AssignedCloseState::Reserved => {
                Err(AssignedCloseSlotError::EffectOutOfOrder {
                    effect: AssignedCloseEffectKind::Complete,
                    close_id: supplied,
                    phase: self.phase(),
                })
            }
        }
    }
}
