//! Exact admission of classic and KIP-848 assignments into one bounded revocation owner.

use kafka_client_core::{
    ClassicGeneration, ClassicGracefulRevocationEffect, ClassicGracefulRevocationInput,
    ClassicGracefulRevocationLease, ClassicGracefulRevocationLossReason,
    ClassicGracefulRevocationTerminal, LiveGroupAssignment, Moment,
};

use super::{
    model::{ClassicGroupRevocationBeginError, PendingGroupRevocation, one_effect},
    owner::ClassicGroupRevocationOwner,
};

impl ClassicGroupRevocationOwner {
    pub(in crate::consumer::group) fn begin(
        &mut self,
        assignment: LiveGroupAssignment,
        generation: ClassicGeneration,
        lease: ClassicGracefulRevocationLease,
        now: Moment,
    ) -> Result<(), (ClassicGroupRevocationBeginError, LiveGroupAssignment)> {
        self.begin_pending(
            PendingGroupRevocation::classic(assignment, generation),
            lease,
            now,
        )
        .map_err(|(error, pending)| (error, pending.into_assignment()))
    }

    pub(in crate::consumer::group) fn begin_consumer(
        &mut self,
        assignment: LiveGroupAssignment,
        lease: ClassicGracefulRevocationLease,
        now: Moment,
    ) -> Result<(), (ClassicGroupRevocationBeginError, LiveGroupAssignment)> {
        self.begin_pending(PendingGroupRevocation::consumer(assignment), lease, now)
            .map_err(|(error, pending)| (error, pending.into_assignment()))
    }

    pub(in crate::consumer::group) fn begin_classic_reconciliation(
        &mut self,
        assignment: LiveGroupAssignment,
        generation: ClassicGeneration,
        lease: ClassicGracefulRevocationLease,
        now: Moment,
    ) -> Result<(), (ClassicGroupRevocationBeginError, LiveGroupAssignment)> {
        self.begin_pending(
            PendingGroupRevocation::classic_reconciliation(assignment, generation),
            lease,
            now,
        )
        .map_err(|(error, pending)| (error, pending.into_assignment()))
    }

    fn begin_pending(
        &mut self,
        pending: PendingGroupRevocation,
        lease: ClassicGracefulRevocationLease,
        now: Moment,
    ) -> Result<(), (ClassicGroupRevocationBeginError, PendingGroupRevocation)> {
        if self.pending.is_some()
            || self.core.active_lease().is_some()
            || self.core.terminal().is_some()
        {
            return Err((ClassicGroupRevocationBeginError::Occupied, pending));
        }
        let transition = match self
            .core
            .apply(ClassicGracefulRevocationInput::Begin { lease, now })
        {
            Ok(transition) => transition,
            Err(error) => {
                return Err((ClassicGroupRevocationBeginError::Core(error), pending));
            }
        };
        match one_effect(&transition) {
            Some(ClassicGracefulRevocationEffect::Arm { lease: armed }) if armed == lease => {}
            Some(ClassicGracefulRevocationEffect::Complete {
                terminal:
                    ClassicGracefulRevocationTerminal::Lost {
                        lease: expired,
                        reason: ClassicGracefulRevocationLossReason::DeadlineElapsed,
                    },
            }) if expired == lease => {}
            _ => {
                return Err((ClassicGroupRevocationBeginError::UnexpectedEffect, pending));
            }
        }
        self.pending = Some(pending);
        Ok(())
    }
}
