//! Sole deterministic machine and activation-state mutation owner for group Fetch.

use kafka_client_core::{AssignedConsumerMachine, GroupPositionFence};

use super::{
    super::classic_group_position::{
        ClassicGroupPositionCompleted, prepare_classic_group_fetch_activation,
    },
    activation::{
        ClassicGroupFetchActivation, ClassicGroupFetchActivationError,
        ClassicGroupFetchActivationFailure, ClassicGroupFetchActivationFault,
        ClassicGroupFetchBinding, ClassicGroupFetchPostCoreFaultKind,
    },
};

/// One group-specific deterministic Fetch policy owner.
pub(in crate::consumer::group) struct ClassicGroupFetchOwner {
    machine: AssignedConsumerMachine,
    activation: Option<ClassicGroupFetchActivation>,
    fault: Option<ClassicGroupFetchActivationFault>,
}

impl ClassicGroupFetchOwner {
    pub(in crate::consumer::group) const fn new() -> Self {
        Self {
            machine: AssignedConsumerMachine::new(),
            activation: None,
            fault: None,
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "the internal lossless boundary returns the exact completed position without hidden boxing"
    )]
    pub(in crate::consumer::group) fn try_activate(
        &mut self,
        completed: ClassicGroupPositionCompleted,
        current_fence: GroupPositionFence,
    ) -> Result<(), ClassicGroupFetchActivationError> {
        if self.activation.is_some() || self.fault.is_some() {
            return Err(ClassicGroupFetchActivationError::Returned(
                ClassicGroupFetchActivationFailure::already_active(completed),
            ));
        }
        let input = match prepare_classic_group_fetch_activation(&completed, current_fence) {
            Ok(input) => input,
            Err(error) => {
                return Err(ClassicGroupFetchActivationError::Returned(
                    ClassicGroupFetchActivationFailure::position(completed, error),
                ));
            }
        };
        let transition = match self.machine.install_resolved_assignment(input) {
            Ok(transition) => transition,
            Err(error) => {
                return Err(ClassicGroupFetchActivationError::Returned(
                    ClassicGroupFetchActivationFailure::core(completed, error),
                ));
            }
        };
        let Some(assignment_epoch) = transition.assignment_epoch() else {
            let kind = ClassicGroupFetchPostCoreFaultKind::MissingAssignmentEpoch;
            self.fault = Some(ClassicGroupFetchActivationFault::new(
                completed, transition, kind,
            ));
            return Err(ClassicGroupFetchActivationError::Retained(kind));
        };
        let binding = ClassicGroupFetchBinding::new(completed.fence(), assignment_epoch);
        self.activation = Some(ClassicGroupFetchActivation::new(binding, transition));
        Ok(())
    }

    pub(in crate::consumer::group) const fn activation(
        &self,
    ) -> Option<&ClassicGroupFetchActivation> {
        self.activation.as_ref()
    }

    pub(in crate::consumer::group) const fn fault(
        &self,
    ) -> Option<&ClassicGroupFetchActivationFault> {
        self.fault.as_ref()
    }

    #[cfg(test)]
    pub(in crate::consumer::group) const fn machine_assignment_epoch(
        &self,
    ) -> Option<kafka_client_core::AssignmentEpoch> {
        self.machine.assignment_epoch()
    }
}
