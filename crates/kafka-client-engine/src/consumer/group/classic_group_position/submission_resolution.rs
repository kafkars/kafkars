//! Core resolution of accepted and definitely rejected position submissions.

use kafka_client_core::{GroupPositionBootstrapEffect, GroupPositionBootstrapInput, Moment};

use crate::driver::{GroupPositionOffsetFetchAccepted, GroupPositionOffsetFetchKey};

use super::{
    ClassicGroupPositionAcceptanceFailure, ClassicGroupPositionCompleted,
    ClassicGroupPositionDriverOwned, ClassicGroupPositionExecution,
    ClassicGroupPositionExecutionError, ClassicGroupPositionExecutionState,
    ClassicGroupPositionHandoff, ClassicGroupPositionRejectionFailure,
};

#[expect(
    clippy::result_large_err,
    reason = "submission failures retain exact handoff, receipt, and core owners without allocation"
)]
impl ClassicGroupPositionExecution {
    pub(in crate::consumer::group) fn confirm_driver_owned(
        &mut self,
        accepted: GroupPositionOffsetFetchAccepted,
    ) -> Result<(), ClassicGroupPositionAcceptanceFailure> {
        let state = self.replace(ClassicGroupPositionExecutionState::Dormant);
        let ClassicGroupPositionExecutionState::Handoff(handoff) = state else {
            self.set(state);
            return Err(ClassicGroupPositionAcceptanceFailure::pre_core(
                accepted,
                ClassicGroupPositionExecutionError::NotInHandoff,
            ));
        };
        let expected = handoff.fence();
        let supplied = accepted.fence();
        if expected != supplied {
            self.set(ClassicGroupPositionExecutionState::Handoff(handoff));
            return Err(ClassicGroupPositionAcceptanceFailure::pre_core(
                accepted,
                ClassicGroupPositionExecutionError::FenceMismatch { expected, supplied },
            ));
        }
        let (mut machine, correlation, result_buffer) = handoff.into_parts();
        let transition = match machine.apply(accepted.driver_accepted()) {
            Ok(transition) => transition,
            Err(error) => {
                self.set(ClassicGroupPositionExecutionState::Handoff(
                    ClassicGroupPositionHandoff::new(machine, correlation, result_buffer),
                ));
                return Err(ClassicGroupPositionAcceptanceFailure::pre_core(
                    accepted,
                    ClassicGroupPositionExecutionError::Core(error.kind()),
                ));
            }
        };
        if transition.into_effect().is_some() {
            self.set(ClassicGroupPositionExecutionState::DriverOwned(
                ClassicGroupPositionDriverOwned::new(machine, correlation, accepted, result_buffer),
            ));
            return Err(ClassicGroupPositionAcceptanceFailure::post_core(
                supplied,
                ClassicGroupPositionExecutionError::DriverAcceptedEffect,
            ));
        }
        self.set(ClassicGroupPositionExecutionState::DriverOwned(
            ClassicGroupPositionDriverOwned::new(machine, correlation, accepted, result_buffer),
        ));
        Ok(())
    }

    #[expect(
        clippy::needless_pass_by_value,
        reason = "the linear key crosses the terminal boundary by ownership and must not remain reusable"
    )]
    pub(in crate::consumer::group) fn finish_driver_rejected(
        &mut self,
        key: GroupPositionOffsetFetchKey,
        now: Moment,
    ) -> Result<(), ClassicGroupPositionRejectionFailure> {
        let state = self.replace(ClassicGroupPositionExecutionState::Dormant);
        let ClassicGroupPositionExecutionState::Handoff(handoff) = state else {
            self.set(state);
            return Err(ClassicGroupPositionRejectionFailure::in_execution(
                key.fence(),
                ClassicGroupPositionExecutionError::NotInHandoff,
            ));
        };
        let expected = handoff.fence();
        let supplied = key.fence();
        if expected != supplied {
            self.set(ClassicGroupPositionExecutionState::Handoff(handoff));
            return Err(ClassicGroupPositionRejectionFailure::in_execution(
                supplied,
                ClassicGroupPositionExecutionError::FenceMismatch { expected, supplied },
            ));
        }
        if handoff.deadline() != key.operation_deadline().core() {
            self.set(ClassicGroupPositionExecutionState::Handoff(handoff));
            return Err(ClassicGroupPositionRejectionFailure::in_execution(
                supplied,
                ClassicGroupPositionExecutionError::DeadlineMismatch,
            ));
        }
        let expected_deadline = key.operation_deadline().core();
        let (mut machine, correlation, result_buffer) = handoff.into_parts();
        let transition = match machine.apply(GroupPositionBootstrapInput::DriverRejected {
            fence: supplied,
            now,
        }) {
            Ok(transition) => transition,
            Err(error) => {
                self.set(ClassicGroupPositionExecutionState::Handoff(
                    ClassicGroupPositionHandoff::new(machine, correlation, result_buffer),
                ));
                return Err(ClassicGroupPositionRejectionFailure::in_execution(
                    supplied,
                    ClassicGroupPositionExecutionError::Core(error.kind()),
                ));
            }
        };
        match transition.into_effect() {
            Some(GroupPositionBootstrapEffect::Complete {
                fence,
                deadline,
                terminal,
            }) => {
                self.set(ClassicGroupPositionExecutionState::Complete(
                    ClassicGroupPositionCompleted::new(machine, terminal),
                ));
                if fence != supplied {
                    return Err(ClassicGroupPositionRejectionFailure::in_execution(
                        supplied,
                        ClassicGroupPositionExecutionError::CompletionFence,
                    ));
                }
                if deadline != expected_deadline {
                    return Err(ClassicGroupPositionRejectionFailure::in_execution(
                        supplied,
                        ClassicGroupPositionExecutionError::CompletionDeadline,
                    ));
                }
                Ok(())
            }
            effect => Err(ClassicGroupPositionRejectionFailure::post_core(
                supplied,
                ClassicGroupPositionExecutionError::DriverRejectedEffect,
                machine,
                correlation,
                result_buffer,
                effect,
            )),
        }
    }
}
