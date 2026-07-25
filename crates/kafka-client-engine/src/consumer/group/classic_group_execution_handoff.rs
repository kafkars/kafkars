//! Lossless Join transfer through the execution owner's guarded state operations.

use super::{
    classic_group_execution::{ClassicGroupExecution, ClassicGroupExecutionError},
    classic_group_join::{
        ClassicGroupExecutionState, ClassicGroupJoinDriverAcceptance, ClassicGroupJoinHandoff,
        ClassicGroupJoinTracking,
    },
};

impl ClassicGroupExecution {
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "consumed by the next Join protocol execution slice"
        )
    )]
    pub(super) fn begin_join_handoff(
        &mut self,
    ) -> Result<ClassicGroupJoinHandoff, ClassicGroupExecutionError> {
        let state = self.replace_execution_state(ClassicGroupExecutionState::Idle);
        let ClassicGroupExecutionState::PreparedJoin(prepared) = state else {
            self.set_execution_state(state);
            return Err(ClassicGroupExecutionError::JoinNotPrepared);
        };
        let handoff = ClassicGroupJoinHandoff::new(prepared);
        self.set_execution_state(ClassicGroupExecutionState::JoinHandoff(handoff.identity()));
        Ok(handoff)
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "consumed by the next Join protocol execution slice"
        )
    )]
    pub(super) fn restore_join(
        &mut self,
        handoff: ClassicGroupJoinHandoff,
    ) -> Result<(), (ClassicGroupExecutionError, ClassicGroupJoinHandoff)> {
        let state = self.borrow_execution_state();
        let matches = matches!(
            state,
            ClassicGroupExecutionState::JoinHandoff(identity)
                if *identity == handoff.identity()
        );
        if matches {
            self.set_execution_state(ClassicGroupExecutionState::PreparedJoin(
                handoff.into_prepared(),
            ));
            Ok(())
        } else {
            Err((ClassicGroupExecutionError::HandoffMismatch, handoff))
        }
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "consumed by the next Join protocol execution slice"
        )
    )]
    pub(super) fn confirm_join_driver_owned(
        &mut self,
        acceptance: ClassicGroupJoinDriverAcceptance,
    ) -> Result<
        ClassicGroupJoinTracking,
        (ClassicGroupExecutionError, ClassicGroupJoinDriverAcceptance),
    > {
        let state = self.borrow_execution_state();
        let matches = matches!(
            state,
            ClassicGroupExecutionState::JoinHandoff(identity)
                if *identity == acceptance.identity()
        );
        if !matches {
            return Err((ClassicGroupExecutionError::HandoffMismatch, acceptance));
        }
        let (driver_owned, tracking) = acceptance.into_driver_owners();
        self.set_execution_state(ClassicGroupExecutionState::JoinDriverOwned(driver_owned));
        Ok(tracking)
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "consumed by Join protocol shutdown recovery once that slice lands"
        )
    )]
    pub(super) fn recover_join_after_driver_shutdown(
        &mut self,
        tracking: ClassicGroupJoinTracking,
    ) -> Result<(), (ClassicGroupExecutionError, ClassicGroupJoinTracking)> {
        let state = self.borrow_execution_state();
        let matches = matches!(
            state,
            ClassicGroupExecutionState::JoinDriverOwned(driver_owned)
                if driver_owned.identity() == tracking.identity()
        );
        if !matches {
            return Err((ClassicGroupExecutionError::HandoffMismatch, tracking));
        }
        let state = self.replace_execution_state(ClassicGroupExecutionState::Idle);
        let ClassicGroupExecutionState::JoinDriverOwned(driver_owned) = state else {
            self.set_execution_state(state);
            return Err((ClassicGroupExecutionError::HandoffMismatch, tracking));
        };
        self.set_execution_state(ClassicGroupExecutionState::PreparedJoin(
            driver_owned.into_prepared(),
        ));
        Ok(())
    }
}
