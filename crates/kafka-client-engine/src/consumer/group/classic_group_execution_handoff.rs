//! Lossless Join transfer through the execution owner's guarded state operations.

use crate::driver::classic_group::{AcceptedJoinGroupCall, JoinGroupCallKey};

use super::{
    classic_group_execution::{ClassicGroupExecution, ClassicGroupExecutionError},
    classic_group_join::{
        ClassicGroupExecutionState, ClassicGroupJoinDriverAcceptance, ClassicGroupJoinHandoff,
    },
    classic_group_join_call::{ClassicGroupJoinAcceptanceFailure, ClassicGroupJoinCallOwner},
};

impl ClassicGroupExecution {
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

    #[expect(
        clippy::result_large_err,
        reason = "failed restoration returns the exact linear Join handoff without allocation"
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

    pub(super) fn confirm_join_driver_owned(
        &mut self,
        acceptance: ClassicGroupJoinDriverAcceptance,
        accepted: AcceptedJoinGroupCall,
    ) -> Result<(), ClassicGroupJoinAcceptanceFailure> {
        let identity = acceptance.identity();
        let expected_key =
            JoinGroupCallKey::new(identity.group_id(), identity.cycle(), identity.deadline());
        let state = self.borrow_execution_state();
        let matches = matches!(
            state,
            ClassicGroupExecutionState::JoinHandoff(identity)
                if *identity == acceptance.identity()
        ) && accepted.key() == expected_key;
        if !matches {
            return Err(ClassicGroupJoinAcceptanceFailure::new(acceptance, accepted));
        }
        let (driver_owned, tracking) = acceptance.into_driver_owners();
        self.set_execution_state(ClassicGroupExecutionState::JoinDriverOwned(
            ClassicGroupJoinCallOwner::new(driver_owned, tracking, accepted),
        ));
        Ok(())
    }
}
