//! Separate linear execution ownership for one classic assignment's heartbeat.

use kafka_client_core::{ClassicHeartbeatSchedule, Deadline};

use crate::{
    driver::classic_group::{AcceptedClassicHeartbeatCall, ClassicHeartbeatCallKey},
    protocol::consumer::PreparedClassicHeartbeatRequest,
};

/// Generated request and exact deadline waiting for bounded driver admission.
#[must_use = "a prepared Heartbeat must be submitted or deliberately released"]
pub(super) struct PreparedClassicHeartbeat {
    key: ClassicHeartbeatCallKey,
    request: PreparedClassicHeartbeatRequest,
}

impl PreparedClassicHeartbeat {
    pub(super) const fn new(
        key: ClassicHeartbeatCallKey,
        request: PreparedClassicHeartbeatRequest,
    ) -> Self {
        Self { key, request }
    }

    pub(super) const fn key(&self) -> ClassicHeartbeatCallKey {
        self.key
    }

    pub(super) fn into_parts(self) -> (ClassicHeartbeatCallKey, PreparedClassicHeartbeatRequest) {
        (self.key, self.request)
    }
}

/// Exact accepted receipt paired with the core attempt that authorized it.
#[must_use = "a driver-owned Heartbeat must settle or reconcile after shutdown"]
pub(super) struct ClassicHeartbeatDriverOwner {
    accepted: AcceptedClassicHeartbeatCall,
}

/// Impossible accepted-receipt mismatch retaining both exact identities.
#[must_use = "a failed Heartbeat acceptance still owns its accepted receipt"]
pub(super) struct ClassicHeartbeatAcceptanceFailure {
    expected: ClassicHeartbeatCallKey,
    accepted: AcceptedClassicHeartbeatCall,
}

impl ClassicHeartbeatAcceptanceFailure {
    pub(super) const fn new(
        expected: ClassicHeartbeatCallKey,
        accepted: AcceptedClassicHeartbeatCall,
    ) -> Self {
        Self { expected, accepted }
    }

    pub(super) const fn retained_owner_count(&self) -> usize {
        let _ = (self.expected, self.accepted.key());
        1
    }

    #[cfg(test)]
    pub(super) const fn expected(&self) -> ClassicHeartbeatCallKey {
        self.expected
    }

    #[cfg(test)]
    pub(super) const fn accepted(&self) -> &AcceptedClassicHeartbeatCall {
        &self.accepted
    }
}

impl ClassicHeartbeatDriverOwner {
    pub(super) const fn new(accepted: AcceptedClassicHeartbeatCall) -> Self {
        Self { accepted }
    }

    pub(super) const fn accepted(&self) -> &AcceptedClassicHeartbeatCall {
        &self.accepted
    }

    pub(super) fn into_accepted(self) -> AcceptedClassicHeartbeatCall {
        self.accepted
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(super) enum ClassicHeartbeatSuccessor {
    Dormant,
    Waiting(ClassicHeartbeatSchedule),
}

impl ClassicHeartbeatSuccessor {
    pub(super) fn into_state(self) -> ClassicHeartbeatExecutionState {
        match self {
            Self::Dormant => ClassicHeartbeatExecutionState::Dormant,
            Self::Waiting(schedule) => ClassicHeartbeatExecutionState::Waiting(schedule),
        }
    }
}

pub(super) enum ClassicHeartbeatExecutionState {
    Dormant,
    Waiting(ClassicHeartbeatSchedule),
    Prepared(PreparedClassicHeartbeat),
    Handoff(ClassicHeartbeatCallKey),
    DriverOwned(ClassicHeartbeatDriverOwner),
    ConfirmationPending {
        owner: ClassicHeartbeatDriverOwner,
        successor: ClassicHeartbeatSuccessor,
    },
}

/// Sole mechanism owner for heartbeat cadence, requests, and accepted receipts.
pub(super) struct ClassicHeartbeatExecution {
    heartbeat_execution_state: ClassicHeartbeatExecutionState,
}

/// Prevalidated schedule installation paired with the catalog's linear commit.
#[must_use = "a prevalidated Heartbeat schedule must be committed"]
pub(super) struct PreparedClassicHeartbeatInstall<'a> {
    execution: &'a mut ClassicHeartbeatExecution,
    schedule: ClassicHeartbeatSchedule,
}

impl ClassicHeartbeatExecution {
    pub(super) const fn new() -> Self {
        Self {
            heartbeat_execution_state: ClassicHeartbeatExecutionState::Dormant,
        }
    }

    pub(super) const fn state(&self) -> &ClassicHeartbeatExecutionState {
        &self.heartbeat_execution_state
    }

    pub(super) const fn is_dormant(&self) -> bool {
        matches!(
            self.heartbeat_execution_state,
            ClassicHeartbeatExecutionState::Dormant
        )
    }

    pub(super) const fn prepared(&self) -> Option<&PreparedClassicHeartbeat> {
        match &self.heartbeat_execution_state {
            ClassicHeartbeatExecutionState::Prepared(prepared) => Some(prepared),
            _ => None,
        }
    }

    pub(super) fn prepare_install(
        &mut self,
        schedule: ClassicHeartbeatSchedule,
    ) -> Result<PreparedClassicHeartbeatInstall<'_>, ClassicHeartbeatExecutionError> {
        if !self.is_dormant() {
            return Err(ClassicHeartbeatExecutionError::Occupied);
        }
        Ok(PreparedClassicHeartbeatInstall {
            execution: self,
            schedule,
        })
    }

    pub(super) fn clear_local(&mut self) -> Result<(), ClassicHeartbeatExecutionError> {
        match self.replace(ClassicHeartbeatExecutionState::Dormant) {
            ClassicHeartbeatExecutionState::Dormant
            | ClassicHeartbeatExecutionState::Waiting(_)
            | ClassicHeartbeatExecutionState::Prepared(_) => Ok(()),
            state @ (ClassicHeartbeatExecutionState::Handoff(_)
            | ClassicHeartbeatExecutionState::DriverOwned(_)
            | ClassicHeartbeatExecutionState::ConfirmationPending { .. }) => {
                self.set(state);
                Err(ClassicHeartbeatExecutionError::DriverOwned)
            }
        }
    }

    pub(super) const fn blocks_close(&self) -> bool {
        matches!(
            self.heartbeat_execution_state,
            ClassicHeartbeatExecutionState::Handoff(_)
                | ClassicHeartbeatExecutionState::DriverOwned(_)
                | ClassicHeartbeatExecutionState::ConfirmationPending { .. }
        )
    }

    pub(super) const fn accepted(&self) -> Option<&AcceptedClassicHeartbeatCall> {
        match &self.heartbeat_execution_state {
            ClassicHeartbeatExecutionState::DriverOwned(owner)
            | ClassicHeartbeatExecutionState::ConfirmationPending { owner, .. } => {
                Some(owner.accepted())
            }
            _ => None,
        }
    }

    pub(super) const fn next_deadline(&self) -> Option<Deadline> {
        match &self.heartbeat_execution_state {
            ClassicHeartbeatExecutionState::Waiting(schedule) => Some(schedule.next_deadline()),
            ClassicHeartbeatExecutionState::Prepared(prepared) => {
                Some(prepared.key().deadline().core())
            }
            ClassicHeartbeatExecutionState::Dormant
            | ClassicHeartbeatExecutionState::Handoff(_)
            | ClassicHeartbeatExecutionState::DriverOwned(_)
            | ClassicHeartbeatExecutionState::ConfirmationPending { .. } => None,
        }
    }

    pub(super) const fn unsettled(&self) -> usize {
        if self.is_dormant() { 0 } else { 1 }
    }

    pub(super) fn replace(
        &mut self,
        replacement: ClassicHeartbeatExecutionState,
    ) -> ClassicHeartbeatExecutionState {
        core::mem::replace(&mut self.heartbeat_execution_state, replacement)
    }

    pub(super) fn set(&mut self, state: ClassicHeartbeatExecutionState) {
        self.heartbeat_execution_state = state;
    }
}

impl PreparedClassicHeartbeatInstall<'_> {
    pub(super) fn commit(self) {
        self.execution
            .set(ClassicHeartbeatExecutionState::Waiting(self.schedule));
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ClassicHeartbeatExecutionError {
    Occupied,
    DriverOwned,
}
