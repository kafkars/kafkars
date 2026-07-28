//! Linear engine owners for one sequential group-position reset.

use kafka_client_core::{
    GroupAssignmentPartition, GroupPositionBootstrapMachine, GroupPositionResetMachine,
    GroupPositionResetTerminal, Moment, StartPosition,
};

use crate::{
    clock::OperationDeadline,
    driver::{
        ClassicGroupPositionResetCall, ClassicGroupPositionResetCompletionError,
        ClassicGroupPositionResetRoute, ListOffsetsResolution,
    },
    protocol::consumer::ListOffsetsIsolation,
};

#[must_use = "a prepared group position reset must be submitted or terminally settled"]
pub(in crate::consumer::group) struct ClassicGroupPositionResetPrepared {
    pub(in crate::consumer::group) bootstrap: GroupPositionBootstrapMachine,
    pub(in crate::consumer::group) reset: GroupPositionResetMachine,
    pub(in crate::consumer::group) operation_deadline: OperationDeadline,
    pub(in crate::consumer::group) partition: GroupAssignmentPartition,
    pub(in crate::consumer::group) position: StartPosition,
}

#[must_use = "a driver-owned group position reset must settle or recover"]
pub(in crate::consumer::group) struct ClassicGroupPositionResetDriverOwned {
    pub(in crate::consumer::group) bootstrap: GroupPositionBootstrapMachine,
    pub(in crate::consumer::group) reset: GroupPositionResetMachine,
    pub(in crate::consumer::group) operation_deadline: OperationDeadline,
    pub(in crate::consumer::group) partition: GroupAssignmentPartition,
    pub(in crate::consumer::group) topic: String,
    pub(in crate::consumer::group) isolation: ListOffsetsIsolation,
    pub(in crate::consumer::group) call: ClassicGroupPositionResetCall,
}

#[must_use = "a failed group position reset must be observed or explicitly retired"]
pub(in crate::consumer::group) struct ClassicGroupPositionResetCompleted {
    pub(in crate::consumer::group) _bootstrap: GroupPositionBootstrapMachine,
    pub(in crate::consumer::group) _reset: GroupPositionResetMachine,
    pub(in crate::consumer::group) terminal: GroupPositionResetTerminal,
    pub(in crate::consumer::group) _operation_deadline: OperationDeadline,
    pub(in crate::consumer::group) _observed_at: Moment,
}

impl ClassicGroupPositionResetCompleted {
    pub(in crate::consumer::group) const fn terminal(&self) -> &GroupPositionResetTerminal {
        &self.terminal
    }
}

#[must_use = "a reset completion fault remains owned until driver shutdown recovery"]
pub(in crate::consumer::group) struct ClassicGroupPositionResetCompletionFault {
    pub(in crate::consumer::group) _bootstrap: GroupPositionBootstrapMachine,
    pub(in crate::consumer::group) _reset: GroupPositionResetMachine,
    pub(in crate::consumer::group) _operation_deadline: OperationDeadline,
    pub(in crate::consumer::group) _source: ClassicGroupPositionResetCompletionError,
}

#[must_use = "a reset terminal fault retains its route capability until shutdown recovery"]
pub(in crate::consumer::group) struct ClassicGroupPositionResetTerminalFault {
    pub(in crate::consumer::group) _bootstrap: GroupPositionBootstrapMachine,
    pub(in crate::consumer::group) _reset: GroupPositionResetMachine,
    pub(in crate::consumer::group) _operation_deadline: OperationDeadline,
    pub(in crate::consumer::group) _partition: GroupAssignmentPartition,
    pub(in crate::consumer::group) _terminal: ListOffsetsResolution,
    pub(in crate::consumer::group) _route: ClassicGroupPositionResetRoute,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::group) enum ClassicGroupPositionResetTurn {
    Idle,
    Progress,
}
