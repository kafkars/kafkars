//! Applied terminal ownership for one assignment-fenced position bootstrap.

use kafka_client_core::{
    GroupPositionBootstrapMachine, GroupPositionBootstrapTerminal, GroupPositionFence, Moment,
};

use crate::{clock::OperationDeadline, driver::GroupPositionOffsetFetchAccepted};

/// Core terminal already applied exactly once and retained for later activation.
#[must_use = "a completed position bootstrap must be consumed by a later owner"]
pub(in crate::consumer::group) struct ClassicGroupPositionCompleted(
    GroupPositionBootstrapMachine,
    GroupPositionBootstrapTerminal,
    Moment,
    OperationDeadline,
);

impl ClassicGroupPositionCompleted {
    pub(in crate::consumer::group) const fn new_with_operation_deadline(
        machine: GroupPositionBootstrapMachine,
        terminal: GroupPositionBootstrapTerminal,
        observed_at: Moment,
        operation_deadline: OperationDeadline,
    ) -> Self {
        Self(machine, terminal, observed_at, operation_deadline)
    }

    #[cfg(test)]
    pub(super) fn new(
        machine: GroupPositionBootstrapMachine,
        terminal: GroupPositionBootstrapTerminal,
        observed_at: Moment,
    ) -> Self {
        let operation_deadline = OperationDeadline::from_core_for_test(machine.deadline());
        Self(machine, terminal, observed_at, operation_deadline)
    }

    pub(in crate::consumer::group) const fn fence(&self) -> GroupPositionFence {
        self.0.fence()
    }

    pub(in crate::consumer::group) const fn terminal(&self) -> &GroupPositionBootstrapTerminal {
        &self.1
    }

    /// Returns when the terminal fact entered deterministic position policy.
    pub(in crate::consumer::group) const fn observed_at(&self) -> Moment {
        self.2
    }

    pub(in crate::consumer::group) fn into_parts(
        self,
    ) -> (
        GroupPositionBootstrapMachine,
        GroupPositionBootstrapTerminal,
        Moment,
        OperationDeadline,
    ) {
        (self.0, self.1, self.2, self.3)
    }
}

/// Applied core terminal waiting only for exact RPC route confirmation.
#[must_use = "a confirmation-pending position bootstrap still owns its receipt"]
pub(in crate::consumer::group) struct ClassicGroupPositionConfirmationPending {
    completed: ClassicGroupPositionCompleted,
    accepted: GroupPositionOffsetFetchAccepted,
}

impl ClassicGroupPositionConfirmationPending {
    pub(super) const fn new(
        completed: ClassicGroupPositionCompleted,
        accepted: GroupPositionOffsetFetchAccepted,
    ) -> Self {
        Self {
            completed,
            accepted,
        }
    }

    pub(super) const fn fence(&self) -> GroupPositionFence {
        self.completed.fence()
    }

    pub(super) const fn accepted(&self) -> &GroupPositionOffsetFetchAccepted {
        &self.accepted
    }

    #[cfg(test)]
    pub(super) const fn completed(&self) -> &ClassicGroupPositionCompleted {
        &self.completed
    }

    pub(super) fn into_parts(
        self,
    ) -> (
        ClassicGroupPositionCompleted,
        GroupPositionOffsetFetchAccepted,
    ) {
        (self.completed, self.accepted)
    }
}
