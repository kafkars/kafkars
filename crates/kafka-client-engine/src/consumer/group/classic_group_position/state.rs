//! Linear ownership states for one assignment-fenced position bootstrap.

#[cfg(test)]
use kafka_client_core::GroupPositionBootstrapState;
use kafka_client_core::{
    GroupPositionBootstrapMachine, GroupPositionFence, GroupPositionPartitionFact,
};

use crate::{
    driver::{GroupPositionOffsetFetchAccepted, GroupPositionOffsetFetchKey},
    protocol::consumer::{GroupOffsetFetchCorrelation, PreparedGroupOffsetFetchRequest},
};

/// Protocol request and deterministic owner waiting for submission.
#[must_use = "a prepared position bootstrap must be submitted or deliberately retained"]
pub(in crate::consumer::group) struct ClassicGroupPositionPrepared {
    key: GroupPositionOffsetFetchKey,
    machine: GroupPositionBootstrapMachine,
    correlation: GroupOffsetFetchCorrelation,
    request: PreparedGroupOffsetFetchRequest,
    result_buffer: Vec<GroupPositionPartitionFact>,
}

impl ClassicGroupPositionPrepared {
    pub(super) const fn new(
        key: GroupPositionOffsetFetchKey,
        machine: GroupPositionBootstrapMachine,
        correlation: GroupOffsetFetchCorrelation,
        request: PreparedGroupOffsetFetchRequest,
        result_buffer: Vec<GroupPositionPartitionFact>,
    ) -> Self {
        Self {
            key,
            machine,
            correlation,
            request,
            result_buffer,
        }
    }

    pub(in crate::consumer::group) const fn key(&self) -> &GroupPositionOffsetFetchKey {
        &self.key
    }

    pub(in crate::consumer::group) fn into_parts(
        self,
    ) -> (
        GroupPositionOffsetFetchKey,
        GroupPositionBootstrapMachine,
        GroupOffsetFetchCorrelation,
        PreparedGroupOffsetFetchRequest,
        Vec<GroupPositionPartitionFact>,
    ) {
        (
            self.key,
            self.machine,
            self.correlation,
            self.request,
            self.result_buffer,
        )
    }
}

/// Core and correlation owners retained during synchronous RPC handoff.
#[must_use = "an in-progress position handoff must resolve exactly once"]
pub(in crate::consumer::group) struct ClassicGroupPositionHandoff {
    machine: GroupPositionBootstrapMachine,
    correlation: GroupOffsetFetchCorrelation,
    result_buffer: Vec<GroupPositionPartitionFact>,
}

impl ClassicGroupPositionHandoff {
    pub(super) const fn new(
        machine: GroupPositionBootstrapMachine,
        correlation: GroupOffsetFetchCorrelation,
        result_buffer: Vec<GroupPositionPartitionFact>,
    ) -> Self {
        Self {
            machine,
            correlation,
            result_buffer,
        }
    }

    pub(super) const fn fence(&self) -> GroupPositionFence {
        self.machine.fence()
    }

    pub(super) const fn deadline(&self) -> kafka_client_core::Deadline {
        self.machine.deadline()
    }

    pub(super) fn correlation(&self) -> &GroupOffsetFetchCorrelation {
        &self.correlation
    }

    pub(super) fn into_parts(
        self,
    ) -> (
        GroupPositionBootstrapMachine,
        GroupOffsetFetchCorrelation,
        Vec<GroupPositionPartitionFact>,
    ) {
        (self.machine, self.correlation, self.result_buffer)
    }
}

/// Exact accepted receipt beside the core and response-correlation owners.
#[must_use = "a driver-owned position bootstrap must settle or recover"]
pub(in crate::consumer::group) struct ClassicGroupPositionDriverOwned {
    machine: GroupPositionBootstrapMachine,
    correlation: GroupOffsetFetchCorrelation,
    accepted: GroupPositionOffsetFetchAccepted,
    result_buffer: Vec<GroupPositionPartitionFact>,
}

impl ClassicGroupPositionDriverOwned {
    pub(super) const fn new(
        machine: GroupPositionBootstrapMachine,
        correlation: GroupOffsetFetchCorrelation,
        accepted: GroupPositionOffsetFetchAccepted,
        result_buffer: Vec<GroupPositionPartitionFact>,
    ) -> Self {
        Self {
            machine,
            correlation,
            accepted,
            result_buffer,
        }
    }

    pub(super) const fn fence(&self) -> GroupPositionFence {
        self.machine.fence()
    }

    pub(in crate::consumer::group) const fn accepted(&self) -> &GroupPositionOffsetFetchAccepted {
        &self.accepted
    }

    #[cfg(test)]
    pub(in crate::consumer::group) const fn core_state(&self) -> GroupPositionBootstrapState {
        self.machine.state()
    }

    pub(in crate::consumer::group) fn into_parts(
        self,
    ) -> (
        GroupPositionBootstrapMachine,
        GroupOffsetFetchCorrelation,
        GroupPositionOffsetFetchAccepted,
        Vec<GroupPositionPartitionFact>,
    ) {
        (
            self.machine,
            self.correlation,
            self.accepted,
            self.result_buffer,
        )
    }
}
