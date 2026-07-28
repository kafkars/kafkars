//! Stable control rejection and exhaustive private-port translation.

use kafka_client_core::AssignedConsumerMachineError;

use crate::consumer::{
    group::{ClassicGroupFetchControlError as Fetch, GroupConsumerControlPortError},
    group_control::partition::GroupConsumerPartition,
    group_control::resume_capture::GroupConsumerResumeCaptureError,
};

/// Stable pre-core batch-control rejection category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupConsumerControlErrorKind {
    /// Engine or group admission has closed.
    Closed,
    /// Another owner currently holds the classic-group shard.
    Contended,
    /// The synchronized engine host can no longer expose the group owner.
    HostUnavailable,
    /// The registered group no longer accepts this operation.
    GroupUnavailable,
    /// The group has no active assignment.
    NoAssignment,
    /// Earlier group Fetch control remains unsettled.
    Pending,
    /// The caller supplied the same topic-partition more than once.
    DuplicatePartition,
    /// A topic-partition is absent from the active assignment.
    UnknownPartition,
    /// A paused topic-partition has no retained position to resume.
    PositionNotRetained,
    /// Bounded event, effect, or allocation capacity is unavailable.
    ResourceExhausted,
    /// Internal ownership was inconsistent.
    InternalInvariant,
}

/// Rejected control retaining the exact caller-owned target vector.
#[derive(Debug)]
#[must_use = "control rejection retains the exact target vector"]
pub struct GroupConsumerControlError {
    kind: GroupConsumerControlErrorKind,
    partitions: Vec<GroupConsumerPartition>,
}

impl GroupConsumerControlError {
    pub(super) fn from_resume_capture(
        _error: GroupConsumerResumeCaptureError,
        partitions: Vec<GroupConsumerPartition>,
    ) -> Self {
        Self {
            kind: GroupConsumerControlErrorKind::HostUnavailable,
            partitions,
        }
    }

    pub(super) fn from_port(
        error: GroupConsumerControlPortError,
        partitions: Vec<GroupConsumerPartition>,
    ) -> Self {
        Self {
            kind: control_error_kind(error),
            partitions,
        }
    }

    /// Returns the stable rejection category.
    pub const fn kind(&self) -> GroupConsumerControlErrorKind {
        self.kind
    }

    /// Borrows the exact rejected caller order.
    pub fn partitions(&self) -> &[GroupConsumerPartition] {
        &self.partitions
    }

    /// Recovers the exact rejected target vector for retry.
    pub fn into_partitions(self) -> Vec<GroupConsumerPartition> {
        self.partitions
    }
}

impl core::fmt::Display for GroupConsumerControlError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(formatter, "classic-group control rejected: {:?}", self.kind)
    }
}

impl std::error::Error for GroupConsumerControlError {}

const fn control_error_kind(error: GroupConsumerControlPortError) -> GroupConsumerControlErrorKind {
    match error {
        GroupConsumerControlPortError::Closed
        | GroupConsumerControlPortError::Fetch(Fetch::Core(
            AssignedConsumerMachineError::ConsumerClosed,
        )) => GroupConsumerControlErrorKind::Closed,
        GroupConsumerControlPortError::Lock(error) if error.is_contended() => {
            GroupConsumerControlErrorKind::Contended
        }
        GroupConsumerControlPortError::Lock(_) => GroupConsumerControlErrorKind::HostUnavailable,
        GroupConsumerControlPortError::UnknownGroup
        | GroupConsumerControlPortError::GroupUnavailable
        | GroupConsumerControlPortError::Fetch(
            Fetch::Faulted
            | Fetch::BindingMismatch
            | Fetch::Core(AssignedConsumerMachineError::StaleAssignment { .. }),
        ) => GroupConsumerControlErrorKind::GroupUnavailable,
        GroupConsumerControlPortError::NoAssignment
        | GroupConsumerControlPortError::Fetch(
            Fetch::Inactive | Fetch::Core(AssignedConsumerMachineError::NoAssignment),
        ) => GroupConsumerControlErrorKind::NoAssignment,
        GroupConsumerControlPortError::DuplicatePartition
        | GroupConsumerControlPortError::Fetch(Fetch::Core(
            AssignedConsumerMachineError::DuplicatePartition { .. },
        )) => GroupConsumerControlErrorKind::DuplicatePartition,
        GroupConsumerControlPortError::UnknownPartition
        | GroupConsumerControlPortError::Fetch(Fetch::Core(
            AssignedConsumerMachineError::UnknownPartition { .. },
        )) => GroupConsumerControlErrorKind::UnknownPartition,
        GroupConsumerControlPortError::Fetch(Fetch::Core(
            AssignedConsumerMachineError::PositionNotRetained { .. },
        )) => GroupConsumerControlErrorKind::PositionNotRetained,
        GroupConsumerControlPortError::Allocation
        | GroupConsumerControlPortError::Fetch(
            Fetch::EffectCapacity
            | Fetch::Event(
                crate::consumer::assigned_event::AssignedConsumerEventStoreError::Capacity,
            )
            | Fetch::Core(
                AssignedConsumerMachineError::ControlAllocationFailed
                | AssignedConsumerMachineError::AssignmentEpochExhausted
                | AssignedConsumerMachineError::PositionEpochExhausted { .. }
                | AssignedConsumerMachineError::FetchRevisionExhausted { .. },
            ),
        ) => GroupConsumerControlErrorKind::ResourceExhausted,
        GroupConsumerControlPortError::Fetch(Fetch::Pending) => {
            GroupConsumerControlErrorKind::Pending
        }
        GroupConsumerControlPortError::Fetch(Fetch::Core(_) | Fetch::Event(_)) => {
            GroupConsumerControlErrorKind::InternalInvariant
        }
    }
}
