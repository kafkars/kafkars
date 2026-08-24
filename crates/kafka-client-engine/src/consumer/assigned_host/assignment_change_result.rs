//! Stable synchronous outcomes for incremental direct-assignment changes.

use kafka_client_core::AssignmentEpoch;

use super::{
    assignment_change_error::assignment_change_error_kind,
    assignment_result::AssignedConsumerAssignmentEpoch,
    result::{
        AssignedConsumerAccepted, AssignedConsumerPortAcceptedFaultKind, AssignedConsumerPortError,
    },
};

/// Accepted change and any advisory post-commit wake fault.
#[must_use = "assignment-change acceptance and wake diagnostics must be inspected"]
#[derive(Clone, Copy)]
pub struct AssignedConsumerTryChangeAssignmentAccepted {
    epoch: Option<AssignedConsumerAssignmentEpoch>,
    fault: Option<super::AssignedConsumerAcceptedFaultKind>,
}

impl AssignedConsumerTryChangeAssignmentAccepted {
    /// Returns the current control epoch, or `None` for an unassigned empty no-op.
    pub const fn epoch(&self) -> Option<AssignedConsumerAssignmentEpoch> {
        self.epoch
    }

    /// Returns post-acceptance degradation without revoking acceptance.
    pub const fn fault(&self) -> Option<super::AssignedConsumerAcceptedFaultKind> {
        self.fault
    }

    pub(super) fn from_port(accepted: AssignedConsumerAccepted<Option<AssignmentEpoch>>) -> Self {
        let fault = accepted.fault().map(|fault| match fault {
            AssignedConsumerPortAcceptedFaultKind::Wake => {
                super::AssignedConsumerAcceptedFaultKind::Wake
            }
        });
        Self {
            epoch: accepted
                .into_value()
                .map(AssignedConsumerAssignmentEpoch::from_core),
            fault,
        }
    }
}

impl std::fmt::Debug for AssignedConsumerTryChangeAssignmentAccepted {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AssignedConsumerTryChangeAssignmentAccepted")
            .field("epoch", &self.epoch)
            .field("fault", &self.fault)
            .finish()
    }
}

/// Stable reason an incremental assignment change did not cross admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AssignedConsumerTryChangeAssignmentErrorKind {
    /// Another caller or the host currently owns the assigned-consumer shard.
    Contended,
    /// Assigned-consumer admission is permanently closed.
    Closed,
    /// Earlier accepted effects must be interpreted before another change.
    Pending,
    /// A nonempty removal was requested before any assignment existed.
    NoAssignment,
    /// The resulting assignment exceeds the configured partition bound.
    AssignmentCapacity,
    /// The retained topic catalog has no capacity for another topic.
    TopicCapacity,
    /// Retained topic-name bytes have reached their configured bound.
    RetainedNameCapacity,
    /// Terminal consumer-event claims occupy the bounded event store.
    EventCapacity,
    /// One topic-partition appeared more than once in the change.
    DuplicatePartition,
    /// An added topic-partition is already assigned.
    AlreadyAssigned,
    /// A removed topic-partition is not currently assigned.
    UnknownPartition,
    /// The requested addition timeout cannot become one absolute deadline.
    DeadlineOverflow,
    /// A bounded allocation, identity, or epoch domain is exhausted.
    ResourceExhausted,
    /// The synchronized host can no longer execute assigned-consumer work.
    HostUnavailable,
    /// A non-semantic engine mechanism violated its ownership contract.
    InternalInvariant,
}

/// Immediate rejection before incremental assignment ownership crossed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AssignedConsumerTryChangeAssignmentError {
    kind: AssignedConsumerTryChangeAssignmentErrorKind,
}

impl AssignedConsumerTryChangeAssignmentError {
    /// Returns the stable rejection category.
    pub const fn kind(&self) -> AssignedConsumerTryChangeAssignmentErrorKind {
        self.kind
    }

    pub(super) const fn from_port(error: &AssignedConsumerPortError) -> Self {
        Self {
            kind: assignment_change_error_kind(error),
        }
    }
}

impl std::fmt::Display for AssignedConsumerTryChangeAssignmentError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "assigned-consumer assignment change failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for AssignedConsumerTryChangeAssignmentError {}
