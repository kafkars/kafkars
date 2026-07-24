//! Stable synchronous acceptance and rejection values for assignment replacement.

use kafka_client_core::AssignmentEpoch;

use super::{
    assignment_error::assignment_error_kind,
    result::{
        AssignedConsumerAccepted, AssignedConsumerPortAcceptedFaultKind, AssignedConsumerPortError,
    },
};

/// Opaque engine representation of one accepted assignment generation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AssignedConsumerAssignmentEpoch(u64);

impl AssignedConsumerAssignmentEpoch {
    /// Returns the stable scalar epoch used by later engine control boundaries.
    pub const fn get(self) -> u64 {
        self.0
    }

    pub(super) const fn from_core(epoch: AssignmentEpoch) -> Self {
        Self(epoch.get())
    }
}

/// Accepted synchronous replacement and any advisory post-commit wake fault.
#[must_use = "assignment acceptance and wake diagnostics must be inspected"]
#[derive(Clone, Copy)]
pub struct AssignedConsumerTryReplaceAssignmentAccepted {
    epoch: AssignedConsumerAssignmentEpoch,
    fault: Option<super::AssignedConsumerAcceptedFaultKind>,
}

impl AssignedConsumerTryReplaceAssignmentAccepted {
    /// Returns the new assignment generation.
    pub const fn epoch(&self) -> AssignedConsumerAssignmentEpoch {
        self.epoch
    }

    /// Returns post-acceptance degradation without revoking assignment acceptance.
    pub const fn fault(&self) -> Option<super::AssignedConsumerAcceptedFaultKind> {
        self.fault
    }

    pub(super) fn from_port(accepted: AssignedConsumerAccepted<AssignmentEpoch>) -> Self {
        let fault = accepted.fault().map(|fault| match fault {
            AssignedConsumerPortAcceptedFaultKind::Wake => {
                super::AssignedConsumerAcceptedFaultKind::Wake
            }
        });
        Self {
            epoch: AssignedConsumerAssignmentEpoch::from_core(accepted.into_value()),
            fault,
        }
    }
}

impl std::fmt::Debug for AssignedConsumerTryReplaceAssignmentAccepted {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AssignedConsumerTryReplaceAssignmentAccepted")
            .field("epoch", &self.epoch)
            .field("fault", &self.fault)
            .finish()
    }
}

/// Stable reason an assignment replacement did not cross admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AssignedConsumerTryReplaceAssignmentErrorKind {
    /// Another caller or the host currently owns the assigned-consumer shard.
    Contended,
    /// Assigned-consumer admission is permanently closed.
    Closed,
    /// Earlier accepted effects must be interpreted before replacement.
    Pending,
    /// The replacement exceeds the configured partition bound.
    AssignmentCapacity,
    /// The retained topic catalog has no capacity for another topic.
    TopicCapacity,
    /// Retained topic-name bytes have reached their configured bound.
    RetainedNameCapacity,
    /// Terminal consumer-event claims currently occupy their bounded store.
    EventCapacity,
    /// A direct assignment must contain at least one partition.
    EmptyAssignment,
    /// One topic-partition appeared more than once.
    DuplicatePartition,
    /// The requested timeout cannot become one absolute deadline.
    DeadlineOverflow,
    /// A bounded local allocation or identity domain is exhausted.
    ResourceExhausted,
    /// The synchronized host can no longer execute new assigned-consumer work.
    HostUnavailable,
    /// A non-semantic engine mechanism violated its ownership contract.
    InternalInvariant,
}

/// Immediate assignment-replacement rejection before ownership crossed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AssignedConsumerTryReplaceAssignmentError {
    kind: AssignedConsumerTryReplaceAssignmentErrorKind,
}

impl AssignedConsumerTryReplaceAssignmentError {
    /// Returns the stable rejection category.
    pub const fn kind(&self) -> AssignedConsumerTryReplaceAssignmentErrorKind {
        self.kind
    }

    pub(super) const fn from_port(error: &AssignedConsumerPortError) -> Self {
        Self {
            kind: assignment_error_kind(error),
        }
    }
}

impl std::fmt::Display for AssignedConsumerTryReplaceAssignmentError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "assigned-consumer assignment replacement failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for AssignedConsumerTryReplaceAssignmentError {}
