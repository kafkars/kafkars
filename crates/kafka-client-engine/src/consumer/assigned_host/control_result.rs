//! Stable synchronous acceptance and rejection values for position control.

use super::{
    control_error::control_error_kind,
    result::{
        AssignedConsumerAccepted, AssignedConsumerPortAcceptedFaultKind, AssignedConsumerPortError,
    },
};

/// Accepted pause, resume, or seek and any advisory post-commit wake fault.
#[must_use = "control acceptance and wake diagnostics must be inspected"]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AssignedConsumerControlAccepted {
    fault: Option<super::AssignedConsumerAcceptedFaultKind>,
}

impl AssignedConsumerControlAccepted {
    /// Returns post-acceptance degradation without revoking the control change.
    pub const fn fault(&self) -> Option<super::AssignedConsumerAcceptedFaultKind> {
        self.fault
    }

    pub(super) fn from_port(accepted: AssignedConsumerAccepted<()>) -> Self {
        let fault = accepted.fault().map(|fault| match fault {
            AssignedConsumerPortAcceptedFaultKind::Wake => {
                super::AssignedConsumerAcceptedFaultKind::Wake
            }
        });
        let () = accepted.into_value();
        Self { fault }
    }
}

/// Stable reason pause, resume, or seek did not cross admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AssignedConsumerControlErrorKind {
    /// Another caller or the host currently owns the assigned-consumer shard.
    Contended,
    /// Assigned-consumer admission is permanently closed.
    Closed,
    /// Earlier accepted effects must be interpreted before control admission.
    Pending,
    /// No direct assignment has been accepted.
    NoAssignment,
    /// The supplied opaque assignment generation has been superseded.
    StaleAssignment,
    /// The named topic-partition is not in the active assignment.
    UnknownPartition,
    /// An explicit seek offset was negative.
    NegativeOffset,
    /// The requested timeout cannot become one absolute deadline.
    DeadlineOverflow,
    /// A bounded claim, allocation, or identity domain is exhausted.
    ResourceExhausted,
    /// The synchronized host can no longer execute new assigned-consumer work.
    HostUnavailable,
    /// A non-semantic engine mechanism violated its ownership contract.
    InternalInvariant,
}

/// Immediate position-control rejection before ownership crossed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AssignedConsumerControlError {
    kind: AssignedConsumerControlErrorKind,
}

impl AssignedConsumerControlError {
    /// Returns the stable rejection category.
    pub const fn kind(&self) -> AssignedConsumerControlErrorKind {
        self.kind
    }

    pub(super) const fn from_port(error: &AssignedConsumerPortError) -> Self {
        Self {
            kind: control_error_kind(error),
        }
    }
}

impl std::fmt::Display for AssignedConsumerControlError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "assigned-consumer position control failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for AssignedConsumerControlError {}
