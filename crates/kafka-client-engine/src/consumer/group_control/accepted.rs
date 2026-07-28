//! Stable accepted-control result and advisory host degradation.

use crate::consumer::group::GroupConsumerControlPortAccepted;

/// Advisory degradation after deterministic control progress was accepted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupConsumerControlAcceptedFaultKind {
    /// The host wake request failed after deterministic progress was accepted.
    Wake,
    /// Post-core retained ownership was inconsistent after acceptance.
    RetainedInvariant,
}

/// Accepted deterministic control progress.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use = "accepted control may report advisory host degradation"]
pub struct GroupConsumerControlAccepted {
    fault: Option<GroupConsumerControlAcceptedFaultKind>,
}

impl GroupConsumerControlAccepted {
    pub(super) const fn inert() -> Self {
        Self { fault: None }
    }

    pub(super) const fn from_port(accepted: GroupConsumerControlPortAccepted) -> Self {
        let fault = if accepted.retained_invariant() {
            Some(GroupConsumerControlAcceptedFaultKind::RetainedInvariant)
        } else if accepted.wake_failed() {
            Some(GroupConsumerControlAcceptedFaultKind::Wake)
        } else {
            None
        };
        Self { fault }
    }

    /// Returns post-acceptance advisory degradation, when present.
    pub const fn fault(self) -> Option<GroupConsumerControlAcceptedFaultKind> {
        self.fault
    }
}
