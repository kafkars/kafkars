//! Linear handoff of one bounded pending identity and its sole send observer.

use super::{super::boundary::ProducerSend, PendingAdmissionId};

/// Successful pending registration before deterministic core admission.
#[must_use = "registration owns the only pending-send observer"]
pub(crate) struct PendingSendRegistration {
    id: PendingAdmissionId,
    send: ProducerSend,
}

impl PendingSendRegistration {
    pub(super) const fn new(id: PendingAdmissionId, send: ProducerSend) -> Self {
        Self { id, send }
    }

    pub(crate) const fn id(&self) -> PendingAdmissionId {
        self.id
    }

    pub(crate) fn into_send(self) -> ProducerSend {
        self.send
    }
}

impl std::fmt::Debug for PendingSendRegistration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PendingSendRegistration")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}
