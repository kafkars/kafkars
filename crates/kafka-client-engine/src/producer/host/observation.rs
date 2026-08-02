//! Read-only observations over producer host state.

use kafka_client_core::{OperationId, ProducerEffect};

use super::ProducerHost;
use crate::clock::OperationDeadline;

impl ProducerHost {
    pub(crate) fn pending_effects(&self) -> &[ProducerEffect] {
        &self.pending_effects
    }

    /// Reports a generated submission that can join one ready deadline cohort next turn.
    pub(crate) fn has_pending_produce_submission_at(&self, deadline: OperationDeadline) -> bool {
        self.pending_effects
            .iter()
            .filter_map(pending_deadline_operation)
            .any(|operation| self.bindings.deadline(operation) == Some(deadline))
    }

    /// Reports the deterministic core's producer admission decision.
    pub(crate) const fn admission_is_open(&self) -> bool {
        self.core.admission_is_open()
    }
}

const fn pending_deadline_operation(effect: &ProducerEffect) -> Option<OperationId> {
    match *effect {
        ProducerEffect::SubmitProduce {
            deadline_operation_id,
            ..
        } => Some(deadline_operation_id),
        _ => None,
    }
}
