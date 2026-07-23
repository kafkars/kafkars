//! Aggregate byte, pending-admission, and deterministic host ownership for one shard.

use std::sync::Arc;

use kafka_client_core::Moment;

use crate::{
    clock::OperationDeadline,
    producer::{
        ProducerHost, ProducerHostInvariantError, ProducerRecord, ProducerRejectionReason,
        ProducerStoreError,
        admission::{AdmittedExplicit, ProducerAdmissionFailure, RejectedExplicit},
        host::ProducerHostStats,
        host_turn::{ProducerTurnBudget, ProducerTurnOutcome},
        pending::{
            PendingAdmissionRegistry, PendingAdmissionRejected, PendingAdmissionStats,
            PendingNotificationPermitPool, PendingSendRegistration,
        },
    },
};

use super::{
    promotion,
    promotion_error::{PendingPromotionFailure, PendingPromotionProgress},
};

/// Combined accounting visible without splitting the shard's synchronization owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProducerShardStats {
    pub(crate) host: ProducerHostStats,
    pub(crate) pending: PendingAdmissionStats,
    pub(crate) aggregate_retained_bytes: usize,
    pub(crate) retained_byte_limit: usize,
    pub(crate) pending_notification_capacity: usize,
    pub(crate) accepting: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProducerShardAdmission {
    Running,
    Closed,
}

/// Sole synchronized owner of accepted and pre-core producer admission.
pub(crate) struct ProducerShardData {
    pub(super) host: ProducerHost,
    pub(super) pending: PendingAdmissionRegistry,
    pub(super) pending_notification_permits: Arc<PendingNotificationPermitPool>,
    retained_byte_limit: usize,
    admission: ProducerShardAdmission,
}

impl ProducerShardData {
    pub(super) fn new(host: ProducerHost) -> Self {
        let retained_byte_limit = host.retained_byte_limit();
        let pending_notification_permits = host.pending_notification_permits();
        let pending = PendingAdmissionRegistry::with_notification_permits(
            pending_notification_permits.capacity(),
            retained_byte_limit,
            Arc::clone(&pending_notification_permits),
        );
        Self {
            host,
            pending,
            pending_notification_permits,
            retained_byte_limit,
            admission: ProducerShardAdmission::Running,
        }
    }

    /// Closes both admission populations while the one shard mutex is held.
    pub(crate) fn close_admission(&mut self) {
        self.admission = ProducerShardAdmission::Closed;
        self.pending.begin_close();
        self.host.close_admission();
    }

    #[allow(
        clippy::result_large_err,
        reason = "ownership-preserving rejection returns the intact record"
    )]
    pub(super) fn try_admit_explicit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        record: ProducerRecord,
    ) -> Result<AdmittedExplicit, ProducerAdmissionFailure> {
        if self.admission == ProducerShardAdmission::Running && !self.allows_record(&record) {
            return Err(ProducerAdmissionFailure::Rejected(RejectedExplicit::new(
                ProducerRejectionReason::Store(ProducerStoreError::ByteCapacity),
                record,
            )));
        }
        self.host.try_admit_explicit(now, deadline, record)
    }

    pub(crate) fn turn(
        &mut self,
        now: Moment,
        budget: ProducerTurnBudget,
    ) -> Result<ProducerTurnOutcome, ProducerHostInvariantError> {
        self.host.turn(now, budget)
    }

    /// Resolves at most one FIFO pending attempt without wiring live scheduling.
    #[allow(
        dead_code,
        reason = "public registration remains guarded until recovery and shutdown integration"
    )]
    pub(crate) fn promote_next(
        &mut self,
        now: Moment,
    ) -> Result<PendingPromotionProgress, PendingPromotionFailure> {
        if self.admission == ProducerShardAdmission::Closed {
            return Err(PendingPromotionFailure::Closed);
        }
        promotion::promote_next(&mut self.host, &mut self.pending, now)
    }

    pub(crate) fn unsettled_completions(&self) -> usize {
        self.host.unsettled_completions()
    }

    /// Checks the one application-byte ceiling before immediate core admission.
    fn allows_record(&self, record: &ProducerRecord) -> bool {
        let Ok(record_bytes) = record.retained_bytes() else {
            return true;
        };
        self.aggregate_retained_bytes()
            .checked_add(record_bytes)
            .is_some_and(|next| next <= self.retained_byte_limit)
    }

    pub(crate) fn shard_stats(&self) -> ProducerShardStats {
        let host = self.host.stats();
        let pending = self.pending.stats();
        ProducerShardStats {
            host,
            pending,
            aggregate_retained_bytes: self.aggregate_retained_bytes(),
            retained_byte_limit: self.retained_byte_limit,
            pending_notification_capacity: self.pending_notification_permits.capacity(),
            accepting: self.admission == ProducerShardAdmission::Running,
        }
    }

    /// Reports whether older pending ownership fences a new immediate caller.
    pub(super) fn has_pending(&self) -> bool {
        self.pending.has_entries()
    }

    #[allow(
        clippy::result_large_err,
        reason = "pending rejection returns the intact record for retry"
    )]
    pub(crate) fn register_pending(
        &mut self,
        record: ProducerRecord,
        deadline: OperationDeadline,
    ) -> Result<PendingSendRegistration, PendingAdmissionRejected> {
        if self.admission == ProducerShardAdmission::Closed {
            return Err(PendingAdmissionRejected::new(
                crate::producer::pending::PendingAdmissionRejectionReason::Closed,
                record,
            ));
        }
        if !self.allows_record(&record) {
            return Err(PendingAdmissionRejected::new(
                crate::producer::pending::PendingAdmissionRejectionReason::ByteCapacity,
                record,
            ));
        }
        self.pending.register(record, deadline)
    }

    fn aggregate_retained_bytes(&self) -> usize {
        self.host
            .stats()
            .store
            .bytes
            .saturating_add(self.pending.stats().retained_bytes)
    }

    #[cfg(test)]
    pub(crate) fn inject_post_acceptance_fault(&mut self, error: ProducerHostInvariantError) {
        self.host.inject_post_acceptance_fault(error);
    }

    #[cfg(test)]
    pub(crate) fn inject_terminal_interpretation_fault(&mut self) {
        self.host.inject_terminal_interpretation_fault();
    }

    #[cfg(test)]
    pub(crate) fn bound_deadline(
        &self,
        operation_id: kafka_client_core::OperationId,
    ) -> Option<OperationDeadline> {
        self.host.bindings.deadline(operation_id)
    }
}
