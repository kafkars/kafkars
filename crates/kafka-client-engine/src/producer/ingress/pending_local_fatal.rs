//! Exact recovery ownership for failed bounded pending-local settlement.

use crate::producer::pending::{
    PendingLocalFailure, PendingNotificationJob, ProducerSendFailure,
    turn_error::PendingTurnFailureOwnership,
};

/// Which local-settlement policy produced an exact recovery owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PendingLocalSettlementMode {
    Expiry,
    ShutdownDrain,
}

/// One failed local-settlement turn with every undelivered owner preserved.
#[must_use = "local settlement faults retain notification and pending-turn ownership"]
pub(crate) struct PendingLocalSettlementFatal {
    mode: PendingLocalSettlementMode,
    inspected: usize,
    retained_prefix: usize,
    refused_failure: Option<ProducerSendFailure>,
    refused: Option<PendingNotificationJob>,
    untouched: Vec<PendingLocalFailure>,
    source: Option<PendingTurnFailureOwnership>,
}

impl PendingLocalSettlementFatal {
    pub(super) const fn route_refusal(
        mode: PendingLocalSettlementMode,
        inspected: usize,
        retained_prefix: usize,
        refused_failure: ProducerSendFailure,
        refused: PendingNotificationJob,
        untouched: Vec<PendingLocalFailure>,
        source: Option<PendingTurnFailureOwnership>,
    ) -> Self {
        Self {
            mode,
            inspected,
            retained_prefix,
            refused_failure: Some(refused_failure),
            refused: Some(refused),
            untouched,
            source,
        }
    }

    pub(super) const fn source_failure(
        mode: PendingLocalSettlementMode,
        inspected: usize,
        retained_prefix: usize,
        source: PendingTurnFailureOwnership,
    ) -> Self {
        Self {
            mode,
            inspected,
            retained_prefix,
            refused_failure: None,
            refused: None,
            untouched: Vec::new(),
            source: Some(source),
        }
    }

    pub(crate) const fn mode(&self) -> PendingLocalSettlementMode {
        self.mode
    }

    #[cfg(test)]
    pub(crate) const fn inspected_for_test(&self) -> usize {
        self.inspected
    }

    #[cfg(test)]
    pub(crate) const fn retained_prefix_for_test(&self) -> usize {
        self.retained_prefix
    }

    #[cfg(test)]
    pub(crate) fn refused_for_test(&self) -> Option<&PendingNotificationJob> {
        self.refused.as_ref()
    }

    #[cfg(test)]
    pub(crate) const fn refused_failure_for_test(&self) -> Option<ProducerSendFailure> {
        self.refused_failure
    }

    #[cfg(test)]
    pub(crate) fn untouched_for_test(&self) -> &[PendingLocalFailure] {
        &self.untouched
    }

    #[cfg(test)]
    pub(crate) const fn source_for_test(&self) -> Option<&PendingTurnFailureOwnership> {
        self.source.as_ref()
    }
}

/// Refusal returning a later local-settlement owner without allocating.
#[must_use = "a later local-settlement fault remains an exact recovery owner"]
pub(crate) struct PendingLocalSettlementRetentionFailure {
    incoming: PendingLocalSettlementFatal,
}

impl PendingLocalSettlementRetentionFailure {
    pub(super) const fn new(incoming: PendingLocalSettlementFatal) -> Self {
        Self { incoming }
    }

    pub(crate) fn into_owner(self) -> PendingLocalSettlementFatal {
        self.incoming
    }
}
