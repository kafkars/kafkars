//! Linear shard-turn refusal retaining every uninstalled pending owner.
#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "turn refusal remains guarded with its dormant caller"
    )
)]

use crate::producer::ProducerHostInvariantError;

use super::{
    pending_fatal::PendingShardFatalRetentionFailure,
    pending_local_fatal::PendingLocalSettlementRetentionFailure,
    shard_turn_progress::ProducerShardTurnProgress,
};

/// Exact pending owner that could not replace an earlier shard fault.
#[must_use = "the refused pending owner must reach terminal recovery"]
pub(crate) enum ProducerShardTurnFailureOwner {
    Local(PendingLocalSettlementRetentionFailure),
    Promotion(PendingShardFatalRetentionFailure),
}

/// Exhaustive reason one turn requires terminal ownership handoff.
#[must_use = "every failed turn cause must reach terminal supervision"]
pub(crate) enum ProducerShardTurnFailureCause {
    Host(ProducerHostInvariantError),
    Pending(ProducerShardTurnFailureOwner),
    /// Structural future handoff; current single-lock shard turns cannot return it.
    HostAndPending {
        host: ProducerHostInvariantError,
        pending: ProducerShardTurnFailureOwner,
    },
}

/// Terminal handoff preserving linear ownership and final Copy evidence.
#[must_use = "a failed shard turn requires terminal ownership handoff"]
pub(crate) struct ProducerShardTurnFailure {
    cause: ProducerShardTurnFailureCause,
    progress: ProducerShardTurnProgress,
}

impl ProducerShardTurnFailure {
    pub(super) const fn new(
        cause: ProducerShardTurnFailureCause,
        progress: ProducerShardTurnProgress,
    ) -> Self {
        Self { cause, progress }
    }

    pub(crate) const fn accepted_invariant(&self) -> Option<ProducerHostInvariantError> {
        match &self.cause {
            ProducerShardTurnFailureCause::Host(error)
            | ProducerShardTurnFailureCause::HostAndPending { host: error, .. } => Some(*error),
            ProducerShardTurnFailureCause::Pending(_) => None,
        }
    }

    pub(crate) const fn progress(&self) -> ProducerShardTurnProgress {
        self.progress
    }

    pub(crate) fn into_parts(self) -> (ProducerShardTurnProgress, ProducerShardTurnFailureCause) {
        (self.progress, self.cause)
    }
}
