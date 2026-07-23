//! Dormant bounded coordination of pending and accepted producer-shard work.
#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "composite turn remains guarded until terminal host handoff"
    )
)]

use std::num::NonZeroUsize;

use kafka_client_core::{Moment, ProducerEffect};

use crate::producer::{
    host_turn::{ProducerTurnBudget, ProducerTurnOutcome},
    pending::{PendingNotificationRouteMode, PendingNotificationRouteProgress},
};

use super::{
    data::{ProducerShardAdmission, ProducerShardData},
    pending_local_settlement::PendingLocalSettlementProgress,
    pending_settlement::PendingSettlementProgress,
    shard_turn_failure::{
        ProducerShardTurnFailure, ProducerShardTurnFailureCause, ProducerShardTurnFailureOwner,
    },
    shard_turn_progress::{
        ProducerShardTurnProgress, ProducerShardTurnSnapshot, ProducerShardTurnState, blocked,
        min_deadline, runnable,
    },
};

/// Caller-owned time and independent nonzero limits for every shard mechanism.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProducerShardTurnInput {
    pub(crate) now: Moment,
    pub(crate) close_requested: bool,
    pub(crate) accepted: ProducerTurnBudget,
    pub(crate) pending_local: NonZeroUsize,
    pub(crate) pending_route: NonZeroUsize,
}

impl ProducerShardData {
    /// Runs one bounded shard turn without wiring it into the live host.
    #[allow(
        clippy::result_large_err,
        reason = "terminal handoff returns exact pending ownership and final stage facts"
    )]
    pub(crate) fn shard_turn(
        &mut self,
        input: ProducerShardTurnInput,
    ) -> Result<ProducerShardTurnProgress, ProducerShardTurnFailure> {
        if input.close_requested {
            self.close_admission();
        }
        let local = self.settle_pending_local(input.now, input.pending_local);
        let accepted = self.turn(input.now, input.accepted);
        let accepted_invariant = accepted.as_ref().err().copied();
        if accepted_invariant.is_some() {
            self.close_admission();
        }

        let local_progress = match local {
            Ok(progress) => progress,
            Err(local) => {
                let route = self.retry_pending_route(input.pending_route);
                let accepted_progress = accepted.ok();
                let snapshot =
                    self.turn_snapshot(input.now, accepted_progress, None, None, route, true);
                let progress = ProducerShardTurnProgress {
                    local: None,
                    accepted: accepted_progress,
                    promotion: None,
                    route,
                    snapshot,
                };
                let pending = ProducerShardTurnFailureOwner::Local(local);
                let cause = match accepted_invariant {
                    None => ProducerShardTurnFailureCause::Pending(pending),
                    Some(host) => ProducerShardTurnFailureCause::HostAndPending { host, pending },
                };
                return Err(ProducerShardTurnFailure::new(cause, progress));
            }
        };

        let mut promotion = None;
        let mut promotion_owner = None;
        if accepted.is_ok()
            && matches!(&self.admission, ProducerShardAdmission::Running)
            && !local_progress.runnable()
        {
            match self.settle_next_pending(input.now) {
                Ok(progress) => promotion = Some(progress),
                Err(failure) => {
                    promotion_owner = Some(ProducerShardTurnFailureOwner::Promotion(failure));
                }
            }
        }

        let route = self.retry_pending_route(input.pending_route);
        let accepted_progress = accepted.ok();
        let snapshot = self.turn_snapshot(
            input.now,
            accepted_progress,
            Some(local_progress),
            promotion,
            route,
            promotion_owner.is_some() || accepted_invariant.is_some(),
        );
        let progress = ProducerShardTurnProgress {
            local: Some(local_progress),
            accepted: accepted_progress,
            promotion,
            route,
            snapshot,
        };
        match (promotion_owner, accepted_invariant) {
            (Some(pending), Some(host)) => Err(ProducerShardTurnFailure::new(
                ProducerShardTurnFailureCause::HostAndPending { host, pending },
                progress,
            )),
            (Some(pending), None) => Err(ProducerShardTurnFailure::new(
                ProducerShardTurnFailureCause::Pending(pending),
                progress,
            )),
            (None, Some(host)) => Err(ProducerShardTurnFailure::new(
                ProducerShardTurnFailureCause::Host(host),
                progress,
            )),
            (None, None) => Ok(progress),
        }
    }

    fn retry_pending_route(&mut self, limit: NonZeroUsize) -> PendingNotificationRouteProgress {
        self.host
            .pending_notifications
            .retry_primary_notifications(&self.host.completions, limit)
    }

    fn turn_snapshot(
        &self,
        now: Moment,
        accepted: Option<ProducerTurnOutcome>,
        local: Option<PendingLocalSettlementProgress>,
        promotion: Option<PendingSettlementProgress>,
        route: PendingNotificationRouteProgress,
        failed: bool,
    ) -> ProducerShardTurnSnapshot {
        let stats = self.shard_stats();
        let state = match &self.admission {
            ProducerShardAdmission::Running => ProducerShardTurnState::Running,
            ProducerShardAdmission::Closed => ProducerShardTurnState::Closed,
            ProducerShardAdmission::Faulted(_) => ProducerShardTurnState::Faulted,
        };
        let accepted_deadline = accepted.and_then(|progress| progress.next_deadline);
        let host_deadline = self.host.next_deadline();
        let pending_deadline = (state == ProducerShardTurnState::Running)
            .then(|| self.pending.next_deadline())
            .flatten();
        let next_deadline = min_deadline(
            min_deadline(accepted_deadline, host_deadline),
            pending_deadline,
        );
        let terminal_handoff = failed
            || state == ProducerShardTurnState::Faulted
            || route.mode() == PendingNotificationRouteMode::Recovery;
        let deadline_due = next_deadline.is_some_and(|deadline| deadline.is_elapsed_at(now));
        let post_host_runnable = self
            .host
            .pending_effects()
            .iter()
            .any(|effect| matches!(effect, ProducerEffect::MaterializeBatch { .. }));
        let runnable = runnable(
            accepted,
            local,
            promotion,
            route,
            deadline_due,
            post_host_runnable,
        );
        let blocked = blocked(accepted, promotion, route);
        let accepted_unsettled = self.unsettled_completions();
        let shutdown_ready = state == ProducerShardTurnState::Closed
            && accepted_unsettled == 0
            && stats.pending.records == 0
            && stats.pending.retained_bytes == 0
            && stats.host.pending_notification_permits == 0
            && stats.host.pending_notification_backlog == 0
            && !terminal_handoff;
        ProducerShardTurnSnapshot {
            state,
            pending_records: stats.pending.records,
            pending_bytes: stats.pending.retained_bytes,
            pending_permits: stats.host.pending_notification_permits,
            accepted_unsettled,
            route_retained: stats.host.pending_notification_backlog,
            next_deadline,
            runnable,
            blocked,
            terminal_handoff,
            shutdown_ready,
        }
    }
}
