//! Copy scheduling facts produced after one bounded producer-shard turn.
#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "turn facts remain guarded with their dormant caller"
    )
)]

use kafka_client_core::Deadline;

use crate::producer::{
    host_turn::ProducerTurnOutcome,
    pending::{PendingNotificationRouteMode, PendingNotificationRouteProgress},
};

use super::{
    pending_local_settlement::PendingLocalSettlementProgress,
    pending_settlement::{PendingSettlementDisposition, PendingSettlementProgress},
};

/// Post-stage lifecycle visible to the future host scheduler.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProducerShardTurnState {
    Running,
    Closed,
    Faulted,
}

/// Resource and scheduling facts sampled only after every stage has run.
#[allow(
    clippy::struct_excessive_bools,
    reason = "runnable, blocked, terminal handoff, and shutdown readiness are independent facts"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProducerShardTurnSnapshot {
    pub(crate) state: ProducerShardTurnState,
    pub(crate) pending_records: usize,
    pub(crate) pending_bytes: usize,
    pub(crate) pending_permits: usize,
    pub(crate) accepted_unsettled: usize,
    pub(crate) route_retained: usize,
    pub(crate) next_deadline: Option<Deadline>,
    pub(crate) runnable: bool,
    pub(crate) blocked: bool,
    pub(crate) terminal_handoff: bool,
    pub(crate) shutdown_ready: bool,
}

/// Concrete Copy results of every stage reached by one bounded turn.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProducerShardTurnProgress {
    pub(crate) local: Option<PendingLocalSettlementProgress>,
    pub(crate) accepted: Option<ProducerTurnOutcome>,
    pub(crate) promotion: Option<PendingSettlementProgress>,
    pub(crate) route: PendingNotificationRouteProgress,
    pub(crate) snapshot: ProducerShardTurnSnapshot,
}

impl ProducerShardTurnProgress {
    pub(crate) const fn state(self) -> ProducerShardTurnState {
        self.snapshot.state
    }

    pub(crate) const fn next_deadline(self) -> Option<Deadline> {
        self.snapshot.next_deadline
    }

    pub(crate) const fn runnable(self) -> bool {
        self.snapshot.runnable
    }

    pub(crate) const fn blocked(self) -> bool {
        self.snapshot.blocked
    }

    pub(crate) const fn terminal_handoff(self) -> bool {
        self.snapshot.terminal_handoff
    }

    pub(crate) const fn shutdown_ready(self) -> bool {
        self.snapshot.shutdown_ready
    }
}

pub(super) fn runnable(
    accepted: Option<ProducerTurnOutcome>,
    local: Option<PendingLocalSettlementProgress>,
    promotion: Option<PendingSettlementProgress>,
    route: PendingNotificationRouteProgress,
    deadline_due: bool,
    post_host_runnable: bool,
) -> bool {
    accepted.is_some_and(|progress| progress.runnable_work)
        || local.is_some_and(PendingLocalSettlementProgress::runnable)
        || promotion.is_some_and(|progress| {
            progress.disposition() == PendingSettlementDisposition::Productive
                && progress.remaining()
        })
        || deadline_due
        || post_host_runnable
        || (route.mode() == PendingNotificationRouteMode::Primary
            && route.remaining()
            && !route.blocked())
}

pub(super) fn blocked(
    accepted: Option<ProducerTurnOutcome>,
    promotion: Option<PendingSettlementProgress>,
    route: PendingNotificationRouteProgress,
) -> bool {
    accepted.is_some_and(|progress| progress.blocked_work)
        || promotion.is_some_and(|progress| {
            progress.disposition() == PendingSettlementDisposition::RestoredBlocked
        })
        || (route.mode() == PendingNotificationRouteMode::Primary && route.blocked())
}

pub(super) const fn min_deadline(
    accepted: Option<Deadline>,
    pending: Option<Deadline>,
) -> Option<Deadline> {
    match (accepted, pending) {
        (Some(accepted), Some(pending)) => Some(accepted.min(pending)),
        (Some(accepted), None) => Some(accepted),
        (None, Some(pending)) => Some(pending),
        (None, None) => None,
    }
}
