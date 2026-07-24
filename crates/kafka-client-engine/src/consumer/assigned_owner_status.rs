//! Scalar scheduling and close-retention observations for the host join point.

use super::{assigned_close_error::AssignedCloseSlotPhase, assigned_owner::AssignedConsumerOwner};

/// Scalar pre-release context retained before abnormal owner consumption.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AssignedConsumerRecoveryAudit {
    close_started: bool,
    close_completed: bool,
    unsettled: usize,
    position_calls: usize,
    fetch_retained: (usize, usize, usize),
    event_retained: (usize, usize),
    reclaim_failures: usize,
}

impl AssignedConsumerOwner {
    /// Reports whether core close admission has left the sole vacant phase.
    pub(crate) fn close_started(&self) -> bool {
        self.close.phase() != AssignedCloseSlotPhase::Vacant
    }

    /// Counts retained work without treating an idle open lifecycle as runnable.
    pub(crate) fn unsettled(&self) -> usize {
        let (fetch_calls, deliveries, _bytes) = self.fetches.retained();
        self.effects
            .len()
            .saturating_add(self.raw_position_deadlines.len())
            .saturating_add(self.pending_positions.len())
            .saturating_add(self.pending_fetches.len())
            .saturating_add(self.timers.timer_count())
            .saturating_add(self.positions.retained_positions())
            .saturating_add(fetch_calls)
            .saturating_add(deliveries)
            .saturating_add(usize::from(matches!(
                self.close.phase(),
                AssignedCloseSlotPhase::Reserved | AssignedCloseSlotPhase::Accepted
            )))
    }

    /// Confirms that core's `CompleteClose` effect is retained, not inferred.
    pub(crate) fn close_completed(&self) -> bool {
        matches!(
            self.close.phase(),
            AssignedCloseSlotPhase::Ready | AssignedCloseSlotPhase::Reclaimed
        )
    }

    pub(crate) fn recovery_audit(&self) -> AssignedConsumerRecoveryAudit {
        AssignedConsumerRecoveryAudit {
            close_started: self.close_started(),
            close_completed: self.close_completed(),
            unsettled: self.unsettled(),
            position_calls: self.positions.retained_positions(),
            fetch_retained: self.fetches.retained(),
            event_retained: self.events.retained(),
            reclaim_failures: self
                .reclaim_faults
                .len()
                .saturating_add(usize::from(self.reclaim_overflow.is_some())),
        }
    }
}

impl AssignedConsumerRecoveryAudit {
    pub(crate) const fn was_cleanly_closed(self) -> bool {
        self.close_completed && self.unsettled == 0
    }

    pub(crate) const fn event_retained(self) -> (usize, usize) {
        self.event_retained
    }
}
