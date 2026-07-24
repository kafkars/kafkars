//! Fair bounded coordination of every runnable producer host mechanism.

use kafka_client_core::{Deadline, Moment, ProducerEffect};

use super::{ProducerHost, ProducerHostInvariantError, reclaim::CompletionReclaimOutcome};

/// Independent nonzero work limits for one producer host turn.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProducerTurnBudget {
    batch_timers: usize,
    prepared_effects: usize,
    submission_expiries: usize,
    completion_retries: usize,
    reclaim_attempts: usize,
}

impl ProducerTurnBudget {
    /// Constructs a budget only when every mechanism gets a chance to run.
    pub(crate) const fn try_new(
        batch_timers: usize,
        prepared_effects: usize,
        submission_expiries: usize,
        completion_retries: usize,
        reclaim_attempts: usize,
    ) -> Option<Self> {
        if batch_timers == 0
            || prepared_effects == 0
            || submission_expiries == 0
            || completion_retries == 0
            || reclaim_attempts == 0
        {
            return None;
        }
        Some(Self {
            batch_timers,
            prepared_effects,
            submission_expiries,
            completion_retries,
            reclaim_attempts,
        })
    }
}

/// Bounded progress and scheduling facts from one producer host turn.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProducerTurnOutcome {
    pub(crate) batch_timers: usize,
    pub(crate) prepared_effects: usize,
    pub(crate) submission_expiries: usize,
    pub(crate) completion_retries: usize,
    pub(crate) reclaim_attempts: usize,
    pub(crate) next_deadline: Option<Deadline>,
    pub(crate) runnable_work: bool,
    pub(crate) blocked_work: bool,
}

impl ProducerHost {
    /// Gives every producer mechanism its own bounded progress opportunity.
    ///
    /// Materialization facts never execute their resulting submission effect in
    /// the same stage. Submission expiry follows prepared work so a newly armed
    /// already-elapsed deadline settles before this turn returns.
    pub(crate) fn turn(
        &mut self,
        now: Moment,
        budget: ProducerTurnBudget,
    ) -> Result<ProducerTurnOutcome, ProducerHostInvariantError> {
        if let Some(error) = self.poison_reason() {
            return Err(error);
        }
        let batch_timers = self.fire_due(now, budget.batch_timers)?;
        let prepared_effects = self.drive_prepared(now, budget.prepared_effects)?;
        let submission_expiries = self.fire_due_submissions(now, budget.submission_expiries)?;
        let completion_retries = self.retry_terminal_backlog(budget.completion_retries)?;
        let reclaim = self.reclaim_many(now, budget.reclaim_attempts)?;
        let next_deadline = min_deadline(self.next_deadline(), pending_submission_deadline(self));
        let due_remains = next_deadline.is_some_and(|deadline| deadline.is_elapsed_at(now));
        let prepared_remains = self
            .pending_effects()
            .iter()
            .copied()
            .any(is_runnable_effect);
        let completion_blocked = !self.terminal_backlog.is_empty();
        let runnable_work = due_remains
            || prepared_remains
            || (reclaim.attempts == budget.reclaim_attempts && !reclaim.blocked);
        let blocked_work = completion_blocked || reclaim.blocked;
        Ok(ProducerTurnOutcome {
            batch_timers,
            prepared_effects,
            submission_expiries,
            completion_retries,
            reclaim_attempts: reclaim.attempts,
            next_deadline,
            runnable_work,
            blocked_work,
        })
    }

    fn reclaim_many(
        &mut self,
        now: Moment,
        limit: usize,
    ) -> Result<ReclaimProgress, ProducerHostInvariantError> {
        let mut attempts = 0;
        let mut blocked = false;
        while attempts < limit {
            let Some(outcome) = self.reclaim_one(now)? else {
                break;
            };
            attempts += 1;
            if outcome == CompletionReclaimOutcome::Retry {
                blocked = true;
                break;
            }
        }
        Ok(ReclaimProgress { attempts, blocked })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ReclaimProgress {
    attempts: usize,
    blocked: bool,
}

const fn is_runnable_effect(effect: ProducerEffect) -> bool {
    matches!(
        effect,
        ProducerEffect::AcquireProducerIdentity { .. } | ProducerEffect::MaterializeBatch { .. }
    )
}

fn pending_submission_deadline(host: &ProducerHost) -> Option<Deadline> {
    host.pending_effects()
        .iter()
        .filter_map(|effect| match effect {
            ProducerEffect::AcquireProducerIdentity { deadline, .. }
            | ProducerEffect::SubmitProduce { deadline, .. } => Some(*deadline),
            _ => None,
        })
        .min()
}

const fn min_deadline(left: Option<Deadline>, right: Option<Deadline>) -> Option<Deadline> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}
