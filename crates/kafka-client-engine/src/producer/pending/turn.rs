//! Bounded pending-registry scans with exact ownership on every failure.

use kafka_client_core::Moment;

use super::{
    PendingAdmission, PendingAdmissionId, PendingCellError, PendingPromotionAttempt,
    PendingRegistryError, ProducerSendFailure, ProducerSendFailureKind,
    registry::PendingAdmissionRegistry,
    turn_error::{
        PendingLocalFailureProgress, PendingTakeFailure, PendingTakeProgress, PendingTurnFailure,
    },
};

impl PendingAdmissionRegistry {
    /// Scans at most `limit` admissions and returns at most one live attempt.
    pub(crate) fn take_next(
        &mut self,
        limit: usize,
    ) -> Result<PendingTakeProgress, PendingTurnFailure> {
        let mut inspected = 0;
        while inspected < limit {
            let Some((_sequence, id)) = self.fifo.first_key_value() else {
                return Ok(PendingTakeProgress::new(None, inspected, false));
            };
            inspected += 1;
            match self.take_attempt(*id) {
                Ok(TakeAttemptOutcome::Live(attempt)) => {
                    return Ok(PendingTakeProgress::new(
                        Some(attempt),
                        inspected,
                        !self.fifo.is_empty(),
                    ));
                }
                Ok(TakeAttemptOutcome::Abandoned(pending)) => drop(pending),
                Err(failure) => {
                    return Err(PendingTurnFailure::take(
                        failure.error(),
                        inspected,
                        Vec::new(),
                        failure,
                    ));
                }
            }
        }
        Ok(PendingTakeProgress::new(
            None,
            inspected,
            !self.fifo.is_empty(),
        ))
    }

    /// Settles elapsed records while counting every inspected tombstone.
    pub(crate) fn expire_due(
        &mut self,
        now: Moment,
        limit: usize,
    ) -> Result<PendingLocalFailureProgress, PendingTurnFailure> {
        let mut expired = Vec::with_capacity(limit.min(self.fifo.len()));
        let mut inspected = 0;
        while inspected < limit {
            let Some((deadline, _sequence, id)) = self.deadlines.first().copied() else {
                break;
            };
            if !deadline.is_elapsed_at(now) {
                break;
            }
            inspected += 1;
            let outcome = match self.take_attempt(id) {
                Ok(outcome) => outcome,
                Err(failure) => {
                    return Err(PendingTurnFailure::take(
                        failure.error(),
                        inspected,
                        expired,
                        failure,
                    ));
                }
            };
            match outcome {
                TakeAttemptOutcome::Live(attempt) => {
                    let failure =
                        ProducerSendFailure::new(ProducerSendFailureKind::DeadlineElapsed);
                    match attempt.settle_local(failure) {
                        Ok(local) => expired.push(local),
                        Err(failure) => {
                            return Err(PendingTurnFailure::settlement(
                                inspected, expired, failure,
                            ));
                        }
                    }
                }
                TakeAttemptOutcome::Abandoned(pending) => drop(pending),
            }
        }
        let remaining = self
            .deadlines
            .first()
            .is_some_and(|(deadline, _, _)| deadline.is_elapsed_at(now));
        Ok(PendingLocalFailureProgress::new(
            expired, inspected, remaining,
        ))
    }

    /// Settles queued records while counting every inspected tombstone.
    pub(crate) fn drain_closed(
        &mut self,
        limit: usize,
    ) -> Result<PendingLocalFailureProgress, PendingTurnFailure> {
        if self.accepting {
            return Err(PendingTurnFailure::registry(
                PendingRegistryError::StillOpen,
            ));
        }
        let mut drained = Vec::with_capacity(limit.min(self.fifo.len()));
        let mut inspected = 0;
        while inspected < limit {
            let Some((_sequence, id)) = self.fifo.first_key_value() else {
                break;
            };
            inspected += 1;
            let outcome = match self.take_attempt(*id) {
                Ok(outcome) => outcome,
                Err(failure) => {
                    return Err(PendingTurnFailure::take(
                        failure.error(),
                        inspected,
                        drained,
                        failure,
                    ));
                }
            };
            match outcome {
                TakeAttemptOutcome::Live(attempt) => {
                    let failure = ProducerSendFailure::new(ProducerSendFailureKind::Shutdown);
                    match attempt.settle_local(failure) {
                        Ok(local) => drained.push(local),
                        Err(failure) => {
                            return Err(PendingTurnFailure::settlement(
                                inspected, drained, failure,
                            ));
                        }
                    }
                }
                TakeAttemptOutcome::Abandoned(pending) => drop(pending),
            }
        }
        Ok(PendingLocalFailureProgress::new(
            drained,
            inspected,
            !self.fifo.is_empty(),
        ))
    }

    fn take_attempt(
        &mut self,
        id: PendingAdmissionId,
    ) -> Result<TakeAttemptOutcome, PendingTakeFailure> {
        let plan = self
            .validate_remove(id)
            .map_err(PendingTakeFailure::registry)?;
        let promotion =
            {
                let slot = self.slots.get(id.slot()).ok_or_else(|| {
                    PendingTakeFailure::registry(PendingRegistryError::UnknownSlot)
                })?;
                if slot.generation != id.generation() {
                    return Err(PendingTakeFailure::registry(
                        PendingRegistryError::StaleGeneration,
                    ));
                }
                let pending = slot.entry.as_ref().ok_or_else(|| {
                    PendingTakeFailure::registry(PendingRegistryError::CorruptIndex)
                })?;
                match pending.begin_promotion() {
                    Ok(promotion) => promotion,
                    Err(PendingCellError::Abandoned) => {
                        return self
                            .commit_remove(plan)
                            .map(TakeAttemptOutcome::Abandoned)
                            .map_err(|failure| PendingTakeFailure::registry(failure.error()));
                    }
                    Err(_) => {
                        return Err(PendingTakeFailure::registry(
                            PendingRegistryError::ObservationState,
                        ));
                    }
                }
            };
        match self.commit_remove(plan) {
            Ok(admission) => Ok(TakeAttemptOutcome::Live(PendingPromotionAttempt::new(
                admission, promotion,
            ))),
            Err(failure) => {
                let (error, plan) = failure.into_parts();
                Err(PendingTakeFailure::claimed(error, promotion, plan))
            }
        }
    }
}

enum TakeAttemptOutcome {
    Live(PendingPromotionAttempt),
    Abandoned(PendingAdmission),
}
