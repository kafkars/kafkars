//! Exact owners and progress reports for bounded pending-registry turns.

use super::{
    PendingLocalFailure, PendingPromotionAttempt, PendingRegistryError, ProducerSendFailure,
    attempt_settlement::PendingAttemptSettleFailure, promotion::PendingPromotion,
    registry::PendingRemovalPlan,
};

/// Bounded scan result containing at most one coordinated live attempt.
#[must_use = "promotion progress may own one exact pending attempt"]
pub(crate) struct PendingTakeProgress {
    attempt: Option<PendingPromotionAttempt>,
    inspected: usize,
    remaining: bool,
}

impl PendingTakeProgress {
    pub(super) const fn new(
        attempt: Option<PendingPromotionAttempt>,
        inspected: usize,
        remaining: bool,
    ) -> Self {
        Self {
            attempt,
            inspected,
            remaining,
        }
    }

    pub(crate) const fn inspected(&self) -> usize {
        self.inspected
    }

    pub(crate) const fn remaining(&self) -> bool {
        self.remaining
    }

    pub(crate) fn into_attempt(self) -> Option<PendingPromotionAttempt> {
        self.attempt
    }
}

/// Bounded local-settlement output with inspected-work accounting.
#[must_use = "local failures retain exact records and notification jobs"]
pub(crate) struct PendingLocalFailureProgress {
    failures: Vec<PendingLocalFailure>,
    inspected: usize,
    remaining: bool,
}

impl PendingLocalFailureProgress {
    pub(super) const fn new(
        failures: Vec<PendingLocalFailure>,
        inspected: usize,
        remaining: bool,
    ) -> Self {
        Self {
            failures,
            inspected,
            remaining,
        }
    }

    pub(crate) const fn len(&self) -> usize {
        self.failures.len()
    }

    pub(crate) const fn is_empty(&self) -> bool {
        self.failures.is_empty()
    }

    pub(crate) const fn inspected(&self) -> usize {
        self.inspected
    }

    pub(crate) const fn remaining(&self) -> bool {
        self.remaining
    }

    pub(crate) fn into_failures(self) -> Vec<PendingLocalFailure> {
        self.failures
    }
}

/// Registry-take failure retaining a promotion claim when claiming won.
#[must_use = "a claimed pending cell remains owned by this failure"]
pub(crate) struct PendingTakeFailure {
    error: PendingRegistryError,
    promotion: Option<PendingPromotion>,
    plan: Option<PendingRemovalPlan>,
}

impl PendingTakeFailure {
    pub(super) const fn registry(error: PendingRegistryError) -> Self {
        Self {
            error,
            promotion: None,
            plan: None,
        }
    }

    pub(super) const fn claimed(
        error: PendingRegistryError,
        promotion: PendingPromotion,
        plan: PendingRemovalPlan,
    ) -> Self {
        Self {
            error,
            promotion: Some(promotion),
            plan: Some(plan),
        }
    }

    pub(crate) const fn error(&self) -> PendingRegistryError {
        self.error
    }

    /// Retries an interrupted post-claim removal without exposing its proof.
    pub(crate) fn recover(
        self,
        registry: &mut super::PendingAdmissionRegistry,
    ) -> Result<PendingPromotionAttempt, Self> {
        let Self {
            error,
            promotion,
            plan,
        } = self;
        let (promotion, plan) = match (promotion, plan) {
            (Some(promotion), Some(plan)) => (promotion, plan),
            (promotion, plan) => {
                return Err(Self {
                    error,
                    promotion,
                    plan,
                });
            }
        };
        match registry.commit_remove(plan) {
            Ok(admission) => Ok(PendingPromotionAttempt::new(admission, promotion)),
            Err(failure) => {
                let (error, plan) = failure.into_parts();
                Err(Self::claimed(error, promotion, plan))
            }
        }
    }
}

/// Failed bounded turn retaining completed jobs and the exact failing owner.
#[must_use = "completed notifications and any in-flight owner require recovery"]
pub(crate) struct PendingTurnFailure {
    error: PendingRegistryError,
    inspected: usize,
    completed: Vec<PendingLocalFailure>,
    ownership: PendingTurnFailureOwnership,
}

pub(crate) enum PendingTurnFailureOwnership {
    Registry,
    Take(PendingTakeFailure),
    Settlement(PendingAttemptSettleFailure<ProducerSendFailure>),
}

impl PendingTurnFailure {
    pub(super) const fn registry(error: PendingRegistryError) -> Self {
        Self {
            error,
            inspected: 0,
            completed: Vec::new(),
            ownership: PendingTurnFailureOwnership::Registry,
        }
    }

    pub(super) const fn take(
        error: PendingRegistryError,
        inspected: usize,
        completed: Vec<PendingLocalFailure>,
        failure: PendingTakeFailure,
    ) -> Self {
        Self {
            error,
            inspected,
            completed,
            ownership: PendingTurnFailureOwnership::Take(failure),
        }
    }

    pub(super) const fn settlement(
        inspected: usize,
        completed: Vec<PendingLocalFailure>,
        failure: PendingAttemptSettleFailure<ProducerSendFailure>,
    ) -> Self {
        Self {
            error: PendingRegistryError::ObservationState,
            inspected,
            completed,
            ownership: PendingTurnFailureOwnership::Settlement(failure),
        }
    }

    pub(crate) const fn error(&self) -> PendingRegistryError {
        self.error
    }

    pub(crate) const fn inspected(&self) -> usize {
        self.inspected
    }

    /// Returns completed notification owners and the exact interrupted owner.
    pub(crate) fn into_parts(self) -> (Vec<PendingLocalFailure>, PendingTurnFailureOwnership) {
        (self.completed, self.ownership)
    }
}

impl std::fmt::Debug for PendingTurnFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PendingTurnFailure")
            .field("error", &self.error)
            .field("inspected", &self.inspected)
            .field("completed", &self.completed.len())
            .finish_non_exhaustive()
    }
}
