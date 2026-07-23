//! Linear recovery owners for failures after registry-first attempt restoration.

use super::{
    PendingAdmission, PendingAttemptStateError, PendingPromotionAttempt, PendingRegistryError,
    promotion::PendingPromotion,
    registry::{PendingAdmissionRegistry, PendingRemovalPlan},
};

/// Outcome after registry insertion precedes the cell restore linearization.
pub(crate) enum PendingAttemptRestoreOutcome {
    Restored,
    Abandoned(PendingAdmission),
}

/// Exact reason coordinated restore could not complete.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PendingAttemptRestoreError {
    Registry(PendingRegistryError),
    State(PendingAttemptStateError),
    Rollback {
        cell: super::PendingCellError,
        registry: PendingRegistryError,
    },
}

/// Failure retaining either the unchanged attempt or its exact rollback owner.
#[must_use = "restore failure retains a linear recovery owner"]
pub(crate) struct PendingAttemptRestoreFailure {
    error: PendingAttemptRestoreError,
    ownership: PendingRestoreFailureOwnership,
}

enum PendingRestoreFailureOwnership {
    Attempt(Box<PendingPromotionAttempt>),
    Rollback(Box<PendingRestoreRecovery>),
}

/// Exact proof and cell claim needed to recover a failed restore rollback.
#[must_use = "retry recovery or settle the recovered promotion attempt"]
pub(crate) struct PendingRestoreRecovery {
    error: PendingAttemptRestoreError,
    cell_error: Option<super::PendingCellError>,
    plan: PendingRemovalPlan,
    promotion: Option<PendingPromotion>,
}

/// Host-usable owner returned after the exact rollback eventually succeeds.
#[must_use = "recovered ownership must be settled or explicitly abandoned"]
pub(crate) enum PendingRestoreRecoveryOutcome {
    Attempt(PendingPromotionAttempt),
    Abandoned(PendingAdmission),
}

impl PendingRestoreRecoveryOutcome {
    /// Separates a live promotion claim from an observer-abandoned record.
    #[allow(
        clippy::result_large_err,
        reason = "observer abandonment must return the exact engine-owned record"
    )]
    pub(crate) fn into_attempt(self) -> Result<PendingPromotionAttempt, PendingAdmission> {
        match self {
            Self::Attempt(attempt) => Ok(attempt),
            Self::Abandoned(admission) => Err(admission),
        }
    }
}

impl PendingAttemptRestoreFailure {
    pub(super) fn attempt(
        error: PendingAttemptRestoreError,
        attempt: PendingPromotionAttempt,
    ) -> Self {
        Self {
            error,
            ownership: PendingRestoreFailureOwnership::Attempt(Box::new(attempt)),
        }
    }

    pub(super) fn rollback(
        cell_error: Option<super::PendingCellError>,
        registry_error: PendingRegistryError,
        plan: PendingRemovalPlan,
        promotion: Option<PendingPromotion>,
    ) -> Self {
        let error = combined_error(cell_error, registry_error);
        let recovery = PendingRestoreRecovery {
            error,
            cell_error,
            plan,
            promotion,
        };
        Self {
            error,
            ownership: PendingRestoreFailureOwnership::Rollback(Box::new(recovery)),
        }
    }

    pub(crate) const fn error(&self) -> PendingAttemptRestoreError {
        self.error
    }

    pub(crate) fn into_attempt(self) -> Result<PendingPromotionAttempt, Self> {
        match self.ownership {
            PendingRestoreFailureOwnership::Attempt(attempt) => Ok(*attempt),
            ownership @ PendingRestoreFailureOwnership::Rollback(_) => Err(Self {
                error: self.error,
                ownership,
            }),
        }
    }

    pub(crate) fn into_recovery(self) -> Result<PendingRestoreRecovery, Self> {
        match self.ownership {
            PendingRestoreFailureOwnership::Rollback(recovery) => Ok(*recovery),
            ownership @ PendingRestoreFailureOwnership::Attempt(_) => Err(Self {
                error: self.error,
                ownership,
            }),
        }
    }
}

impl PendingRestoreRecovery {
    pub(crate) const fn error(&self) -> PendingAttemptRestoreError {
        self.error
    }

    #[allow(
        clippy::result_large_err,
        reason = "failure must retain the exact rollback proof and promotion claim"
    )]
    pub(crate) fn recover(
        self,
        registry: &mut PendingAdmissionRegistry,
    ) -> Result<PendingRestoreRecoveryOutcome, Self> {
        let Self {
            error: _,
            cell_error,
            plan,
            promotion,
        } = self;
        match registry.commit_remove(plan) {
            Ok(admission) => match promotion {
                Some(promotion) => Ok(PendingRestoreRecoveryOutcome::Attempt(
                    PendingPromotionAttempt::new(admission, promotion),
                )),
                None => Ok(PendingRestoreRecoveryOutcome::Abandoned(admission)),
            },
            Err(failure) => {
                let (registry_error, plan) = failure.into_parts();
                Err(Self {
                    error: combined_error(cell_error, registry_error),
                    cell_error,
                    plan,
                    promotion,
                })
            }
        }
    }
}

const fn combined_error(
    cell_error: Option<super::PendingCellError>,
    registry_error: PendingRegistryError,
) -> PendingAttemptRestoreError {
    match cell_error {
        Some(cell) => PendingAttemptRestoreError::Rollback {
            cell,
            registry: registry_error,
        },
        None => PendingAttemptRestoreError::Registry(registry_error),
    }
}
