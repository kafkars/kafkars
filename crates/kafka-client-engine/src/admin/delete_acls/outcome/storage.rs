//! Two-phase host-owned outer and nested terminal storage.

use core::{fmt, mem::size_of};
use std::collections::TryReserveError;

use super::{DeleteAclFilterOutcome, DeleteAclMatchingBinding, DeleteAclsBatch};

impl DeleteAclsBatch {
    /// Fallibly reserves known positional storage before operation admission.
    pub(crate) fn try_prepare_outcomes(
        filter_count: usize,
    ) -> Result<DeleteAclsPreparedOutcomes, TryReserveError> {
        let mut outcomes = Vec::new();
        outcomes.try_reserve_exact(filter_count)?;
        let mut matching = Vec::new();
        matching.try_reserve_exact(filter_count)?;
        for _ in 0..filter_count {
            matching.push(Vec::new());
        }
        Ok(DeleteAclsPreparedOutcomes { outcomes, matching })
    }
}

/// Opaque host-owned terminal vectors prepared before translation.
pub(crate) struct DeleteAclsPreparedOutcomes {
    pub(super) outcomes: Vec<DeleteAclFilterOutcome>,
    pub(super) matching: Vec<Vec<DeleteAclMatchingBinding>>,
}

impl DeleteAclsPreparedOutcomes {
    /// Fallibly reserves response-sized nested storage under the admitted byte envelope.
    pub(crate) fn try_prepare_matching<I>(
        &mut self,
        matching_capacities: I,
    ) -> Result<(), DeleteAclsPrepareMatchingError>
    where
        I: ExactSizeIterator<Item = usize>,
    {
        if matching_capacities.len() != self.matching.len() {
            return Err(DeleteAclsPrepareMatchingError::FilterCount {
                expected: self.matching.len(),
                actual: matching_capacities.len(),
            });
        }
        for (filter_index, (bindings, capacity)) in self
            .matching
            .iter_mut()
            .zip(matching_capacities)
            .enumerate()
        {
            bindings.try_reserve_exact(capacity).map_err(|source| {
                DeleteAclsPrepareMatchingError::Reserve {
                    filter_index,
                    source,
                }
            })?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn outcomes(&self) -> &[DeleteAclFilterOutcome] {
        &self.outcomes
    }

    /// Returns the exact public-result capacity reserved at admission.
    pub(crate) fn outcomes_capacity(&self) -> usize {
        self.outcomes.capacity()
    }

    #[cfg(test)]
    pub(crate) fn matching(&self) -> &[Vec<DeleteAclMatchingBinding>] {
        &self.matching
    }

    /// Returns checked actual-capacity bytes owned by all prepared vectors.
    pub(crate) fn retained_heap_bytes(&self) -> Option<usize> {
        let outcomes = self
            .outcomes
            .capacity()
            .checked_mul(size_of::<DeleteAclFilterOutcome>())?;
        let matching_slots = self
            .matching
            .capacity()
            .checked_mul(size_of::<Vec<DeleteAclMatchingBinding>>())?;
        self.matching
            .iter()
            .try_fold(outcomes.checked_add(matching_slots)?, |bytes, bindings| {
                bytes.checked_add(
                    bindings
                        .capacity()
                        .checked_mul(size_of::<DeleteAclMatchingBinding>())?,
                )
            })
    }
}

/// Failure to make actual response-sized nested storage ready for translation.
#[derive(Debug)]
pub(crate) enum DeleteAclsPrepareMatchingError {
    FilterCount {
        expected: usize,
        actual: usize,
    },
    Reserve {
        filter_index: usize,
        source: TryReserveError,
    },
}

impl fmt::Display for DeleteAclsPrepareMatchingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FilterCount { expected, actual } => write!(
                formatter,
                "DeleteAcls matching storage count mismatch: expected {expected}, got {actual}"
            ),
            Self::Reserve { filter_index, .. } => write!(
                formatter,
                "DeleteAcls matching storage reservation failed at filter {filter_index}"
            ),
        }
    }
}

impl std::error::Error for DeleteAclsPrepareMatchingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::FilterCount { .. } => None,
            Self::Reserve { source, .. } => Some(source),
        }
    }
}
