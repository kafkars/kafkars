//! Atomic validation and deterministic gap-preserving range normalization.

use super::{
    ShareAcknowledgeType, ShareAcknowledgement, ShareAcknowledgementBatch,
    ShareAcknowledgementBuildError, ShareAcknowledgementBuildErrorKind as ErrorKind,
    ShareRecordDecision,
};
use crate::consumer::share_fetch::ShareAcquisition;

impl ShareAcknowledgement {
    /// Atomically consumes one exact acquisition batch and every record decision.
    pub fn try_new(
        acquisitions: Vec<ShareAcquisition>,
        decisions: Vec<ShareRecordDecision>,
    ) -> Result<Self, ShareAcknowledgementBuildError> {
        let batches = preflight(&acquisitions, &decisions);
        match batches {
            Ok(batches) => {
                let fence = acquisitions.first().map_or_else(
                    || unreachable!("preflight requires acquisitions"),
                    ShareAcquisition::fence,
                );
                Ok(Self::new(fence, acquisitions, batches))
            }
            Err(kind) => Err(ShareAcknowledgementBuildError::new(
                kind,
                acquisitions,
                decisions,
            )),
        }
    }
}

fn preflight(
    acquisitions: &[ShareAcquisition],
    decisions: &[ShareRecordDecision],
) -> Result<Vec<ShareAcknowledgementBatch>, ErrorKind> {
    let Some(first) = acquisitions.first() else {
        return Err(ErrorKind::EmptyAcquisitions);
    };
    if decisions.is_empty() {
        return Err(ErrorKind::EmptyDecisions);
    }
    if acquisitions
        .iter()
        .any(|acquisition| !same_session(first, acquisition))
    {
        return Err(ErrorKind::MixedSession);
    }
    validate_decisions(acquisitions, decisions)?;
    let mut batches = Vec::new();
    batches
        .try_reserve_exact(acquisitions.len())
        .map_err(|_error| ErrorKind::AllocationFailed)?;
    for acquisition in acquisitions {
        batches.push(normalize_acquisition(acquisition, decisions)?);
    }
    batches.sort_unstable_by_key(|batch| {
        (
            batch.topic_uuid().bytes(),
            batch.partition().partition().get(),
            batch.first_offset(),
        )
    });
    Ok(batches)
}

fn validate_decisions(
    acquisitions: &[ShareAcquisition],
    decisions: &[ShareRecordDecision],
) -> Result<(), ErrorKind> {
    for (index, decision) in decisions.iter().copied().enumerate() {
        if decision.offset() < 0 {
            return Err(ErrorKind::InvalidOffset);
        }
        let acquisition = acquisitions
            .iter()
            .find(|candidate| candidate.generation() == decision.acquisition())
            .ok_or(ErrorKind::UnknownAcquisition)?;
        let range = acquisition.range();
        if !(range.first_offset()..=range.last_offset()).contains(&decision.offset()) {
            return Err(ErrorKind::OffsetOutsideRange);
        }
        if decisions[..index].iter().any(|candidate| {
            candidate.acquisition() == decision.acquisition()
                && candidate.offset() == decision.offset()
        }) {
            return Err(ErrorKind::DuplicateDecision);
        }
    }
    if acquisitions.iter().any(|acquisition| {
        !decisions
            .iter()
            .any(|decision| decision.acquisition() == acquisition.generation())
    }) {
        return Err(ErrorKind::MissingDecision);
    }
    Ok(())
}

fn normalize_acquisition(
    acquisition: &ShareAcquisition,
    decisions: &[ShareRecordDecision],
) -> Result<ShareAcknowledgementBatch, ErrorKind> {
    let range = acquisition.range();
    let count =
        usize::try_from(range.record_count()).map_err(|_error| ErrorKind::AccountingInvariant)?;
    let mut types = Vec::new();
    types
        .try_reserve_exact(count)
        .map_err(|_error| ErrorKind::AllocationFailed)?;
    for offset in range.first_offset()..=range.last_offset() {
        let value = decisions
            .iter()
            .find(|decision| {
                decision.acquisition() == acquisition.generation() && decision.offset() == offset
            })
            .map_or(ShareAcknowledgeType::Gap, |decision| {
                ShareAcknowledgeType::from(decision.disposition())
            });
        types.push(value);
    }
    if types
        .first()
        .is_some_and(|first| types.iter().all(|value| value == first))
    {
        types.truncate(1);
    }
    Ok(ShareAcknowledgementBatch::new(
        range.topic_uuid(),
        range.partition(),
        range.first_offset(),
        range.last_offset(),
        types,
    ))
}

fn same_session(first: &ShareAcquisition, candidate: &ShareAcquisition) -> bool {
    first.fence() == candidate.fence()
}
