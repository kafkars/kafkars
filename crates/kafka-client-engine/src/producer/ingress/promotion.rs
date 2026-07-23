//! Dormant one-attempt FIFO promotion without notification or host-loop integration.

use kafka_client_core::{Moment, OperationId};

use crate::{
    ProducerDeliveryObserver,
    producer::{
        ProducerHost, ProducerRecord,
        admission::{AdmittedExplicit, ProducerAdmissionFailure},
        pending::{
            PendingAdmissionRegistry, PendingAttemptStateError, PendingPromotionAttempt,
            ProducerSendFailure,
        },
    },
};

use super::promotion_error::{
    PendingAcceptedCommitFailure, PendingAcceptedResolution, PendingPromotionFailure,
    PendingPromotionInvariant, PendingPromotionProgress, PendingPromotionResolution,
    PendingStartResolution,
};
use super::promotion_rejection::{RejectionAction, classify_rejection};

/// Removes and resolves at most one live FIFO attempt.
///
/// The caller supplies a fresh monotonic observation. The exact deadline pair
/// remains owned by the pending attempt and is passed through unchanged.
pub(super) fn promote_next(
    host: &mut ProducerHost,
    pending: &mut PendingAdmissionRegistry,
    now: Moment,
) -> Result<PendingPromotionProgress, PendingPromotionFailure> {
    let take = pending
        .take_next(1)
        .map_err(|failure| PendingPromotionFailure::Take(Box::new(failure)))?;
    let inspected = take.inspected();
    let remaining = take.remaining();
    let Some(mut attempt) = take.into_attempt() else {
        return Ok(PendingPromotionProgress::new(inspected, remaining, None));
    };
    let Some(deadline) = attempt.operation_deadline() else {
        return Err(PendingPromotionFailure::Detach {
            error: PendingAttemptStateError::Invariant,
            attempt: Box::new(attempt),
        });
    };
    let record = match attempt.detach_record() {
        Ok(record) => record,
        Err(error) => {
            return Err(PendingPromotionFailure::Detach {
                error,
                attempt: Box::new(attempt),
            });
        }
    };
    let resolution = match host.try_admit_explicit(now, deadline, record) {
        Ok(admitted) => accept(attempt, admitted, None)?,
        Err(ProducerAdmissionFailure::AcceptedInvariant(poisoned)) => {
            let (error, operation_id, observer) = poisoned.into_parts();
            accept_observer(
                attempt,
                operation_id,
                ProducerDeliveryObserver::from_completion(observer),
                Some(PendingPromotionInvariant::Host(error)),
            )?
        }
        Err(ProducerAdmissionFailure::Invariant(poisoned)) => {
            let (error, record) = poisoned.into_parts();
            let attempt = restore_record(attempt, record)?;
            settle_start(
                attempt,
                crate::ProducerSendStartFailure::new(
                    crate::ProducerSendStartFailureKind::InternalInvariant,
                ),
                Some(PendingPromotionInvariant::Host(error)),
            )?
        }
        Err(ProducerAdmissionFailure::Rejected(rejected)) => {
            let reason = rejected.reason();
            let attempt = restore_record(attempt, rejected.into_record())?;
            resolve_rejection(attempt, pending, reason)?
        }
    };
    Ok(PendingPromotionProgress::new(
        inspected,
        remaining,
        Some(resolution),
    ))
}

fn resolve_rejection(
    attempt: PendingPromotionAttempt,
    pending: &mut PendingAdmissionRegistry,
    reason: super::super::ProducerRejectionReason,
) -> Result<PendingPromotionResolution, PendingPromotionFailure> {
    match classify_rejection(reason) {
        RejectionAction::Restore => match attempt.restore(pending) {
            Ok(super::super::pending::PendingAttemptRestoreOutcome::Restored) => {
                Ok(PendingPromotionResolution::Restored)
            }
            Ok(super::super::pending::PendingAttemptRestoreOutcome::Abandoned(admission)) => {
                Ok(PendingPromotionResolution::Abandoned(admission))
            }
            Err(failure) => Err(PendingPromotionFailure::Restore(Box::new(failure))),
        },
        RejectionAction::Local(failure) => settle_local(attempt, failure),
        RejectionAction::Start { failure, invariant } => settle_start(attempt, failure, invariant),
        RejectionAction::Fatal(invariant) => Err(PendingPromotionFailure::Fatal {
            invariant,
            attempt: Box::new(attempt),
        }),
    }
}

fn restore_record(
    mut attempt: PendingPromotionAttempt,
    record: ProducerRecord,
) -> Result<PendingPromotionAttempt, PendingPromotionFailure> {
    match attempt.restore_record(record) {
        Ok(()) => Ok(attempt),
        Err(failure) => Err(PendingPromotionFailure::RecordRestore {
            attempt: Box::new(attempt),
            failure: Box::new(failure),
        }),
    }
}

fn settle_local(
    attempt: PendingPromotionAttempt,
    failure: ProducerSendFailure,
) -> Result<PendingPromotionResolution, PendingPromotionFailure> {
    attempt
        .settle_local(failure)
        .map(PendingPromotionResolution::Local)
        .map_err(|failure| PendingPromotionFailure::Local(Box::new(failure)))
}

fn settle_start(
    attempt: PendingPromotionAttempt,
    failure: crate::ProducerSendStartFailure,
    invariant: Option<PendingPromotionInvariant>,
) -> Result<PendingPromotionResolution, PendingPromotionFailure> {
    attempt
        .settle_start(failure)
        .map(|failure| {
            PendingPromotionResolution::Start(PendingStartResolution::new(failure, invariant))
        })
        .map_err(|failure| PendingPromotionFailure::Start(Box::new(failure)))
}

fn accept(
    attempt: PendingPromotionAttempt,
    admitted: AdmittedExplicit,
    invariant: Option<PendingPromotionInvariant>,
) -> Result<PendingPromotionResolution, PendingPromotionFailure> {
    let operation_id = Some(admitted.operation_id());
    accept_observer(
        attempt,
        operation_id,
        admitted.into_delivery_observer(),
        invariant,
    )
}

fn accept_observer(
    mut attempt: PendingPromotionAttempt,
    operation_id: Option<OperationId>,
    observer: ProducerDeliveryObserver,
    invariant: Option<PendingPromotionInvariant>,
) -> Result<PendingPromotionResolution, PendingPromotionFailure> {
    if let Err(error) = attempt.commit_record() {
        return Err(PendingPromotionFailure::AcceptedCommit(Box::new(
            PendingAcceptedCommitFailure {
                error,
                attempt: Box::new(attempt),
                observer,
                operation_id,
                invariant,
            },
        )));
    }
    match attempt.accept(observer) {
        Ok(accepted) => Ok(PendingPromotionResolution::Accepted(
            PendingAcceptedResolution::new(operation_id, accepted.into_notification(), invariant),
        )),
        Err(failure) => Err(PendingPromotionFailure::Accept {
            failure: Box::new(failure),
            operation_id,
            invariant,
        }),
    }
}
