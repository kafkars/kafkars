//! Exact correlation, lifecycle consequence, and terminal assignment.

use kafka_client_core::{
    DeliveryStatus, Moment, TransactionOffsetCommitConsequence, TransactionOffsetCommitEffect,
    TransactionOffsetCommitInput, TransactionOffsetCommitStage,
    TransactionOffsetCommitTerminal as CoreTerminal,
};

use crate::transaction::TransactionLifecycleHost;

use super::{
    model::{
        TransactionOffsetCommitFailure, TransactionOffsetCommitFailureKind,
        TransactionOffsetCommitHostError, TransactionOffsetCommitOutcome,
        TransactionOffsetCommitResult,
    },
    owner::TransactionOffsetCommitOwner,
    port::{TransactionOffsetCommitPortEvidence, TransactionOffsetCommitPortFact},
    turn::{PendingTransactionOffsetCommit, TransactionOffsetCommitSlot},
};

impl TransactionOffsetCommitOwner {
    pub(super) fn reject_ready(
        &mut self,
        pending: PendingTransactionOffsetCommit,
        stage: TransactionOffsetCommitStage,
        failure: TransactionOffsetCommitFailure,
    ) -> Result<(), TransactionOffsetCommitHostError> {
        let transition = self
            .machine
            .apply(TransactionOffsetCommitInput::DriverRejected {
                epoch: pending.request.epoch(),
                operation_id: pending.operation_id,
                stage,
            })?;
        let terminal = complete_effect(
            transition.into_effect(),
            &pending,
            CoreTerminal::RejectedNotSent { stage },
        )?;
        self.slot = TransactionOffsetCommitSlot::Terminal(
            pending.completion_id,
            TransactionOffsetCommitResult::new(
                pending.operation_id,
                pending.request,
                TransactionOffsetCommitOutcome::RejectedNotSent { stage, failure },
            ),
        );
        debug_assert_eq!(terminal, CoreTerminal::RejectedNotSent { stage });
        Ok(())
    }

    pub(super) fn settle_evidence(
        &mut self,
        mut pending: PendingTransactionOffsetCommit,
        stage: TransactionOffsetCommitStage,
        evidence: Box<dyn TransactionOffsetCommitPortEvidence>,
        lifecycle: &mut TransactionLifecycleHost,
        retry_at: Option<Moment>,
    ) -> Result<(), TransactionOffsetCommitHostError> {
        let expected = (pending.request.epoch(), pending.operation_id, stage);
        let mut fact = if evidence.correlation() == expected {
            evidence.fact()
        } else {
            TransactionOffsetCommitPortFact::Failed {
                consequence: TransactionOffsetCommitConsequence::Fatal,
                kind: TransactionOffsetCommitFailureKind::Correlation,
                delivery: DeliveryStatus::PossiblySent,
            }
        };
        if let TransactionOffsetCommitPortFact::RetryableCoordinatorLoss { kind, delivery } = fact {
            if let Some(now) = retry_at
                && self.authorize_retry(&mut pending, stage, now)?
            {
                evidence.discard();
                self.slot = TransactionOffsetCommitSlot::Ready(pending, stage);
                return Ok(());
            }
            fact = TransactionOffsetCommitPortFact::Failed {
                consequence: TransactionOffsetCommitConsequence::AbortRequired,
                kind,
                delivery,
            };
        }
        let (input, failure) = match fact {
            TransactionOffsetCommitPortFact::Succeeded => (
                TransactionOffsetCommitInput::Succeeded {
                    epoch: expected.0,
                    operation_id: expected.1,
                    stage,
                },
                None,
            ),
            TransactionOffsetCommitPortFact::Failed {
                consequence,
                kind,
                delivery,
            } => (
                TransactionOffsetCommitInput::AcceptedFailed {
                    epoch: expected.0,
                    operation_id: expected.1,
                    stage,
                    consequence,
                },
                Some((
                    consequence,
                    TransactionOffsetCommitFailure::new(kind, delivery),
                )),
            ),
            TransactionOffsetCommitPortFact::RetryableCoordinatorLoss { .. } => {
                unreachable!("retryable coordinator loss is normalized before settlement")
            }
        };
        let transition = self.machine.apply(input)?;
        match transition.into_effect() {
            Some(TransactionOffsetCommitEffect::SubmitTxnOffsetCommit {
                epoch,
                operation_id,
                deadline,
                group_fence,
            }) if stage == TransactionOffsetCommitStage::AddOffsets
                && failure.is_none()
                && epoch == expected.0
                && operation_id == expected.1
                && deadline == pending.request.deadline().core()
                && group_fence == pending.request.group().fence() =>
            {
                self.slot = TransactionOffsetCommitSlot::Ready(
                    pending,
                    TransactionOffsetCommitStage::TxnOffsetCommit,
                );
            }
            Some(effect @ TransactionOffsetCommitEffect::Complete { .. }) => {
                self.finish_accepted(pending, stage, effect, failure, lifecycle)?;
            }
            _ => return Err(TransactionOffsetCommitHostError::UnexpectedEffect),
        }
        evidence.discard();
        Ok(())
    }

    fn finish_accepted(
        &mut self,
        pending: PendingTransactionOffsetCommit,
        stage: TransactionOffsetCommitStage,
        effect: TransactionOffsetCommitEffect,
        failure: Option<(
            TransactionOffsetCommitConsequence,
            TransactionOffsetCommitFailure,
        )>,
        lifecycle: &mut TransactionLifecycleHost,
    ) -> Result<(), TransactionOffsetCommitHostError> {
        let expected_terminal = match failure {
            Some((consequence, _)) => CoreTerminal::Failed { stage, consequence },
            None => CoreTerminal::Succeeded,
        };
        complete_effect(Some(effect), &pending, expected_terminal)?;
        let outcome = match failure {
            None => TransactionOffsetCommitOutcome::Succeeded,
            Some((TransactionOffsetCommitConsequence::AbortRequired, failure)) => {
                lifecycle
                    .settle_offset_commit(
                        pending.request.epoch(),
                        TransactionOffsetCommitConsequence::AbortRequired,
                    )
                    .map_err(|_| TransactionOffsetCommitHostError::Lifecycle)?;
                TransactionOffsetCommitOutcome::AbortRequired { stage, failure }
            }
            Some((TransactionOffsetCommitConsequence::Fatal, failure)) => {
                lifecycle
                    .settle_offset_commit(
                        pending.request.epoch(),
                        TransactionOffsetCommitConsequence::Fatal,
                    )
                    .map_err(|_| TransactionOffsetCommitHostError::Lifecycle)?;
                TransactionOffsetCommitOutcome::Fatal { stage, failure }
            }
        };
        self.slot = TransactionOffsetCommitSlot::Terminal(
            pending.completion_id,
            TransactionOffsetCommitResult::new(pending.operation_id, pending.request, outcome),
        );
        Ok(())
    }
}

fn complete_effect(
    effect: Option<TransactionOffsetCommitEffect>,
    pending: &PendingTransactionOffsetCommit,
    expected_terminal: CoreTerminal,
) -> Result<CoreTerminal, TransactionOffsetCommitHostError> {
    let Some(TransactionOffsetCommitEffect::Complete {
        epoch,
        operation_id,
        deadline,
        group_fence,
        terminal,
    }) = effect
    else {
        return Err(TransactionOffsetCommitHostError::UnexpectedEffect);
    };
    if epoch != pending.request.epoch()
        || operation_id != pending.operation_id
        || deadline != pending.request.deadline().core()
        || group_fence != pending.request.group().fence()
        || terminal != expected_terminal
    {
        return Err(TransactionOffsetCommitHostError::UnexpectedEffect);
    }
    Ok(terminal)
}
