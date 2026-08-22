//! Exact identity acquisition handoff and retry-schedule execution.

use std::{error::Error, fmt};

use kafka_client_core::{
    Deadline, Moment, OperationId, ProducerEffect, ProducerIdentityGeneration, ProducerInput,
};

use crate::clock::OperationDeadline;

use super::{ProducerHost, ProducerHostInvariantError};

/// Original-deadline identity request ready for one tracked driver slot.
#[derive(Debug)]
pub(crate) struct ProducerIdentitySubmission {
    generation: ProducerIdentityGeneration,
    deadline: OperationDeadline,
}

impl ProducerIdentitySubmission {
    pub(crate) const fn into_parts(self) -> (ProducerIdentityGeneration, OperationDeadline) {
        (self.generation, self.deadline)
    }
}

/// Disagreement between core identity effects and admission bindings.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ProducerIdentityHandoffError {
    UnknownDeadlineOperation(OperationId),
    DeadlineMismatch {
        operation_id: OperationId,
        effect: Deadline,
        bound: Deadline,
    },
    DuplicateAcquisition,
}

impl fmt::Display for ProducerIdentityHandoffError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownDeadlineOperation(operation_id) => write!(
                formatter,
                "producer identity deadline operation {} is not bound",
                operation_id.get()
            ),
            Self::DeadlineMismatch {
                operation_id,
                effect,
                bound,
            } => write!(
                formatter,
                "producer identity deadline for operation {} changed after admission: effect \
                 {}, bound {}",
                operation_id.get(),
                effect.tick(),
                bound.tick(),
            ),
            Self::DuplicateAcquisition => {
                formatter.write_str("multiple producer identity acquisitions are pending")
            }
        }
    }
}

impl Error for ProducerIdentityHandoffError {}

impl ProducerHost {
    /// Applies the one due identity retry before an equal-tick batch deadline.
    pub(super) fn fire_due_identity_retry(
        &mut self,
        now: Moment,
    ) -> Result<usize, ProducerHostInvariantError> {
        if let Some(error) = self.poison_reason() {
            return Err(error);
        }
        let Some(index) = self.pending_effects.iter().position(|effect| {
            matches!(
                effect,
                ProducerEffect::ArmProducerIdentityRetry { schedule }
                    if schedule.not_before().is_elapsed_at(now)
            )
        }) else {
            return Ok(0);
        };
        let ProducerEffect::ArmProducerIdentityRetry { schedule } =
            self.pending_effects.remove(index)
        else {
            unreachable!("identity retry position must select an identity retry")
        };
        self.apply_generated(
            now,
            ProducerInput::ProducerIdentityRetryDue { schedule, now },
        )?;
        Ok(1)
    }

    pub(super) fn pending_identity_retry_deadline(&self) -> Option<Deadline> {
        self.pending_effects
            .iter()
            .filter_map(|effect| match effect {
                ProducerEffect::ArmProducerIdentityRetry { schedule } => {
                    Some(schedule.not_before())
                }
                _ => None,
            })
            .min()
    }

    pub(crate) fn take_identity_submission(
        &mut self,
    ) -> Result<Option<ProducerIdentitySubmission>, ProducerIdentityHandoffError> {
        let mut found = None;
        for (index, effect) in self.pending_effects.iter().copied().enumerate() {
            let ProducerEffect::AcquireProducerIdentity {
                generation,
                deadline_operation_id,
                deadline,
            } = effect
            else {
                continue;
            };
            if found.is_some() {
                return Err(ProducerIdentityHandoffError::DuplicateAcquisition);
            }
            let bound = self.bindings.deadline(deadline_operation_id).ok_or(
                ProducerIdentityHandoffError::UnknownDeadlineOperation(deadline_operation_id),
            )?;
            if bound.core() != deadline {
                return Err(ProducerIdentityHandoffError::DeadlineMismatch {
                    operation_id: deadline_operation_id,
                    effect: deadline,
                    bound: bound.core(),
                });
            }
            found = Some((
                index,
                ProducerIdentitySubmission {
                    generation,
                    deadline: bound,
                },
            ));
        }
        let Some((index, submission)) = found else {
            return Ok(None);
        };
        self.pending_effects.remove(index);
        Ok(Some(submission))
    }
}
