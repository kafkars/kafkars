//! Terminal settlement and conservative fallback after execution loss.

use std::{error::Error, fmt};

use kafka_client_core::{
    DeliveryStatus, Moment, ProducerCompletion, ProducerFailure, ProducerInput,
};

use crate::completion::{CompletionId, CompletionRegistryError};

use super::{ProducerHost, ProducerHostInvariantError};

/// Failure to publish every conservative terminal during catastrophic recovery.
#[derive(Debug)]
pub(crate) enum ProducerExecutionStopError {
    Invariant(ProducerHostInvariantError),
    Settlement {
        completion_id: CompletionId,
        error: CompletionRegistryError,
        queued: usize,
        remaining: usize,
        terminal: ProducerCompletion,
    },
}

impl fmt::Display for ProducerExecutionStopError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invariant(error) => write!(formatter, "execution-stop invariant failed: {error}"),
            Self::Settlement {
                completion_id,
                error,
                queued,
                remaining,
                terminal,
            } => write!(
                formatter,
                "completion {completion_id:?} rejected retained terminal {terminal:?} after \
                 queueing {queued} with {remaining} remaining: {error}"
            ),
        }
    }
}

impl Error for ProducerExecutionStopError {}

impl ProducerHost {
    /// Settles every accepted operation after permanent execution loss.
    ///
    /// If deterministic planning or effect interpretation is itself damaged,
    /// the completion owner publishes conservative `PossiblySent` fallbacks so
    /// no accepted observer can remain pending.
    pub(crate) fn execution_unavailable(
        &mut self,
        now: Moment,
    ) -> Result<(), ProducerExecutionStopError> {
        self.core.close_admission();
        let transition = match self.core.apply(ProducerInput::ExecutionUnavailable) {
            Ok(transition) => transition,
            Err(_error) => {
                self.publish_execution_fallback()?;
                return Ok(());
            }
        };
        if self.interpret_transition(now, transition).is_err() {
            self.publish_execution_fallback()?;
        }
        Ok(())
    }

    fn publish_execution_fallback(&mut self) -> Result<(), ProducerExecutionStopError> {
        let failure = ProducerFailure::execution_unavailable(DeliveryStatus::PossiblySent);
        let mut remaining = self.completions.unsettled_len();
        while remaining != 0 {
            let progress = match self
                .completions
                .settle_reserved_with(remaining, |_id| ProducerCompletion::Failed(failure))
            {
                Ok(progress) => progress,
                Err(failed) => {
                    let progress = failed.progress();
                    return Err(ProducerExecutionStopError::Settlement {
                        completion_id: failed.completion_id(),
                        error: failed.error(),
                        queued: progress.queued(),
                        remaining: progress.remaining(),
                        terminal: failed.into_terminal(),
                    });
                }
            };
            if progress.queued() == 0 && progress.remaining() != 0 {
                return Err(ProducerExecutionStopError::Invariant(
                    ProducerHostInvariantError::Completion(
                        CompletionRegistryError::UnsettledCompletion,
                    ),
                ));
            }
            remaining = progress.remaining();
        }
        Ok(())
    }
}
