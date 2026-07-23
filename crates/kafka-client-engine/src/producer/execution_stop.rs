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
    Fallback {
        primary: ProducerHostInvariantError,
        settlement: Box<ProducerExecutionStopError>,
    },
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
            Self::Fallback {
                primary,
                settlement,
            } => write!(
                formatter,
                "execution-stop invariant failed: {primary}; conservative fallback also failed: \
                 {settlement}"
            ),
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
        if let Some(primary) = self.poison_reason() {
            return self.publish_fallback_after(primary);
        }
        #[cfg(test)]
        if self.take_terminal_planning_fault() {
            return self.publish_fallback_after(ProducerHostInvariantError::ForcedTerminalPlanning);
        }
        let transition = match self.core.apply(ProducerInput::ExecutionUnavailable) {
            Ok(transition) => transition,
            Err(error) => {
                return self.publish_fallback_after(ProducerHostInvariantError::Core(error));
            }
        };
        #[cfg(test)]
        let interpreted = if self.take_terminal_interpretation_fault() {
            Err(ProducerHostInvariantError::ForcedTerminalInterpretation)
        } else {
            self.interpret_transition(now, transition)
        };
        #[cfg(not(test))]
        let interpreted = self.interpret_transition(now, transition);
        if let Err(error) = interpreted {
            return self.publish_fallback_after(error);
        }
        Ok(())
    }

    fn publish_fallback_after(
        &mut self,
        error: ProducerHostInvariantError,
    ) -> Result<(), ProducerExecutionStopError> {
        self.drain_terminal_mechanisms_preserving_completions();
        match self.publish_execution_fallback() {
            Ok(()) => Err(ProducerExecutionStopError::Invariant(error)),
            Err(settlement) => Err(ProducerExecutionStopError::Fallback {
                primary: error,
                settlement: Box::new(settlement),
            }),
        }
    }

    fn publish_execution_fallback(&mut self) -> Result<(), ProducerExecutionStopError> {
        let backlog = self.terminal_backlog.len();
        let retry_error = self.retry_terminal_backlog_for_recovery(backlog).err();
        if !self.terminal_backlog.is_empty() {
            return Err(ProducerExecutionStopError::Invariant(
                retry_error.unwrap_or(ProducerHostInvariantError::Completion(
                    CompletionRegistryError::NotificationBackpressure,
                )),
            ));
        }
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
        self.bindings.clear_terminal();
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn inject_terminal_interpretation_fault(&mut self) {
        self.terminal_interpretation_fault = true;
    }

    #[cfg(test)]
    pub(crate) fn inject_terminal_planning_fault(&mut self) {
        self.terminal_planning_fault = true;
    }

    #[cfg(test)]
    fn take_terminal_interpretation_fault(&mut self) -> bool {
        std::mem::take(&mut self.terminal_interpretation_fault)
    }

    #[cfg(test)]
    fn take_terminal_planning_fault(&mut self) -> bool {
        std::mem::take(&mut self.terminal_planning_fault)
    }
}
