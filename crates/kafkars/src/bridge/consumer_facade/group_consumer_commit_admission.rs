//! Capture-first facade admission of one exact classic-group checkpoint commit.

use kafka_client_engine::GroupConsumerCommitAdmissionErrorKind;

use crate::{ErrorKind, KafkaError};

use super::{
    group_consumer::GroupConsumerEngine, group_consumer_checkpoint::GroupConsumerCheckpoint,
    group_consumer_commit::GroupConsumerCommit,
};

impl GroupConsumerEngine {
    /// Admits one exact checkpoint under a deadline captured by the engine call.
    #[expect(
        clippy::result_large_err,
        reason = "pre-admission rejection must return the exact caller checkpoint"
    )]
    pub(crate) fn try_commit(
        &mut self,
        checkpoint: GroupConsumerCheckpoint,
        timeout: std::time::Duration,
    ) -> Result<GroupConsumerCommit, (GroupConsumerCheckpoint, KafkaError)> {
        if let Some(error) = &self.startup_fault {
            return Err((checkpoint, error.clone()));
        }
        match self.handle.try_commit(checkpoint.into_engine(), timeout) {
            Ok(accepted) => {
                let advisory_error = if accepted.host_faulted() {
                    Some(KafkaError::new(
                        ErrorKind::Internal,
                        "group commit was accepted with a retained engine fault",
                    ))
                } else if accepted.wake_failed() {
                    Some(KafkaError::new(
                        ErrorKind::Internal,
                        "group commit was accepted but host wakeup failed",
                    ))
                } else {
                    None
                };
                Ok(GroupConsumerCommit::new(
                    accepted.into_observer(),
                    advisory_error,
                ))
            }
            Err(error) => {
                let semantic = translate_commit_admission(error.kind());
                Err((
                    GroupConsumerCheckpoint::from_engine(error.into_checkpoint()),
                    semantic,
                ))
            }
        }
    }
}

pub(super) fn translate_commit_admission(
    kind: GroupConsumerCommitAdmissionErrorKind,
) -> KafkaError {
    match kind {
        GroupConsumerCommitAdmissionErrorKind::InvalidDeadline => KafkaError::new(
            ErrorKind::Configuration,
            "group commit timeout is outside the supported deadline domain",
        ),
        GroupConsumerCommitAdmissionErrorKind::Closed
        | GroupConsumerCommitAdmissionErrorKind::GroupUnavailable => KafkaError::new(
            ErrorKind::State,
            "group commit admission is closed or unavailable",
        ),
        GroupConsumerCommitAdmissionErrorKind::Contended
        | GroupConsumerCommitAdmissionErrorKind::Backpressure => KafkaError::new(
            ErrorKind::Backpressure,
            "bounded group commit admission is temporarily full",
        )
        .with_safe_retry(),
        GroupConsumerCommitAdmissionErrorKind::StaleCheckpoint => KafkaError::new(
            ErrorKind::State,
            "checkpoint no longer matches the live group assignment",
        ),
        GroupConsumerCommitAdmissionErrorKind::HostUnavailable => {
            KafkaError::new(ErrorKind::Internal, "group commit host is unavailable")
        }
    }
}
