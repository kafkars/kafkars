//! Exact checkpoint recovery across synchronous processing acknowledgment.

use kafka_client_engine::GroupConsumerAcknowledgeErrorKind;

use crate::{ErrorKind, KafkaError};

use super::{
    group_consumer::GroupConsumerEngine, group_consumer_checkpoint::GroupConsumerCheckpoint,
};

impl GroupConsumerEngine {
    /// Records assignment-fenced processing progress without committing offsets.
    #[expect(
        clippy::result_large_err,
        reason = "rejection returns the exact assignment-fenced checkpoint"
    )]
    pub(crate) fn acknowledge(
        &mut self,
        checkpoint: GroupConsumerCheckpoint,
    ) -> Result<(), (GroupConsumerCheckpoint, KafkaError)> {
        if let Some(error) = &self.startup_fault {
            return Err((checkpoint, error.clone()));
        }
        self.handle
            .acknowledge(checkpoint.into_engine())
            .map_err(|error| {
                let kind = error.kind();
                (
                    GroupConsumerCheckpoint::from_engine(error.into_checkpoint()),
                    translate_group_consumer_acknowledgment(kind),
                )
            })
    }
}

pub(super) fn translate_group_consumer_acknowledgment(
    kind: GroupConsumerAcknowledgeErrorKind,
) -> KafkaError {
    let (kind, message) = match kind {
        GroupConsumerAcknowledgeErrorKind::Contended => (
            ErrorKind::Backpressure,
            "group checkpoint acknowledgment is contended",
        ),
        GroupConsumerAcknowledgeErrorKind::Closed
        | GroupConsumerAcknowledgeErrorKind::GroupUnavailable
        | GroupConsumerAcknowledgeErrorKind::StaleCheckpoint
        | GroupConsumerAcknowledgeErrorKind::DeadlineElapsed => (
            ErrorKind::State,
            "group checkpoint acknowledgment is no longer current",
        ),
        GroupConsumerAcknowledgeErrorKind::Clock
        | GroupConsumerAcknowledgeErrorKind::HostUnavailable
        | GroupConsumerAcknowledgeErrorKind::InternalInvariant => (
            ErrorKind::Internal,
            "group checkpoint acknowledgment ownership is unavailable",
        ),
    };
    KafkaError::new(kind, message)
}
