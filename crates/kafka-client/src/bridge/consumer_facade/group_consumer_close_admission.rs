//! Lossless facade admission of one explicit hosted group close.

use kafka_client_engine::GroupConsumerCloseAdmissionErrorKind;

use crate::{ErrorKind, KafkaError};

use super::{group_consumer::GroupConsumerEngine, group_consumer_close::GroupConsumerClose};

impl GroupConsumerEngine {
    /// Consumes the bridge only after engine close admission accepts.
    #[expect(
        clippy::result_large_err,
        reason = "failed close admission must return the exact still-live consumer owner"
    )]
    pub(crate) fn try_close(self) -> Result<GroupConsumerClose, (GroupConsumerEngine, KafkaError)> {
        let GroupConsumerEngine {
            handle,
            startup_fault,
        } = self;
        match handle.try_close() {
            Ok(accepted) => {
                let advisory_error = accepted.wake_failed().then(|| {
                    KafkaError::new(
                        ErrorKind::Internal,
                        "group close was accepted but host wakeup failed",
                    )
                });
                Ok(GroupConsumerClose::new(accepted, advisory_error))
            }
            Err(error) => {
                let semantic = translate_close_admission(error.kind());
                Err((
                    GroupConsumerEngine {
                        handle: error.into_handle(),
                        startup_fault,
                    },
                    semantic,
                ))
            }
        }
    }
}

fn translate_close_admission(kind: GroupConsumerCloseAdmissionErrorKind) -> KafkaError {
    match kind {
        GroupConsumerCloseAdmissionErrorKind::Closed
        | GroupConsumerCloseAdmissionErrorKind::GroupUnavailable => KafkaError::new(
            ErrorKind::State,
            "group close admission is closed or the group is unavailable",
        ),
        GroupConsumerCloseAdmissionErrorKind::Contended => KafkaError::new(
            ErrorKind::Backpressure,
            "group close owner is temporarily contended",
        ),
        GroupConsumerCloseAdmissionErrorKind::HostUnavailable
        | GroupConsumerCloseAdmissionErrorKind::InternalInvariant => {
            KafkaError::new(ErrorKind::Internal, "group close ownership is unavailable")
        }
    }
}
