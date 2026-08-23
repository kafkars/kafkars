//! Capture-first public-port admission of one exact share acknowledgement.

use std::time::Duration;

use kafka_client_core::GroupId;

use crate::{
    completion::CompletionObserver,
    consumer::{ShareAcknowledgeOutcome, ShareAcknowledgement},
};

use super::{
    ShareConsumerShardWakeError,
    fetch_session_set::ShareSessionAcknowledgementAdmissionFailureKind as SessionFailureKind,
    port::ShareConsumerPort,
    registry_acknowledgement::ShareAcknowledgementAdmissionFailureKind as RegistryFailureKind,
    shard::ShareConsumerShardLockError,
};

#[must_use = "accepted acknowledgement retains terminal observation"]
pub(in crate::consumer) struct ShareAcknowledgementPortAdmission {
    pub(in crate::consumer) observer: CompletionObserver<ShareAcknowledgeOutcome>,
    pub(in crate::consumer) wake: Option<ShareConsumerShardWakeError>,
}

#[must_use = "rejected acknowledgement admission retains exact caller ownership"]
pub(in crate::consumer) struct ShareAcknowledgementPortFailure {
    pub(in crate::consumer) source: ShareAcknowledgementPortFailureSource,
    pub(in crate::consumer) acknowledgement: ShareAcknowledgement,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer) enum ShareAcknowledgementPortFailureSource {
    InvalidDeadline,
    ForeignRegistry,
    Closed,
    Contended,
    Unavailable,
    Backpressure,
    DeadlineElapsed,
    Stale,
    InvalidRequest,
    Internal,
}

impl ShareConsumerPort {
    pub(in crate::consumer) fn try_acknowledge(
        &self,
        group_id: GroupId,
        acknowledgement: ShareAcknowledgement,
        timeout: Duration,
    ) -> Result<ShareAcknowledgementPortAdmission, ShareAcknowledgementPortFailure> {
        let capture = match self.capture_deadline_after(timeout) {
            Ok(capture) if !timeout.is_zero() => capture,
            Ok(_) | Err(_) => {
                return Err(port_failure(
                    ShareAcknowledgementPortFailureSource::InvalidDeadline,
                    acknowledgement,
                ));
            }
        };
        if !acknowledgement.shares_registry_with(self) {
            return Err(port_failure(
                ShareAcknowledgementPortFailureSource::ForeignRegistry,
                acknowledgement,
            ));
        }
        if self.shared.admission_is_closed() {
            return Err(port_failure(
                ShareAcknowledgementPortFailureSource::Closed,
                acknowledgement,
            ));
        }
        let parts = acknowledgement.into_admission_parts();
        let mut registry = match self.shared.try_registry() {
            Ok(registry) => registry,
            Err(error) => {
                return Err(ShareAcknowledgementPortFailure {
                    source: lock_failure(error),
                    acknowledgement: parts.recovery.recover(parts.inner),
                });
            }
        };
        if self.shared.admission_is_closed() {
            return Err(ShareAcknowledgementPortFailure {
                source: ShareAcknowledgementPortFailureSource::Closed,
                acknowledgement: parts.recovery.recover(parts.inner),
            });
        }
        let observer = match registry.begin_acknowledgement(group_id, parts, capture) {
            Ok(observer) => observer,
            Err(failure) => {
                return Err(ShareAcknowledgementPortFailure {
                    source: registry_failure(failure.kind),
                    acknowledgement: failure.parts.recovery.recover(failure.parts.inner),
                });
            }
        };
        drop(registry);
        Ok(ShareAcknowledgementPortAdmission {
            observer,
            wake: self.shared.request_turn().err(),
        })
    }
}

const fn lock_failure(error: ShareConsumerShardLockError) -> ShareAcknowledgementPortFailureSource {
    match error {
        ShareConsumerShardLockError::Contended => ShareAcknowledgementPortFailureSource::Contended,
        ShareConsumerShardLockError::Poisoned => ShareAcknowledgementPortFailureSource::Internal,
    }
}

const fn registry_failure(error: RegistryFailureKind) -> ShareAcknowledgementPortFailureSource {
    use crate::completion::CompletionRegistryError;
    match error {
        RegistryFailureKind::UnknownConsumer => ShareAcknowledgementPortFailureSource::Unavailable,
        RegistryFailureKind::Completion(CompletionRegistryError::Full) => {
            ShareAcknowledgementPortFailureSource::Backpressure
        }
        RegistryFailureKind::Completion(CompletionRegistryError::NotifierStopped) => {
            ShareAcknowledgementPortFailureSource::Closed
        }
        RegistryFailureKind::Completion(_) | RegistryFailureKind::Rollback(_) => {
            ShareAcknowledgementPortFailureSource::Internal
        }
        RegistryFailureKind::Session(session) => session_failure(session),
    }
}

const fn session_failure(error: SessionFailureKind) -> ShareAcknowledgementPortFailureSource {
    use kafka_client_core::ShareAcknowledgementApplyErrorKind as CoreKind;
    match error {
        SessionFailureKind::UnknownSession => ShareAcknowledgementPortFailureSource::Stale,
        SessionFailureKind::Occupied => ShareAcknowledgementPortFailureSource::Backpressure,
        SessionFailureKind::Preparation(
            super::fetch_acknowledgement::ShareAcknowledgementPreparationFailureKind::Protocol(_),
        ) => ShareAcknowledgementPortFailureSource::InvalidRequest,
        SessionFailureKind::Preparation(
            super::fetch_acknowledgement::ShareAcknowledgementPreparationFailureKind::Rollback(_),
        ) => ShareAcknowledgementPortFailureSource::Internal,
        SessionFailureKind::Preparation(
            super::fetch_acknowledgement::ShareAcknowledgementPreparationFailureKind::Core(kind),
        ) => match kind {
            CoreKind::DeadlineElapsed => ShareAcknowledgementPortFailureSource::DeadlineElapsed,
            CoreKind::InvalidState => ShareAcknowledgementPortFailureSource::Backpressure,
            CoreKind::SessionMismatch | CoreKind::StaleAttempt | CoreKind::Acquisition(_) => {
                ShareAcknowledgementPortFailureSource::Stale
            }
            CoreKind::SessionEpochExhausted => ShareAcknowledgementPortFailureSource::Internal,
        },
    }
}

const fn port_failure(
    source: ShareAcknowledgementPortFailureSource,
    acknowledgement: ShareAcknowledgement,
) -> ShareAcknowledgementPortFailure {
    ShareAcknowledgementPortFailure {
        source,
        acknowledgement,
    }
}
