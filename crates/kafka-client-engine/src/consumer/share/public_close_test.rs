//! Public share-close category and certainty-preserving ownership scenarios.

use kafka_client_core::ShareGroupHeartbeatFailure;

use super::{
    port::ShareClosePortError,
    public_close::{
        ShareConsumerCloseAdmissionErrorKind, ShareConsumerCloseErrorKind, close_admission_kind,
        terminal_error,
    },
    registry_close::ShareConsumerCloseAdmissionError as RegistryCloseError,
    shard::ShareConsumerShardLockError,
};

#[test]
fn only_pre_admission_contention_is_publicly_retryable_by_ownership() {
    assert_eq!(
        close_admission_kind(ShareClosePortError::Lock(
            ShareConsumerShardLockError::Contended,
        )),
        ShareConsumerCloseAdmissionErrorKind::Contended
    );
    assert_eq!(
        close_admission_kind(ShareClosePortError::Registry(
            RegistryCloseError::AlreadyClosing,
        )),
        ShareConsumerCloseAdmissionErrorKind::Unavailable
    );
}

#[test]
fn terminal_mapping_preserves_exact_broker_code_and_deadline() {
    let broker = terminal_error(ShareGroupHeartbeatFailure::Broker(27));
    assert_eq!(broker.kind(), ShareConsumerCloseErrorKind::BrokerRejected);
    assert_eq!(broker.broker_code(), Some(27));
    let deadline = terminal_error(ShareGroupHeartbeatFailure::DeadlineElapsed);
    assert_eq!(
        deadline.kind(),
        ShareConsumerCloseErrorKind::DeadlineElapsed
    );
    assert_eq!(deadline.broker_code(), None);
}
