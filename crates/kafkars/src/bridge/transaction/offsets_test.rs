//! Private transactional-offset ownership and operation contracts.

use std::{future::Future, time::Duration};

use crate::{
    Checkpoint, GroupMetadata, KafkaError,
    bridge::transaction::{TransactionEngine, TransactionOffsetsEngine},
};

type SendOffsetsMethod<'send, 'producer> = fn(
    &'send mut TransactionEngine<'producer>,
    GroupMetadata,
    Checkpoint,
    Duration,
) -> Result<
    TransactionOffsetsEngine<'send, 'producer>,
    (GroupMetadata, Checkpoint, KafkaError),
>;

#[test]
fn private_offsets_reborrow_transaction_until_observer_release() {
    fn require_send(_method: SendOffsetsMethod<'_, '_>) {}
    fn require_future<T: Future<Output = Result<(), KafkaError>>>() {}
    fn require_wait(
        _method: fn(TransactionOffsetsEngine<'static, 'static>) -> Result<(), KafkaError>,
    ) {
    }

    require_send(TransactionEngine::send_offsets);
    require_future::<TransactionOffsetsEngine<'static, 'static>>();
    require_wait(TransactionOffsetsEngine::wait);
}

#[test]
fn only_transactional_offset_contention_grants_safe_unsent_retry() {
    use kafka_client_engine::TransactionOffsetsAdmissionErrorKind as Kind;

    for kind in [
        Kind::InvalidDeadline,
        Kind::StaleCheckpoint,
        Kind::Contended,
        Kind::Closed,
        Kind::StaleOwner,
        Kind::Busy,
        Kind::Backpressure,
        Kind::InvalidLifecycle,
        Kind::InvalidInput,
        Kind::IdentityExhausted,
    ] {
        let error = super::offsets_result::translate_admission(kind);
        assert_eq!(
            error.delivery_status(),
            Some(crate::DeliveryStatus::NotSent)
        );
        assert_eq!(
            error.retry_advice(),
            if kind == Kind::Contended {
                crate::RetryAdvice::RetrySafe
            } else {
                crate::RetryAdvice::DoNotRetry
            },
            "unexpected retry authority for {kind:?}"
        );
    }
}
