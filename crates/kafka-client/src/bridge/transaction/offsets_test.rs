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
