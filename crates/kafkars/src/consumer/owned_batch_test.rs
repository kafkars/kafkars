//! Public shape and linearity evidence for direct-consumer owned batches.

use super::{ConsumerFetchEvidence, OwnedConsumerBatch, OwnedConsumerRecords, RecordBatch};

macro_rules! assert_not_impl {
    ($type:ty: $trait:path) => {
        const _: fn() = || {
            struct Implemented;
            trait AmbiguousIfImplemented<A> {
                fn check() {}
            }
            impl<T: ?Sized> AmbiguousIfImplemented<()> for T {}
            impl<T: ?Sized + $trait> AmbiguousIfImplemented<Implemented> for T {}
            let _ = <$type as AmbiguousIfImplemented<_>>::check;
        };
    };
}

#[test]
fn owned_batch_is_send_linear_and_consumes_into_owned_records() {
    fn require_send<T: Send>() {}
    fn require_conversion(_convert: fn(RecordBatch) -> OwnedConsumerBatch) {}
    fn require_records(_records: fn(OwnedConsumerBatch) -> OwnedConsumerRecords) {}
    fn evidence(batch: &OwnedConsumerBatch) -> &ConsumerFetchEvidence {
        batch.evidence()
    }

    require_send::<OwnedConsumerBatch>();
    require_conversion(RecordBatch::into_owned);
    require_records(OwnedConsumerBatch::into_records);
    let _ = evidence as fn(&OwnedConsumerBatch) -> &ConsumerFetchEvidence;
    assert_not_impl!(OwnedConsumerBatch: Clone);
    assert_not_impl!(OwnedConsumerBatch: Copy);
}
