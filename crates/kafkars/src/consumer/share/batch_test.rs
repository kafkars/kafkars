//! Public linear share-batch and borrowed record-view contract.

use super::{ShareConsumerBatch, ShareConsumerHeader, ShareConsumerRecord, ShareConsumerRecords};

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
fn share_batch_is_send_linear_and_exposes_borrowed_record_facts() {
    fn require_send<T: Send>() {}
    fn batch_contract(batch: &ShareConsumerBatch) {
        let _: usize = batch.len();
        let _: bool = batch.is_empty();
        let _: usize = batch.partition_count();
        let _: usize = batch.acquisition_count();
        let _: ShareConsumerRecords<'_> = batch.records();
    }
    fn record_contract(record: &ShareConsumerRecord<'_>) {
        let _: &str = record.topic();
        let _: u32 = record.partition();
        let _: i64 = record.offset();
        let _: i16 = record.delivery_count();
        let _: Option<i64> = record.timestamp_millis();
        let _: Option<&[u8]> = record.key();
        let _: Option<&[u8]> = record.value();
        let _: Vec<ShareConsumerHeader<'_>> = record.headers().collect();
    }

    require_send::<ShareConsumerBatch>();
    assert_not_impl!(ShareConsumerBatch: Clone);
    assert_not_impl!(ShareConsumerBatch: Copy);
    let _ = batch_contract as fn(&ShareConsumerBatch);
    let _ = record_contract as fn(&ShareConsumerRecord<'_>);
}
