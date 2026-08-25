//! Public linear share-batch and borrowed record-view contract.

use super::{
    ShareAcknowledgement, ShareAcknowledgementBuildError, ShareConsumerBatch, ShareConsumerHeader,
    ShareConsumerRecord, ShareConsumerRecords, ShareDisposition, ShareRecordDecision,
};

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
    type HeaderParts<'record> = (&'record [u8], Option<&'record [u8]>);
    type HeaderContract = for<'record> fn(ShareConsumerHeader<'record>) -> HeaderParts<'record>;

    fn require_send<T: Send>() {}
    fn batch_contract(batch: &ShareConsumerBatch) {
        let _: usize = batch.len();
        let _: bool = batch.is_empty();
        let _: usize = batch.partition_count();
        let _: usize = batch.acquisition_count();
        let _: ShareConsumerRecords<'_> = batch.records();
    }
    fn acknowledgement_contract(batch: ShareConsumerBatch) {
        let _: Result<ShareAcknowledgement, ShareAcknowledgementBuildError> = batch.accept_all();
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
        let decision = record.decision(ShareDisposition::Release);
        let _: i64 = decision.offset();
        let _: ShareDisposition = decision.disposition();
    }
    #[expect(
        clippy::needless_pass_by_value,
        reason = "consuming the iterator item proves returned references retain the record lifetime"
    )]
    fn retained_header_contract(header: ShareConsumerHeader<'_>) -> HeaderParts<'_> {
        let key = header.key();
        let value = header.value();
        (key, value)
    }

    require_send::<ShareConsumerBatch>();
    assert_not_impl!(ShareConsumerBatch: Clone);
    assert_not_impl!(ShareConsumerBatch: Copy);
    let _ = batch_contract as fn(&ShareConsumerBatch);
    let _ = acknowledgement_contract as fn(ShareConsumerBatch);
    let _: fn(
        ShareConsumerBatch,
        Vec<ShareRecordDecision>,
    ) -> Result<ShareAcknowledgement, ShareAcknowledgementBuildError> =
        ShareConsumerBatch::into_acknowledgement;
    let _ = record_contract as fn(&ShareConsumerRecord<'_>);
    let _: HeaderContract = retained_header_contract;
    require_send::<ShareAcknowledgement>();
    assert_not_impl!(ShareAcknowledgement: Clone);
    assert_not_impl!(ShareAcknowledgement: Copy);
}
