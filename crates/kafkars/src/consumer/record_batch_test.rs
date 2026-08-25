//! Public record-batch linearity and immediate-observation shape contract.

use super::{ConsumerFetchEvidence, ConsumerRecord, RecordBatch};

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
fn batch_is_send_linear_and_exposes_borrowed_records() {
    fn require_send<T: Send>() {}
    fn batch_contract(batch: &RecordBatch) {
        let _: &str = batch.topic();
        let _: i32 = batch.partition();
        let _: i64 = batch.checkpoint_next_offset();
        let _: &ConsumerFetchEvidence = batch.evidence();
        let _: usize = batch.len();
        let _: bool = batch.is_empty();
        let _: Option<ConsumerRecord<'_>> = batch.records().next();
    }

    require_send::<RecordBatch>();
    assert_not_impl!(RecordBatch: Clone);
    assert_not_impl!(RecordBatch: Copy);
    let _ = batch_contract as fn(&RecordBatch);
}
