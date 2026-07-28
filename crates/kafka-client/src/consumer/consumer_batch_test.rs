//! Public group-consumer batch ownership and iteration shape contract.

use super::{Checkpoint, ConsumerBatch, GroupConsumerRecord, GroupConsumerRecords};

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
fn batch_is_send_linear_iterable_and_yields_one_checkpoint() {
    fn require_send<T: Send>() {}
    fn batch_contract(batch: &ConsumerBatch) {
        let _: &str = batch.topic();
        let _: i32 = batch.partition();
        let _: i64 = batch.checkpoint_next_offset();
        let _: usize = batch.len();
        let _: bool = batch.is_empty();
        let _: Option<GroupConsumerRecord<'_>> = batch.records().next();
    }
    fn checkpoint(batch: ConsumerBatch) -> Checkpoint {
        batch.checkpoint()
    }
    fn iterator_contract(batch: &ConsumerBatch) -> GroupConsumerRecords<'_> {
        batch.into_iter()
    }

    require_send::<ConsumerBatch>();
    assert_not_impl!(ConsumerBatch: Clone);
    assert_not_impl!(ConsumerBatch: Copy);
    let _ = batch_contract as fn(&ConsumerBatch);
    let _ = checkpoint as fn(ConsumerBatch) -> Checkpoint;
    let _ = iterator_contract as fn(&ConsumerBatch) -> GroupConsumerRecords<'_>;
}
