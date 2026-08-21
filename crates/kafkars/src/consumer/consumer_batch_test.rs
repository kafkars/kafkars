//! Public group-consumer batch ownership and iteration shape contract.

use super::{
    Checkpoint, CheckpointBuilder, ConsumerBatch, GroupConsumerRecord, GroupConsumerRecords,
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
fn batch_is_send_linear_iterable_and_exposes_both_consuming_checkpoint_spellings() {
    fn require_send<T: Send>() {}
    fn batch_contract(batch: &ConsumerBatch) {
        let _: &str = batch.topic();
        let _: i32 = batch.partition();
        let _: i64 = batch.checkpoint_next_offset();
        let _: usize = batch.len();
        let _: bool = batch.is_empty();
        let _: Option<GroupConsumerRecord<'_>> = batch.records().next();
        let _: Option<GroupConsumerRecord<'_>> = batch.iter().next();
        let _: Option<GroupConsumerRecord<'_>> = batch.into_iter().next();
        let _: CheckpointBuilder<'_> = batch.checkpoint_builder();
    }
    fn canonical_checkpoint(batch: ConsumerBatch) -> Checkpoint {
        batch.checkpoint()
    }
    fn compatibility_checkpoint(batch: ConsumerBatch) -> Checkpoint {
        batch.into_checkpoint()
    }
    fn iterator_contract(batch: &ConsumerBatch) -> GroupConsumerRecords<'_> {
        batch.into_iter()
    }

    require_send::<ConsumerBatch>();
    assert_not_impl!(ConsumerBatch: Clone);
    assert_not_impl!(ConsumerBatch: Copy);
    let _ = batch_contract as fn(&ConsumerBatch);
    let _ = canonical_checkpoint as fn(ConsumerBatch) -> Checkpoint;
    let _ = compatibility_checkpoint as fn(ConsumerBatch) -> Checkpoint;
    let _ = iterator_contract as fn(&ConsumerBatch) -> GroupConsumerRecords<'_>;
}
