//! Public assigned-consumer receive operation contract.

use std::future::Future;

use super::{AssignedConsumer, RecordBatch, RecvAssignedBatch};
use crate::KafkaError;

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
fn recv_is_send_linear_and_borrows_the_unique_consumer() {
    fn require<T: Future<Output = Result<Option<RecordBatch>, KafkaError>> + Send>() {}
    fn borrow(consumer: &mut AssignedConsumer) -> RecvAssignedBatch<'_> {
        consumer.recv()
    }
    fn require_borrow(_borrow: for<'a> fn(&'a mut AssignedConsumer) -> RecvAssignedBatch<'a>) {}

    require::<RecvAssignedBatch<'static>>();
    require_borrow(borrow);
    assert_not_impl!(RecvAssignedBatch<'static>: Clone);
    assert_not_impl!(RecvAssignedBatch<'static>: Copy);
}
