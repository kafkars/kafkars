//! Public hosted group-consumer receive operation contract.

use std::future::Future;

use super::{Consumer, ConsumerBatch, RecvConsumerBatch};
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
    fn require<T: Future<Output = Result<Option<ConsumerBatch>, KafkaError>> + Send>() {}
    fn borrow(consumer: &mut Consumer) -> RecvConsumerBatch<'_> {
        consumer.recv()
    }
    require::<RecvConsumerBatch<'static>>();
    assert_not_impl!(RecvConsumerBatch<'static>: Clone);
    assert_not_impl!(RecvConsumerBatch<'static>: Copy);
    let _ = borrow as for<'a> fn(&'a mut Consumer) -> RecvConsumerBatch<'a>;
}
