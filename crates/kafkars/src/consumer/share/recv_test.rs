//! Public hosted share-consumer receive operation contract.

use std::future::Future;

use super::{RecvShareConsumerBatch, ShareConsumer, ShareConsumerBatch};
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
fn recv_is_send_linear_and_borrows_the_unique_share_consumer() {
    fn require<T: Future<Output = Result<Option<ShareConsumerBatch>, KafkaError>> + Send>() {}
    fn borrow(consumer: &mut ShareConsumer) -> RecvShareConsumerBatch<'_> {
        consumer.recv()
    }

    require::<RecvShareConsumerBatch<'static>>();
    assert_not_impl!(RecvShareConsumerBatch<'static>: Clone);
    assert_not_impl!(RecvShareConsumerBatch<'static>: Copy);
    let _ = borrow as for<'a> fn(&'a mut ShareConsumer) -> RecvShareConsumerBatch<'a>;
}
