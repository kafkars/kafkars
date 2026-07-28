//! Public hosted group-consumer seek operation contract.

use std::future::Future;

use super::{Consumer, Seek, StartPosition, TopicPartition};
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
fn seek_is_a_send_linear_future_borrowing_the_unique_consumer() {
    fn require<T: Future<Output = Result<(), KafkaError>> + Send>() {}
    fn borrow(
        consumer: &mut Consumer,
        partition: TopicPartition,
        position: StartPosition,
    ) -> Seek<'_> {
        consumer.seek(partition, position)
    }
    fn require_borrow(
        _borrow: for<'a> fn(&'a mut Consumer, TopicPartition, StartPosition) -> Seek<'a>,
    ) {
    }
    fn blocking(operation: Seek<'_>) -> Result<(), KafkaError> {
        operation.wait()
    }
    fn require_blocking(_blocking: for<'a> fn(Seek<'a>) -> Result<(), KafkaError>) {}

    require::<Seek<'static>>();
    require_borrow(borrow);
    require_blocking(blocking);
    assert_not_impl!(Seek<'static>: Clone);
    assert_not_impl!(Seek<'static>: Copy);
}
