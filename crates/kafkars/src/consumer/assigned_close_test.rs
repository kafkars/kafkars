//! Public assigned-consumer close operation trait contract.

use std::future::Future;

use super::CloseAssignedConsumer;
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
fn close_is_one_send_runtime_neutral_observer() {
    fn require<T: Future<Output = Result<(), KafkaError>> + Send>() {}

    require::<CloseAssignedConsumer>();
    assert_not_impl!(CloseAssignedConsumer: Clone);
    assert_not_impl!(CloseAssignedConsumer: Copy);
}
