//! Accepted transaction-end runtime-neutral operation contracts.

use std::future::Future;

use crate::KafkaError;

use super::{AbortTransaction, CommitTransaction};

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
fn commit_and_abort_are_named_linear_future_operations() {
    fn require_future<T: Future<Output = Result<(), KafkaError>>>() {}
    fn require_commit_status(_method: fn(&CommitTransaction<'static>) -> bool) {}
    fn require_abort_status(_method: fn(&AbortTransaction<'static>) -> bool) {}
    fn require_commit_wait(_method: fn(CommitTransaction<'static>) -> Result<(), KafkaError>) {}
    fn require_abort_wait(_method: fn(AbortTransaction<'static>) -> Result<(), KafkaError>) {}

    require_future::<CommitTransaction<'static>>();
    require_future::<AbortTransaction<'static>>();
    require_commit_status(CommitTransaction::begin_wake_failed);
    require_commit_status(CommitTransaction::end_wake_failed);
    require_abort_status(AbortTransaction::begin_wake_failed);
    require_abort_status(AbortTransaction::end_wake_failed);
    require_commit_wait(CommitTransaction::wait);
    require_abort_wait(AbortTransaction::wait);
    assert_not_impl!(CommitTransaction<'static>: Clone);
    assert_not_impl!(AbortTransaction<'static>: Clone);
}
