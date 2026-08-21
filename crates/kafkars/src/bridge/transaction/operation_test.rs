//! Private transaction initialization observation contract.

use std::future::Future;

use super::TransactionInitialization;
use crate::{ErrorKind, KafkaError};

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
fn wait_and_future_share_one_linear_concrete_result() {
    fn require_future<T: Future + Send>() {}

    require_future::<TransactionInitialization>();
    assert_not_impl!(TransactionInitialization: Clone);
    assert_not_impl!(TransactionInitialization: Copy);

    let error = TransactionInitialization::ready(Err(KafkaError::new(
        ErrorKind::Configuration,
        "rejected",
    )))
    .wait()
    .err()
    .unwrap_or_else(|| panic!("ready rejection must remain an error"));
    assert_eq!(error.kind(), ErrorKind::Configuration);
}
