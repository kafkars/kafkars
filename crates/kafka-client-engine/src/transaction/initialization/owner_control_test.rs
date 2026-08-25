//! Public opaque transaction-token admission and cleanup scenarios.

use std::time::{Duration, Instant};

use super::{TransactionControlErrorKind, TransactionToken, host_test::Fixture};

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
fn begin_returns_an_opaque_linear_token_with_advisory_wake_status() {
    let fixture = Fixture::new();
    let mut owner = fixture.initialize(41);
    let accepted = owner
        .begin_transaction()
        .unwrap_or_else(|error| panic!("begin transaction: {error:?}"));

    assert!(!accepted.wake_failed());
    let transaction = accepted.into_transaction();
    assert!(transaction.epoch().get() > 0);
    drop(transaction);
}

#[test]
fn rejected_end_retains_the_exact_token_for_an_abort_retry() {
    let fixture = Fixture::new();
    let mut owner = fixture.initialize(42);
    let transaction = owner
        .begin_transaction()
        .unwrap_or_else(|error| panic!("begin transaction: {error:?}"))
        .into_transaction();

    let Err(rejection) = transaction.commit(Duration::ZERO) else {
        panic!("zero-timeout transaction end was unexpectedly admitted");
    };
    assert_eq!(
        rejection.kind(),
        TransactionControlErrorKind::InvalidDeadline
    );
    let accepted = rejection
        .into_transaction()
        .abort(Duration::from_secs(4))
        .unwrap_or_else(|error| panic!("same token remains available for abort: {error:?}"));
    assert!(!accepted.wake_failed());
    drop(accepted.into_observer());
}

#[test]
fn elapsed_outer_deadline_rejects_without_consuming_the_transaction_token() {
    let fixture = Fixture::new();
    let mut owner = fixture.initialize(43);
    let transaction = owner
        .begin_transaction()
        .unwrap_or_else(|error| panic!("begin transaction: {error:?}"))
        .into_transaction();

    let Err(rejection) = transaction.commit_until(Instant::now()) else {
        panic!("elapsed outer deadline was unexpectedly admitted");
    };
    assert_eq!(
        rejection.kind(),
        TransactionControlErrorKind::InvalidDeadline
    );
    let accepted = rejection
        .into_transaction()
        .abort_until(
            Instant::now()
                .checked_add(Duration::from_secs(4))
                .unwrap_or_else(|| panic!("short abort deadline should be representable")),
        )
        .unwrap_or_else(|error| panic!("same token remains available for abort: {error:?}"));
    drop(accepted.into_observer());
}

#[test]
fn transaction_token_is_sendable_and_noncloneable() {
    fn require_send<T: Send>() {}

    require_send::<TransactionToken<'static>>();
    assert_not_impl!(TransactionToken<'static>: Clone);
    assert_not_impl!(TransactionToken<'static>: Copy);
}
