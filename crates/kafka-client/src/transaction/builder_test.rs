//! Transactional producer builder deadline and linearity contract.

use std::time::Duration;

use super::TransactionalProducerBuilder;
use crate::{Client, ErrorKind};

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
fn builder_is_sendable_but_linear() {
    fn require_send<T: Send>() {}

    require_send::<TransactionalProducerBuilder>();
    assert_not_impl!(TransactionalProducerBuilder: Clone);
    assert_not_impl!(TransactionalProducerBuilder: Copy);
}

#[test]
fn initialization_deadline_is_captured_before_request_validation() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("start client: {error}"));
    let error = client
        .transactional_producer("")
        .deadline_after(Duration::MAX)
        .build()
        .wait()
        .err()
        .unwrap_or_else(|| panic!("unrepresentable deadline must reject"));

    assert_eq!(error.kind(), ErrorKind::Timeout);
}

#[test]
fn broker_timeout_remains_distinct_from_operation_deadline() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("start client: {error}"));
    let error = client
        .transactional_producer("writer")
        .transaction_timeout(Duration::ZERO)
        .deadline_after(Duration::from_secs(1))
        .build()
        .wait()
        .err()
        .unwrap_or_else(|| panic!("zero broker timeout must reject"));

    assert_eq!(error.kind(), ErrorKind::Configuration);
}
