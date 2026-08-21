//! Public assigned-consumer event observation contract.

use std::future::Future;

use super::{AssignedConsumer, AssignedConsumerEvent, NextAssignedEvent};
use crate::{Client, KafkaError};

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
fn next_event_is_send_linear_and_borrows_the_unique_consumer() {
    fn require<T: Future<Output = Result<Option<AssignedConsumerEvent>, KafkaError>> + Send>() {}
    fn borrow(consumer: &mut AssignedConsumer) -> NextAssignedEvent<'_> {
        consumer.next_event()
    }
    fn require_borrow(_borrow: for<'a> fn(&'a mut AssignedConsumer) -> NextAssignedEvent<'a>) {}

    require::<NextAssignedEvent<'static>>();
    require_borrow(borrow);
    assert_not_impl!(NextAssignedEvent<'static>: Clone);
    assert_not_impl!(NextAssignedEvent<'static>: Copy);
}

#[test]
fn named_event_observation_reports_end_after_close_admission() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("start client: {error}"));
    let mut consumer = client
        .assigned_consumer()
        .build()
        .unwrap_or_else(|error| panic!("claim assigned consumer: {error}"));
    let close = consumer
        .try_close()
        .unwrap_or_else(|error| panic!("admit close: {error}"));

    assert!(
        consumer
            .next_event()
            .wait()
            .unwrap_or_else(|error| panic!("observe event end: {error}"))
            .is_none()
    );

    close
        .wait()
        .unwrap_or_else(|error| panic!("observe close: {error}"));
}
