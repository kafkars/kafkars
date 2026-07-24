//! Public assigned-consumer linearity, threading, and deadline contract.

use std::time::Duration;

use super::{AssignedConsumer, CloseAssignedConsumer, StartPosition, TopicPartition};
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
fn assigned_consumer_is_send_linear_and_not_shared() {
    fn require_send<T: Send>() {}

    require_send::<AssignedConsumer>();
    assert_not_impl!(AssignedConsumer: Clone);
    assert_not_impl!(AssignedConsumer: Copy);
    assert_not_impl!(AssignedConsumer: Sync);
}

#[test]
fn close_rejection_retains_the_unique_consumer_for_retry() {
    fn require_close(
        _close: fn(&mut AssignedConsumer) -> Result<CloseAssignedConsumer, crate::KafkaError>,
    ) {
    }

    require_close(AssignedConsumer::try_close);
}

#[test]
fn deadline_capture_precedes_facade_input_conversion() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("start client: {error}"));
    let mut consumer = client
        .assigned_consumer()
        .build()
        .unwrap_or_else(|error| panic!("claim assigned consumer: {error}"));

    let error = consumer
        .try_replace_assignment(
            [TopicPartition::new("", -1).start_at(StartPosition::Offset(-1))],
            Duration::MAX,
        )
        .err()
        .unwrap_or_else(|| panic!("unrepresentable deadline must win"));
    assert_eq!(error.kind(), ErrorKind::Timeout);

    consumer
        .try_close()
        .unwrap_or_else(|error| panic!("admit close after rejection: {error}"))
        .wait()
        .unwrap_or_else(|error| panic!("observe close: {error}"));
}

#[test]
fn duplicate_rejection_keeps_the_handle_available_for_close() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("start client: {error}"));
    let mut consumer = client
        .assigned_consumer()
        .build()
        .unwrap_or_else(|error| panic!("claim assigned consumer: {error}"));
    let entry = TopicPartition::new("orders", 0).start_at(StartPosition::Beginning);

    let error = consumer
        .try_replace_assignment([entry.clone(), entry], Duration::from_secs(1))
        .err()
        .unwrap_or_else(|| panic!("duplicate assignment must fail"));
    assert_eq!(error.kind(), ErrorKind::Configuration);

    consumer
        .try_close()
        .unwrap_or_else(|error| panic!("admit close after rejection: {error}"))
        .wait()
        .unwrap_or_else(|error| panic!("observe close: {error}"));
}
