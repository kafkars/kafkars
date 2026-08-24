//! Public incremental direct-assignment admission contract.

use std::time::Duration;

use super::{AssignedConsumer, StartPosition, TopicPartition};
use crate::{Client, ErrorKind};

#[test]
fn incremental_assignment_api_is_synchronous_and_handle_preserving() {
    fn require_add<I>(_add: fn(&mut AssignedConsumer, I, Duration) -> Result<(), crate::KafkaError>)
    where
        I: IntoIterator<Item = TopicPartition>,
    {
    }
    fn require_remove<I>(_remove: fn(&mut AssignedConsumer, I) -> Result<(), crate::KafkaError>)
    where
        I: IntoIterator<Item = TopicPartition>,
    {
    }

    require_add::<[TopicPartition; 1]>(AssignedConsumer::try_add_assignments);
    require_remove::<[TopicPartition; 1]>(AssignedConsumer::try_remove_assignments);
}

#[test]
fn addition_deadline_capture_precedes_input_iteration_and_conversion() {
    struct PanicsOnIteration;

    impl IntoIterator for PanicsOnIteration {
        type Item = TopicPartition;
        type IntoIter = std::iter::Empty<TopicPartition>;

        fn into_iter(self) -> Self::IntoIter {
            panic!("deadline capture must precede input iteration")
        }
    }

    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("start client: {error}"));
    let mut consumer = client
        .assigned_consumer()
        .build()
        .unwrap_or_else(|error| panic!("claim assigned consumer: {error}"));
    let invalid = TopicPartition::new("", -1).start_at(StartPosition::Offset(-1));

    let empty_error = consumer
        .try_add_assignments(std::iter::empty::<TopicPartition>(), Duration::MAX)
        .err()
        .unwrap_or_else(|| panic!("unrepresentable deadline must precede an empty delta"));
    let iteration_error = consumer
        .try_add_assignments(PanicsOnIteration, Duration::MAX)
        .err()
        .unwrap_or_else(|| panic!("unrepresentable deadline must precede input iteration"));
    let error = consumer
        .try_add_assignments([invalid], Duration::MAX)
        .err()
        .unwrap_or_else(|| panic!("unrepresentable deadline must win"));
    assert_eq!(empty_error.kind(), ErrorKind::Timeout);
    assert_eq!(iteration_error.kind(), ErrorKind::Timeout);
    assert_eq!(error.kind(), ErrorKind::Timeout);

    consumer
        .try_close()
        .unwrap_or_else(|error| panic!("admit close after rejection: {error}"))
        .wait()
        .unwrap_or_else(|error| panic!("observe close: {error}"));
}

#[test]
fn missing_addition_start_is_configuration_and_preserves_the_handle() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("start client: {error}"));
    let mut consumer = client
        .assigned_consumer()
        .build()
        .unwrap_or_else(|error| panic!("claim assigned consumer: {error}"));

    let error = consumer
        .try_add_assignments([TopicPartition::new("orders", 0)], Duration::from_secs(1))
        .err()
        .unwrap_or_else(|| panic!("missing start must fail"));
    assert_eq!(error.kind(), ErrorKind::Configuration);

    consumer
        .try_close()
        .unwrap_or_else(|error| panic!("admit close after rejection: {error}"))
        .wait()
        .unwrap_or_else(|error| panic!("observe close: {error}"));
}

#[test]
fn empty_deltas_are_inert_without_inventing_an_assignment() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("start client: {error}"));
    let mut consumer = client
        .assigned_consumer()
        .build()
        .unwrap_or_else(|error| panic!("claim assigned consumer: {error}"));

    consumer
        .try_add_assignments(std::iter::empty::<TopicPartition>(), Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("accept empty addition: {error}"));
    consumer
        .try_remove_assignments(std::iter::empty::<TopicPartition>())
        .unwrap_or_else(|error| panic!("accept empty removal: {error}"));

    let removal = consumer
        .try_remove_assignments([
            TopicPartition::new("orders", 0).start_at(StartPosition::Offset(-1))
        ])
        .err()
        .unwrap_or_else(|| panic!("nonempty removal requires an assignment"));
    assert_eq!(removal.kind(), ErrorKind::State);

    let error = consumer
        .try_pause(&TopicPartition::new("orders", 0))
        .err()
        .unwrap_or_else(|| panic!("empty deltas must not invent an assignment"));
    assert_eq!(error.kind(), ErrorKind::State);

    consumer
        .try_close()
        .unwrap_or_else(|error| panic!("admit close after empty deltas: {error}"))
        .wait()
        .unwrap_or_else(|error| panic!("observe close: {error}"));
}
