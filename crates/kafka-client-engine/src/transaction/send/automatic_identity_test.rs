//! Expected topic-UUID proof before transactional partition enrollment.

use kafka_client_core::{CompressionPolicy, PartitionIndex};

use super::{
    TransactionSendFailureKind, TransactionSendTerminal,
    automatic_test::{topic_view, topic_view_with_uuid},
    partitioning::TransactionPartitioningFailure,
    test_support::{FakeAggregate, automatic_request_with_expected_uuid},
};

#[test]
fn expected_topic_uuid_is_proven_before_explicit_partition_enrollment() {
    let mut aggregate = FakeAggregate::new();
    let epoch = aggregate.epoch;
    let mut owner = aggregate.send_owner(CompressionPolicy::None);
    let accepted = owner
        .try_send_with(
            &mut aggregate,
            automatic_request_with_expected_uuid(
                epoch,
                "orders",
                Some(PartitionIndex::from_raw(4)),
                [7; 16],
                1_024,
            ),
        )
        .unwrap_or_else(|error| panic!("identity-bound send accepted: {error:?}"));

    owner
        .apply_partitioning_for_test(&topic_view(), &mut aggregate)
        .unwrap_or_else(|error| panic!("matching immutable topic view: {error:?}"));

    assert_eq!(aggregate.captured_partition(), Some(4));
    drop(accepted.into_observer());
    owner
        .recover_with(&mut aggregate)
        .unwrap_or_else(|error| panic!("accepted send recovers: {error:?}"));
    owner
        .publish_terminal_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recovered terminal publishes: {error:?}"));
}

#[test]
fn expected_topic_uuid_mismatch_never_enrolls_or_produces() {
    assert_identity_rejection([8; 16], Some([7; 16]));
}

#[test]
fn missing_topic_uuid_never_enrolls_or_produces() {
    assert_identity_rejection([7; 16], None);
}

fn assert_identity_rejection(expected: [u8; 16], observed: Option<[u8; 16]>) {
    let mut aggregate = FakeAggregate::new();
    let epoch = aggregate.epoch;
    let mut owner = aggregate.send_owner(CompressionPolicy::None);
    let accepted = owner
        .try_send_with(
            &mut aggregate,
            automatic_request_with_expected_uuid(epoch, "orders", None, expected, 1_024),
        )
        .unwrap_or_else(|error| panic!("identity-bound send accepted: {error:?}"));
    let observer = accepted.into_observer();

    owner
        .apply_partitioning_for_test(&topic_view_with_uuid(observed), &mut aggregate)
        .unwrap_or_else(|error| panic!("invalid identity view settles exactly: {error:?}"));
    owner
        .turn_completion()
        .unwrap_or_else(|error| panic!("identity terminal publishes: {error:?}"));

    assert_eq!(aggregate.captured_partition(), None);
    assert!(aggregate.prepared_identities.is_empty());
    assert!(matches!(
        observer.wait(),
        Ok(TransactionSendTerminal::AbortRequired { failure, .. })
            if failure.kind()
                == TransactionSendFailureKind::Partitioning(
                    TransactionPartitioningFailure::TopicIdentityMismatch
                )
                && failure.delivery() == kafka_client_core::DeliveryStatus::NotSent
    ));
}
