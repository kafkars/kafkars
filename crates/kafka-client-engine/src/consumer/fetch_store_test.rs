//! Direct-consumer Fetch reservation, authorization, and reclamation scenarios.

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine, AssignedPartition,
    AssignedTopicPartition, Deadline, FetchFence, Moment, NextFetchOffset, PartitionIndex,
    StartPosition, TopicId,
};

use crate::protocol::fetch::{
    FetchBrokerLevel, FetchOutputReservation, RetainedFetchOutcome, encoded_data_batch_for_test,
    retained_broker_failure_for_test, retained_success_for_test,
};

use super::fetch_store::{FetchDeliveryStore, FetchStageKind, FetchStageProof, FetchStoreFailure};

const TOPIC: &str = "events";
const PARTITION: u32 = 3;
const OFFSET: i64 = 10;

#[test]
fn count_and_bytes_are_reserved_atomically_before_driver_admission() {
    let [first, second] = fences();
    let mut store = FetchDeliveryStore::new(2, 100);
    let reservation = store
        .try_reserve(first, 60)
        .unwrap_or_else(|error| panic!("first reservation: {error:?}"));

    assert_eq!(
        store.try_reserve(first, 1).err(),
        Some(FetchStoreFailure::DuplicateFence)
    );
    assert_eq!(
        store.try_reserve(second, 50).err(),
        Some(FetchStoreFailure::ByteCapacity)
    );
    assert_eq!(store.retained(), (1, 60));

    let (proof, output) = reservation.into_protocol_parts();
    store
        .rollback(proof, output)
        .unwrap_or_else(|(error, _)| panic!("intact rollback: {error:?}"));
    assert_eq!(store.retained(), (0, 0));

    let mut count_store = FetchDeliveryStore::new(1, 100);
    let held = count_store
        .try_reserve(first, 1)
        .unwrap_or_else(|error| panic!("count reservation: {error:?}"));
    assert_eq!(
        count_store.try_reserve(second, 1).err(),
        Some(FetchStoreFailure::CountCapacity)
    );
    assert_eq!(count_store.retained(), (1, 1));
    let (proof, output) = held.into_protocol_parts();
    count_store
        .rollback(proof, output)
        .unwrap_or_else(|(error, _)| panic!("count rollback: {error:?}"));
}

#[test]
fn broker_batch_reservation_is_all_or_nothing() {
    let [first, second] = fences();
    let mut store = FetchDeliveryStore::new(2, 100);
    assert_eq!(
        store.try_reserve_batch(&[(first, 60), (second, 50)]).err(),
        Some(FetchStoreFailure::ByteCapacity)
    );
    assert_eq!(store.retained(), (0, 0));

    let reservations = store
        .try_reserve_batch(&[(first, 40), (second, 50)])
        .unwrap_or_else(|error| panic!("batch reservation: {error:?}"));
    assert_eq!(store.retained(), (2, 90));
    for reservation in reservations {
        let (proof, output) = reservation.into_protocol_parts();
        store
            .rollback(proof, output)
            .unwrap_or_else(|(error, _)| panic!("batch rollback: {error:?}"));
    }
    assert_eq!(store.retained(), (0, 0));
}

#[test]
fn deliverable_bytes_stay_hidden_and_charged_until_exact_authorization_and_reclaim() {
    let [first, second] = fences();
    let mut store = FetchDeliveryStore::new(2, 32 * 1024);
    let (first_proof, first_output) = reserve_parts(&mut store, first, 16 * 1024);
    let (second_proof, second_output) = reserve_parts(&mut store, second, 16 * 1024);
    let first_outcome = record_outcome(first_output);
    let second_outcome = record_outcome(second_output);
    let exact = first_outcome.retained_bytes();

    assert_eq!(
        store
            .stage(first_proof, first_outcome)
            .unwrap_or_else(|(error, _)| panic!("stage delivery: {error:?}")),
        FetchStageKind::Deliverable(offset(11), 7_000_000)
    );
    store
        .stage(second_proof, second_outcome)
        .unwrap_or_else(|(error, _)| panic!("stage second delivery: {error:?}"));
    assert_eq!(store.retained(), (2, exact * 2));
    assert!(
        store
            .take_ready()
            .unwrap_or_else(|error| panic!("hidden delivery: {error:?}"))
            .is_none()
    );
    assert_eq!(
        store.authorize(first, offset(12)),
        Err(FetchStoreFailure::NextOffsetMismatch)
    );
    store
        .authorize(second, offset(11))
        .unwrap_or_else(|error| panic!("authorize second delivery: {error:?}"));
    store
        .authorize(first, offset(11))
        .unwrap_or_else(|error| panic!("authorize exact delivery: {error:?}"));
    let first_delivery = store
        .take_ready()
        .unwrap_or_else(|error| panic!("take first delivery: {error:?}"))
        .unwrap_or_else(|| panic!("first delivery ready"));
    assert_eq!(first_delivery.fence(), second);
    assert_eq!(first_delivery.next_offset(), offset(11));
    assert_eq!(first_delivery.outcome().retained_bytes(), exact);
    let second_delivery = store
        .take_ready()
        .unwrap_or_else(|error| panic!("take second delivery: {error:?}"))
        .unwrap_or_else(|| panic!("second delivery ready"));
    assert_eq!(second_delivery.fence(), first);
    assert_eq!(store.retained(), (2, exact * 2));

    let mut wrong_store = FetchDeliveryStore::new(1, exact);
    let (error, first_delivery) = wrong_store
        .reclaim(first_delivery)
        .err()
        .unwrap_or_else(|| panic!("wrong store must return the intact lease"));
    assert_eq!(error, FetchStoreFailure::UnknownFence);
    store
        .reclaim(first_delivery)
        .unwrap_or_else(|(error, _)| panic!("explicit first reclaim: {error:?}"));
    assert_eq!(store.retained(), (1, exact));
    store
        .reclaim(second_delivery)
        .unwrap_or_else(|(error, _)| panic!("explicit second reclaim: {error:?}"));
    assert_eq!(store.retained(), (0, 0));
}

#[test]
fn empty_and_broker_outcomes_release_without_application_delivery() {
    let [empty_fence, broker_fence] = fences();
    let mut store = FetchDeliveryStore::new(2, 8_192);

    let (empty_proof, empty_output) = reserve_parts(&mut store, empty_fence, 4_096);
    assert_eq!(
        store
            .stage(empty_proof, empty_outcome(empty_output))
            .unwrap_or_else(|(error, _)| panic!("empty stage: {error:?}")),
        FetchStageKind::Empty(offset(10), 7_000_000)
    );
    store
        .discard_non_delivery(empty_fence)
        .unwrap_or_else(|error| panic!("discard empty: {error:?}"));

    let (broker_proof, broker_output) = reserve_parts(&mut store, broker_fence, 4_096);
    let kind = store
        .stage(broker_proof, broker_failure_outcome(6, broker_output))
        .unwrap_or_else(|(error, _)| panic!("broker stage: {error:?}"));
    assert!(matches!(
        kind,
        FetchStageKind::BrokerFailure(failure)
            if failure.level() == FetchBrokerLevel::TopLevel && failure.code().get() == 6
    ));
    store
        .discard_non_delivery(broker_fence)
        .unwrap_or_else(|error| panic!("discard broker failure: {error:?}"));
    assert_eq!(store.retained(), (0, 0));
}

#[test]
fn equal_sized_cross_wired_stage_returns_the_intact_proof_and_outcome() {
    let [first, second] = fences();
    let mut store = FetchDeliveryStore::new(2, 8_192);
    let (first_proof, first_output) = reserve_parts(&mut store, first, 4_096);
    let (second_proof, second_output) = reserve_parts(&mut store, second, 4_096);
    let first_outcome = empty_outcome(first_output);

    let (error, (second_proof, first_outcome)) = store
        .stage(second_proof, first_outcome)
        .err()
        .unwrap_or_else(|| panic!("cross-wired provenance must fail"));
    assert_eq!(error, FetchStoreFailure::ReservationMismatch);
    assert_eq!(first_outcome.reserved_bytes(), 4_096);
    store
        .stage(first_proof, first_outcome)
        .unwrap_or_else(|(error, _)| panic!("returned outcome remains intact: {error:?}"));
    store
        .rollback(second_proof, second_output)
        .unwrap_or_else(|(error, _)| panic!("returned proof remains intact: {error:?}"));
    store
        .discard_non_delivery(first)
        .unwrap_or_else(|error| panic!("discard staged first outcome: {error:?}"));
    assert_eq!(store.retained(), (0, 0));
}

pub(super) fn fences() -> [FetchFence; 2] {
    let partitions = [PARTITION, PARTITION + 1]
        .into_iter()
        .map(|partition| {
            AssignedPartition::new(
                AssignedTopicPartition::new(
                    TopicId::from_raw(1),
                    PartitionIndex::from_raw(partition),
                ),
                StartPosition::Offset(offset(OFFSET)),
            )
        })
        .collect();
    let mut machine = AssignedConsumerMachine::new();
    let transition = machine
        .apply(AssignedConsumerInput::Assign {
            partitions,
            now: Moment::from_tick(0),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("direct assignment: {error}"));
    let mut fences = transition
        .effects()
        .iter()
        .filter_map(|effect| match effect {
            AssignedConsumerEffect::FetchReady { fence, .. } => Some(*fence),
            _ => None,
        });
    [
        fences.next().unwrap_or_else(|| panic!("first Fetch fence")),
        fences
            .next()
            .unwrap_or_else(|| panic!("second Fetch fence")),
    ]
}

pub(super) fn reserve_parts(
    store: &mut FetchDeliveryStore,
    fence: FetchFence,
    bytes: usize,
) -> (FetchStageProof, FetchOutputReservation) {
    store
        .try_reserve(fence, bytes)
        .unwrap_or_else(|error| panic!("reserve Fetch output: {error:?}"))
        .into_protocol_parts()
}

pub(super) fn empty_outcome(reservation: FetchOutputReservation) -> RetainedFetchOutcome {
    retained_success_for_test(TOPIC, PARTITION, OFFSET, None, reservation)
}

pub(super) fn record_outcome(reservation: FetchOutputReservation) -> RetainedFetchOutcome {
    retained_success_for_test(
        TOPIC,
        PARTITION,
        OFFSET,
        Some(encoded_data_batch_for_test(OFFSET)),
        reservation,
    )
}

fn broker_failure_outcome(
    error_code: i16,
    reservation: FetchOutputReservation,
) -> RetainedFetchOutcome {
    retained_broker_failure_for_test(TOPIC, PARTITION, OFFSET, error_code, reservation)
}

pub(super) fn offset(value: i64) -> NextFetchOffset {
    NextFetchOffset::try_from_raw(value).unwrap_or_else(|| panic!("nonnegative test offset"))
}
