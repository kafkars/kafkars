//! Distinct-store Fetch reservation-domain collision scenarios.

use super::{
    fetch_store::{FetchDeliveryStore, FetchStoreFailure},
    fetch_store_test::{empty_outcome, fences, offset, record_outcome, reserve_parts},
};

#[test]
fn identical_cross_store_stage_is_rejected_with_owners_intact() {
    let [fence, _] = fences();
    let mut first = FetchDeliveryStore::new(1, 4_096);
    let mut second = FetchDeliveryStore::new(1, 4_096);
    let (first_proof, first_output) = reserve_parts(&mut first, fence, 4_096);
    let (second_proof, second_output) = reserve_parts(&mut second, fence, 4_096);
    let first_outcome = empty_outcome(first_output);

    let (failure, (second_proof, first_outcome)) = second
        .stage(second_proof, first_outcome)
        .err()
        .unwrap_or_else(|| panic!("distinct store domain must reject stage"));
    assert_eq!(failure, FetchStoreFailure::ReservationMismatch);
    first
        .stage(first_proof, first_outcome)
        .unwrap_or_else(|(error, _)| panic!("first owner must remain intact: {error:?}"));
    second
        .rollback(second_proof, second_output)
        .unwrap_or_else(|(error, _)| panic!("second proof must remain intact: {error:?}"));
    first
        .discard_non_delivery(fence)
        .unwrap_or_else(|error| panic!("discard first outcome: {error:?}"));
}

#[test]
fn identical_cross_store_rollback_is_rejected_with_tokens_intact() {
    let [fence, _] = fences();
    let mut first = FetchDeliveryStore::new(1, 4_096);
    let mut second = FetchDeliveryStore::new(1, 4_096);
    let (first_proof, first_output) = reserve_parts(&mut first, fence, 4_096);
    let (second_proof, second_output) = reserve_parts(&mut second, fence, 4_096);

    let (failure, (first_proof, second_output)) = second
        .rollback(first_proof, second_output)
        .err()
        .unwrap_or_else(|| panic!("distinct store domain must reject rollback"));
    assert_eq!(failure, FetchStoreFailure::ReservationMismatch);
    first
        .rollback(first_proof, first_output)
        .unwrap_or_else(|(error, _)| panic!("first rollback remains intact: {error:?}"));
    second
        .rollback(second_proof, second_output)
        .unwrap_or_else(|(error, _)| panic!("second rollback remains intact: {error:?}"));
}

#[test]
fn leased_delivery_reclaim_requires_the_matching_store_slot() {
    let [fence, _] = fences();
    let mut first = FetchDeliveryStore::new(1, 16 * 1024);
    let mut second = FetchDeliveryStore::new(1, 16 * 1024);
    let (first_proof, first_output) = reserve_parts(&mut first, fence, 16 * 1024);
    let (second_proof, second_output) = reserve_parts(&mut second, fence, 16 * 1024);
    first
        .stage(first_proof, record_outcome(first_output))
        .unwrap_or_else(|(error, _)| panic!("first stage: {error:?}"));
    second
        .stage(second_proof, record_outcome(second_output))
        .unwrap_or_else(|(error, _)| panic!("second stage: {error:?}"));
    first
        .authorize(fence, offset(11))
        .unwrap_or_else(|error| panic!("first authorize: {error:?}"));
    second
        .authorize(fence, offset(11))
        .unwrap_or_else(|error| panic!("second authorize: {error:?}"));
    let first_delivery = first
        .take_ready()
        .unwrap_or_else(|error| panic!("first ready: {error:?}"))
        .unwrap_or_else(|| panic!("first delivery"));
    let second_delivery = second
        .take_ready()
        .unwrap_or_else(|error| panic!("second ready: {error:?}"))
        .unwrap_or_else(|| panic!("second delivery"));

    let (failure, first_delivery) = second
        .reclaim(first_delivery)
        .err()
        .unwrap_or_else(|| panic!("foreign delivery must remain intact"));
    assert_eq!(failure, FetchStoreFailure::ReservationMismatch);
    first
        .reclaim(first_delivery)
        .unwrap_or_else(|(error, _)| panic!("first reclaim: {error:?}"));
    second
        .reclaim(second_delivery)
        .unwrap_or_else(|(error, _)| panic!("second reclaim: {error:?}"));
    assert_eq!(first.retained(), (0, 0));
    assert_eq!(second.retained(), (0, 0));
}

#[test]
fn stale_discard_removes_only_staged_ownership() {
    let [stale, reserved] = fences();
    let mut store = FetchDeliveryStore::new(2, 32 * 1024);
    let (stale_proof, stale_output) = reserve_parts(&mut store, stale, 16 * 1024);
    let (reserved_proof, reserved_output) = reserve_parts(&mut store, reserved, 16 * 1024);
    store
        .stage(stale_proof, record_outcome(stale_output))
        .unwrap_or_else(|(error, _)| panic!("stage stale delivery: {error:?}"));

    assert_eq!(
        store.discard_stale(reserved),
        Err(FetchStoreFailure::InvalidState)
    );
    store
        .discard_stale(stale)
        .unwrap_or_else(|error| panic!("discard staged delivery: {error:?}"));
    assert_eq!(store.retained(), (1, 16 * 1024));
    store
        .rollback(reserved_proof, reserved_output)
        .unwrap_or_else(|(error, _)| panic!("reserved owner remains intact: {error:?}"));
    assert_eq!(store.retained(), (0, 0));

    let mut live = FetchDeliveryStore::new(1, 16 * 1024);
    let (proof, output) = reserve_parts(&mut live, stale, 16 * 1024);
    live.stage(proof, record_outcome(output))
        .unwrap_or_else(|(error, _)| panic!("stage live delivery: {error:?}"));
    live.authorize(stale, offset(11))
        .unwrap_or_else(|error| panic!("authorize live delivery: {error:?}"));
    assert_eq!(
        live.discard_stale(stale),
        Err(FetchStoreFailure::InvalidState)
    );
    let delivery = live
        .take_ready()
        .unwrap_or_else(|error| panic!("take live delivery: {error:?}"))
        .unwrap_or_else(|| panic!("live delivery"));
    assert_eq!(
        live.discard_stale(stale),
        Err(FetchStoreFailure::InvalidState)
    );
    live.reclaim(delivery)
        .unwrap_or_else(|(error, _)| panic!("reclaim live delivery: {error:?}"));
}
