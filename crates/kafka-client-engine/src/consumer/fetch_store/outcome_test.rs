//! Closed staged-outcome classification evidence for the Fetch delivery store.

use super::super::fetch_store_test::{empty_outcome, fences, offset, reserve_parts};
use super::{FetchDeliveryStore, FetchStageKind};

#[test]
fn empty_fetch_is_classified_without_becoming_application_delivery() {
    let [fence, _] = fences();
    let mut store = FetchDeliveryStore::new(1, 4_096);
    let (proof, output) = reserve_parts(&mut store, fence, 4_096);
    let kind = store
        .stage(proof, empty_outcome(output))
        .unwrap_or_else(|(error, _)| panic!("stage empty Fetch: {error:?}"));

    assert_eq!(kind, FetchStageKind::Empty(offset(10), 7_000_000));
    assert!(
        store
            .take_ready()
            .unwrap_or_else(|error| panic!("inspect delivery: {error:?}"))
            .is_none()
    );
    store
        .discard_non_delivery(fence)
        .unwrap_or_else(|error| panic!("discard empty Fetch: {error:?}"));
}
