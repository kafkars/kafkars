//! Exact authorization and reclamation evidence for Fetch delivery leases.

use super::super::fetch_store_test::{fences, offset, record_outcome, reserve_parts};
use super::FetchDeliveryStore;

#[test]
fn authorized_delivery_retains_its_exact_charge_until_reclaim() {
    let [fence, _] = fences();
    let mut store = FetchDeliveryStore::new(1, 16 * 1024);
    let (proof, output) = reserve_parts(&mut store, fence, 16 * 1024);
    let outcome = record_outcome(output);
    let retained = outcome.retained_bytes();
    store
        .stage(proof, outcome)
        .unwrap_or_else(|(error, _)| panic!("stage delivery: {error:?}"));

    store
        .authorize(fence, offset(11))
        .unwrap_or_else(|error| panic!("authorize delivery: {error:?}"));
    let delivery = store
        .take_ready()
        .unwrap_or_else(|error| panic!("take delivery: {error:?}"))
        .unwrap_or_else(|| panic!("authorized delivery must be ready"));
    assert_eq!(store.retained(), (1, retained));

    store
        .reclaim(delivery)
        .unwrap_or_else(|(error, _)| panic!("reclaim delivery: {error:?}"));
    assert_eq!(store.retained(), (0, 0));
}
