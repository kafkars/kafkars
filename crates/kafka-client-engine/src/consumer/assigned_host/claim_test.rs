//! Claim-slot scenarios for exact transfer and retained shutdown fencing.

use std::{
    sync::{Arc, Barrier},
    thread,
    time::Duration,
};

use super::{
    claim::{AssignedConsumerClaimError, AssignedConsumerClaimSlot},
    result::AssignedConsumerPortError,
    shard_test::setup,
};
use crate::{Engine, EngineConfig};

#[test]
fn one_claim_transfers_the_port_and_rejects_every_later_claim() {
    let (_owner, port, _wake) = setup();
    let (slot, _closer) = AssignedConsumerClaimSlot::create_for_engine(port);
    let lifetime: Arc<dyn Send + Sync> = Arc::new(());

    let handle = slot
        .claim(Arc::clone(&lifetime))
        .unwrap_or_else(|error| panic!("first claim: {error}"));
    assert_eq!(
        slot.claim(lifetime).err(),
        Some(AssignedConsumerClaimError::AlreadyClaimed)
    );
    assert!(handle.port.begin_close().is_ok());
}

#[test]
fn retained_closer_fences_admission_after_the_port_transfers() {
    let (_owner, port, _wake) = setup();
    let (slot, closer) = AssignedConsumerClaimSlot::create_for_engine(port);
    let lifetime: Arc<dyn Send + Sync> = Arc::new(());
    let handle = slot
        .claim(lifetime)
        .unwrap_or_else(|error| panic!("claim: {error}"));

    closer
        .close()
        .unwrap_or_else(|error| panic!("close admission: {error:?}"));

    assert!(matches!(
        handle.port.begin_close(),
        Err(AssignedConsumerPortError::Closed)
    ));
}

#[test]
fn claim_is_a_public_send_capability() {
    fn require_claim(
        _claim: fn(
            &Engine,
        )
            -> Result<super::handle::AssignedConsumerHandle, AssignedConsumerClaimError>,
    ) {
    }
    fn require_send<T: Send>() {}

    require_claim(Engine::claim_assigned_consumer);
    require_send::<super::handle::AssignedConsumerHandle>();
}

#[test]
fn failed_second_engine_claim_leaves_the_live_handle_operational() {
    let engine = start();
    let handle = engine
        .claim_assigned_consumer()
        .unwrap_or_else(|error| panic!("first claim: {error}"));

    assert_eq!(
        engine.claim_assigned_consumer().err(),
        Some(AssignedConsumerClaimError::AlreadyClaimed)
    );
    assert!(handle.port.begin_close().is_ok());
    assert!(engine.shutdown().is_ok());
}

#[test]
fn clone_shared_engine_contention_selects_exactly_one_claim() {
    let engine = start();
    let first = engine.clone();
    let second = engine.clone();
    let barrier = Arc::new(Barrier::new(2));
    let first_barrier = Arc::clone(&barrier);
    let second_barrier = Arc::clone(&barrier);
    let (first_result, second_result) = thread::scope(|scope| {
        let first = scope.spawn(move || {
            first_barrier.wait();
            first.claim_assigned_consumer()
        });
        let second = scope.spawn(move || {
            second_barrier.wait();
            second.claim_assigned_consumer()
        });
        (
            first
                .join()
                .unwrap_or_else(|_panic| panic!("first claimant panicked")),
            second
                .join()
                .unwrap_or_else(|_panic| panic!("second claimant panicked")),
        )
    });

    let (winner, rejection) = match (first_result, second_result) {
        (Ok(handle), Err(error)) | (Err(error), Ok(handle)) => (handle, error),
        (Ok(_first), Ok(_second)) => panic!("two engine clones claimed one consumer"),
        (Err(first), Err(second)) => panic!("both claims failed: {first}, {second}"),
    };
    assert_eq!(rejection, AssignedConsumerClaimError::AlreadyClaimed);
    drop(winner);
    assert!(engine.shutdown().is_ok());
}

fn start() -> Engine {
    Engine::start(
        EngineConfig::new(vec!["192.0.2.1:9092".to_owned()])
            .with_delivery_timeout(Duration::from_millis(80)),
    )
    .unwrap_or_else(|error| panic!("engine should start: {error}"))
}
