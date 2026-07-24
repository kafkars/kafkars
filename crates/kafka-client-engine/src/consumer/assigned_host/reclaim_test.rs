//! Reclaim rejection retains the exact delivery until owner transfer.

use std::time::Duration;

use kafka_client_core::StartPosition;

use super::super::{
    assigned_owner_close_test::install_pending_ready, assigned_owner_effect::FrontEffect,
    assigned_owner_test::input,
};
use super::{shard::AssignedConsumerShardLockError, shard_test::setup};

#[test]
fn contended_reclaim_returns_the_exact_delivery_before_transfer() {
    let (owner, port, wake) = setup();
    let _accepted = port
        .replace_assignment(
            vec![input("orders", 0, StartPosition::Offset(offset(10)))],
            Duration::from_secs(1),
        )
        .unwrap_or_else(|error| panic!("assign: {error:?}"));
    owner
        .try_with_owner(|assigned| {
            assert_eq!(assigned.interpret_front_effect(), FrontEffect::Interpreted);
            install_pending_ready(assigned, 10);
        })
        .unwrap_or_else(|error| panic!("prepare delivery: {error:?}"));
    let delivery = port
        .take_delivery()
        .unwrap_or_else(|error| panic!("take delivery: {error:?}"))
        .unwrap_or_else(|| panic!("ready delivery"));
    let fence = delivery.fence();
    let guard = owner.lock_for_test();

    let rejected = port
        .reclaim_delivery(delivery)
        .err()
        .unwrap_or_else(|| panic!("held slot must reject reclaim"));
    assert_eq!(rejected.reason(), AssignedConsumerShardLockError::Contended);
    let delivery = rejected.into_delivery();
    assert_eq!(delivery.fence(), fence);
    drop(guard);

    let accepted = port
        .reclaim_delivery(delivery)
        .unwrap_or_else(|_rejection| panic!("released slot must reclaim"));
    assert_eq!(accepted.into_value(), Ok(()));
    assert_eq!(wake.count(), 2);
}

fn offset(value: i64) -> kafka_client_core::NextFetchOffset {
    kafka_client_core::NextFetchOffset::try_from_raw(value)
        .unwrap_or_else(|| panic!("nonnegative offset"))
}
