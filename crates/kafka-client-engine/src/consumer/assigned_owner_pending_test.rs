//! Pending-work deadline and backpressure scenarios.

use std::time::Duration;

use kafka_client_core::{AssignedConsumerEffect, StartPosition};

use super::{
    assigned_owner_effect::FrontEffect,
    assigned_owner_pending::PendingAttempt,
    assigned_owner_test::{driver, input, owner, shutdown},
};

#[test]
fn elapsed_pending_position_settles_before_driver_capacity() {
    let mut owner = owner(1);
    owner
        .replace_assignment(
            vec![input("orders", 0, StartPosition::Beginning)],
            Duration::from_secs(30),
        )
        .unwrap_or_else(|error| panic!("assign: {error:?}"));
    assert_eq!(owner.interpret_front_effect(), FrontEffect::Interpreted);
    let now =
        kafka_client_core::Moment::from_tick(owner.pending_positions[0].deadline.core().tick());
    let mut driver = driver();

    assert_eq!(
        owner.submit_position(&driver, now),
        PendingAttempt::Progressed
    );
    assert!(matches!(
        owner.effects.front(),
        Some(AssignedConsumerEffect::PositionResolutionFailed { .. })
    ));
    assert!(owner.pending_positions.is_empty());
    shutdown(&mut driver);
}
