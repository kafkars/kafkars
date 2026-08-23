//! Session-set scheduling, acknowledgement priority, lock expiry, and release evidence.

use std::time::Duration;

use kafka_client_core::Moment;

use crate::clock::MonotonicClock;

use super::{
    super::{
        fetch_acknowledgement_execution::ShareAcknowledgementExecutionOutcome,
        fetch_acknowledgement_test::delivered_acknowledgement,
    },
    ShareFetchSessionSetTurn,
    owner_test::{driver, owner_for, session_set, shutdown, stage_success},
};

#[test]
fn scheduler_settles_acknowledgement_before_resuming_fetch() {
    let (mut owner, acknowledgement) = delivered_acknowledgement();
    let clock = MonotonicClock::new();
    let capture = clock
        .capture_deadline_after(Duration::from_secs(30))
        .unwrap_or_else(|error| panic!("deadline: {error:?}"));
    owner
        .prepare_acknowledgement(acknowledgement, capture, capture.now())
        .unwrap_or_else(|failure| panic!("prepare acknowledgement: {:?}", failure.kind));
    let mut set = session_set(vec![owner]);
    let mut driver = driver();

    assert_eq!(
        set.turn(&driver, Moment::from_tick(0)),
        Ok(ShareFetchSessionSetTurn::Progress)
    );
    shutdown(&mut driver);
    assert_eq!(
        set.turn(&driver, Moment::from_tick(1)),
        Ok(ShareFetchSessionSetTurn::Progress)
    );
    assert_eq!(
        set.turn(&driver, Moment::from_tick(1)),
        Ok(ShareFetchSessionSetTurn::Progress)
    );
    assert!(matches!(
        set.sessions[0].acknowledgement_outcome,
        Some(ShareAcknowledgementExecutionOutcome::Failed { .. })
    ));
    assert_eq!(
        set.turn(&driver, Moment::from_tick(1)),
        Ok(ShareFetchSessionSetTurn::Blocked)
    );
}

#[test]
fn scheduler_reclaims_abandoned_locks_before_preparing_the_next_fetch() {
    let mut set = session_set(vec![owner_for(1, 1, [7; 16], 0)]);
    stage_success(&mut set.sessions[0], [7; 16], 10);
    let mut driver = driver();
    assert_eq!(
        set.turn(&driver, Moment::from_tick(7)),
        Ok(ShareFetchSessionSetTurn::Progress)
    );
    let delivery = set
        .take_delivery(Moment::from_tick(8))
        .unwrap_or_else(|error| panic!("take delivery: {error:?}"))
        .unwrap_or_else(|| panic!("delivery"));
    let lock_deadline = delivery.acquisitions()[0].range().lock_deadline();
    set.reclaim_delivery(delivery)
        .unwrap_or_else(|error| panic!("reclaim delivery: {:?}", error.into_delivery().fence()));
    assert_eq!(
        set.turn(&driver, Moment::from_tick(9)),
        Ok(ShareFetchSessionSetTurn::Blocked)
    );
    assert_eq!(
        set.turn(&driver, Moment::from_tick(lock_deadline.tick())),
        Ok(ShareFetchSessionSetTurn::Progress)
    );
    assert_eq!(
        set.turn(&driver, Moment::from_tick(lock_deadline.tick())),
        Ok(ShareFetchSessionSetTurn::NeedsPreparation(0))
    );
    set.release_unsubmitted()
        .unwrap_or_else(|error| panic!("release: {error:?}"));
    shutdown(&mut driver);
}
