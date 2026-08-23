//! Session-set scheduling, lock expiry, throttle, and release evidence.

use kafka_client_core::Moment;

use super::{
    ShareFetchSessionSetTurn,
    owner_test::{driver, owner_for, session_set, shutdown, stage_success},
};

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
