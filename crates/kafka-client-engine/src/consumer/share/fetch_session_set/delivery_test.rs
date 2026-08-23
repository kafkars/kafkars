//! Fair multi-session delivery transfer and exact-fence reclaim evidence.

use kafka_client_core::Moment;

use super::{
    ShareFetchSessionSetTurn,
    owner_test::{driver, owner_for, session_set, shutdown, stage_success},
};

#[test]
fn delivery_cursor_visits_each_broker_and_reclaim_returns_to_the_exact_session() {
    let mut set = session_set(vec![
        owner_for(1, 1, [7; 16], 0),
        owner_for(2, 2, [8; 16], 0),
    ]);
    stage_success(&mut set.sessions[0], [7; 16], 10);
    stage_success(&mut set.sessions[1], [8; 16], 20);
    let mut driver = driver();
    assert_eq!(
        set.turn(&driver, Moment::from_tick(7)),
        Ok(ShareFetchSessionSetTurn::Progress)
    );
    assert_eq!(
        set.turn(&driver, Moment::from_tick(7)),
        Ok(ShareFetchSessionSetTurn::Progress)
    );

    let first = set
        .take_delivery(Moment::from_tick(8))
        .unwrap_or_else(|error| panic!("first delivery: {error:?}"))
        .unwrap_or_else(|| panic!("first delivery missing"));
    let second = set
        .take_delivery(Moment::from_tick(8))
        .unwrap_or_else(|error| panic!("second delivery: {error:?}"))
        .unwrap_or_else(|| panic!("second delivery missing"));
    assert_eq!(first.fence().broker_id().get(), 1);
    assert_eq!(second.fence().broker_id().get(), 2);

    set.reclaim_delivery(second)
        .unwrap_or_else(|error| panic!("second reclaim: {:?}", error.into_delivery().fence()));
    set.reclaim_delivery(first)
        .unwrap_or_else(|error| panic!("first reclaim: {:?}", error.into_delivery().fence()));
    assert_eq!(set.abandon_turn(), Ok(ShareFetchSessionSetTurn::Progress));
    assert_eq!(set.abandon_turn(), Ok(ShareFetchSessionSetTurn::Progress));
    assert_eq!(set.abandon_turn(), Ok(ShareFetchSessionSetTurn::Released));
    set.release_unsubmitted()
        .unwrap_or_else(|error| panic!("release: {error:?}"));
    shutdown(&mut driver);
}
