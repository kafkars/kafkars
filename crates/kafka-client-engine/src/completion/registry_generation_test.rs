//! Generation-fenced completion slot reuse scenarios.

use super::{
    CompletionRegistry, CompletionRegistryError,
    test_support::{finish_reclaims, stop},
};

#[test]
fn stale_publish_after_slot_reuse_is_not_a_duplicate_of_the_live_operation() {
    let Ok(mut registry) = CompletionRegistry::new(1, 1) else {
        panic!("notifier should start");
    };
    let Ok((stale_id, stale_observer)) = registry.reserve() else {
        panic!("first slot should reserve");
    };
    assert_eq!(registry.rollback_reservation(stale_id), Ok(()));
    drop(stale_observer);

    let Ok((live_id, live_observer)) = registry.reserve() else {
        panic!("reused slot should reserve");
    };
    assert_eq!(registry.publish(live_id, 11), Ok(()));
    assert_eq!(
        registry.publish(stale_id, 13),
        Err((CompletionRegistryError::UnknownCompletion, 13))
    );
    assert_eq!(live_observer.wait(), Ok(11));
    assert_eq!(finish_reclaims(&mut registry), Ok(1));
    stop(&mut registry);
}
