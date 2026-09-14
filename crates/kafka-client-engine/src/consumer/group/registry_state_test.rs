//! Startup-failure observations retain classic and consumer protocol terminals.

use kafka_client_core::{ClassicBrokerError, ClassicGroupInput, Moment};

use crate::consumer::GroupConsumerStartupFailureKind;
use crate::consumer::group::{
    classic_group_rejection_install::install_stage_rejection, classic_group_test_support,
    registry_test_support,
};

#[test]
fn classic_startup_fatal_preserves_the_exact_broker_code() {
    let mut registry = registry_test_support::started_registry();
    let group_id = registry_test_support::register(&mut registry, "workers");
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("registered group"));
    let cycle = classic_group_test_support::begin(&mut entry.classic);
    let transition = entry
        .classic
        .apply(ClassicGroupInput::JoinRejected {
            cycle,
            now: Moment::from_tick(2),
            error: ClassicBrokerError::try_from_code(30)
                .unwrap_or_else(|| panic!("nonzero broker error")),
        })
        .unwrap_or_else(|error| panic!("join rejection: {error}"));
    install_stage_rejection(entry, transition)
        .unwrap_or_else(|_fault| panic!("fatal installation"));

    assert_eq!(
        registry.consumer_group_startup_failure(group_id),
        Ok(Some(GroupConsumerStartupFailureKind::Broker(30)))
    );
}
