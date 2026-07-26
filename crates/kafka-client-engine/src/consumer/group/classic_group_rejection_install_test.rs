//! Join-stage Retain, Fatal, and deferred Rediscover installation scenarios.

use kafka_client_core::{
    ClassicBrokerError, ClassicCoordinatorRecovery, ClassicGroupEffect, ClassicGroupInput,
    ClassicGroupPhase, GroupId, Moment,
};

use super::{
    classic_group_rejection_fault::ClassicRejectionInstallFailure,
    classic_group_rejection_install::install_stage_rejection, classic_group_test_support,
    registry_entry::GroupConsumerEntry,
};

#[test]
fn retained_coordinator_rejection_installs_the_exact_core_schedule() {
    let mut entry = entry();
    let cycle = classic_group_test_support::begin(&mut entry.classic);
    let transition = rejection(&mut entry, cycle, 14);

    install_stage_rejection(&mut entry, transition)
        .unwrap_or_else(|_fault| panic!("retained rejoin installation failed"));

    assert_eq!(
        entry.classic.machine().phase(),
        ClassicGroupPhase::WaitingToRejoin
    );
    assert_eq!(
        entry.rejoin.schedule(),
        entry.classic.machine().pending_rejoin()
    );
}

#[test]
fn unknown_rejection_accepts_only_the_exact_core_fatal() {
    let mut entry = entry();
    let cycle = classic_group_test_support::begin(&mut entry.classic);
    let transition = rejection(&mut entry, cycle, 1_234);

    install_stage_rejection(&mut entry, transition)
        .unwrap_or_else(|_fault| panic!("fatal installation failed"));

    assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Fatal);
    assert!(entry.classic.machine().fatal().is_some());
    assert!(entry.rejoin.is_dormant());
}

#[test]
fn rediscovery_is_retained_without_attempting_driver_invalidation() {
    let mut entry = entry();
    let cycle = classic_group_test_support::begin(&mut entry.classic);
    let transition = rejection(&mut entry, cycle, 15);
    let fault = install_stage_rejection(&mut entry, transition)
        .err()
        .unwrap_or_else(|| panic!("rediscovery must remain deferred"));

    assert_eq!(
        fault.failure(),
        ClassicRejectionInstallFailure::CoordinatorRediscovery
    );
    assert!(matches!(
        &fault.effects()[0],
        Some(ClassicGroupEffect::ArmRejoin {
            coordinator: ClassicCoordinatorRecovery::Rediscover,
            ..
        })
    ));
    assert!(entry.rejoin.is_dormant());
}

fn rejection(
    entry: &mut GroupConsumerEntry,
    cycle: kafka_client_core::MembershipCycle,
    error_code: i16,
) -> kafka_client_core::ClassicGroupTransition {
    entry
        .classic
        .apply(ClassicGroupInput::JoinRejected {
            cycle,
            now: Moment::from_tick(2),
            error: ClassicBrokerError::try_from_code(error_code)
                .unwrap_or_else(|| panic!("nonzero broker error")),
        })
        .unwrap_or_else(|error| panic!("Join rejection failed: {error}"))
}

fn entry() -> GroupConsumerEntry {
    let group_id =
        GroupId::try_from_raw(77).unwrap_or_else(|| panic!("nonzero group identity expected"));
    GroupConsumerEntry::try_new(
        group_id,
        &std::sync::Arc::from("workers"),
        &[std::sync::Arc::from("orders")],
        classic_group_test_support::timing(),
        classic_group_test_support::heartbeat_policy(),
        classic_group_test_support::rejoin_policy(),
    )
    .unwrap_or_else(|error| panic!("entry creation failed: {error:?}"))
}
