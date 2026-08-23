//! Tracked call handoff and conservative session-loss evidence.

use std::{sync::Arc, time::Duration};

use kafka_client_core::{
    ByteCount, DeliveryStatus, GroupAssignmentPartition, GroupId, MemberId, PartitionIndex,
    ShareAcquisitionPolicy, ShareFetchBrokerId, ShareFetchSessionEpoch, ShareFetchSessionFence,
    ShareFetchSessionPhase, ShareGroupMemberEpoch, TopicId,
};

use crate::{
    EngineConfig,
    clock::MonotonicClock,
    driver::{DriverOwner, ShareFetchResolution},
    protocol::consumer::share_fetch::{ShareFetchRequestSettings, ShareFetchResponseLimits},
    protocol::fetch::FetchDecodeLimits,
};

use super::{
    catalog::{ShareMembershipCatalog, ShareTopicIdentity},
    fetch_plan::ShareBrokerSessionPlan,
    fetch_session::ShareFetchSessionOwner,
    fetch_session_set::ShareFetchSessionConfig,
};

#[test]
fn accepted_call_preserves_the_driver_terminal_before_session_policy() {
    let mut driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver: {error}"));
    let mut owner = owner();
    owner
        .submit_prepared(&driver, kafka_client_core::Moment::from_tick(0))
        .unwrap_or_else(|error| panic!("submit: {error:?}"));
    assert!(owner.has_active_call());
    driver
        .shutdown_with_turn_limit(32, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("shutdown: {error}"));
    assert_eq!(
        owner.poll_execution(),
        Ok(super::fetch_session_execution::ShareFetchExecutionPoll::Terminal)
    );
    let terminal = owner
        .take_terminal()
        .unwrap_or_else(|| panic!("retained terminal"));
    let ShareFetchResolution::Failed { delivery, .. } = terminal.resolution else {
        panic!("shutdown must retain one failure terminal");
    };
    assert_eq!(delivery, DeliveryStatus::NotSent);
    terminal.route.accept();
    owner
        .settle_attempt_failure(terminal.attempt, delivery)
        .unwrap_or_else(|error| panic!("terminal settlement: {error:?}"));
    assert_eq!(owner.machine().phase(), ShareFetchSessionPhase::Ready);
}

#[test]
fn post_driver_recovery_conservatively_loses_the_accepted_session() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver: {error}"));
    let mut owner = owner();
    owner
        .submit_prepared(&driver, kafka_client_core::Moment::from_tick(0))
        .unwrap_or_else(|error| panic!("submit: {error:?}"));
    drop(driver);
    assert_eq!(owner.recover_call_after_driver_shutdown(), Ok(true));
    assert_eq!(owner.machine().phase(), ShareFetchSessionPhase::Lost);
    assert_eq!(owner.recover_call_after_driver_shutdown(), Ok(false));
}

fn owner() -> ShareFetchSessionOwner {
    let clock = MonotonicClock::new();
    ShareFetchSessionOwner::try_open(
        ShareBrokerSessionPlan::try_initial(
            &catalog(),
            broker(),
            &[GroupAssignmentPartition::new(
                TopicId::from_raw(1),
                PartitionIndex::from_raw(0),
            )],
        )
        .unwrap_or_else(|error| panic!("plan: {error:?}")),
        ShareFetchSessionFence::new(
            broker(),
            GroupId::try_from_raw(1).unwrap_or_else(|| panic!("group")),
            MemberId::try_from_raw(1).unwrap_or_else(|| panic!("member")),
            ShareGroupMemberEpoch::try_from_raw(1).unwrap_or_else(|| panic!("epoch")),
            ShareFetchSessionEpoch::initial(),
        ),
        ShareFetchSessionConfig::new(
            Arc::from("workers"),
            Arc::from("member-a"),
            ShareAcquisitionPolicy::try_new(8, 32, ByteCount::new(1_024))
                .unwrap_or_else(|error| panic!("policy: {error:?}")),
            ShareFetchRequestSettings {
                max_wait_ms: 500,
                min_bytes: 1,
                max_bytes: 1_024,
                max_records: 32,
                batch_size: 8,
            },
            ShareFetchResponseLimits::new(32, 1_024),
            FetchDecodeLimits::default(),
        ),
        clock
            .capture_deadline_after(Duration::from_secs(30))
            .unwrap_or_else(|error| panic!("deadline: {error:?}")),
    )
    .unwrap_or_else(|error| panic!("owner: {error:?}"))
}

fn catalog() -> ShareMembershipCatalog {
    ShareMembershipCatalog::try_new(
        Arc::from("workers"),
        Arc::from("member-a"),
        None,
        vec![ShareTopicIdentity::new(
            TopicId::from_raw(1),
            Arc::from("jobs"),
            [7; 16],
            1,
        )],
    )
    .unwrap_or_else(|error| panic!("catalog: {error:?}"))
}

fn broker() -> ShareFetchBrokerId {
    ShareFetchBrokerId::try_from_raw(1).unwrap_or_else(|| panic!("broker"))
}
