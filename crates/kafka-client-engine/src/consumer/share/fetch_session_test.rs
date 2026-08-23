//! Prepared broker-local share-session ownership evidence.

use std::{sync::Arc, time::Duration};

use kafka_client_core::{
    ByteCount, GroupAssignmentPartition, GroupId, MemberId, PartitionIndex, ShareAcquisitionPolicy,
    ShareFetchBrokerId, ShareFetchSessionEpoch, ShareFetchSessionFence, ShareFetchSessionPhase,
    ShareGroupMemberEpoch, TopicId,
};

use crate::{
    clock::MonotonicClock,
    protocol::consumer::share_fetch::{ShareFetchRequestSettings, ShareFetchResponseLimits},
    protocol::fetch::FetchDecodeLimits,
};

use super::{
    catalog::{ShareMembershipCatalog, ShareTopicIdentity},
    fetch_plan::ShareBrokerSessionPlan,
    fetch_session::{ShareFetchSessionOwner, ShareFetchSessionOwnerError},
    fetch_session_set::ShareFetchSessionConfig,
};

#[test]
fn initial_preparation_keeps_one_session_and_deadline_owner() {
    let clock = MonotonicClock::new();
    let initial = capture(&clock);
    let mut owner = ShareFetchSessionOwner::try_open(plan(), fence(), config(), initial)
        .unwrap_or_else(|error| panic!("open session: {error:?}"));
    assert_eq!(owner.machine().phase(), ShareFetchSessionPhase::InFlight);
    assert_eq!(
        owner.response_limits(),
        ShareFetchResponseLimits::new(32, 1_024)
    );

    let prepared = owner
        .take_prepared()
        .unwrap_or_else(|| panic!("initial request"));
    let (attempt, request, capture) = prepared.into_parts();
    assert_eq!(attempt.deadline(), initial.deadline());
    assert_eq!(capture, initial);
    let (request, correlation) = request.into_parts();
    assert_eq!(request.share_session_epoch, 0);
    assert_eq!(request.topics.len(), 1);
    assert!(correlation.contains([7; 16], 0));

    assert_eq!(attempt.fence(), owner.machine().fence());
}

#[test]
fn broker_mismatch_and_overlapping_preparation_fail_before_second_owner() {
    let clock = MonotonicClock::new();
    let error = ShareFetchSessionOwner::try_open(
        plan(),
        ShareFetchSessionFence::new(
            broker(2),
            group(),
            member(),
            epoch(),
            ShareFetchSessionEpoch::initial(),
        ),
        config(),
        capture(&clock),
    )
    .err();
    assert_eq!(error, Some(ShareFetchSessionOwnerError::BrokerMismatch));

    let mut owner = ShareFetchSessionOwner::try_open(plan(), fence(), config(), capture(&clock))
        .unwrap_or_else(|error| panic!("open session: {error:?}"));
    assert_eq!(
        owner.prepare_next(capture(&clock)),
        Err(ShareFetchSessionOwnerError::Occupied)
    );
    let prepared = owner
        .take_prepared()
        .unwrap_or_else(|| panic!("prepared owner"));
    owner
        .settle_unsubmitted(prepared)
        .unwrap_or_else(|error| panic!("settle unsubmitted: {error:?}"));
    assert_eq!(owner.machine().phase(), ShareFetchSessionPhase::Ready);
    let replacement_capture = capture(&clock);
    owner
        .prepare_next(replacement_capture)
        .unwrap_or_else(|error| panic!("replacement: {error:?}"));
    let replacement = owner
        .take_prepared()
        .unwrap_or_else(|| panic!("replacement owner"));
    let (attempt, request, capture) = replacement.into_parts();
    assert_eq!(attempt.deadline(), replacement_capture.deadline());
    assert_eq!(capture, replacement_capture);
    assert_eq!(request.into_parts().0.share_session_epoch, 0);
}

fn plan() -> ShareBrokerSessionPlan {
    ShareBrokerSessionPlan::try_initial(
        &catalog(),
        broker(1),
        &[GroupAssignmentPartition::new(
            TopicId::from_raw(1),
            PartitionIndex::from_raw(0),
        )],
    )
    .unwrap_or_else(|error| panic!("plan: {error:?}"))
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

fn fence() -> ShareFetchSessionFence {
    ShareFetchSessionFence::new(
        broker(1),
        group(),
        member(),
        epoch(),
        ShareFetchSessionEpoch::initial(),
    )
}

fn policy() -> ShareAcquisitionPolicy {
    ShareAcquisitionPolicy::try_new(8, 32, ByteCount::new(1_024))
        .unwrap_or_else(|error| panic!("policy: {error:?}"))
}

fn config() -> ShareFetchSessionConfig {
    ShareFetchSessionConfig::new(
        Arc::from("workers"),
        Arc::from("member-a"),
        policy(),
        settings(),
        ShareFetchResponseLimits::new(32, 1_024),
        FetchDecodeLimits::default(),
    )
}

const fn settings() -> ShareFetchRequestSettings {
    ShareFetchRequestSettings {
        max_wait_ms: 500,
        min_bytes: 1,
        max_bytes: 1_024,
        max_records: 32,
        batch_size: 8,
    }
}

fn capture(clock: &MonotonicClock) -> crate::clock::DeadlineCapture {
    clock
        .capture_deadline_after(Duration::from_secs(30))
        .unwrap_or_else(|error| panic!("deadline: {error:?}"))
}

fn broker(raw: i32) -> ShareFetchBrokerId {
    ShareFetchBrokerId::try_from_raw(raw).unwrap_or_else(|| panic!("broker"))
}

fn group() -> GroupId {
    GroupId::try_from_raw(1).unwrap_or_else(|| panic!("group"))
}

fn member() -> MemberId {
    MemberId::try_from_raw(1).unwrap_or_else(|| panic!("member"))
}

fn epoch() -> ShareGroupMemberEpoch {
    ShareGroupMemberEpoch::try_from_raw(1).unwrap_or_else(|| panic!("epoch"))
}
