//! Session-set construction, exact assignment generation, and rollback evidence.

use std::{sync::Arc, time::Duration};

use kafka_client_core::{
    AssignmentGeneration, ByteCount, GroupAssignmentPartition, GroupId, MemberId, Moment,
    PartitionIndex, ShareAcquisitionPolicy, ShareFetchBrokerId, ShareFetchSessionEpoch,
    ShareFetchSessionFence, ShareGroupMemberEpoch, TopicId,
};

use crate::{
    EngineConfig,
    clock::MonotonicClock,
    driver::{DriverOwner, ShareFetchResolution, ShareFetchRoute, ShareFetchTerminalContext},
    protocol::{
        consumer::share_fetch::{
            ShareFetchAcquiredRange, ShareFetchPartition, ShareFetchRequestSettings,
            ShareFetchResponseLimits, ShareFetchSuccess, ShareFetchTopic,
        },
        fetch::{FetchDecodeLimits, fixture::encoded_data_batch_for_test},
    },
};

use super::{
    super::{
        catalog::{ShareMembershipCatalog, ShareTopicIdentity},
        fetch_plan::ShareBrokerSessionPlan,
        fetch_session::ShareFetchSessionOwner,
        fetch_session_execution::ShareFetchSessionTerminal,
    },
    ShareFetchSessionConfig, ShareFetchSessionSet, ShareFetchSessionSetTurn,
};

#[test]
fn session_set_retains_the_assignment_generation_and_every_broker_owner() {
    let set = session_set(vec![
        owner_for(1, 1, [7; 16], 0),
        owner_for(2, 2, [8; 16], 0),
    ]);
    assert_eq!(set.generation().get(), 1);
    assert_eq!(set.len(), 2);
    set.release_unsubmitted()
        .unwrap_or_else(|error| panic!("release: {error:?}"));
}

pub(super) fn session_set(sessions: Vec<ShareFetchSessionOwner>) -> ShareFetchSessionSet {
    ShareFetchSessionSet {
        generation: AssignmentGeneration::try_from_raw(1).unwrap_or_else(|| panic!("generation")),
        sessions,
        delivery_cursor: 0,
        recovery: None,
    }
}

pub(in crate::consumer::share) fn first_session_mut_for_test(
    set: &mut ShareFetchSessionSet,
) -> &mut ShareFetchSessionOwner {
    set.sessions
        .first_mut()
        .unwrap_or_else(|| panic!("first share session"))
}

pub(super) fn owner_for(
    broker_raw: i32,
    topic_raw: u64,
    topic_uuid: [u8; 16],
    partition_raw: u32,
) -> ShareFetchSessionOwner {
    owner_for_topic(
        broker_raw,
        topic_raw,
        Arc::from(format!("jobs-{topic_raw}")),
        topic_uuid,
        partition_raw,
    )
}

fn owner_for_topic(
    broker_raw: i32,
    topic_raw: u64,
    topic_name: Arc<str>,
    topic_uuid: [u8; 16],
    partition_raw: u32,
) -> ShareFetchSessionOwner {
    let broker = ShareFetchBrokerId::try_from_raw(broker_raw).unwrap_or_else(|| panic!("broker"));
    let topic_id = TopicId::from_raw(topic_raw);
    let catalog = ShareMembershipCatalog::try_new(
        Arc::from("workers"),
        Arc::from("member-a"),
        None,
        vec![ShareTopicIdentity::new(topic_id, topic_name, topic_uuid, 1)],
    )
    .unwrap_or_else(|error| panic!("catalog: {error:?}"));
    let clock = MonotonicClock::new();
    ShareFetchSessionOwner::try_open(
        ShareBrokerSessionPlan::try_initial(
            &catalog,
            broker,
            &[GroupAssignmentPartition::new(
                topic_id,
                PartitionIndex::from_raw(partition_raw),
            )],
        )
        .unwrap_or_else(|error| panic!("plan: {error:?}")),
        ShareFetchSessionFence::new(
            broker,
            GroupId::try_from_raw(1).unwrap_or_else(|| panic!("group")),
            MemberId::try_from_raw(1).unwrap_or_else(|| panic!("member")),
            ShareGroupMemberEpoch::try_from_raw(1).unwrap_or_else(|| panic!("member epoch")),
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

pub(in crate::consumer::share) fn staged_session_set_for_test(offset: i64) -> ShareFetchSessionSet {
    let mut set = session_set(vec![owner_for_topic(1, 1, Arc::from("events"), [7; 16], 0)]);
    stage_success(&mut set.sessions[0], [7; 16], offset);
    let mut driver = driver();
    assert_eq!(
        set.turn(&driver, Moment::from_tick(7)),
        Ok(ShareFetchSessionSetTurn::Progress)
    );
    shutdown(&mut driver);
    set
}

pub(super) fn stage_success(owner: &mut ShareFetchSessionOwner, topic_uuid: [u8; 16], offset: i64) {
    let (attempt, request, capture) = owner
        .take_prepared()
        .unwrap_or_else(|| panic!("prepared attempt"))
        .into_parts();
    drop(request);
    owner.terminal = Some(ShareFetchSessionTerminal {
        attempt,
        resolution: ShareFetchResolution::Succeeded(ShareFetchSuccess {
            throttle_time_ms: 7,
            acquisition_lock_timeout_ms: Some(30_000),
            topics: vec![ShareFetchTopic {
                topic_id: topic_uuid,
                partitions: vec![ShareFetchPartition {
                    partition: 0,
                    rejection: None,
                    records: encoded_data_batch_for_test(offset),
                    acquired: vec![ShareFetchAcquiredRange {
                        first_offset: offset,
                        last_offset: offset,
                        delivery_count: 1,
                    }],
                }],
            }],
            endpoints: Vec::new(),
            retained_records: 1,
            retained_bytes: 5,
        }),
        route: ShareFetchRoute::without_token_for_test(attempt.fence().broker_id()),
        context: ShareFetchTerminalContext {
            broker_id: attempt.fence().broker_id(),
            submitted_at: Moment::from_tick(5),
        },
        capture,
    });
}

pub(super) fn driver() -> DriverOwner {
    DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver: {error}"))
}

pub(super) fn shutdown(driver: &mut DriverOwner) {
    driver
        .shutdown_with_turn_limit(32, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("shutdown: {error}"));
}
