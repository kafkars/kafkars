//! Session-epoch, lock-boundary, and retained-delivery settlement evidence.

use std::sync::Arc;

use kafka_client_core::{
    ByteCount, GroupAssignmentPartition, GroupId, MemberId, Moment, PartitionIndex,
    ShareAcquisitionPolicy, ShareFetchBrokerId, ShareFetchSessionEpoch, ShareFetchSessionFence,
    ShareFetchSessionPhase, ShareGroupMemberEpoch, TopicId,
};

use crate::{
    driver::{ShareFetchResolution, ShareFetchRoute, ShareFetchTerminalContext},
    protocol::{
        consumer::share_fetch::{
            ShareFetchAcquiredRange, ShareFetchPartition, ShareFetchRequestSettings,
            ShareFetchResponseLimits, ShareFetchSuccess, ShareFetchTopic,
        },
        fetch::{FetchDecodeLimits, encoded_data_batch_for_test},
    },
};

use super::{
    catalog::{ShareMembershipCatalog, ShareTopicIdentity},
    fetch_plan::ShareBrokerSessionPlan,
    fetch_session::{ShareFetchSessionConfig, ShareFetchSessionOwner},
    fetch_session_execution::ShareFetchSessionTerminal,
    fetch_session_settlement::{ShareFetchSettlementTurn, ShareFetchTerminalSettlementError},
};

#[test]
fn successful_terminal_advances_session_and_stages_exact_delivery() {
    let mut owner = owner();
    stage(&mut owner, success(Some(30_000)));
    assert_eq!(
        owner.settle_terminal(Moment::from_tick(7)),
        Ok(ShareFetchSettlementTurn::Acquired(1))
    );
    assert_eq!(owner.machine().phase(), ShareFetchSessionPhase::Ready);
    assert_eq!(owner.machine().fence().session_epoch().get(), 1);
    assert_eq!(owner.machine().ledger().len(), 1);
    assert_eq!(owner.machine().ledger().retained_records(), 1,);
    assert_eq!(owner.lock_timeout_ms(), Some(30_000));
    let staged = owner
        .take_staged_delivery()
        .unwrap_or_else(|| panic!("staged delivery"));
    assert_eq!(staged.acquisitions, 1);
    assert_eq!(staged.throttle_time_ms, 7);
    assert_eq!(staged.partitions[0].partition.partition().get(), 0);
    assert!(staged.endpoints.is_empty());
    staged.route.accept();
}

#[test]
fn initial_missing_lock_timeout_loses_possibly_sent_session() {
    let mut owner = owner();
    stage(&mut owner, success(None));
    assert_eq!(
        owner.settle_terminal(Moment::from_tick(7)),
        Err(ShareFetchTerminalSettlementError::MissingLockTimeout)
    );
    assert_eq!(owner.machine().phase(), ShareFetchSessionPhase::Lost);
    assert!(owner.take_staged_delivery().is_none());
}

fn stage(owner: &mut ShareFetchSessionOwner, success: ShareFetchSuccess) {
    let attempt = owner
        .machine()
        .in_flight()
        .unwrap_or_else(|| panic!("attempt"));
    owner.terminal = Some(ShareFetchSessionTerminal {
        attempt,
        resolution: ShareFetchResolution::Succeeded(success),
        route: ShareFetchRoute::without_token_for_test(broker()),
        context: ShareFetchTerminalContext {
            broker_id: broker(),
            submitted_at: Moment::from_tick(5),
        },
    });
}

fn success(acquisition_lock_timeout_ms: Option<u32>) -> ShareFetchSuccess {
    ShareFetchSuccess {
        throttle_time_ms: 7,
        acquisition_lock_timeout_ms,
        topics: vec![ShareFetchTopic {
            topic_id: [7; 16],
            partitions: vec![ShareFetchPartition {
                partition: 0,
                rejection: None,
                records: encoded_data_batch_for_test(10),
                acquired: vec![ShareFetchAcquiredRange {
                    first_offset: 10,
                    last_offset: 10,
                    delivery_count: 1,
                }],
            }],
        }],
        endpoints: Vec::new(),
        retained_records: 1,
        retained_bytes: 5,
    }
}

fn owner() -> ShareFetchSessionOwner {
    let clock = crate::clock::MonotonicClock::new();
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
            .capture_deadline_after(std::time::Duration::from_secs(30))
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
