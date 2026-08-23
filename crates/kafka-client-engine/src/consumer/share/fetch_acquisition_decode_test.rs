//! `ShareFetch` record-to-acquisition correlation and fail-closed evidence.

use std::sync::Arc;

use kafka_client_core::{
    GroupAssignmentPartition, Moment, PartitionIndex, ShareFetchBrokerId, TopicId,
};

use crate::protocol::{
    consumer::share_fetch::{
        ShareFetchAcquiredRange, ShareFetchPartition, ShareFetchSuccess, ShareFetchTopic,
    },
    fetch::{FetchDecodeLimits, encoded_delivery_batches_for_test},
};

use super::{
    catalog::{ShareMembershipCatalog, ShareTopicIdentity},
    fetch_acquisition_decode::{ShareFetchAcquisitionDecodeError, decode_share_fetch_success},
    fetch_plan::ShareBrokerSessionPlan,
};

#[test]
fn decoded_records_map_to_exact_local_ranges_and_byte_charges() {
    let (_broker, _assignment, plan) = plan().into_parts();
    let decoded = decode_share_fetch_success(
        response(vec![range(10, 11, 1), range(12, 12, 2)]),
        &plan,
        kafka_client_core::Deadline::from_tick(100),
        Moment::from_tick(1),
        FetchDecodeLimits::default(),
    )
    .unwrap_or_else(|error| panic!("decode acquisitions: {error:?}"));
    assert_eq!(decoded.ranges.len(), 2);
    assert_eq!(
        decoded.ranges[0].partition().topic_id(),
        TopicId::from_raw(1)
    );
    assert!(decoded.ranges[0].retained_bytes().get() > 10);
    assert_eq!(decoded.ranges[1].retained_bytes().get(), 6);
    assert_eq!(decoded.ranges[1].delivery_count().get(), 2);
    assert_eq!(decoded.partitions[0].batches.len(), 2);
    assert_eq!(decoded.throttle_time_ms, 7);
    assert_eq!(decoded.acquisition_lock_timeout_ms, Some(30_000));
    assert!(decoded.endpoints.is_empty());
}

#[test]
fn unacquired_records_and_empty_ranges_fail_before_core_ownership() {
    let (_broker, _assignment, plan) = plan().into_parts();
    assert_eq!(
        decode_share_fetch_success(
            response(vec![range(10, 10, 1)]),
            &plan,
            kafka_client_core::Deadline::from_tick(100),
            Moment::from_tick(1),
            FetchDecodeLimits::default(),
        )
        .err(),
        Some(ShareFetchAcquisitionDecodeError::UnacquiredRecord(11))
    );
    assert_eq!(
        decode_share_fetch_success(
            response(vec![range(9, 9, 1), range(10, 12, 1)]),
            &plan,
            kafka_client_core::Deadline::from_tick(100),
            Moment::from_tick(1),
            FetchDecodeLimits::default(),
        )
        .err(),
        Some(ShareFetchAcquisitionDecodeError::EmptyAcquiredRange)
    );
}

fn response(acquired: Vec<ShareFetchAcquiredRange>) -> ShareFetchSuccess {
    ShareFetchSuccess {
        throttle_time_ms: 7,
        acquisition_lock_timeout_ms: Some(30_000),
        topics: vec![ShareFetchTopic {
            topic_id: [7; 16],
            partitions: vec![ShareFetchPartition {
                partition: 0,
                rejection: None,
                records: encoded_delivery_batches_for_test(10),
                acquired,
            }],
        }],
        endpoints: Vec::new(),
        retained_records: 3,
        retained_bytes: 16,
    }
}

const fn range(
    first_offset: i64,
    last_offset: i64,
    delivery_count: i16,
) -> ShareFetchAcquiredRange {
    ShareFetchAcquiredRange {
        first_offset,
        last_offset,
        delivery_count,
    }
}

fn plan() -> ShareBrokerSessionPlan {
    ShareBrokerSessionPlan::try_initial(
        &ShareMembershipCatalog::try_new(
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
        .unwrap_or_else(|error| panic!("catalog: {error:?}")),
        ShareFetchBrokerId::try_from_raw(1).unwrap_or_else(|| panic!("broker")),
        &[GroupAssignmentPartition::new(
            TopicId::from_raw(1),
            PartitionIndex::from_raw(0),
        )],
    )
    .unwrap_or_else(|error| panic!("plan: {error:?}"))
}
