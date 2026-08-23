//! Canonical `ShareFetch` assignment and session-delta plan evidence.

use super::{ShareFetchRequestFailure, ShareFetchRequestPlan, ShareFetchRequestTopic};

#[test]
fn plan_canonicalizes_topics_and_partitions_before_correlation() {
    let plan = ShareFetchRequestPlan::try_new(
        vec![topic(2, &[3, 1]), topic(1, &[2, 0])],
        vec![topic(2, &[1]), topic(1, &[0])],
        vec![topic(3, &[4])],
    )
    .unwrap_or_else(|error| panic!("valid plan: {error:?}"));
    let (active, included, forgotten) = plan.into_parts();

    assert_eq!(active[0].topic_id, id(1));
    assert_eq!(active[0].partitions, vec![0, 2]);
    assert_eq!(active[1].topic_id, id(2));
    assert_eq!(active[1].partitions, vec![1, 3]);
    assert_eq!(included[0].topic_id, id(1));
    assert_eq!(included[1].topic_id, id(2));
    assert_eq!(forgotten[0].topic_id, id(3));
}

#[test]
fn plan_rejects_duplicate_topics_and_invalid_session_deltas() {
    assert_eq!(
        ShareFetchRequestPlan::try_new(vec![topic(1, &[0]), topic(1, &[1])], vec![], vec![],),
        Err(ShareFetchRequestFailure::DuplicateTopic)
    );
    assert_eq!(
        ShareFetchRequestPlan::try_new(vec![topic(1, &[0])], vec![topic(1, &[1])], vec![],),
        Err(ShareFetchRequestFailure::IncludedPartitionNotActive)
    );
    assert_eq!(
        ShareFetchRequestPlan::try_new(vec![topic(1, &[0])], vec![], vec![topic(1, &[0])],),
        Err(ShareFetchRequestFailure::ForgottenPartitionStillActive)
    );
}

fn topic(value: u8, partitions: &[u32]) -> ShareFetchRequestTopic {
    ShareFetchRequestTopic::try_new(id(value), partitions.to_vec())
        .unwrap_or_else(|error| panic!("valid topic: {error:?}"))
}

fn id(value: u8) -> [u8; 16] {
    let mut id = [0; 16];
    id[0] = value;
    id
}
