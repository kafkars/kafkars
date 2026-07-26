//! Protocol correlation and normalized-result scalar access scenarios.

use std::sync::Arc;

use core::num::NonZeroI16;

use super::model::{
    GroupOffsetFetchCorrelation, GroupOffsetFetchPartitionValueRef, GroupOffsetFetchTopic,
    NormalizedGroupOffsetFetch,
};

#[test]
fn correlation_retains_exact_group_topic_and_partition_order() {
    let correlation = GroupOffsetFetchCorrelation::new(
        Arc::from("readers"),
        vec![
            GroupOffsetFetchTopic::new(Arc::from("z"), vec![2, 0]),
            GroupOffsetFetchTopic::new(Arc::from("a"), vec![7]),
        ],
        3,
    );

    assert_eq!(correlation.group_id(), "readers");
    assert_eq!(correlation.partition_count(), 3);
    assert_eq!(correlation.topics()[0].name(), "z");
    assert_eq!(correlation.topics()[0].partition_indexes(), [2, 0]);
    assert_eq!(correlation.topics()[1].name(), "a");
}

#[test]
fn normalized_result_keeps_exact_signed_codes_and_scalar_observations() {
    let code = NonZeroI16::new(-917).unwrap_or_else(|| panic!("nonzero test code"));
    let result = NormalizedGroupOffsetFetch::new(
        11,
        None,
        vec![
            GroupOffsetFetchPartitionValueRef::Fetched {
                committed_offset: None,
                committed_leader_epoch: Some(4),
                metadata: Some(""),
            },
            GroupOffsetFetchPartitionValueRef::Rejected { code },
        ],
        72,
    );

    assert_eq!(result.throttle_time_ms(), 11);
    assert_eq!(result.top_level_error(), None);
    assert_eq!(result.entries().len(), 2);
    assert_eq!(result.retained_charge(), 72);
    assert!(matches!(
        result.entries()[1],
        GroupOffsetFetchPartitionValueRef::Rejected { code } if code.get() == -917
    ));
}
