//! Scenarios for bounded API-92 request-intent validation.

use super::{
    DELETE_SHARE_GROUP_OFFSETS_MAX_REQUEST_TEXT_BYTES, DELETE_SHARE_GROUP_OFFSETS_MAX_TOPICS,
    DeleteShareGroupOffsetsPlan, DeleteShareGroupOffsetsPlanError,
};

#[test]
fn plan_preserves_explicit_group_and_caller_topic_order() {
    let plan = DeleteShareGroupOffsetsPlan::new(
        "payments-share".to_owned(),
        vec!["orders".to_owned(), "audit".to_owned()],
    )
    .unwrap_or_else(|error| panic!("valid plan: {error}"));

    assert_eq!(plan.group_id(), "payments-share");
    assert_eq!(plan.topics(), ["orders", "audit"]);
}

#[test]
fn plan_rejects_empty_or_unrepresentable_group_identity() {
    assert_eq!(
        DeleteShareGroupOffsetsPlan::new(String::new(), vec!["orders".to_owned()]),
        Err(DeleteShareGroupOffsetsPlanError::EmptyGroupId)
    );
    assert_eq!(
        DeleteShareGroupOffsetsPlan::new(
            "g".repeat(i16::MAX as usize + 1),
            vec!["orders".to_owned()],
        ),
        Err(DeleteShareGroupOffsetsPlanError::GroupIdTooLong)
    );
}

#[test]
fn plan_rejects_empty_oversized_or_duplicate_topic_sets() {
    assert_eq!(
        DeleteShareGroupOffsetsPlan::new("group".to_owned(), Vec::new()),
        Err(DeleteShareGroupOffsetsPlanError::EmptyTopicBatch)
    );
    assert_eq!(
        DeleteShareGroupOffsetsPlan::new("group".to_owned(), vec![String::new()],),
        Err(DeleteShareGroupOffsetsPlanError::EmptyTopicName)
    );
    assert_eq!(
        DeleteShareGroupOffsetsPlan::new(
            "group".to_owned(),
            vec!["t".repeat(i16::MAX as usize + 1)],
        ),
        Err(DeleteShareGroupOffsetsPlanError::TopicNameTooLong)
    );
    assert_eq!(
        DeleteShareGroupOffsetsPlan::new(
            "group".to_owned(),
            vec!["orders".to_owned(), "orders".to_owned()],
        ),
        Err(DeleteShareGroupOffsetsPlanError::DuplicateTopicName)
    );
    assert_eq!(
        DeleteShareGroupOffsetsPlan::new(
            "group".to_owned(),
            (0..=DELETE_SHARE_GROUP_OFFSETS_MAX_TOPICS)
                .map(|index| format!("topic-{index}"))
                .collect(),
        ),
        Err(DeleteShareGroupOffsetsPlanError::TooManyTopics)
    );
}

#[test]
fn plan_rejects_request_text_beyond_one_mib() {
    let segment_bytes =
        DELETE_SHARE_GROUP_OFFSETS_MAX_REQUEST_TEXT_BYTES / DELETE_SHARE_GROUP_OFFSETS_MAX_TOPICS;
    let topics = (0..DELETE_SHARE_GROUP_OFFSETS_MAX_TOPICS)
        .map(|index| format!("{index:04}-{}", "x".repeat(segment_bytes)))
        .collect();

    assert_eq!(
        DeleteShareGroupOffsetsPlan::new("group".to_owned(), topics),
        Err(DeleteShareGroupOffsetsPlanError::RequestTextTooLarge)
    );
}
