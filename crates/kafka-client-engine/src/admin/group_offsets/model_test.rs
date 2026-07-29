//! Scenarios for inert group-offset request representation.

use super::{
    ListConsumerGroupOffsetTarget, ListConsumerGroupOffsetsQuery, ListConsumerGroupOffsetsRequest,
    ListConsumerGroupsOffsetsRequest,
};

#[test]
fn request_preserves_group_and_stability_until_core_validation() {
    let request = ListConsumerGroupOffsetsRequest::new("payments".to_owned(), true).canonicalize();
    assert!(request.storage_is_canonical());

    let plan = request
        .into_plan()
        .unwrap_or_else(|error| panic!("valid group-offset plan: {error}"));
    assert_eq!(plan.group_id(), "payments");
    assert!(plan.require_stable());
}

#[test]
fn invalid_group_remains_inert_until_plan_conversion() {
    let request = ListConsumerGroupOffsetsRequest::new(String::new(), false);
    assert!(request.into_plan().is_err());
}

#[test]
fn selected_request_canonicalizes_and_preserves_caller_target_order() {
    let query = ListConsumerGroupOffsetsQuery::selected(
        oversized("payments"),
        vec![
            ListConsumerGroupOffsetTarget::new(oversized("z-topic"), 8),
            ListConsumerGroupOffsetTarget::new(oversized("a-topic"), 3),
            ListConsumerGroupOffsetTarget::new(oversized("z-topic"), 1),
        ],
    );
    let request = ListConsumerGroupOffsetsRequest::from_query(query, false).canonicalize();
    assert!(request.storage_is_canonical());
    let plan = request
        .into_plan()
        .unwrap_or_else(|error| panic!("valid selected request: {error}"));
    let kafka_client_core::ListConsumerGroupOffsetsSelection::Selected(targets) = plan.selection()
    else {
        panic!("selected plan expected");
    };
    assert_eq!(
        targets
            .iter()
            .map(|target| (target.topic(), target.partition()))
            .collect::<Vec<_>>(),
        [("z-topic", 8), ("a-topic", 3), ("z-topic", 1)]
    );
}

#[test]
fn empty_and_duplicate_selected_targets_remain_inert_until_plan_conversion() {
    let empty = ListConsumerGroupOffsetsRequest::from_query(
        ListConsumerGroupOffsetsQuery::selected("payments".to_owned(), Vec::new()),
        false,
    );
    assert!(empty.into_plan().is_err());

    let duplicate = ListConsumerGroupOffsetsRequest::from_query(
        ListConsumerGroupOffsetsQuery::selected(
            "payments".to_owned(),
            vec![
                ListConsumerGroupOffsetTarget::new("orders".to_owned(), 1),
                ListConsumerGroupOffsetTarget::new("orders".to_owned(), 1),
            ],
        ),
        false,
    );
    assert!(duplicate.into_plan().is_err());
}

#[test]
fn plural_request_canonicalizes_all_group_storage_and_preserves_order() {
    let request = ListConsumerGroupsOffsetsRequest::new(
        vec![oversized("z-readers"), oversized("a-readers")],
        true,
    )
    .canonicalize();
    assert!(request.storage_is_canonical());

    let plan = request
        .into_plan()
        .unwrap_or_else(|error| panic!("valid plural request: {error}"));
    assert_eq!(plan.group_ids(), ["z-readers", "a-readers"]);
    assert!(plan.require_stable());
}

#[test]
fn plural_request_preserves_each_groups_independent_selection() {
    let request = ListConsumerGroupsOffsetsRequest::from_queries(
        vec![
            ListConsumerGroupOffsetsQuery::selected(
                "z-readers".to_owned(),
                vec![ListConsumerGroupOffsetTarget::new("orders".to_owned(), 2)],
            ),
            ListConsumerGroupOffsetsQuery::all("a-readers".to_owned()),
        ],
        true,
    );
    let plan = request
        .into_plan()
        .unwrap_or_else(|error| panic!("valid selected batch: {error}"));
    assert!(matches!(
        &plan.selections()[0],
        kafka_client_core::ListConsumerGroupOffsetsSelection::Selected(targets)
            if targets[0].partition() == 2
    ));
    assert!(matches!(
        &plan.selections()[1],
        kafka_client_core::ListConsumerGroupOffsetsSelection::All
    ));
}

fn oversized(value: &str) -> String {
    let mut text = String::with_capacity(value.len() + 64);
    text.push_str(value);
    text
}
