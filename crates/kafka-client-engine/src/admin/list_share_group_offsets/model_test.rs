//! Inert and canonical API-90 request scenarios.

use super::{
    ListShareGroupOffsetsRequest, ListShareGroupOffsetsTarget, ListShareGroupsOffsetsRequest,
};

#[test]
fn request_canonicalizes_owned_storage_and_preserves_caller_order() {
    let request = ListShareGroupOffsetsRequest::selected(
        oversized("payments-share"),
        vec![
            ListShareGroupOffsetsTarget::new(oversized("orders"), 2),
            ListShareGroupOffsetsTarget::new(oversized("audit"), 1),
        ],
    )
    .canonicalize();

    assert!(request.storage_is_canonical());
    let plan = request
        .into_plan()
        .unwrap_or_else(|error| panic!("valid plan: {error}"));
    assert_eq!(plan.group_id(), "payments-share");
    let kafka_client_core::ListShareGroupOffsetsSelection::Selected(targets) = plan.selection()
    else {
        panic!("selected plan");
    };
    assert_eq!(targets[0].topic(), "orders");
    assert_eq!(targets[0].partition(), 2);
    assert_eq!(targets[1].topic(), "audit");
    assert_eq!(targets[1].partition(), 1);
}

#[test]
fn invalid_intent_remains_inert_until_core_plan_conversion() {
    let request = ListShareGroupOffsetsRequest::selected(String::new(), Vec::new());

    assert!(request.canonicalize().into_plan().is_err());
}

#[test]
fn all_selection_remains_explicit() {
    let plan = ListShareGroupOffsetsRequest::all("payments-share".to_owned())
        .canonicalize()
        .into_plan()
        .unwrap_or_else(|error| panic!("valid all plan: {error}"));

    assert!(matches!(
        plan.selection(),
        kafka_client_core::ListShareGroupOffsetsSelection::All
    ));
}

#[test]
fn batch_canonicalizes_each_query_and_preserves_independent_selections() {
    let request = ListShareGroupsOffsetsRequest::new(vec![
        ListShareGroupOffsetsRequest::selected(
            oversized("share-a"),
            vec![ListShareGroupOffsetsTarget::new(oversized("orders"), 2)],
        ),
        ListShareGroupOffsetsRequest::all(oversized("share-b")),
    ])
    .canonicalize();

    assert!(request.storage_is_canonical());
    let plan = request
        .into_plan()
        .unwrap_or_else(|error| panic!("batch plan: {error}"));
    assert_eq!(
        plan.queries()
            .iter()
            .map(|query| query.group_id())
            .collect::<Vec<_>>(),
        ["share-a", "share-b"]
    );
    assert!(matches!(
        plan.queries()[0].selection(),
        kafka_client_core::ListShareGroupOffsetsSelection::Selected(targets)
            if targets[0].topic() == "orders" && targets[0].partition() == 2
    ));
    assert!(matches!(
        plan.queries()[1].selection(),
        kafka_client_core::ListShareGroupOffsetsSelection::All
    ));
}

fn oversized(value: &str) -> String {
    let mut owned = String::with_capacity(128);
    owned.push_str(value);
    owned
}
