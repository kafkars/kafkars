//! Engine request canonicalization and plan validation scenarios.

use kafka_client_core::AdminDescribeLogDirsSelection;

use super::{
    DescribeLogDirTarget, DescribeLogDirsAdmissionErrorKind, DescribeLogDirsPlanFailure,
    DescribeLogDirsRequest,
};
use crate::admin::describe_log_dirs::DescribeLogDirsAdmissionError;

#[test]
fn request_preserves_order_and_rejects_invalid_broker_sets_in_core() {
    let plan = DescribeLogDirsRequest::new(vec![9, 2, 7])
        .canonicalize()
        .into_plan()
        .unwrap_or_else(|error| panic!("valid plan: {error:?}"));
    assert_eq!(plan.broker_ids(), &[9, 2, 7]);

    assert!(DescribeLogDirsRequest::new(Vec::new()).into_plan().is_err());
    assert!(
        DescribeLogDirsRequest::new(vec![1, -1])
            .into_plan()
            .is_err()
    );
    assert!(DescribeLogDirsRequest::new(vec![1, 1]).into_plan().is_err());

    let error =
        DescribeLogDirsAdmissionError::new(DescribeLogDirsAdmissionErrorKind::InvalidRequest);
    assert_eq!(
        error.kind(),
        DescribeLogDirsAdmissionErrorKind::InvalidRequest
    );
}

#[test]
fn selected_request_canonicalizes_and_preserves_flat_caller_order() {
    let request = DescribeLogDirsRequest::selected(
        vec![9, 2],
        vec![
            DescribeLogDirTarget::new("orders".to_owned(), 2),
            DescribeLogDirTarget::new("audit".to_owned(), 0),
            DescribeLogDirTarget::new("orders".to_owned(), 1),
        ],
    );
    assert_eq!(request, request.clone().canonicalize());
    let plan = request
        .canonicalize()
        .into_plan()
        .unwrap_or_else(|error| panic!("valid plan: {error:?}"));
    let Some(partitions) = plan.selection().selected_partitions() else {
        panic!("selected partitions expected");
    };
    assert_eq!(
        partitions
            .iter()
            .map(|partition| (partition.topic(), partition.partition()))
            .collect::<Vec<_>>(),
        vec![("orders", 2), ("audit", 0), ("orders", 1)]
    );
}

#[test]
fn all_and_legacy_new_preserve_nullable_all_topic_intent() {
    for request in [
        DescribeLogDirsRequest::new(vec![1]),
        DescribeLogDirsRequest::all(vec![1]),
    ] {
        let plan = request
            .into_plan()
            .unwrap_or_else(|error| panic!("valid plan: {error:?}"));
        assert_eq!(plan.selection(), &AdminDescribeLogDirsSelection::AllTopics);
    }
}

#[test]
fn explicit_empty_selection_is_not_conflated_with_all_topics() {
    assert!(matches!(
        DescribeLogDirsRequest::selected(vec![1], Vec::new()).into_plan(),
        Err(DescribeLogDirsPlanFailure::Invalid(_))
    ));
}
