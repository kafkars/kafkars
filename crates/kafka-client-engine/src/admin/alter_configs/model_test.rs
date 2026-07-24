//! Canonical request-storage and retained-capacity scenarios.

use super::{
    IncrementalAlterConfigsRequest, IncrementalConfigAlteration, IncrementalConfigOperation,
    TopicConfigAlterations,
};

#[test]
fn canonical_request_preserves_exact_operations_order_and_validate_only() {
    let mut topic = String::with_capacity(64);
    topic.push_str("orders");
    let mut value = String::with_capacity(64);
    value.push_str("compact");
    let mut topics = Vec::with_capacity(8);
    topics.push(TopicConfigAlterations::new(
        topic,
        vec![
            alteration(
                "retention.ms",
                IncrementalConfigOperation::Set(String::new()),
            ),
            alteration("segment.ms", IncrementalConfigOperation::Delete),
            alteration("cleanup.policy", IncrementalConfigOperation::Append(value)),
            alteration(
                "compression.type",
                IncrementalConfigOperation::Subtract("gzip".to_owned()),
            ),
        ],
    ));

    let request = IncrementalAlterConfigsRequest::new(topics)
        .with_validate_only(true)
        .canonicalize();
    assert!(request.storage_is_canonical());
    let plan = request
        .into_plan()
        .unwrap_or_else(|error| panic!("valid incremental plan: {error}"));
    assert!(plan.validate_only());
    assert_eq!(plan.topics()[0].topic(), "orders");
    assert_eq!(
        plan.topics()[0].alterations()[0].operation().value(),
        Some("")
    );
    assert_eq!(plan.topics()[0].alterations()[1].operation().value(), None);
    assert_eq!(
        plan.topics()[0].alterations()[2].operation().value(),
        Some("compact")
    );
    assert_eq!(
        plan.topics()[0].alterations()[3].operation().value(),
        Some("gzip")
    );
}

#[test]
fn shared_operation_charge_covers_the_separate_terminal_result_limit() {
    let request = IncrementalAlterConfigsRequest::new(vec![TopicConfigAlterations::new(
        "orders".to_owned(),
        vec![alteration(
            "cleanup.policy",
            IncrementalConfigOperation::Set("compact".to_owned()),
        )],
    )]);
    let retention = request
        .retention()
        .unwrap_or_else(|| panic!("small request retention fits"));

    let topic_result_floor = crate::admin::retention::result_fixed_charge(1, "orders".len())
        .and_then(|fixed| {
            fixed.checked_add(crate::admin::retention::RESULT_DIAGNOSTIC_BYTES_PER_TOPIC)
        })
        .unwrap_or_else(|| panic!("small result retention fits"));
    assert_eq!(retention.result_limit(), topic_result_floor);
    assert!(retention.total() >= retention.result_limit());
}

#[test]
fn core_validation_rejects_ambiguous_engine_request_without_machine_construction() {
    let request = IncrementalAlterConfigsRequest::new(vec![TopicConfigAlterations::new(
        "orders".to_owned(),
        vec![
            alteration("retention.ms", IncrementalConfigOperation::Delete),
            alteration(
                "retention.ms",
                IncrementalConfigOperation::Set("10".to_owned()),
            ),
        ],
    )]);
    assert!(request.into_plan().is_err());
}

fn alteration(key: &str, operation: IncrementalConfigOperation) -> IncrementalConfigAlteration {
    IncrementalConfigAlteration::new(key.to_owned(), operation)
}
