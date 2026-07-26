//! Request-reference construction scenarios for offset deletion.

use kafka_client_core::{DeleteConsumerGroupOffsetTarget, DeleteConsumerGroupOffsetsPlan};

use super::delete_consumer_group_offsets::target_refs;

#[test]
fn target_references_preserve_the_validated_plan_order() {
    let Ok(plan) = DeleteConsumerGroupOffsetsPlan::new(
        "payments".to_owned(),
        vec![
            DeleteConsumerGroupOffsetTarget::new("orders".to_owned(), 2),
            DeleteConsumerGroupOffsetTarget::new("audit".to_owned(), 0),
        ],
    ) else {
        panic!("fixture must be a valid deletion plan");
    };
    let Some(targets) = target_refs(&plan) else {
        panic!("two target references should fit");
    };

    assert_eq!(targets.len(), 2);
    assert_eq!(targets[0].topic(), "orders");
    assert_eq!(targets[0].partition(), 2);
    assert_eq!(targets[1].topic(), "audit");
    assert_eq!(targets[1].partition(), 0);
}
