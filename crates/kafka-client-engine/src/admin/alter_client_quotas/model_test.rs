//! Engine request conversion for Admin `AlterClientQuotas`.

use super::{
    AlterClientQuotaEntity, AlterClientQuotaEntityComponent, AlterClientQuotaEntry,
    AlterClientQuotaOperation, AlterClientQuotasRequest,
};

#[test]
fn request_preserves_entity_order_operations_and_validate_only() {
    let request = AlterClientQuotasRequest::new(
        vec![AlterClientQuotaEntry::new(
            AlterClientQuotaEntity::new(vec![
                AlterClientQuotaEntityComponent::new(
                    "client-id".to_owned(),
                    Some("orders".to_owned()),
                ),
                AlterClientQuotaEntityComponent::new("user".to_owned(), None),
            ]),
            vec![
                AlterClientQuotaOperation::set("producer_byte_rate".to_owned(), 4096.0),
                AlterClientQuotaOperation::remove("request_percentage".to_owned()),
            ],
        )],
        true,
    );

    let plan = request
        .canonicalize()
        .into_plan()
        .expect("valid client-quota alteration");
    assert!(plan.validate_only());
    assert_eq!(plan.entries().len(), 1);
    assert_eq!(
        plan.entries()[0].entity().components()[0].entity_type(),
        "client-id"
    );
    assert_eq!(plan.entries()[0].operations().len(), 2);
}

#[test]
fn invalid_or_duplicate_request_facts_are_deferred_to_submission() {
    let request = AlterClientQuotasRequest::new(
        vec![AlterClientQuotaEntry::new(
            AlterClientQuotaEntity::new(vec![AlterClientQuotaEntityComponent::new(
                String::new(),
                Some(String::new()),
            )]),
            vec![AlterClientQuotaOperation::set(String::new(), f64::NAN)],
        )],
        false,
    );

    assert!(request.canonicalize().into_plan().is_err());
}
