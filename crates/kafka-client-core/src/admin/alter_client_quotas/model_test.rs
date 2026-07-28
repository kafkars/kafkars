//! Request validation scenarios for deterministic Admin `AlterClientQuotas`.

use super::{
    ALTER_CLIENT_QUOTAS_MAX_COMPONENTS_PER_ENTITY, ALTER_CLIENT_QUOTAS_MAX_ENTRIES,
    ALTER_CLIENT_QUOTAS_MAX_OPERATIONS_PER_ENTITY, AlterClientQuotaEntity,
    AlterClientQuotaEntityComponent, AlterClientQuotaEntry, AlterClientQuotaOperation,
    AlterClientQuotaOperationKind, AlterClientQuotasPlan, AlterClientQuotasPlanError,
};

#[test]
fn plan_canonicalizes_entity_identity_without_reordering_entries_or_operations() {
    let plan = AlterClientQuotasPlan::new(
        vec![
            AlterClientQuotaEntry::new(
                entity(vec![("user", Some("alice")), ("client-id", Some("app"))]),
                vec![
                    AlterClientQuotaOperation::remove("producer_byte_rate".to_owned()),
                    AlterClientQuotaOperation::set("consumer_byte_rate".to_owned(), 12.5),
                ],
            ),
            entry(entity(vec![("ip", None)])),
        ],
        true,
    )
    .unwrap_or_else(|error| panic!("valid plan: {error}"));

    assert!(plan.validate_only());
    assert_eq!(
        plan.entries()[0].entity().components()[0].entity_type(),
        "client-id"
    );
    assert_eq!(
        plan.entries()[0].entity().components()[1].entity_type(),
        "user"
    );
    assert_eq!(
        plan.entries()[1].entity().components()[0].entity_type(),
        "ip"
    );
    assert_eq!(
        plan.entries()[0].operations()[0].kind(),
        AlterClientQuotaOperationKind::Remove
    );
    assert_eq!(
        plan.entries()[0].operations()[1].kind(),
        AlterClientQuotaOperationKind::Set(12.5)
    );
}

#[test]
fn plan_rejects_empty_oversized_and_ambiguous_entity_sets() {
    assert_plan_error(Vec::new(), AlterClientQuotasPlanError::EmptyBatch);
    assert_plan_error(
        vec![entry(entity(vec![("user", Some("alice"))])); ALTER_CLIENT_QUOTAS_MAX_ENTRIES + 1],
        AlterClientQuotasPlanError::TooManyEntries,
    );
    assert_plan_error(
        vec![entry(AlterClientQuotaEntity::new(Vec::new()))],
        AlterClientQuotasPlanError::EmptyEntity,
    );
    assert_plan_error(
        vec![entry(AlterClientQuotaEntity::new(vec![
            component(
                "user",
                Some("alice")
            );
            ALTER_CLIENT_QUOTAS_MAX_COMPONENTS_PER_ENTITY
                + 1
        ]))],
        AlterClientQuotasPlanError::TooManyEntityComponents,
    );
    for (components, expected) in [
        (
            vec![component("", Some("alice"))],
            AlterClientQuotasPlanError::EmptyEntityType,
        ),
        (
            vec![component(&"t".repeat(257), Some("alice"))],
            AlterClientQuotasPlanError::EntityTypeTooLong,
        ),
        (
            vec![component("user", Some(""))],
            AlterClientQuotasPlanError::EmptyEntityName,
        ),
        (
            vec![component("user", Some(&"n".repeat(257)))],
            AlterClientQuotasPlanError::EntityNameTooLong,
        ),
        (
            vec![
                component("user", Some("alice")),
                component("user", Some("bob")),
            ],
            AlterClientQuotasPlanError::DuplicateEntityType,
        ),
    ] {
        assert_plan_error(
            vec![entry(AlterClientQuotaEntity::new(components))],
            expected,
        );
    }
}

#[test]
fn canonical_identity_rejects_duplicate_entities_even_with_different_component_order() {
    assert_plan_error(
        vec![
            entry(entity(vec![
                ("user", Some("alice")),
                ("client-id", Some("app")),
            ])),
            entry(entity(vec![
                ("client-id", Some("app")),
                ("user", Some("alice")),
            ])),
        ],
        AlterClientQuotasPlanError::DuplicateEntity,
    );
}

#[test]
fn plan_rejects_empty_oversized_duplicate_and_nonfinite_operations() {
    assert_eq!(
        AlterClientQuotasPlan::new(
            vec![AlterClientQuotaEntry::new(
                entity(vec![("user", Some("alice"))]),
                Vec::new(),
            )],
            false,
        ),
        Err(AlterClientQuotasPlanError::EmptyOperations)
    );
    assert_operations_error(
        vec![
            AlterClientQuotaOperation::remove("quota".to_owned());
            ALTER_CLIENT_QUOTAS_MAX_OPERATIONS_PER_ENTITY + 1
        ],
        AlterClientQuotasPlanError::TooManyOperations,
    );
    for (operations, expected) in [
        (
            vec![AlterClientQuotaOperation::remove(String::new())],
            AlterClientQuotasPlanError::EmptyQuotaKey,
        ),
        (
            vec![AlterClientQuotaOperation::remove("q".repeat(257))],
            AlterClientQuotasPlanError::QuotaKeyTooLong,
        ),
        (
            vec![
                AlterClientQuotaOperation::remove("quota".to_owned()),
                AlterClientQuotaOperation::set("quota".to_owned(), 1.0),
            ],
            AlterClientQuotasPlanError::DuplicateQuotaKey,
        ),
        (
            vec![AlterClientQuotaOperation::set("quota".to_owned(), f64::NAN)],
            AlterClientQuotasPlanError::NonFiniteQuotaValue,
        ),
        (
            vec![AlterClientQuotaOperation::set(
                "quota".to_owned(),
                f64::INFINITY,
            )],
            AlterClientQuotasPlanError::NonFiniteQuotaValue,
        ),
    ] {
        assert_operations_error(operations, expected);
    }
}

fn assert_operations_error(
    operations: Vec<AlterClientQuotaOperation>,
    expected: AlterClientQuotasPlanError,
) {
    assert_eq!(
        AlterClientQuotasPlan::new(
            vec![AlterClientQuotaEntry::new(
                entity(vec![("user", Some("alice"))]),
                operations,
            )],
            false,
        ),
        Err(expected)
    );
}

fn assert_plan_error(entries: Vec<AlterClientQuotaEntry>, expected: AlterClientQuotasPlanError) {
    assert_eq!(AlterClientQuotasPlan::new(entries, false), Err(expected));
}

fn entry(entity: AlterClientQuotaEntity) -> AlterClientQuotaEntry {
    AlterClientQuotaEntry::new(
        entity,
        vec![AlterClientQuotaOperation::remove("quota".to_owned())],
    )
}

fn entity(parts: Vec<(&str, Option<&str>)>) -> AlterClientQuotaEntity {
    AlterClientQuotaEntity::new(
        parts
            .into_iter()
            .map(|(entity_type, entity_name)| component(entity_type, entity_name))
            .collect(),
    )
}

fn component(entity_type: &str, entity_name: Option<&str>) -> AlterClientQuotaEntityComponent {
    AlterClientQuotaEntityComponent::new(entity_type.to_owned(), entity_name.map(str::to_owned))
}
