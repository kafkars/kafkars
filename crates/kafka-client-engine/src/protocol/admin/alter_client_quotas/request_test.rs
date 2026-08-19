//! Focused evidence for bounded generated quota-alteration construction.

use super::{
    AlterClientQuotaAlterationRef, AlterClientQuotaEntityComponentRef,
    AlterClientQuotaOperationKindRef, AlterClientQuotaOperationRef,
    AlterClientQuotasRequestFailure, AlterClientQuotasRequestRef, alter_client_quotas_request,
    retention::{MAX_ALTERATIONS, MAX_ENTITY_NAME_BYTES, request_peak_charge},
};

#[test]
fn request_preserves_outer_and_operation_order_while_canonicalizing_identity() {
    let first_entity = [component("user", None)];
    let first_ops = [remove("z"), set("a", 12.5)];
    let second_entity = [
        component("user", Some("User:a")),
        component("client-id", Some("orders")),
    ];
    let second_ops = [set("rate", 1.0)];
    let alterations = [
        alteration(&first_entity, &first_ops),
        alteration(&second_entity, &second_ops),
    ];

    let request = build(&alterations, true, usize::MAX)
        .unwrap_or_else(|error| panic!("valid quota alterations: {error:?}"));
    assert!(request.validate_only);
    assert_eq!(request.entries.len(), 2);
    assert_eq!(request.entries[0].entity[0].entity_type.as_str(), "user");
    assert_eq!(request.entries[0].ops[0].key.as_str(), "z");
    assert_eq!(request.entries[0].ops[0].value.to_bits(), 0.0_f64.to_bits());
    assert!(request.entries[0].ops[0].remove);
    assert_eq!(request.entries[0].ops[1].key.as_str(), "a");
    assert_eq!(
        request.entries[0].ops[1].value.to_bits(),
        12.5_f64.to_bits()
    );
    assert!(!request.entries[0].ops[1].remove);
    assert_eq!(
        request.entries[1].entity[0].entity_type.as_str(),
        "client-id"
    );
    assert_eq!(request.entries[1].entity[1].entity_type.as_str(), "user");
}

#[test]
fn request_rejects_empty_and_hostile_counts() {
    assert_eq!(
        build(&[], false, usize::MAX),
        Err(AlterClientQuotasRequestFailure::EmptyAlterations)
    );
    let entity = [component("user", None)];
    let operations = [remove("rate")];
    let one = alteration(&entity, &operations);
    let hostile = vec![one; MAX_ALTERATIONS + 1];
    assert_eq!(
        build(&hostile, false, usize::MAX),
        Err(AlterClientQuotasRequestFailure::TooManyAlterations {
            actual: MAX_ALTERATIONS + 1,
            max: MAX_ALTERATIONS,
        })
    );
    let empty_entity = [alteration(&[], &operations)];
    assert_eq!(
        build(&empty_entity, false, usize::MAX),
        Err(AlterClientQuotasRequestFailure::EmptyEntity)
    );
    let empty_operations = [alteration(&entity, &[])];
    assert_eq!(
        build(&empty_operations, false, usize::MAX),
        Err(AlterClientQuotasRequestFailure::EmptyOperations)
    );
}

#[test]
fn request_rejects_invalid_strings_values_and_duplicates() {
    let operations = [remove("rate")];
    let empty_type = [component("", None)];
    assert_eq!(
        build(&[alteration(&empty_type, &operations)], false, usize::MAX),
        Err(AlterClientQuotasRequestFailure::EmptyEntityType)
    );

    let empty_name = [component("user", Some(""))];
    assert_eq!(
        build(&[alteration(&empty_name, &operations)], false, usize::MAX),
        Err(AlterClientQuotasRequestFailure::EmptyEntityName)
    );

    let oversized = "x".repeat(MAX_ENTITY_NAME_BYTES + 1);
    let oversized_name = [component("user", Some(&oversized))];
    assert_eq!(
        build(
            &[alteration(&oversized_name, &operations)],
            false,
            usize::MAX
        ),
        Err(AlterClientQuotasRequestFailure::EntityNameTooLong {
            actual: MAX_ENTITY_NAME_BYTES + 1,
            max: MAX_ENTITY_NAME_BYTES,
        })
    );

    let entity = [component("user", None)];
    let duplicate_keys = [remove("rate"), set("rate", 1.0)];
    assert_eq!(
        build(&[alteration(&entity, &duplicate_keys)], false, usize::MAX),
        Err(AlterClientQuotasRequestFailure::DuplicateQuotaKey)
    );

    let non_finite = [set("rate", f64::NAN)];
    assert_eq!(
        build(&[alteration(&entity, &non_finite)], false, usize::MAX),
        Err(AlterClientQuotasRequestFailure::NonFiniteQuotaValue)
    );
}

#[test]
fn request_rejects_duplicate_component_types_and_entities() {
    let operations = [remove("rate")];
    let duplicate_types = [component("user", None), component("user", Some("User:a"))];
    assert_eq!(
        build(
            &[alteration(&duplicate_types, &operations)],
            false,
            usize::MAX
        ),
        Err(AlterClientQuotasRequestFailure::DuplicateEntityType)
    );

    let left = [component("user", None), component("client-id", Some("a"))];
    let right = [component("client-id", Some("a")), component("user", None)];
    assert_eq!(
        build(
            &[
                alteration(&left, &operations),
                alteration(&right, &operations)
            ],
            false,
            usize::MAX
        ),
        Err(AlterClientQuotasRequestFailure::DuplicateEntity)
    );
}

#[test]
fn request_checks_generated_canonical_and_caller_reference_peak() {
    let entity = [component("user", Some("User:a"))];
    let operations = [set("producer_byte_rate", 1024.0)];
    let alterations = [alteration(&entity, &operations)];
    let request_ref = AlterClientQuotasRequestRef::new(&alterations, false);
    let required = request_peak_charge(request_ref).unwrap_or_else(|| panic!("bounded charge"));

    assert_eq!(
        alter_client_quotas_request(request_ref, required - 1),
        Err(AlterClientQuotasRequestFailure::RetainedBytes {
            required,
            limit: required - 1,
        })
    );
    assert!(alter_client_quotas_request(request_ref, required).is_ok());
}

pub(super) const fn component<'a>(
    entity_type: &'a str,
    entity_name: Option<&'a str>,
) -> AlterClientQuotaEntityComponentRef<'a> {
    AlterClientQuotaEntityComponentRef::new(entity_type, entity_name)
}

pub(super) const fn set(key: &str, value: f64) -> AlterClientQuotaOperationRef<'_> {
    AlterClientQuotaOperationRef::new(key, AlterClientQuotaOperationKindRef::Set(value))
}

pub(super) const fn remove(key: &str) -> AlterClientQuotaOperationRef<'_> {
    AlterClientQuotaOperationRef::new(key, AlterClientQuotaOperationKindRef::Remove)
}

pub(super) const fn alteration<'a>(
    entity: &'a [AlterClientQuotaEntityComponentRef<'a>],
    operations: &'a [AlterClientQuotaOperationRef<'a>],
) -> AlterClientQuotaAlterationRef<'a> {
    AlterClientQuotaAlterationRef::new(entity, operations)
}

fn build(
    alterations: &[AlterClientQuotaAlterationRef<'_>],
    validate_only: bool,
    limit: usize,
) -> Result<kafka_wire::AlterClientQuotasRequest, AlterClientQuotasRequestFailure> {
    alter_client_quotas_request(
        AlterClientQuotasRequestRef::new(alterations, validate_only),
        limit,
    )
}
