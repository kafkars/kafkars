//! Bounded concrete ACL creation-plan validation tests.

use super::{CreateAclBinding, CreateAclsPlan, CreateAclsPlanError, MAX_CREATE_ACLS_BINDINGS};

#[test]
fn valid_unique_bindings_retain_exact_caller_order_and_future_codes() {
    let plan = CreateAclsPlan::new(vec![
        binding(101, "first", 102, "User:alice", "*", 103, 104),
        binding(2, "second", 3, "User:bob", "10.0.0.1", 3, 3),
    ])
    .unwrap_or_else(|error| panic!("valid plan: {error}"));

    assert_eq!(plan.required_result_capacity(), 2);
    assert_eq!(plan.bindings()[0].resource_name(), "first");
    assert_eq!(plan.bindings()[0].resource_type(), 101);
    assert_eq!(plan.bindings()[1].resource_name(), "second");
}

#[test]
fn batch_must_be_nonempty_bounded_and_unique() {
    assert_eq!(
        CreateAclsPlan::new(Vec::new()),
        Err(CreateAclsPlanError::EmptyBatch)
    );
    let duplicate = binding(2, "orders", 3, "User:alice", "*", 3, 3);
    assert_eq!(
        CreateAclsPlan::new(vec![duplicate.clone(), duplicate]),
        Err(CreateAclsPlanError::DuplicateBinding)
    );
    let bindings = (0..=MAX_CREATE_ACLS_BINDINGS)
        .map(|index| binding(2, &format!("topic-{index}"), 3, "User:alice", "*", 3, 3))
        .collect();
    assert_eq!(
        CreateAclsPlan::new(bindings),
        Err(CreateAclsPlanError::BatchTooLarge)
    );
}

#[test]
fn filter_only_and_unknown_scalar_domains_are_rejected() {
    for (binding, expected) in [
        (
            binding(1, "orders", 3, "User:alice", "*", 3, 3),
            CreateAclsPlanError::InvalidResourceType,
        ),
        (
            binding(2, "orders", 2, "User:alice", "*", 3, 3),
            CreateAclsPlanError::InvalidPatternType,
        ),
        (
            binding(2, "orders", 3, "User:alice", "*", 1, 3),
            CreateAclsPlanError::InvalidOperation,
        ),
        (
            binding(2, "orders", 3, "User:alice", "*", 3, 1),
            CreateAclsPlanError::InvalidPermissionType,
        ),
    ] {
        assert_eq!(CreateAclsPlan::new(vec![binding]), Err(expected));
    }
}

#[test]
fn every_owned_string_is_nonempty_and_bounded() {
    let oversized = "x".repeat(i16::MAX as usize + 1);
    for (binding, expected) in [
        (
            binding(2, "", 3, "User:alice", "*", 3, 3),
            CreateAclsPlanError::EmptyResourceName,
        ),
        (
            binding(2, &oversized, 3, "User:alice", "*", 3, 3),
            CreateAclsPlanError::ResourceNameTooLong,
        ),
        (
            binding(2, "orders", 3, "", "*", 3, 3),
            CreateAclsPlanError::EmptyPrincipal,
        ),
        (
            binding(2, "orders", 3, &oversized, "*", 3, 3),
            CreateAclsPlanError::PrincipalTooLong,
        ),
        (
            binding(2, "orders", 3, "User:alice", "", 3, 3),
            CreateAclsPlanError::EmptyHost,
        ),
        (
            binding(2, "orders", 3, "User:alice", &oversized, 3, 3),
            CreateAclsPlanError::HostTooLong,
        ),
    ] {
        assert_eq!(CreateAclsPlan::new(vec![binding]), Err(expected));
    }
}

fn binding(
    resource_type: i8,
    resource_name: &str,
    pattern_type: i8,
    principal: &str,
    host: &str,
    operation: i8,
    permission_type: i8,
) -> CreateAclBinding {
    CreateAclBinding::new(
        resource_type,
        resource_name.to_owned(),
        pattern_type,
        principal.to_owned(),
        host.to_owned(),
        operation,
        permission_type,
    )
}
