//! Engine ACL creation ownership, canonicalization, and core validation tests.

use super::{
    CreateAclBinding, CreateAclsAdmissionError, CreateAclsAdmissionErrorKind, CreateAclsRequest,
};

#[test]
fn request_preserves_exact_binding_scalars_and_caller_order_in_core_plan() {
    let request =
        CreateAclsRequest::new(vec![binding("first", 3), binding("second", 15)]).canonicalize();
    assert_eq!(request.bindings()[0].resource_name(), "first");
    assert_eq!(request.bindings()[1].resource_name(), "second");

    let plan = request
        .into_plan()
        .unwrap_or_else(|error| panic!("valid ACL creation plan: {error}"));
    assert_eq!(plan.bindings()[0].resource_name(), "first");
    assert_eq!(plan.bindings()[0].operation(), 3);
    assert_eq!(plan.bindings()[1].resource_name(), "second");
    assert_eq!(plan.bindings()[1].operation(), 15);
}

#[test]
fn core_rejects_empty_duplicate_and_filter_only_binding_intent() {
    assert!(CreateAclsRequest::new(Vec::new()).into_plan().is_err());

    let duplicate = binding("orders", 3);
    assert!(
        CreateAclsRequest::new(vec![duplicate.clone(), duplicate])
            .into_plan()
            .is_err()
    );

    for invalid in [
        CreateAclBinding::new(1, "orders", 3, "User:a", "*", 3, 3),
        CreateAclBinding::new(2, "", 3, "User:a", "*", 3, 3),
        CreateAclBinding::new(2, "orders", 2, "User:a", "*", 3, 3),
        CreateAclBinding::new(2, "orders", 3, "", "*", 3, 3),
        CreateAclBinding::new(2, "orders", 3, "User:a", "", 3, 3),
        CreateAclBinding::new(2, "orders", 3, "User:a", "*", 1, 3),
        CreateAclBinding::new(2, "orders", 3, "User:a", "*", 3, 1),
    ] {
        assert!(CreateAclsRequest::new(vec![invalid]).into_plan().is_err());
    }
}

#[test]
fn public_binding_parts_and_admission_error_are_stable() {
    assert_eq!(
        binding("orders", 15).into_parts(),
        (
            2,
            "orders".to_owned(),
            3,
            "User:alice".to_owned(),
            "*".to_owned(),
            15,
            3,
        )
    );

    let error = CreateAclsAdmissionError::new(CreateAclsAdmissionErrorKind::InvalidRequest);
    assert_eq!(error.kind(), CreateAclsAdmissionErrorKind::InvalidRequest);
}

fn binding(resource_name: &str, operation: i8) -> CreateAclBinding {
    CreateAclBinding::new(2, resource_name, 3, "User:alice", "*", operation, 3)
}
