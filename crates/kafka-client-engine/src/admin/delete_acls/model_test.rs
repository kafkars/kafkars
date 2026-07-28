//! ACL deletion filter ownership, validation, and positional preservation tests.

use super::{
    DeleteAclsAdmissionError, DeleteAclsAdmissionErrorKind, DeleteAclsFilter, DeleteAclsRequest,
};

#[test]
fn request_preserves_nullable_exact_scalars_order_and_duplicate_positions() {
    let duplicate = filter(Some("orders"), Some("User:alice"), None, 15);
    let request = DeleteAclsRequest::new(vec![
        duplicate.clone(),
        duplicate,
        filter(None, None, None, 1),
    ])
    .canonicalize();
    assert_eq!(request.filters()[0], request.filters()[1]);
    assert_eq!(request.filters()[2].resource_name(), None);

    let plan = request
        .into_plan()
        .unwrap_or_else(|error| panic!("valid ACL deletion plan: {error}"));
    assert_eq!(plan.filters().len(), 3);
    assert_eq!(plan.filters()[0], plan.filters()[1]);
    assert_eq!(plan.filters()[0].resource_type(), 2);
    assert_eq!(plan.filters()[0].resource_name(), Some("orders"));
    assert_eq!(plan.filters()[0].pattern_type(), 3);
    assert_eq!(plan.filters()[0].principal(), Some("User:alice"));
    assert_eq!(plan.filters()[0].host(), None);
    assert_eq!(plan.filters()[0].operation(), 15);
    assert_eq!(plan.filters()[0].permission_type(), 3);
}

#[test]
fn core_rejects_empty_invalid_scalar_and_invalid_present_string_intent() {
    assert!(DeleteAclsRequest::new(Vec::new()).into_plan().is_err());

    for invalid in [
        DeleteAclsFilter::new(0, None, 3, None, None, 3, 3),
        DeleteAclsFilter::new(2, Some(String::new()), 3, None, None, 3, 3),
        DeleteAclsFilter::new(2, None, 0, None, None, 3, 3),
        DeleteAclsFilter::new(2, None, 3, Some(String::new()), None, 3, 3),
        DeleteAclsFilter::new(2, None, 3, None, Some(String::new()), 3, 3),
        DeleteAclsFilter::new(2, None, 3, None, None, 0, 3),
        DeleteAclsFilter::new(2, None, 3, None, None, 3, 0),
    ] {
        assert!(DeleteAclsRequest::new(vec![invalid]).into_plan().is_err());
    }
}

#[test]
fn public_filter_parts_and_admission_error_are_stable() {
    assert_eq!(
        filter(Some("orders"), Some("User:alice"), Some("*"), 15).into_parts(),
        (
            2,
            Some("orders".to_owned()),
            3,
            Some("User:alice".to_owned()),
            Some("*".to_owned()),
            15,
            3,
        )
    );

    let error = DeleteAclsAdmissionError::new(DeleteAclsAdmissionErrorKind::InvalidRequest);
    assert_eq!(error.kind(), DeleteAclsAdmissionErrorKind::InvalidRequest);
}

fn filter(
    resource_name: Option<&str>,
    principal: Option<&str>,
    host: Option<&str>,
    operation: i8,
) -> DeleteAclsFilter {
    DeleteAclsFilter::new(
        2,
        resource_name.map(str::to_owned),
        3,
        principal.map(str::to_owned),
        host.map(str::to_owned),
        operation,
        3,
    )
}
