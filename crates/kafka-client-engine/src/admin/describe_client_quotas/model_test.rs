//! Engine request ownership and core validation scenarios.

use kafka_client_core::ClientQuotaMatch as CoreMatch;

use super::{
    DescribeClientQuotaFilterComponent, DescribeClientQuotaMatch,
    DescribeClientQuotasAdmissionError, DescribeClientQuotasAdmissionErrorKind,
    DescribeClientQuotasRequest,
};

#[test]
fn request_preserves_order_match_modes_strictness_and_empty_list_all() {
    let empty = DescribeClientQuotasRequest::new(Vec::new(), false)
        .into_plan()
        .unwrap_or_else(|error| panic!("all quotas filter: {error}"));
    assert!(empty.components().is_empty());
    assert!(!empty.strict());

    let request = DescribeClientQuotasRequest::new(
        vec![
            DescribeClientQuotaFilterComponent::new(
                "user".to_owned(),
                DescribeClientQuotaMatch::Exact("alice".to_owned()),
            ),
            DescribeClientQuotaFilterComponent::new(
                "client-id".to_owned(),
                DescribeClientQuotaMatch::Default,
            ),
        ],
        true,
    );
    let plan = request
        .into_plan()
        .unwrap_or_else(|error| panic!("valid quota filter: {error}"));
    assert_eq!(plan.components()[0].entity_type(), "user");
    assert_eq!(
        plan.components()[0].match_kind(),
        &CoreMatch::Exact("alice".to_owned())
    );
    assert_eq!(plan.components()[1].match_kind(), &CoreMatch::Default);
    assert!(plan.strict());
}

#[test]
fn core_rejects_invalid_and_duplicate_filter_components() {
    for components in [
        vec![DescribeClientQuotaFilterComponent::new(
            String::new(),
            DescribeClientQuotaMatch::Default,
        )],
        vec![DescribeClientQuotaFilterComponent::new(
            "user".to_owned(),
            DescribeClientQuotaMatch::Exact(String::new()),
        )],
        vec![
            DescribeClientQuotaFilterComponent::new(
                "user".to_owned(),
                DescribeClientQuotaMatch::Default,
            ),
            DescribeClientQuotaFilterComponent::new(
                "user".to_owned(),
                DescribeClientQuotaMatch::AnySpecified,
            ),
        ],
    ] {
        assert!(
            DescribeClientQuotasRequest::new(components, false)
                .into_plan()
                .is_err()
        );
    }
}

#[test]
fn stable_component_parts_and_admission_error_remain_explicit() {
    let parts = DescribeClientQuotaFilterComponent::new(
        "client-id".to_owned(),
        DescribeClientQuotaMatch::AnySpecified,
    )
    .into_parts();
    assert_eq!(
        parts,
        (
            "client-id".to_owned(),
            DescribeClientQuotaMatch::AnySpecified
        )
    );
    let error = DescribeClientQuotasAdmissionError::new(
        DescribeClientQuotasAdmissionErrorKind::InvalidRequest,
    );
    assert_eq!(
        error.kind(),
        DescribeClientQuotasAdmissionErrorKind::InvalidRequest
    );
}
