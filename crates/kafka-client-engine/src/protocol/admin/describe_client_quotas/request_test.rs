//! Focused evidence for bounded generated client-quota filter construction.

use super::{
    DescribeClientQuotaFilterComponentRef, DescribeClientQuotaMatchRef,
    DescribeClientQuotasFilterRef, DescribeClientQuotasRequestFailure,
    describe_client_quotas_request,
    retention::{
        MAX_ENTITY_NAME_BYTES, MAX_ENTITY_TYPE_BYTES, MAX_FILTER_COMPONENTS, request_peak_charge,
    },
};

#[test]
fn empty_filter_preserves_the_describe_all_shape() {
    let filter = DescribeClientQuotasFilterRef::new(&[], false);
    let request = describe_client_quotas_request(filter, usize::MAX)
        .unwrap_or_else(|error| panic!("valid all-entity filter: {error:?}"));

    assert!(request.components.is_empty());
    assert!(!request.strict);
}

#[test]
fn request_maps_all_match_modes_without_nullable_ambiguity() {
    let components = [
        component("user", DescribeClientQuotaMatchRef::Exact("User:a")),
        component("client-id", DescribeClientQuotaMatchRef::Default),
        component("ip", DescribeClientQuotaMatchRef::AnySpecified),
    ];
    let request = describe_client_quotas_request(
        DescribeClientQuotasFilterRef::new(&components, true),
        usize::MAX,
    )
    .unwrap_or_else(|error| panic!("valid filter: {error:?}"));

    assert!(request.strict);
    assert_eq!(request.components.len(), 3);
    assert_eq!(request.components[0].entity_type.as_str(), "user");
    assert_eq!(request.components[0].match_type, 0);
    assert_eq!(request.components[0].match_.as_deref(), Some("User:a"));
    assert_eq!(request.components[1].match_type, 1);
    assert_eq!(request.components[1].match_, None);
    assert_eq!(request.components[2].match_type, 2);
    assert_eq!(request.components[2].match_, None);
}

#[test]
fn request_rejects_empty_and_oversized_text() {
    let empty_type = [component("", DescribeClientQuotaMatchRef::Default)];
    assert_eq!(
        build(&empty_type),
        Err(DescribeClientQuotasRequestFailure::EmptyEntityType)
    );

    let empty_match = [component("user", DescribeClientQuotaMatchRef::Exact(""))];
    assert_eq!(
        build(&empty_match),
        Err(DescribeClientQuotasRequestFailure::EmptyExactMatch)
    );

    let oversized_type = "x".repeat(MAX_ENTITY_TYPE_BYTES + 1);
    let oversized = [component(
        &oversized_type,
        DescribeClientQuotaMatchRef::Default,
    )];
    assert_eq!(
        build(&oversized),
        Err(DescribeClientQuotasRequestFailure::EntityTypeTooLong {
            actual: MAX_ENTITY_TYPE_BYTES + 1,
            max: MAX_ENTITY_TYPE_BYTES,
        })
    );

    let oversized_match = "x".repeat(MAX_ENTITY_NAME_BYTES + 1);
    let oversized = [component(
        "user",
        DescribeClientQuotaMatchRef::Exact(&oversized_match),
    )];
    assert_eq!(
        build(&oversized),
        Err(DescribeClientQuotasRequestFailure::ExactMatchTooLong {
            actual: MAX_ENTITY_NAME_BYTES + 1,
            max: MAX_ENTITY_NAME_BYTES,
        })
    );
}

#[test]
fn request_rejects_duplicate_types_and_hostile_counts() {
    let duplicate = [
        component("user", DescribeClientQuotaMatchRef::Default),
        component("user", DescribeClientQuotaMatchRef::AnySpecified),
    ];
    assert_eq!(
        build(&duplicate),
        Err(DescribeClientQuotasRequestFailure::DuplicateEntityType)
    );

    let component = component("user", DescribeClientQuotaMatchRef::Default);
    let hostile = vec![component; MAX_FILTER_COMPONENTS + 1];
    assert_eq!(
        build(&hostile),
        Err(DescribeClientQuotasRequestFailure::TooManyComponents {
            actual: MAX_FILTER_COMPONENTS + 1,
            max: MAX_FILTER_COMPONENTS,
        })
    );
}

#[test]
fn request_checks_peak_capacity_before_copying() {
    let components = [component(
        "user",
        DescribeClientQuotaMatchRef::Exact("User:a"),
    )];
    let required = request_peak_charge(&components).unwrap_or_else(|| panic!("bounded charge"));

    assert_eq!(
        describe_client_quotas_request(
            DescribeClientQuotasFilterRef::new(&components, false),
            required - 1,
        ),
        Err(DescribeClientQuotasRequestFailure::RetainedBytes {
            required,
            limit: required - 1,
        })
    );
    assert!(
        describe_client_quotas_request(
            DescribeClientQuotasFilterRef::new(&components, false),
            required,
        )
        .is_ok()
    );
}

const fn component<'a>(
    entity_type: &'a str,
    match_: DescribeClientQuotaMatchRef<'a>,
) -> DescribeClientQuotaFilterComponentRef<'a> {
    DescribeClientQuotaFilterComponentRef::new(entity_type, match_)
}

fn build(
    components: &[DescribeClientQuotaFilterComponentRef<'_>],
) -> Result<kafka_wire::DescribeClientQuotasRequest, DescribeClientQuotasRequestFailure> {
    describe_client_quotas_request(
        DescribeClientQuotasFilterRef::new(components, false),
        usize::MAX,
    )
}
