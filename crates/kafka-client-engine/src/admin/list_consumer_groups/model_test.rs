//! General `ListGroups` request canonicalization scenarios.

use kafka_client_core::AdminGroupListingFiltersError;

use super::AdminListGroupsRequest;

#[test]
fn request_preserves_all_filter_kinds_in_caller_order() {
    let filters = AdminListGroupsRequest::new(
        vec!["Stable".to_owned(), "Empty".to_owned()],
        vec!["consumer".to_owned(), "classic".to_owned()],
        vec!["consumer".to_owned(), "connect".to_owned()],
    )
    .canonicalize()
    .into_filters()
    .unwrap_or_else(|error| panic!("valid filters: {error}"));
    assert_eq!(filters.state_filters(), ["Stable", "Empty"]);
    assert_eq!(filters.group_type_filters(), ["consumer", "classic"]);
    assert_eq!(filters.protocol_type_filters(), ["consumer", "connect"]);
    assert_eq!(filters.minimum_list_groups_version(), 5);
}

#[test]
fn request_rejects_empty_and_duplicate_values_before_admission() {
    let empty = AdminListGroupsRequest::new(vec![String::new()], Vec::new(), Vec::new())
        .canonicalize()
        .into_filters();
    assert_eq!(empty, Err(AdminGroupListingFiltersError::EmptyStateFilter));

    let duplicate = AdminListGroupsRequest::new(
        Vec::new(),
        vec!["share".to_owned(), "share".to_owned()],
        Vec::new(),
    )
    .canonicalize()
    .into_filters();
    assert_eq!(
        duplicate,
        Err(AdminGroupListingFiltersError::DuplicateGroupTypeFilter)
    );
}
