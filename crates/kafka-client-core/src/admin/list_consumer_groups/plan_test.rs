//! Bounded cluster group-listing filter scenarios.

use super::{
    AdminGroupListingFilters, AdminGroupListingFiltersError, plan::LIST_GROUPS_MAX_FILTERS_PER_KIND,
};

#[test]
fn empty_filters_preserve_legacy_version_compatibility() {
    let filters = AdminGroupListingFilters::empty();
    assert!(filters.state_filters().is_empty());
    assert!(filters.group_type_filters().is_empty());
    assert!(filters.protocol_type_filters().is_empty());
    assert_eq!(filters.minimum_list_groups_version(), 0);
    assert!(filters.retains_protocol_type(""));
    assert!(filters.retains_protocol_type("future"));
}

#[test]
fn broker_filters_raise_the_exact_api_16_floor() {
    let states = filters(vec!["Stable"], Vec::<&str>::new(), Vec::<&str>::new());
    assert_eq!(states.minimum_list_groups_version(), 4);

    let types = filters(Vec::<&str>::new(), vec!["share"], Vec::<&str>::new());
    assert_eq!(types.minimum_list_groups_version(), 5);

    let both = filters(vec!["Empty"], vec!["classic"], Vec::<&str>::new());
    assert_eq!(both.minimum_list_groups_version(), 5);
}

#[test]
fn protocol_filters_are_exact_client_side_selection_without_a_version_floor() {
    let filters = filters(
        Vec::<&str>::new(),
        Vec::<&str>::new(),
        vec!["consumer", "connect"],
    );
    assert_eq!(filters.minimum_list_groups_version(), 0);
    assert!(filters.retains_protocol_type("consumer"));
    assert!(filters.retains_protocol_type("connect"));
    assert!(!filters.retains_protocol_type(""));
}

#[test]
fn every_filter_kind_rejects_empty_duplicate_and_excess_count() {
    assert_eq!(
        AdminGroupListingFilters::new(vec![String::new()], Vec::new(), Vec::new()),
        Err(AdminGroupListingFiltersError::EmptyStateFilter)
    );
    assert_eq!(
        AdminGroupListingFilters::new(
            Vec::new(),
            vec!["share".to_owned(), "share".to_owned()],
            Vec::new()
        ),
        Err(AdminGroupListingFiltersError::DuplicateGroupTypeFilter)
    );
    assert_eq!(
        AdminGroupListingFilters::new(Vec::new(), Vec::new(), vec![String::new()]),
        Err(AdminGroupListingFiltersError::EmptyProtocolTypeFilter)
    );
    assert_eq!(
        AdminGroupListingFilters::new(
            Vec::new(),
            Vec::new(),
            vec!["consumer".to_owned(), "consumer".to_owned()]
        ),
        Err(AdminGroupListingFiltersError::DuplicateProtocolTypeFilter)
    );
    assert_eq!(
        AdminGroupListingFilters::new(
            vec!["Stable".to_owned(); LIST_GROUPS_MAX_FILTERS_PER_KIND + 1],
            Vec::new(),
            Vec::new()
        ),
        Err(AdminGroupListingFiltersError::TooManyStateFilters)
    );
}

fn filters<S, T, P>(states: Vec<S>, types: Vec<T>, protocols: Vec<P>) -> AdminGroupListingFilters
where
    S: Into<String>,
    T: Into<String>,
    P: Into<String>,
{
    AdminGroupListingFilters::new(
        states.into_iter().map(Into::into).collect(),
        types.into_iter().map(Into::into).collect(),
        protocols.into_iter().map(Into::into).collect(),
    )
    .unwrap_or_else(|error| panic!("valid group filters: {error}"))
}
