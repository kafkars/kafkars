//! Bounded filter intent for one cluster-wide `ListGroups` operation.

use core::fmt;
use std::collections::BTreeSet;

const MAX_FILTER_VALUE_BYTES: usize = i16::MAX as usize;

/// Maximum filters retained for any one `ListGroups` filter kind.
pub(crate) const LIST_GROUPS_MAX_FILTERS_PER_KIND: usize = 4 * 1024;
/// Maximum aggregate text retained across all `ListGroups` filters.
pub(crate) const LIST_GROUPS_MAX_FILTER_BYTES: usize = 256 * 1024;

/// Validated, caller-ordered filters for one cluster-wide group listing.
#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(
    clippy::struct_field_names,
    reason = "the filter suffix distinguishes the three independent Kafka filter domains"
)]
pub struct AdminGroupListingFilters {
    state_filters: Vec<String>,
    group_type_filters: Vec<String>,
    protocol_type_filters: Vec<String>,
}

impl AdminGroupListingFilters {
    /// Validates bounded filters without interpreting Kafka-owned names.
    pub fn new(
        state_filters: Vec<String>,
        group_type_filters: Vec<String>,
        protocol_type_filters: Vec<String>,
    ) -> Result<Self, AdminGroupListingFiltersError> {
        validate_kind(&state_filters, FilterKind::State)?;
        validate_kind(&group_type_filters, FilterKind::GroupType)?;
        validate_kind(&protocol_type_filters, FilterKind::ProtocolType)?;
        let retained_bytes = state_filters
            .iter()
            .chain(&group_type_filters)
            .chain(&protocol_type_filters)
            .try_fold(0usize, |bytes, value| bytes.checked_add(value.len()))
            .ok_or(AdminGroupListingFiltersError::FilterBytesExceeded)?;
        if retained_bytes > LIST_GROUPS_MAX_FILTER_BYTES {
            return Err(AdminGroupListingFiltersError::FilterBytesExceeded);
        }
        Ok(Self {
            state_filters,
            group_type_filters,
            protocol_type_filters,
        })
    }

    /// Creates the version-neutral empty-filter default.
    pub const fn empty() -> Self {
        Self {
            state_filters: Vec::new(),
            group_type_filters: Vec::new(),
            protocol_type_filters: Vec::new(),
        }
    }

    /// Returns broker-side state filters in exact caller order.
    pub fn state_filters(&self) -> &[String] {
        &self.state_filters
    }

    /// Returns broker-side group-type filters in exact caller order.
    pub fn group_type_filters(&self) -> &[String] {
        &self.group_type_filters
    }

    /// Returns client-side protocol-type filters in exact caller order.
    pub fn protocol_type_filters(&self) -> &[String] {
        &self.protocol_type_filters
    }

    /// Returns whether this listing retains the supplied protocol type.
    pub fn retains_protocol_type(&self, protocol_type: &str) -> bool {
        self.protocol_type_filters.is_empty()
            || self
                .protocol_type_filters
                .iter()
                .any(|filter| filter == protocol_type)
    }

    /// Returns the exact API 16 version floor required by broker-side filters.
    pub const fn minimum_list_groups_version(&self) -> i16 {
        if !self.group_type_filters.is_empty() {
            5
        } else if !self.state_filters.is_empty() {
            4
        } else {
            0
        }
    }
}

#[derive(Clone, Copy)]
enum FilterKind {
    State,
    GroupType,
    ProtocolType,
}

fn validate_kind(
    filters: &[String],
    kind: FilterKind,
) -> Result<(), AdminGroupListingFiltersError> {
    if filters.len() > LIST_GROUPS_MAX_FILTERS_PER_KIND {
        return Err(match kind {
            FilterKind::State => AdminGroupListingFiltersError::TooManyStateFilters,
            FilterKind::GroupType => AdminGroupListingFiltersError::TooManyGroupTypeFilters,
            FilterKind::ProtocolType => AdminGroupListingFiltersError::TooManyProtocolTypeFilters,
        });
    }
    let mut identities = BTreeSet::new();
    for filter in filters {
        if filter.is_empty() {
            return Err(match kind {
                FilterKind::State => AdminGroupListingFiltersError::EmptyStateFilter,
                FilterKind::GroupType => AdminGroupListingFiltersError::EmptyGroupTypeFilter,
                FilterKind::ProtocolType => AdminGroupListingFiltersError::EmptyProtocolTypeFilter,
            });
        }
        if filter.len() > MAX_FILTER_VALUE_BYTES {
            return Err(match kind {
                FilterKind::State => AdminGroupListingFiltersError::StateFilterTooLong,
                FilterKind::GroupType => AdminGroupListingFiltersError::GroupTypeFilterTooLong,
                FilterKind::ProtocolType => {
                    AdminGroupListingFiltersError::ProtocolTypeFilterTooLong
                }
            });
        }
        if !identities.insert(filter.as_str()) {
            return Err(match kind {
                FilterKind::State => AdminGroupListingFiltersError::DuplicateStateFilter,
                FilterKind::GroupType => AdminGroupListingFiltersError::DuplicateGroupTypeFilter,
                FilterKind::ProtocolType => {
                    AdminGroupListingFiltersError::DuplicateProtocolTypeFilter
                }
            });
        }
    }
    Ok(())
}

/// Invalid deterministic `ListGroups` filter intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminGroupListingFiltersError {
    /// One operation cannot retain more than 4,096 state filters.
    TooManyStateFilters,
    /// One operation cannot retain more than 4,096 group-type filters.
    TooManyGroupTypeFilters,
    /// One operation cannot retain more than 4,096 protocol-type filters.
    TooManyProtocolTypeFilters,
    /// An explicit state filter cannot be empty.
    EmptyStateFilter,
    /// An explicit group-type filter cannot be empty.
    EmptyGroupTypeFilter,
    /// An explicit protocol-type filter cannot be empty.
    EmptyProtocolTypeFilter,
    /// One state filter exceeded the bounded Kafka-string domain.
    StateFilterTooLong,
    /// One group-type filter exceeded the bounded Kafka-string domain.
    GroupTypeFilterTooLong,
    /// One protocol-type filter exceeded the bounded Kafka-string domain.
    ProtocolTypeFilterTooLong,
    /// Repeated state filters are not canonical request intent.
    DuplicateStateFilter,
    /// Repeated group-type filters are not canonical request intent.
    DuplicateGroupTypeFilter,
    /// Repeated protocol-type filters are not canonical request intent.
    DuplicateProtocolTypeFilter,
    /// Aggregate filter text exceeded the deterministic retained-byte bound.
    FilterBytesExceeded,
}

impl fmt::Display for AdminGroupListingFiltersError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid ListGroups filters: {self:?}")
    }
}

impl std::error::Error for AdminGroupListingFiltersError {}
