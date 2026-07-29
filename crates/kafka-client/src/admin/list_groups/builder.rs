//! Inert all-group listing intent with optional bounded filters and one submission boundary.

use std::time::Duration;

use crate::bridge::{admin::AdminEngine, admin_list_groups::ListGroupsAdminRequest};

use super::ListGroups;

/// Inert cluster-wide listing request with optional state, group-type, and protocol-type filters.
#[must_use = "call submit to admit the ListGroups operation"]
pub struct ListGroupsBuilder {
    engine: AdminEngine,
    state_filters: Vec<String>,
    group_type_filters: Vec<String>,
    protocol_type_filters: Vec<String>,
    timeout: Duration,
}

impl ListGroupsBuilder {
    pub(crate) const fn new(engine: AdminEngine, timeout: Duration) -> Self {
        Self {
            engine,
            state_filters: Vec::new(),
            group_type_filters: Vec::new(),
            protocol_type_filters: Vec::new(),
            timeout,
        }
    }

    /// Replaces the caller-ordered broker-side group-state filters.
    pub fn state_filters<I, S>(mut self, filters: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.state_filters = filters.into_iter().map(Into::into).collect();
        self
    }

    /// Replaces the caller-ordered broker-side group-type filters.
    pub fn group_type_filters<I, S>(mut self, filters: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.group_type_filters = filters.into_iter().map(Into::into).collect();
        self
    }

    /// Replaces exact client-side protocol-type filters.
    pub fn protocol_type_filters<I, S>(mut self, filters: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.protocol_type_filters = filters.into_iter().map(Into::into).collect();
        self
    }

    /// Replaces the duration converted into one absolute deadline at submission.
    pub const fn deadline_after(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Captures the public deadline and attempts immediate bounded admission.
    pub fn submit(self) -> ListGroups {
        let request = ListGroupsAdminRequest::new(
            self.state_filters,
            self.group_type_filters,
            self.protocol_type_filters,
        );
        ListGroups::from_bridge(self.engine.submit_list_groups(request, self.timeout))
    }
}

impl std::fmt::Debug for ListGroupsBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ListGroupsBuilder")
            .field("state_filters", &self.state_filters)
            .field("group_type_filters", &self.group_type_filters)
            .field("protocol_type_filters", &self.protocol_type_filters)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
