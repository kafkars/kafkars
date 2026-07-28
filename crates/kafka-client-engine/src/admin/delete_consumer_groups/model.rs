//! Engine-owned scalar intent for one Admin `DeleteConsumerGroups` query.

use kafka_client_core::{
    DeleteConsumerGroupsPlan, DeleteConsumerGroupsPlanError, DeleteConsumerGroupsTarget,
};

/// One caller-ordered bounded request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteConsumerGroupsRequest {
    group_ids: Vec<String>,
}

impl DeleteConsumerGroupsRequest {
    /// Creates inert request intent for validation at the operation boundary.
    pub const fn new(group_ids: Vec<String>) -> Self {
        Self { group_ids }
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        self.group_ids = self
            .group_ids
            .into_iter()
            .map(|group_id| group_id.into_boxed_str().into_string())
            .collect();
        self.group_ids.shrink_to_fit();
        self
    }

    pub(crate) fn into_plan(
        self,
    ) -> Result<DeleteConsumerGroupsPlan, DeleteConsumerGroupsPlanError> {
        DeleteConsumerGroupsPlan::new(
            self.group_ids
                .into_iter()
                .map(DeleteConsumerGroupsTarget::new)
                .collect(),
        )
    }
}
