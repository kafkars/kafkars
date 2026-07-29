//! Caller-ordered generated construction for one destructive API-92 request.

use std::{error::Error, fmt};

use kafka_client_core::DeleteShareGroupOffsetsPlan;
use kafka_wire::{
    DeleteShareGroupOffsetsRequest,
    delete_share_group_offsets_request::DeleteShareGroupOffsetsRequestTopic,
};

/// Allocation failure before generated request ownership reaches the driver.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DeleteShareGroupOffsetsRequestFailure {
    /// Number of topic entries whose reservation failed.
    pub(crate) requested: usize,
}

impl fmt::Display for DeleteShareGroupOffsetsRequestFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "failed to reserve {} DeleteShareGroupOffsets topic entries",
            self.requested
        )
    }
}

impl Error for DeleteShareGroupOffsetsRequestFailure {}

/// Builds one exact-v0 request while preserving the plan's caller topic order.
pub(crate) fn delete_share_group_offsets_request(
    plan: &DeleteShareGroupOffsetsPlan,
) -> Result<DeleteShareGroupOffsetsRequest, DeleteShareGroupOffsetsRequestFailure> {
    let mut topics = Vec::new();
    topics.try_reserve_exact(plan.topics().len()).map_err(|_| {
        DeleteShareGroupOffsetsRequestFailure {
            requested: plan.topics().len(),
        }
    })?;
    topics.extend(plan.topics().iter().map(|topic| {
        let mut requested = DeleteShareGroupOffsetsRequestTopic::default();
        requested.topic_name = topic.as_str().into();
        requested
    }));

    let mut request = DeleteShareGroupOffsetsRequest::default();
    request.group_id = plan.group_id().into();
    request.topics = topics;
    Ok(request)
}
