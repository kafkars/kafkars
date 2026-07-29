//! Linear public ShareGroup offset-alteration intent translated at submission.

use crate::admin::ShareGroupOffsetAlteration;

use super::engine::{Request as EngineRequest, Target as EngineTarget};

/// Request retained by the inert public builder before submission.
pub(crate) struct AlterShareGroupOffsetsAdminRequest {
    group_id: String,
    alterations: Vec<ShareGroupOffsetAlteration>,
}

impl AlterShareGroupOffsetsAdminRequest {
    pub(crate) const fn new(
        group_id: String,
        alterations: Vec<ShareGroupOffsetAlteration>,
    ) -> Self {
        Self {
            group_id,
            alterations,
        }
    }

    pub(in crate::bridge) fn into_engine(self) -> EngineRequest {
        EngineRequest::new(
            self.group_id,
            self.alterations
                .into_iter()
                .map(into_engine_target)
                .collect(),
        )
    }
}

impl std::fmt::Debug for AlterShareGroupOffsetsAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AlterShareGroupOffsetsAdminRequest")
            .field("group_id", &self.group_id)
            .field("alterations", &self.alterations)
            .finish()
    }
}

fn into_engine_target(alteration: ShareGroupOffsetAlteration) -> EngineTarget {
    let (topic, partition, start_offset) = alteration.into_parts();
    EngineTarget::new(topic, partition, start_offset)
}
