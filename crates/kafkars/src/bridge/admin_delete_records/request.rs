//! Inert public Admin `DeleteRecords` intent translated at the engine boundary.

use kafka_client_engine::{
    DeleteRecordsRequest as EngineRequest, DeleteRecordsRequestTarget as EngineTarget,
};

use crate::admin::DeleteRecordsTarget;

/// Linear request retained by the public builder before submission.
pub(crate) struct DeleteRecordsAdminRequest {
    targets: Vec<DeleteRecordsTarget>,
}

impl DeleteRecordsAdminRequest {
    pub(crate) const fn new(targets: Vec<DeleteRecordsTarget>) -> Self {
        Self { targets }
    }

    pub(in crate::bridge) fn into_engine(self) -> EngineRequest {
        EngineRequest::new(
            self.targets
                .into_iter()
                .map(|target| {
                    let (topic, partition, before_offset) = target.into_parts();
                    EngineTarget::new(topic, partition, before_offset)
                })
                .collect(),
        )
    }
}

impl std::fmt::Debug for DeleteRecordsAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DeleteRecordsAdminRequest")
            .field("targets", &self.targets)
            .finish()
    }
}
