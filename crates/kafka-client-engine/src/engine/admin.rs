//! Admin handle construction and coordinated admission closure for one engine.

use std::sync::Arc;

use crate::AdminHandle;

use super::{Engine, EngineInner};

impl Engine {
    /// Returns a runtime-neutral handle over concrete admin operations.
    pub fn admin(&self) -> AdminHandle {
        let lifetime: Arc<dyn Send + Sync> = self.inner.clone();
        AdminHandle::new(
            crate::admin::AdminAdmissionPorts {
                create_topics: self.inner.create_topics_admission.clone(),
                delete_topics: self.inner.delete_topics_admission.clone(),
                describe_cluster: self.inner.describe_cluster_admission.clone(),
                create_partitions: self.inner.create_partitions_admission.clone(),
                describe_topics: self.inner.describe_topics_admission.clone(),
                describe_configs: self.inner.describe_configs_admission.clone(),
                incremental_alter_configs: self.inner.incremental_alter_configs_admission.clone(),
                list_consumer_group_offsets: self
                    .inner
                    .list_consumer_group_offsets_admission
                    .clone(),
                delete_consumer_group_offsets: self
                    .inner
                    .delete_consumer_group_offsets_admission
                    .clone(),
                alter_consumer_group_offsets: self
                    .inner
                    .alter_consumer_group_offsets_admission
                    .clone(),
                list_offsets: self.inner.list_offsets_admission.clone(),
                list_partition_reassignments: self
                    .inner
                    .list_partition_reassignments_admission
                    .clone(),
            },
            Arc::clone(&self.inner.clock),
            lifetime,
        )
    }
}

impl EngineInner {
    pub(super) fn close_admin_admission(&self) {
        let _close_result = self.create_topics_admission.close_admission();
        let _close_result = self.delete_topics_admission.close_admission();
        let _close_result = self.describe_cluster_admission.close_admission();
        let _close_result = self.create_partitions_admission.close_admission();
        let _close_result = self.describe_topics_admission.close_admission();
        let _close_result = self.describe_configs_admission.close_admission();
        let _close_result = self.incremental_alter_configs_admission.close_admission();
        let _close_result = self.list_consumer_group_offsets_admission.close_admission();
        let _close_result = self
            .delete_consumer_group_offsets_admission
            .close_admission();
        let _close_result = self
            .alter_consumer_group_offsets_admission
            .close_admission();
        let _close_result = self.list_offsets_admission.close_admission();
        let _close_result = self
            .list_partition_reassignments_admission
            .close_admission();
    }
}
