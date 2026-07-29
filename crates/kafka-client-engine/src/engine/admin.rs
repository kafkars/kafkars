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
                abort_partition_transaction: self
                    .inner
                    .abort_partition_transaction_admission
                    .clone(),
                create_topics: self.inner.create_topics_admission.clone(),
                create_acls: self.inner.create_acls_admission.clone(),
                create_delegation_token: self.inner.create_delegation_token_admission.clone(),
                describe_delegation_tokens: self.inner.describe_delegation_tokens_admission.clone(),
                renew_delegation_token: self.inner.renew_delegation_token_admission.clone(),
                expire_delegation_token: self.inner.expire_delegation_token_admission.clone(),
                delete_acls: self.inner.delete_acls_admission.clone(),
                delete_topics: self.inner.delete_topics_admission.clone(),
                delete_consumer_groups: self.inner.delete_consumer_groups_admission.clone(),
                delete_records: self.inner.delete_records_admission.clone(),
                describe_acls: self.inner.describe_acls_admission.clone(),
                describe_client_quotas: self.inner.describe_client_quotas_admission.clone(),
                alter_client_quotas: self.inner.alter_client_quotas_admission.clone(),
                alter_user_scram_credentials: self
                    .inner
                    .alter_user_scram_credentials_admission
                    .clone(),
                update_features: self.inner.update_features_admission.clone(),
                unregister_broker: self.inner.unregister_broker_admission.clone(),
                add_raft_voter: self.inner.add_raft_voter_admission.clone(),
                remove_raft_voter: self.inner.remove_raft_voter_admission.clone(),
                describe_user_scram_credentials: self
                    .inner
                    .describe_user_scram_credentials_admission
                    .clone(),
                describe_metadata_quorum: self.inner.describe_metadata_quorum_admission.clone(),
                describe_producers: self.inner.describe_producers_admission.clone(),
                describe_topic_partitions: self.inner.describe_topic_partitions_admission.clone(),
                describe_transactions: self.inner.describe_transactions_admission.clone(),
                fence_producers: self.inner.fence_producers_admission.clone(),
                list_transactions: self.inner.list_transactions_admission.clone(),
                list_client_metrics_resources: self
                    .inner
                    .list_client_metrics_resources_admission
                    .clone(),
                list_config_resources: self.inner.list_config_resources_admission.clone(),
                describe_cluster: self.inner.describe_cluster_admission.clone(),
                describe_consumer_groups: self.inner.describe_consumer_groups_admission.clone(),
                describe_features: self.inner.describe_features_admission.clone(),
                describe_log_dirs: self.inner.describe_log_dirs_admission.clone(),
                describe_replica_log_dirs: self.inner.describe_replica_log_dirs_admission.clone(),
                create_partitions: self.inner.create_partitions_admission.clone(),
                describe_topics: self.inner.describe_topics_admission.clone(),
                describe_configs: self.inner.describe_configs_admission.clone(),
                incremental_alter_configs: self.inner.incremental_alter_configs_admission.clone(),
                legacy_alter_configs: self.inner.legacy_alter_configs_admission.clone(),
                list_consumer_group_offsets: self
                    .inner
                    .list_consumer_group_offsets_admission
                    .clone(),
                list_consumer_groups: self.inner.list_consumer_groups_admission.clone(),
                delete_consumer_group_offsets: self
                    .inner
                    .delete_consumer_group_offsets_admission
                    .clone(),
                delete_share_group_offsets: self.inner.delete_share_group_offsets_admission.clone(),
                list_share_group_offsets: self.inner.list_share_group_offsets_admission.clone(),
                alter_share_group_offsets: self.inner.alter_share_group_offsets_admission.clone(),
                describe_share_group: self.inner.describe_share_group_admission.clone(),
                describe_streams_group: self.inner.describe_streams_group_admission.clone(),
                alter_consumer_group_offsets: self
                    .inner
                    .alter_consumer_group_offsets_admission
                    .clone(),
                list_offsets: self.inner.list_offsets_admission.clone(),
                list_partition_reassignments: self
                    .inner
                    .list_partition_reassignments_admission
                    .clone(),
                alter_partition_reassignments: self
                    .inner
                    .alter_partition_reassignments_admission
                    .clone(),
                alter_replica_log_dirs: self.inner.alter_replica_log_dirs_admission.clone(),
                elect_leaders: self.inner.elect_leaders_admission.clone(),
                remove_consumer_group_members: self
                    .inner
                    .remove_consumer_group_members_admission
                    .clone(),
            },
            Arc::clone(&self.inner.clock),
            lifetime,
        )
    }
}

impl EngineInner {
    pub(super) fn close_admin_admission(&self) {
        let _close_result = self.abort_partition_transaction_admission.close_admission();
        let _close_result = self.create_topics_admission.close_admission();
        let _close_result = self.create_acls_admission.close_admission();
        let _close_result = self.create_delegation_token_admission.close_admission();
        let _close_result = self.describe_delegation_tokens_admission.close_admission();
        let _close_result = self.renew_delegation_token_admission.close_admission();
        let _close_result = self.expire_delegation_token_admission.close_admission();
        let _close_result = self.delete_acls_admission.close_admission();
        let _close_result = self.delete_topics_admission.close_admission();
        let _close_result = self.delete_consumer_groups_admission.close_admission();
        let _close_result = self.delete_records_admission.close_admission();
        let _close_result = self.describe_acls_admission.close_admission();
        let _close_result = self.describe_client_quotas_admission.close_admission();
        let _close_result = self.alter_client_quotas_admission.close_admission();
        let _close_result = self
            .alter_user_scram_credentials_admission
            .close_admission();
        let _close_result = self.update_features_admission.close_admission();
        let _close_result = self.unregister_broker_admission.close_admission();
        let _close_result = self.add_raft_voter_admission.close_admission();
        let _close_result = self.remove_raft_voter_admission.close_admission();
        let _close_result = self
            .describe_user_scram_credentials_admission
            .close_admission();
        let _close_result = self.describe_metadata_quorum_admission.close_admission();
        let _close_result = self.describe_producers_admission.close_admission();
        let _close_result = self.describe_topic_partitions_admission.close_admission();
        let _close_result = self.describe_transactions_admission.close_admission();
        let _close_result = self.fence_producers_admission.close_admission();
        let _close_result = self.list_transactions_admission.close_admission();
        let _close_result = self
            .list_client_metrics_resources_admission
            .close_admission();
        let _close_result = self.list_config_resources_admission.close_admission();
        let _close_result = self.describe_cluster_admission.close_admission();
        let _close_result = self.describe_consumer_groups_admission.close_admission();
        let _close_result = self.describe_features_admission.close_admission();
        let _close_result = self.describe_log_dirs_admission.close_admission();
        let _close_result = self.describe_replica_log_dirs_admission.close_admission();
        let _close_result = self.create_partitions_admission.close_admission();
        let _close_result = self.describe_topics_admission.close_admission();
        let _close_result = self.describe_configs_admission.close_admission();
        let _close_result = self.incremental_alter_configs_admission.close_admission();
        let _close_result = self.legacy_alter_configs_admission.close_admission();
        let _close_result = self.list_consumer_group_offsets_admission.close_admission();
        let _close_result = self.list_consumer_groups_admission.close_admission();
        let delete_offsets = &self.delete_consumer_group_offsets_admission;
        let delete_share_offsets = &self.delete_share_group_offsets_admission;
        let list_share_offsets = &self.list_share_group_offsets_admission;
        let alter_share_offsets = &self.alter_share_group_offsets_admission;
        let describe_share_group = &self.describe_share_group_admission;
        let describe_streams_group = &self.describe_streams_group_admission;
        let alter_offsets = &self.alter_consumer_group_offsets_admission;
        let _close_result = delete_offsets.close_admission();
        let _close_result = delete_share_offsets.close_admission();
        let _close_result = list_share_offsets.close_admission();
        let _close_result = alter_share_offsets.close_admission();
        let _close_result = describe_share_group.close_admission();
        let _close_result = describe_streams_group.close_admission();
        let _close_result = alter_offsets.close_admission();
        let _close_result = self.list_offsets_admission.close_admission();
        let _close_result = self
            .list_partition_reassignments_admission
            .close_admission();
        let _close_result = self
            .alter_partition_reassignments_admission
            .close_admission();
        let _close_result = self.alter_replica_log_dirs_admission.close_admission();
        let _close_result = self.elect_leaders_admission.close_admission();
        let _close_result = self
            .remove_consumer_group_members_admission
            .close_admission();
    }
}
