//! Sole conversion and admission boundary from public admin values to the engine.

mod abort_partition_transaction_submit;
mod add_raft_voter_submit;
mod alter_share_group_offsets_submit;
mod delete_share_group_offsets_submit;
mod describe_cluster_submit;
mod describe_features_submit;
mod describe_share_group_submit;
mod describe_share_groups_submit;
mod describe_streams_group_submit;
mod describe_streams_groups_submit;
mod describe_topic_partitions_submit;
mod fence_producers_submit;
mod group_submissions;
mod legacy_replace_topic_configs;
mod list_client_metrics_resources_submit;
mod list_config_resources_submit;
mod list_share_group_offsets_submit;
mod list_transactions_submit;
mod remove_raft_voter_submit;
mod request;
mod unregister_broker_submit;
mod update_features_submit;
use std::time::{Duration, Instant};

use kafka_client_engine::AdminHandle as EngineAdminHandle;

pub(crate) use request::{AdminRequest, DeleteAdminRequest, PartitionsAdminRequest};

use super::admin_alter_config_resources_operation::AdminIncrementalAlterConfigResources;
use super::admin_alter_configs_operation::AdminIncrementalAlterConfigs;
use super::admin_alter_configs_request::IncrementalAlterConfigsAdminRequest;
use super::admin_alter_replica_log_dirs::{
    AdminAlterReplicaLogDirs, AlterReplicaLogDirsAdminRequest,
};
use super::admin_config_resources_operation::AdminDescribeConfigResources;
use super::admin_configs_operation::AdminDescribeConfigs;
use super::admin_configs_request::DescribeConfigsAdminRequest;
use super::admin_create_acls::{AdminCreateAcls, CreateAclsAdminRequest};
use super::admin_delete_acls::{AdminDeleteAcls, DeleteAclsAdminRequest};
use super::admin_delete_by_id_operation::AdminDeleteTopicsById;
use super::admin_delete_operation::AdminDeleteTopics;
use super::admin_delete_records::{AdminDeleteRecords, DeleteRecordsAdminRequest};
use super::admin_describe_acls::{AdminDescribeAcls, DescribeAclsAdminRequest};
use super::admin_describe_log_dirs::{AdminDescribeLogDirs, DescribeLogDirsAdminRequest};
use super::admin_describe_replica_log_dirs::{
    AdminDescribeReplicaLogDirs, DescribeReplicaLogDirsAdminRequest,
};
use super::admin_elect_leaders::{AdminElectLeaders, ElectLeadersAdminRequest};
use super::admin_operation::AdminCreateTopics;
use super::admin_partitions_operation::AdminCreatePartitions;
use super::admin_topics_by_id_operation::AdminDescribeTopicsById;
use super::admin_topics_operation::AdminDescribeTopics;
use super::admin_topics_request::DescribeTopicsAdminRequest;
use super::alter_client_quotas::{AdminAlterClientQuotas, AlterClientQuotasAdminRequest};
use super::alter_user_scram_credentials::{
    AdminAlterUserScramCredentials, AlterUserScramCredentialsAdminRequest,
};
use super::create_delegation_token::{
    AdminCreateDelegationToken, CreateDelegationTokenAdminRequest,
};
use super::describe_client_quotas::{AdminDescribeClientQuotas, DescribeClientQuotasAdminRequest};
use super::describe_delegation_tokens::{
    AdminDescribeDelegationTokens, DescribeDelegationTokensAdminRequest,
};
use super::describe_metadata_quorum::AdminDescribeMetadataQuorum;
use super::describe_producers::{AdminDescribeProducers, DescribeProducersAdminRequest};
use super::describe_transactions::{AdminDescribeTransactions, DescribeTransactionsAdminRequest};
use super::describe_user_scram_credentials::{
    AdminDescribeUserScramCredentials, DescribeUserScramCredentialsAdminRequest,
};
use super::expire_delegation_token::{
    AdminExpireDelegationToken, ExpireDelegationTokenAdminRequest,
};
use super::renew_delegation_token::{AdminRenewDelegationToken, RenewDelegationTokenAdminRequest};

/// Cloneable facade owner of the engine's concrete admin handle and default.
#[derive(Debug, Clone)]
pub(crate) struct AdminEngine {
    pub(super) handle: EngineAdminHandle,
    default_timeout: Duration,
}

impl AdminEngine {
    pub(crate) const fn new(handle: EngineAdminHandle, default_timeout: Duration) -> Self {
        Self {
            handle,
            default_timeout,
        }
    }

    pub(crate) const fn default_timeout(&self) -> Duration {
        self.default_timeout
    }

    pub(crate) fn submit_alter_replica_log_dirs(
        &self,
        request: AlterReplicaLogDirsAdminRequest,
        timeout: Duration,
    ) -> AdminAlterReplicaLogDirs {
        AdminAlterReplicaLogDirs::from_admission(
            self.handle
                .try_alter_replica_log_dirs(request.into_engine(), timeout),
        )
    }

    pub(crate) fn submit(&self, request: AdminRequest, timeout: Duration) -> AdminCreateTopics {
        AdminCreateTopics::from_admission(self.handle.try_create_topics(request.inner, timeout))
    }

    pub(crate) fn submit_delete(
        &self,
        request: DeleteAdminRequest,
        timeout: Duration,
    ) -> AdminDeleteTopics {
        AdminDeleteTopics::from_admission(self.handle.try_delete_topics(request.inner, timeout))
    }

    pub(crate) fn submit_delete_by_id(
        &self,
        request: DeleteAdminRequest,
        timeout: Duration,
    ) -> AdminDeleteTopicsById {
        AdminDeleteTopicsById::from_admission(self.handle.try_delete_topics(request.inner, timeout))
    }

    pub(crate) fn submit_delete_records(
        &self,
        request: DeleteRecordsAdminRequest,
        timeout: Duration,
    ) -> AdminDeleteRecords {
        AdminDeleteRecords::from_admission(
            self.handle
                .try_delete_records(request.into_engine(), timeout),
        )
    }

    pub(crate) fn submit_create_acls(
        &self,
        request: CreateAclsAdminRequest,
        deadline: Instant,
    ) -> AdminCreateAcls {
        AdminCreateAcls::submit_with(request, deadline, |request, remaining| {
            self.handle.try_create_acls(request, remaining)
        })
    }

    pub(crate) fn submit_delete_acls(
        &self,
        request: DeleteAclsAdminRequest,
        deadline: Instant,
    ) -> AdminDeleteAcls {
        AdminDeleteAcls::submit_with(request, deadline, |request, remaining| {
            self.handle.try_delete_acls(request, remaining)
        })
    }

    pub(crate) fn submit_describe_acls(
        &self,
        request: DescribeAclsAdminRequest,
        deadline: Instant,
    ) -> AdminDescribeAcls {
        let request = request.into_engine();
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return AdminDescribeAcls::deadline_elapsed();
        }
        AdminDescribeAcls::from_admission(self.handle.try_describe_acls(request, remaining))
    }

    pub(crate) fn submit_describe_client_quotas(
        &self,
        request: DescribeClientQuotasAdminRequest,
        timeout: Duration,
    ) -> AdminDescribeClientQuotas {
        AdminDescribeClientQuotas::from_admission(
            self.handle
                .try_describe_client_quotas(request.into_engine(), timeout),
        )
    }

    pub(crate) fn submit_describe_user_scram_credentials(
        &self,
        request: DescribeUserScramCredentialsAdminRequest,
        timeout: Duration,
    ) -> AdminDescribeUserScramCredentials {
        AdminDescribeUserScramCredentials::from_admission(
            self.handle
                .try_describe_user_scram_credentials(request.into_engine(), timeout),
        )
    }

    pub(crate) fn submit_alter_client_quotas(
        &self,
        request: AlterClientQuotasAdminRequest,
        timeout: Duration,
    ) -> AdminAlterClientQuotas {
        AdminAlterClientQuotas::from_admission(
            self.handle
                .try_alter_client_quotas(request.into_engine(), timeout),
        )
    }

    pub(crate) fn submit_alter_user_scram_credentials(
        &self,
        request: AlterUserScramCredentialsAdminRequest,
        timeout: Duration,
    ) -> AdminAlterUserScramCredentials {
        let admission = match self.handle.capture_alter_user_scram_credentials(timeout) {
            Ok(capture) => capture.try_submit(request.into_engine()),
            Err(error) => Err(error),
        };
        AdminAlterUserScramCredentials::from_admission(admission)
    }

    pub(crate) fn submit_create_delegation_token(
        &self,
        request: CreateDelegationTokenAdminRequest,
        timeout: Duration,
    ) -> AdminCreateDelegationToken {
        let admission = match self.handle.capture_create_delegation_token(timeout) {
            Ok(capture) => capture.try_submit(request.into_engine()),
            Err(error) => Err(error),
        };
        AdminCreateDelegationToken::from_admission(admission)
    }

    pub(crate) fn submit_describe_delegation_tokens(
        &self,
        request: DescribeDelegationTokensAdminRequest,
        timeout: Duration,
    ) -> AdminDescribeDelegationTokens {
        let admission = match self.handle.capture_describe_delegation_tokens(timeout) {
            Ok(capture) => capture.try_submit(request.into_engine()),
            Err(error) => Err(error),
        };
        AdminDescribeDelegationTokens::from_admission(admission)
    }

    pub(crate) fn submit_renew_delegation_token(
        &self,
        request: RenewDelegationTokenAdminRequest,
        timeout: Duration,
    ) -> AdminRenewDelegationToken {
        let admission = match self.handle.capture_renew_delegation_token(timeout) {
            Ok(capture) => capture.try_submit(request.into_engine()),
            Err(error) => Err(error),
        };
        AdminRenewDelegationToken::from_admission(admission)
    }

    pub(crate) fn submit_expire_delegation_token(
        &self,
        request: ExpireDelegationTokenAdminRequest,
        timeout: Duration,
    ) -> AdminExpireDelegationToken {
        let admission = match self.handle.capture_expire_delegation_token(timeout) {
            Ok(capture) => capture.try_submit(request.into_engine()),
            Err(error) => Err(error),
        };
        AdminExpireDelegationToken::from_admission(admission)
    }

    pub(crate) fn submit_describe_log_dirs(
        &self,
        request: DescribeLogDirsAdminRequest,
        timeout: Duration,
    ) -> AdminDescribeLogDirs {
        AdminDescribeLogDirs::from_admission(
            self.handle
                .try_describe_log_dirs(request.into_engine(), timeout),
        )
    }

    pub(crate) fn submit_describe_replica_log_dirs(
        &self,
        request: DescribeReplicaLogDirsAdminRequest,
        timeout: Duration,
    ) -> AdminDescribeReplicaLogDirs {
        let admission = match self.handle.capture_describe_replica_log_dirs(timeout) {
            Ok(capture) => capture.try_submit(request.into_engine()),
            Err(error) => Err(error),
        };
        AdminDescribeReplicaLogDirs::from_admission(admission)
    }

    pub(crate) fn submit_describe_metadata_quorum(
        &self,
        timeout: Duration,
    ) -> AdminDescribeMetadataQuorum {
        AdminDescribeMetadataQuorum::from_admission(
            self.handle.try_describe_metadata_quorum(timeout),
        )
    }

    pub(crate) fn submit_describe_producers(
        &self,
        request: DescribeProducersAdminRequest,
        timeout: Duration,
    ) -> AdminDescribeProducers {
        AdminDescribeProducers::from_admission(
            self.handle
                .try_describe_producers(request.into_engine(), timeout),
        )
    }

    pub(crate) fn submit_describe_transactions(
        &self,
        request: DescribeTransactionsAdminRequest,
        timeout: Duration,
    ) -> AdminDescribeTransactions {
        AdminDescribeTransactions::from_admission(
            self.handle
                .try_describe_transactions(request.into_engine(), timeout),
        )
    }

    pub(crate) fn submit_describe_topics(
        &self,
        request: DescribeTopicsAdminRequest,
        timeout: Duration,
    ) -> AdminDescribeTopics {
        AdminDescribeTopics::from_admission(
            self.handle
                .try_describe_topics(request.into_engine(), timeout),
        )
    }

    pub(crate) fn submit_describe_topics_by_id(
        &self,
        request: DescribeTopicsAdminRequest,
        timeout: Duration,
    ) -> AdminDescribeTopicsById {
        AdminDescribeTopicsById::from_admission(
            self.handle
                .try_describe_topics(request.into_engine(), timeout),
        )
    }

    pub(crate) fn submit_describe_configs(
        &self,
        request: DescribeConfigsAdminRequest,
        timeout: Duration,
    ) -> AdminDescribeConfigs {
        AdminDescribeConfigs::from_admission(
            self.handle
                .try_describe_configs(request.into_engine(), timeout),
        )
    }

    pub(crate) fn submit_describe_config_resources(
        &self,
        request: DescribeConfigsAdminRequest,
        timeout: Duration,
    ) -> AdminDescribeConfigResources {
        AdminDescribeConfigResources::from_admission(
            self.handle
                .try_describe_configs(request.into_engine(), timeout),
        )
    }

    pub(crate) fn submit_incremental_alter_configs(
        &self,
        request: IncrementalAlterConfigsAdminRequest,
        timeout: Duration,
    ) -> AdminIncrementalAlterConfigs {
        AdminIncrementalAlterConfigs::from_admission(
            self.handle
                .try_incremental_alter_configs(request.into_engine(), timeout),
        )
    }

    pub(crate) fn submit_incremental_alter_config_resources(
        &self,
        request: IncrementalAlterConfigsAdminRequest,
        timeout: Duration,
    ) -> AdminIncrementalAlterConfigResources {
        AdminIncrementalAlterConfigResources::from_admission(
            self.handle
                .try_incremental_alter_configs(request.into_engine(), timeout),
        )
    }

    pub(crate) fn submit_create_partitions(
        &self,
        request: PartitionsAdminRequest,
        timeout: Duration,
    ) -> AdminCreatePartitions {
        AdminCreatePartitions::from_admission(
            self.handle.try_create_partitions(request.inner, timeout),
        )
    }
    pub(crate) fn submit_elect_leaders(
        &self,
        request: ElectLeadersAdminRequest,
        timeout: Duration,
    ) -> AdminElectLeaders {
        AdminElectLeaders::from_admission(
            self.handle
                .try_elect_leaders(request.into_engine(), timeout),
        )
    }
}
