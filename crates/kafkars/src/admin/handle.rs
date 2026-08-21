//! Cloneable public admin handle over the private engine bridge.

mod abort_partition_transaction;
mod add_raft_voter;
mod alter_share_group_offsets;
mod constructor;
mod create_delegation_token;
mod delete_share_group_offsets;
mod describe_delegation_tokens;
mod describe_features;
mod describe_log_dirs;
mod describe_metadata_quorum;
mod describe_producers;
mod describe_replica_log_dirs;
mod describe_share_group;
mod describe_share_groups;
mod describe_streams_group;
mod describe_streams_groups;
mod describe_topic_partitions;
mod describe_transactions;
mod elect_leaders;
mod expire_delegation_token;
mod fence_producers;
mod force_terminate_transaction;
mod group_administration;
mod group_discovery;
mod legacy_replace_topic_configs;
mod list_client_metrics_resources;
mod list_config_resources;
mod list_offsets;
mod list_share_group_offsets;
mod list_transactions;
mod partition_reassignments;
mod remove_consumer_group_members;
mod remove_raft_voter;
mod renew_delegation_token;
mod unregister_broker;
mod update_features;

#[cfg(test)]
mod alter_share_group_offsets_test;
#[cfg(test)]
mod delete_share_group_offsets_test;
#[cfg(test)]
mod describe_share_group_test;
#[cfg(test)]
mod describe_streams_group_test;
#[cfg(test)]
mod list_config_resources_test;
#[cfg(test)]
mod list_share_group_offsets_test;
#[cfg(test)]
mod unregister_broker_test;

use crate::bridge::admin::{AdminEngine, AdminRequest, DeleteAdminRequest, PartitionsAdminRequest};
use crate::bridge::admin_alter_configs_request::IncrementalAlterConfigsAdminRequest;
use crate::bridge::admin_alter_replica_log_dirs::AlterReplicaLogDirsAdminRequest;
use crate::bridge::admin_configs_request::DescribeConfigsAdminRequest;
use crate::bridge::admin_create_acls::CreateAclsAdminRequest;
use crate::bridge::admin_delete_acls::DeleteAclsAdminRequest;
use crate::bridge::admin_delete_records::DeleteRecordsAdminRequest;
use crate::bridge::admin_describe_acls::DescribeAclsAdminRequest;
use crate::bridge::admin_topics_request::DescribeTopicsAdminRequest;
use crate::bridge::alter_client_quotas::AlterClientQuotasAdminRequest;
use crate::bridge::alter_user_scram_credentials::AlterUserScramCredentialsAdminRequest;
use crate::bridge::describe_client_quotas::DescribeClientQuotasAdminRequest;
use crate::bridge::describe_user_scram_credentials::DescribeUserScramCredentialsAdminRequest;

use super::{
    AlterClientQuotasBuilder, AlterReplicaLogDirsBuilder, AlterUserScramCredentialsBuilder,
    ConfigResourceAlterations, ConfigResourceQuery, CreateAclsBuilder, CreatePartitionsBuilder,
    CreateTopicsBuilder, DeleteAclsBuilder, DeleteRecordsBuilder, DeleteRecordsTarget,
    DeleteTopicsBuilder, DeleteTopicsByIdBuilder, DescribeAclsBuilder, DescribeClientQuotasBuilder,
    DescribeClusterBuilder, DescribeConfigResourcesBuilder, DescribeConfigsBuilder,
    DescribeTopicsBuilder, DescribeTopicsByIdBuilder, DescribeUserScramCredentialsBuilder,
    IncrementalAlterConfigResourcesBuilder, IncrementalAlterConfigsBuilder, ListTopicsBuilder,
    NewPartitions, NewTopic, ReplicaLogDirAssignment, TopicConfigAlterations, TopicConfigQuery,
};

/// Cheaply cloneable, thread-safe admin handle.
#[derive(Debug, Clone)]
pub struct Admin {
    engine: AdminEngine,
}

impl Admin {
    /// Builds inert caller-ordered ACL creation intent.
    ///
    /// No timeout starts and no operation is admitted until
    /// [`CreateAclsBuilder::submit`] is called.
    pub fn create_acls<I>(&self, bindings: I) -> CreateAclsBuilder
    where
        I: IntoIterator<Item = super::AclBinding>,
    {
        let request = CreateAclsAdminRequest::new(bindings.into_iter().collect());
        CreateAclsBuilder::new(self.engine.clone(), request, self.engine.default_timeout())
    }

    /// Builds inert caller-ordered ACL deletion filters.
    ///
    /// No timeout starts and no operation is admitted until
    /// [`DeleteAclsBuilder::submit`] is called.
    pub fn delete_acls<I>(&self, filters: I) -> DeleteAclsBuilder
    where
        I: IntoIterator<Item = super::AclBindingFilter>,
    {
        let request = DeleteAclsAdminRequest::new(filters.into_iter().collect());
        DeleteAclsBuilder::new(self.engine.clone(), request, self.engine.default_timeout())
    }

    /// Builds an inert ACL description selected by one exact filter.
    ///
    /// No timeout starts and no operation is admitted until
    /// [`DescribeAclsBuilder::submit`] is called.
    pub fn describe_acls(&self, filter: super::AclBindingFilter) -> DescribeAclsBuilder {
        let request = DescribeAclsAdminRequest::new(filter);
        DescribeAclsBuilder::new(self.engine.clone(), request, self.engine.default_timeout())
    }

    /// Builds an inert client-quota filter. An empty component set lists all quotas.
    ///
    /// No timeout starts and no operation is admitted until
    /// [`DescribeClientQuotasBuilder::submit`] is called.
    pub fn describe_client_quotas<I>(&self, components: I) -> DescribeClientQuotasBuilder
    where
        I: IntoIterator<Item = super::ClientQuotaFilterComponent>,
    {
        let request = DescribeClientQuotasAdminRequest::new(components.into_iter().collect());
        DescribeClientQuotasBuilder::new(
            self.engine.clone(),
            request,
            self.engine.default_timeout(),
        )
    }

    /// Builds an inert all-user SCRAM credential metadata query.
    ///
    /// No timeout starts and no operation is admitted until
    /// [`DescribeUserScramCredentialsBuilder::submit`] is called. Use
    /// [`DescribeUserScramCredentialsBuilder::users`] to select explicit users.
    pub fn describe_user_scram_credentials(&self) -> DescribeUserScramCredentialsBuilder {
        DescribeUserScramCredentialsBuilder::new(
            self.engine.clone(),
            DescribeUserScramCredentialsAdminRequest::new(),
            self.engine.default_timeout(),
        )
    }

    /// Builds inert caller-ordered client-quota alterations.
    ///
    /// No timeout starts and no operation is admitted until
    /// [`AlterClientQuotasBuilder::submit`] is called.
    pub fn alter_client_quotas<I>(&self, alterations: I) -> AlterClientQuotasBuilder
    where
        I: IntoIterator<Item = super::ClientQuotaAlteration>,
    {
        let request = AlterClientQuotasAdminRequest::new(alterations.into_iter().collect());
        AlterClientQuotasBuilder::new(self.engine.clone(), request, self.engine.default_timeout())
    }

    /// Builds inert caller-ordered SCRAM credential deletions and upsertions.
    ///
    /// No timeout starts and no operation is admitted until
    /// [`AlterUserScramCredentialsBuilder::submit`] is called.
    pub fn alter_user_scram_credentials<I>(
        &self,
        alterations: I,
    ) -> AlterUserScramCredentialsBuilder
    where
        I: IntoIterator<Item = super::UserScramCredentialAlteration>,
    {
        let request = AlterUserScramCredentialsAdminRequest::new(alterations.into_iter().collect());
        AlterUserScramCredentialsBuilder::new(
            self.engine.clone(),
            request,
            self.engine.default_timeout(),
        )
    }

    /// Builds inert caller-ordered broker-local replica log-directory assignments.
    ///
    /// No timeout starts and no operation is admitted until
    /// [`AlterReplicaLogDirsBuilder::submit`] is called.
    pub fn alter_replica_log_dirs<I>(&self, assignments: I) -> AlterReplicaLogDirsBuilder
    where
        I: IntoIterator<Item = ReplicaLogDirAssignment>,
    {
        let request = AlterReplicaLogDirsAdminRequest::new(assignments.into_iter().collect());
        AlterReplicaLogDirsBuilder::new(self.engine.clone(), request, self.engine.default_timeout())
    }

    /// Builds an inert ordered `CreateTopics` request.
    ///
    /// No timeout starts and no operation is admitted until
    /// [`CreateTopicsBuilder::submit`] is called.
    pub fn create_topics<I>(&self, topics: I) -> CreateTopicsBuilder
    where
        I: IntoIterator<Item = NewTopic>,
    {
        let request = AdminRequest::from_topics(topics);
        CreateTopicsBuilder::new(self.engine.clone(), request, self.engine.default_timeout())
    }

    /// Builds an inert ordered name-based `DeleteTopics` request.
    ///
    /// No timeout starts and no operation is admitted until
    /// [`DeleteTopicsBuilder::submit`] is called.
    pub fn delete_topics<I, T>(&self, topics: I) -> DeleteTopicsBuilder
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        let request = DeleteAdminRequest::from_topics(topics);
        DeleteTopicsBuilder::new(self.engine.clone(), request, self.engine.default_timeout())
    }

    /// Builds an inert caller-ordered topic-ID deletion request.
    ///
    /// The all-zero protocol sentinel and duplicate IDs are rejected at
    /// submission. No timeout starts and no destructive work is admitted until
    /// [`DeleteTopicsByIdBuilder::submit`] is called.
    pub fn delete_topics_by_id<I>(&self, topic_ids: I) -> DeleteTopicsByIdBuilder
    where
        I: IntoIterator<Item = [u8; 16]>,
    {
        let request = DeleteAdminRequest::from_topic_ids(topic_ids);
        DeleteTopicsByIdBuilder::new(self.engine.clone(), request, self.engine.default_timeout())
    }

    /// Builds an inert broker-endpoint `DescribeCluster` request.
    ///
    /// No timeout starts and no operation is admitted until
    /// [`DescribeClusterBuilder::submit`] is called.
    pub fn describe_cluster(&self) -> DescribeClusterBuilder {
        DescribeClusterBuilder::new(self.engine.clone(), self.engine.default_timeout())
    }

    /// Builds an inert ordered name-based `DescribeTopics` request.
    ///
    /// No timeout starts and no operation is admitted until
    /// [`DescribeTopicsBuilder::submit`] is called.
    pub fn describe_topics<I, T>(&self, topics: I) -> DescribeTopicsBuilder
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        let request = DescribeTopicsAdminRequest::from_topics(topics);
        DescribeTopicsBuilder::new(self.engine.clone(), request, self.engine.default_timeout())
    }

    /// Builds an inert ordered topic-ID `DescribeTopics` request.
    ///
    /// The all-zero protocol sentinel and duplicate IDs are rejected at
    /// submission. No timeout starts and no operation is admitted until
    /// [`DescribeTopicsByIdBuilder::submit`] is called.
    pub fn describe_topics_by_id<I>(&self, topic_ids: I) -> DescribeTopicsByIdBuilder
    where
        I: IntoIterator<Item = [u8; 16]>,
    {
        let request = DescribeTopicsAdminRequest::from_topic_ids(topic_ids);
        DescribeTopicsByIdBuilder::new(self.engine.clone(), request, self.engine.default_timeout())
    }

    /// Builds an inert query for topics visible to the authenticated principal.
    ///
    /// Internal topics are excluded by default. No timeout starts and no
    /// operation is admitted until [`ListTopicsBuilder::submit`] is called.
    pub fn list_topics(&self) -> ListTopicsBuilder {
        let request = DescribeTopicsAdminRequest::all(false);
        ListTopicsBuilder::new(self.engine.clone(), request, self.engine.default_timeout())
    }

    /// Builds an inert ordered topic `DescribeConfigs` request.
    ///
    /// String items request all configurations. [`TopicConfigQuery`] items can
    /// select an ordered set of keys per topic. No timeout starts and no
    /// operation is admitted until [`DescribeConfigsBuilder::submit`] is called.
    pub fn describe_configs<I, T>(&self, topics: I) -> DescribeConfigsBuilder
    where
        I: IntoIterator<Item = T>,
        T: Into<TopicConfigQuery>,
    {
        let request = DescribeConfigsAdminRequest::from_topics(topics);
        DescribeConfigsBuilder::new(self.engine.clone(), request, self.engine.default_timeout())
    }

    /// Builds an inert ordered resource-generic `DescribeConfigs` request.
    ///
    /// Known and future positive [`super::ConfigResourceType`] codes remain
    /// representable. Validation and bounded admission occur only when
    /// [`DescribeConfigResourcesBuilder::submit`] captures one public deadline.
    pub fn describe_config_resources<I, T>(&self, resources: I) -> DescribeConfigResourcesBuilder
    where
        I: IntoIterator<Item = T>,
        T: Into<ConfigResourceQuery>,
    {
        let request = DescribeConfigsAdminRequest::from_resources(resources);
        DescribeConfigResourcesBuilder::new(
            self.engine.clone(),
            request,
            self.engine.default_timeout(),
        )
    }

    /// Builds an inert ordered topic `IncrementalAlterConfigs` request.
    ///
    /// Empty or duplicate topics and keys remain inert input until
    /// [`IncrementalAlterConfigsBuilder::submit`] captures the public absolute
    /// deadline and attempts bounded engine admission.
    pub fn incremental_alter_configs<I>(&self, topics: I) -> IncrementalAlterConfigsBuilder
    where
        I: IntoIterator<Item = TopicConfigAlterations>,
    {
        let request = IncrementalAlterConfigsAdminRequest::from_topics(topics);
        IncrementalAlterConfigsBuilder::new(
            self.engine.clone(),
            request,
            self.engine.default_timeout(),
        )
    }

    /// Builds inert caller-ordered generic `IncrementalAlterConfigs` intent.
    ///
    /// Known and future positive resource types remain exact. Validation,
    /// deadline capture, and bounded admission occur only at
    /// [`IncrementalAlterConfigResourcesBuilder::submit`].
    pub fn incremental_alter_config_resources<I>(
        &self,
        resources: I,
    ) -> IncrementalAlterConfigResourcesBuilder
    where
        I: IntoIterator<Item = ConfigResourceAlterations>,
    {
        let request = IncrementalAlterConfigsAdminRequest::from_resources(resources);
        IncrementalAlterConfigResourcesBuilder::new(
            self.engine.clone(),
            request,
            self.engine.default_timeout(),
        )
    }

    /// Builds an inert automatic or explicit-placement `CreatePartitions` request.
    ///
    /// No timeout starts and no operation is admitted until
    /// [`CreatePartitionsBuilder::submit`] is called.
    pub fn create_partitions<I>(&self, topics: I) -> CreatePartitionsBuilder
    where
        I: IntoIterator<Item = NewPartitions>,
    {
        let request = PartitionsAdminRequest::from_topics(topics);
        CreatePartitionsBuilder::new(self.engine.clone(), request, self.engine.default_timeout())
    }

    /// Builds an inert caller-ordered record-deletion request.
    ///
    /// No timeout starts and no destructive operation is admitted until
    /// [`DeleteRecordsBuilder::submit`] is called.
    pub fn delete_records<I>(&self, targets: I) -> DeleteRecordsBuilder
    where
        I: IntoIterator<Item = DeleteRecordsTarget>,
    {
        let request = DeleteRecordsAdminRequest::new(targets.into_iter().collect());
        DeleteRecordsBuilder::new(self.engine.clone(), request, self.engine.default_timeout())
    }
}
