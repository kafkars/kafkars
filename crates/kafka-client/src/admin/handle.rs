//! Cloneable public admin handle over the private engine bridge.

mod list_offsets;
mod partition_reassignments;

use crate::TopicPartition;
use crate::bridge::admin::{AdminEngine, AdminRequest, DeleteAdminRequest, PartitionsAdminRequest};
use crate::bridge::admin_alter_configs_request::IncrementalAlterConfigsAdminRequest;
use crate::bridge::admin_configs_request::DescribeConfigsAdminRequest;
use crate::bridge::admin_group_offset_delete_request::DeleteConsumerGroupOffsetsAdminRequest;
use crate::bridge::admin_group_offsets::{
    AlterConsumerGroupOffsetsAdminRequest, ListConsumerGroupOffsetsAdminRequest,
};
use crate::bridge::admin_topics_request::DescribeTopicsAdminRequest;

use super::{
    AlterConsumerGroupOffsetsBuilder, ConsumerGroupOffsetAlteration, CreatePartitionsBuilder,
    CreateTopicsBuilder, DeleteConsumerGroupOffsetsBuilder, DeleteTopicsBuilder,
    DescribeClusterBuilder, DescribeConfigsBuilder, DescribeTopicsBuilder,
    IncrementalAlterConfigsBuilder, ListConsumerGroupOffsetsBuilder, ListTopicsBuilder,
    NewPartitions, NewTopic, TopicConfigAlterations, TopicConfigQuery,
};

/// Cheaply cloneable, thread-safe admin handle.
#[derive(Debug, Clone)]
pub struct Admin {
    engine: AdminEngine,
}

impl Admin {
    pub(crate) const fn new(engine: AdminEngine) -> Self {
        Self { engine }
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

    /// Builds an inert query for topics visible to the authenticated principal.
    ///
    /// Internal topics are excluded by default. No timeout starts and no
    /// operation is admitted until [`ListTopicsBuilder::submit`] is called.
    pub fn list_topics(&self) -> ListTopicsBuilder {
        let request = DescribeTopicsAdminRequest::all(false);
        ListTopicsBuilder::new(self.engine.clone(), request, self.engine.default_timeout())
    }

    /// Builds an inert all-partition committed-offset query for one group.
    ///
    /// Stable offsets are not required by default. No timeout starts and no
    /// operation is admitted until [`ListConsumerGroupOffsetsBuilder::submit`]
    /// is called.
    pub fn list_consumer_group_offsets(
        &self,
        group_id: impl Into<String>,
    ) -> ListConsumerGroupOffsetsBuilder {
        let request = ListConsumerGroupOffsetsAdminRequest::new(group_id.into());
        ListConsumerGroupOffsetsBuilder::new(
            self.engine.clone(),
            request,
            self.engine.default_timeout(),
        )
    }

    /// Builds an inert caller-ordered committed-offset deletion for one group.
    ///
    /// [`TopicPartition::start_at`](crate::TopicPartition::start_at) is
    /// assignment-only and causes a definitely-unsent configuration rejection
    /// at [`DeleteConsumerGroupOffsetsBuilder::submit`]. No timeout starts and
    /// no operation is admitted before that submission boundary.
    pub fn delete_consumer_group_offsets<I>(
        &self,
        group_id: impl Into<String>,
        targets: I,
    ) -> DeleteConsumerGroupOffsetsBuilder
    where
        I: IntoIterator<Item = TopicPartition>,
    {
        let request = DeleteConsumerGroupOffsetsAdminRequest::new(
            group_id.into(),
            targets.into_iter().collect(),
        );
        DeleteConsumerGroupOffsetsBuilder::new(
            self.engine.clone(),
            request,
            self.engine.default_timeout(),
        )
    }

    /// Builds an inert caller-ordered committed-offset alteration for one group.
    ///
    /// Invalid offsets, epochs, identities, and duplicate topic-partitions
    /// remain inert until [`AlterConsumerGroupOffsetsBuilder::submit`] captures
    /// the public absolute deadline and attempts bounded engine admission.
    pub fn alter_consumer_group_offsets<I>(
        &self,
        group_id: impl Into<String>,
        alterations: I,
    ) -> AlterConsumerGroupOffsetsBuilder
    where
        I: IntoIterator<Item = ConsumerGroupOffsetAlteration>,
    {
        let request = AlterConsumerGroupOffsetsAdminRequest::new(
            group_id.into(),
            alterations.into_iter().collect(),
        );
        AlterConsumerGroupOffsetsBuilder::new(
            self.engine.clone(),
            request,
            self.engine.default_timeout(),
        )
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

    /// Builds an inert automatic-assignment `CreatePartitions` request.
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
}
