//! Consumer, Streams, share, and generic group administration entry points.

use super::Admin;
use crate::{
    TopicPartition,
    admin::{
        AlterConsumerGroupOffsetsBuilder, AlterStreamsGroupOffsetsBuilder,
        ConsumerGroupOffsetAlteration, DeleteConsumerGroupOffsetsBuilder,
        DeleteConsumerGroupsBuilder, DeleteShareGroupsBuilder, DeleteStreamsGroupOffsetsBuilder,
        DeleteStreamsGroupsBuilder, ListConsumerGroupOffsetsBuilder, ListConsumerGroupOffsetsQuery,
        ListConsumerGroupsOffsetsBuilder, ListStreamsGroupOffsetsBuilder,
        ListStreamsGroupOffsetsQuery, ListStreamsGroupsOffsetsBuilder,
    },
    bridge::{
        admin_delete_consumer_groups::DeleteConsumerGroupsAdminRequest,
        admin_group_offset_delete_request::DeleteConsumerGroupOffsetsAdminRequest,
        admin_group_offsets::{
            AlterConsumerGroupOffsetsAdminRequest, ListConsumerGroupOffsetsAdminRequest,
            ListConsumerGroupsOffsetsAdminRequest,
        },
    },
};

impl Admin {
    /// Builds an inert all-partition committed-offset query for one group.
    ///
    /// [`ListConsumerGroupOffsetsBuilder::partitions`] narrows the query to a
    /// caller-ordered explicit selection. Stable offsets are not required by
    /// default. No timeout starts and no operation is admitted until
    /// [`ListConsumerGroupOffsetsBuilder::submit`] is called.
    pub fn list_consumer_group_offsets(
        &self,
        group_id: impl Into<String>,
    ) -> ListConsumerGroupOffsetsBuilder {
        let request = ListConsumerGroupOffsetsAdminRequest::all(group_id.into());
        ListConsumerGroupOffsetsBuilder::new(
            self.engine.clone(),
            request,
            self.engine.default_timeout(),
        )
    }

    /// Builds one inert caller-ordered offset query for multiple consumer groups.
    ///
    /// Each [`ListConsumerGroupOffsetsQuery`] independently selects all or an
    /// explicit caller-ordered topic-partition set. Plain string items remain
    /// shorthand for all partitions. The accepted operation routes one
    /// explicit singleton request to each group's coordinator under the
    /// original public deadline. No timeout starts and no work is admitted until
    /// [`ListConsumerGroupsOffsetsBuilder::submit`] is called.
    pub fn list_consumer_groups_offsets<I, Q>(&self, queries: I) -> ListConsumerGroupsOffsetsBuilder
    where
        I: IntoIterator<Item = Q>,
        Q: Into<ListConsumerGroupOffsetsQuery>,
    {
        let request = ListConsumerGroupsOffsetsAdminRequest::new(
            queries.into_iter().map(Into::into).collect(),
        );
        ListConsumerGroupsOffsetsBuilder::new(
            self.engine.clone(),
            request,
            self.engine.default_timeout(),
        )
    }

    /// Builds an inert committed-offset query for one streams group.
    ///
    /// Kafka defines this operation over the consumer-group `OffsetFetch`
    /// path. [`ListStreamsGroupOffsetsBuilder::partitions`] selects explicit
    /// topic-partitions. No timeout starts and no operation is admitted until
    /// [`ListStreamsGroupOffsetsBuilder::submit`] is called.
    pub fn list_streams_group_offsets(
        &self,
        group_id: impl Into<String>,
    ) -> ListStreamsGroupOffsetsBuilder {
        ListStreamsGroupOffsetsBuilder::from_consumer_group(
            self.list_consumer_group_offsets(group_id),
        )
    }

    /// Builds one inert caller-ordered committed-offset query for multiple Streams groups.
    ///
    /// Kafka defines this over the consumer-group `OffsetFetch` path. Each
    /// [`ListStreamsGroupOffsetsQuery`] selects all or explicit partitions;
    /// plain strings remain all-partition shorthand. No timeout starts and no
    /// work is admitted until
    /// [`ListStreamsGroupsOffsetsBuilder::submit`] is called.
    pub fn list_streams_groups_offsets<I, Q>(&self, queries: I) -> ListStreamsGroupsOffsetsBuilder
    where
        I: IntoIterator<Item = Q>,
        Q: Into<ListStreamsGroupOffsetsQuery>,
    {
        let consumer_queries = queries
            .into_iter()
            .map(|query| query.into().into_consumer_group());
        ListStreamsGroupsOffsetsBuilder::from_consumer_groups(
            self.list_consumer_groups_offsets(consumer_queries),
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

    /// Builds an inert caller-ordered offset deletion for one streams group.
    ///
    /// Kafka defines this operation over the consumer-group `OffsetDelete`
    /// path. No timeout starts and no operation is admitted until
    /// [`DeleteStreamsGroupOffsetsBuilder::submit`] is called.
    pub fn delete_streams_group_offsets<I>(
        &self,
        group_id: impl Into<String>,
        targets: I,
    ) -> DeleteStreamsGroupOffsetsBuilder
    where
        I: IntoIterator<Item = TopicPartition>,
    {
        DeleteStreamsGroupOffsetsBuilder::from_consumer(
            self.delete_consumer_group_offsets(group_id, targets),
        )
    }

    /// Builds an inert caller-ordered consumer-group deletion request.
    ///
    /// No timeout starts and no destructive call is admitted until
    /// [`DeleteConsumerGroupsBuilder::submit`] is called.
    pub fn delete_consumer_groups<I, T>(&self, group_ids: I) -> DeleteConsumerGroupsBuilder
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        let request =
            DeleteConsumerGroupsAdminRequest::new(group_ids.into_iter().map(Into::into).collect());
        DeleteConsumerGroupsBuilder::new(
            self.engine.clone(),
            request,
            self.engine.default_timeout(),
        )
    }

    /// Builds an inert caller-ordered share-group deletion request.
    ///
    /// Kafka defines share-group deletion over the common `DeleteGroups`
    /// path. No timeout starts and no destructive call is admitted until
    /// [`DeleteShareGroupsBuilder::submit`] is called.
    pub fn delete_share_groups<I, T>(&self, group_ids: I) -> DeleteShareGroupsBuilder
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        DeleteShareGroupsBuilder::from_consumer(self.delete_consumer_groups(group_ids))
    }

    /// Builds an inert caller-ordered streams-group deletion request.
    ///
    /// Kafka defines streams-group deletion over the common `DeleteGroups`
    /// path. No timeout starts and no destructive call is admitted until
    /// [`DeleteStreamsGroupsBuilder::submit`] is called.
    pub fn delete_streams_groups<I, T>(&self, group_ids: I) -> DeleteStreamsGroupsBuilder
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        DeleteStreamsGroupsBuilder::from_consumer(self.delete_consumer_groups(group_ids))
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

    /// Builds an inert caller-ordered offset alteration for one streams group.
    ///
    /// Kafka defines this operation over the consumer-group `OffsetCommit`
    /// path. No timeout starts and no operation is admitted until
    /// [`AlterStreamsGroupOffsetsBuilder::submit`] is called.
    pub fn alter_streams_group_offsets<I>(
        &self,
        group_id: impl Into<String>,
        alterations: I,
    ) -> AlterStreamsGroupOffsetsBuilder
    where
        I: IntoIterator<Item = ConsumerGroupOffsetAlteration>,
    {
        AlterStreamsGroupOffsetsBuilder::from_consumer_group(
            self.alter_consumer_group_offsets(group_id, alterations),
        )
    }
}
