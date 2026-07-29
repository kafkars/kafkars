//! Consumer and generic group description and discovery entry points.

use super::Admin;
use crate::{
    admin::{
        DescribeClassicGroupsBuilder, DescribeConsumerGroupsBuilder, ListConsumerGroupsBuilder,
        ListGroupsBuilder,
    },
    bridge::admin_describe_consumer_groups::DescribeConsumerGroupsAdminRequest,
};

impl Admin {
    /// Builds an inert caller-ordered classic-group description.
    ///
    /// This path uses Kafka's classic `DescribeGroups` API directly. Authorized
    /// operations are omitted by default. No timeout starts and no operation is
    /// admitted until [`DescribeClassicGroupsBuilder::submit`] is called.
    pub fn describe_classic_groups<I, T>(&self, groups: I) -> DescribeClassicGroupsBuilder
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        let request =
            DescribeConsumerGroupsAdminRequest::new(groups.into_iter().map(Into::into).collect());
        DescribeClassicGroupsBuilder::new(
            self.engine.clone(),
            request,
            self.engine.default_timeout(),
        )
    }

    /// Builds an inert caller-ordered classic consumer-group description.
    ///
    /// Authorized operations are omitted by default. No timeout starts and no
    /// operation is admitted until [`DescribeConsumerGroupsBuilder::submit`] is
    /// called.
    pub fn describe_consumer_groups<I, T>(&self, groups: I) -> DescribeConsumerGroupsBuilder
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        let request =
            DescribeConsumerGroupsAdminRequest::new(groups.into_iter().map(Into::into).collect());
        DescribeConsumerGroupsBuilder::new(
            self.engine.clone(),
            request,
            self.engine.default_timeout(),
        )
    }

    /// Builds an inert cluster-wide consumer-group listing request.
    ///
    /// No timeout starts and no operation is admitted until
    /// [`ListConsumerGroupsBuilder::submit`] is called.
    pub fn list_consumer_groups(&self) -> ListConsumerGroupsBuilder {
        ListConsumerGroupsBuilder::new(self.engine.clone(), self.engine.default_timeout())
    }

    /// Builds an inert unfiltered cluster-wide group listing request.
    ///
    /// Unlike the legacy consumer-only view, this retains every broker-reported
    /// group type. No timeout starts and no operation is admitted until
    /// [`ListGroupsBuilder::submit`] is called.
    pub fn list_groups(&self) -> ListGroupsBuilder {
        ListGroupsBuilder::new(self.engine.clone(), self.engine.default_timeout())
    }
}
