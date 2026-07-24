//! Leak-free construction and rollback of the independent admin hosts.

use crate::admin::{CreateTopicsHost, DeleteTopicsHost, DescribeClusterHost};

use super::EngineStartError;

pub(super) struct AdminHosts {
    pub(super) create_topics: CreateTopicsHost,
    pub(super) delete_topics: DeleteTopicsHost,
    pub(super) describe_cluster: DescribeClusterHost,
}

impl AdminHosts {
    pub(super) fn start() -> Result<Self, EngineStartError> {
        let mut create_topics =
            CreateTopicsHost::new().map_err(|error| EngineStartError::create_topics(&error))?;
        let mut delete_topics = match DeleteTopicsHost::new() {
            Ok(owner) => owner,
            Err(error) => {
                join_create_topics_notifier(&mut create_topics);
                return Err(EngineStartError::delete_topics(&error));
            }
        };
        let describe_cluster = match DescribeClusterHost::new() {
            Ok(owner) => owner,
            Err(error) => {
                join_create_topics_notifier(&mut create_topics);
                join_delete_topics_notifier(&mut delete_topics);
                return Err(EngineStartError::describe_cluster(&error));
            }
        };
        Ok(Self {
            create_topics,
            delete_topics,
            describe_cluster,
        })
    }

    pub(super) fn join_notifiers(&mut self) {
        join_create_topics_notifier(&mut self.create_topics);
        join_delete_topics_notifier(&mut self.delete_topics);
        if let Some(notifier) = self.describe_cluster.recover_notifier() {
            let _join_result = notifier.join_off_notifier();
        }
    }
}

fn join_create_topics_notifier(host: &mut CreateTopicsHost) {
    if let Some(notifier) = host.recover_notifier() {
        let _join_result = notifier.join_off_notifier();
    }
}

fn join_delete_topics_notifier(host: &mut DeleteTopicsHost) {
    if let Some(notifier) = host.recover_notifier() {
        let _join_result = notifier.join_off_notifier();
    }
}
