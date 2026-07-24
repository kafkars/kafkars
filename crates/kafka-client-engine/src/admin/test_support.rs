//! Shared-admin-notifier ownership helpers for concrete host scenarios.

use super::{
    CreatePartitionsHost, CreateTopicsHost, DeleteTopicsHost, DescribeClusterHost,
    DescribeConfigsHost, DescribeTopicsHost, IncrementalAlterConfigsHost,
    completion::{AdminCompletionNotifier, AdminCompletionPorts},
};

pub(super) fn create_topics_host() -> (CreateTopicsHost, AdminCompletionNotifier) {
    let (notifier, ports) = completion_owner();
    (CreateTopicsHost::new(ports.create_topics), notifier)
}

pub(super) fn delete_topics_host() -> (DeleteTopicsHost, AdminCompletionNotifier) {
    let (notifier, ports) = completion_owner();
    (DeleteTopicsHost::new(ports.delete_topics), notifier)
}

pub(super) fn describe_cluster_host() -> (DescribeClusterHost, AdminCompletionNotifier) {
    let (notifier, ports) = completion_owner();
    (DescribeClusterHost::new(ports.describe_cluster), notifier)
}

pub(super) fn create_partitions_host() -> (CreatePartitionsHost, AdminCompletionNotifier) {
    let (notifier, ports) = completion_owner();
    (CreatePartitionsHost::new(ports.create_partitions), notifier)
}

pub(super) fn describe_topics_host() -> (DescribeTopicsHost, AdminCompletionNotifier) {
    let (notifier, ports) = completion_owner();
    (DescribeTopicsHost::new(ports.describe_topics), notifier)
}

pub(super) fn describe_configs_host() -> (DescribeConfigsHost, AdminCompletionNotifier) {
    let (notifier, ports) = completion_owner();
    (DescribeConfigsHost::new(ports.describe_configs), notifier)
}

pub(super) fn incremental_alter_configs_host()
-> (IncrementalAlterConfigsHost, AdminCompletionNotifier) {
    let (notifier, ports) = completion_owner();
    (
        IncrementalAlterConfigsHost::new(ports.incremental_alter_configs),
        notifier,
    )
}

pub(super) fn completion_owner() -> (AdminCompletionNotifier, AdminCompletionPorts) {
    AdminCompletionNotifier::start()
        .unwrap_or_else(|error| panic!("start shared admin notifier: {error}"))
}

pub(super) fn stop_notifier(mut notifier: AdminCompletionNotifier) {
    let join = notifier
        .stop()
        .unwrap_or_else(|error| panic!("stop shared admin notifier: {error}"));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("join shared admin notifier: {error}"));
}
