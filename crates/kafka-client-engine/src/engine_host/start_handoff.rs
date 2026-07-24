//! Startup capabilities and rollback joining for the native engine host.

use std::{
    sync::{Arc, mpsc::SyncSender},
    thread::JoinHandle,
};

use crate::{
    admin::{
        CreatePartitionsAdmissionPort, CreateTopicsAdmissionPort, DeleteTopicsAdmissionPort,
        DescribeClusterAdmissionPort, DescribeConfigsAdmissionPort, DescribeTopicsAdmissionPort,
    },
    clock::MonotonicClock,
    consumer::AssignedConsumerPort,
    producer::ingress::ProducerAdmissionPort,
};

use super::{EngineHostControl, EngineHostResources, EngineLifecycle, EngineStartError};

pub(crate) struct StartedEngineHost {
    pub(crate) admission: ProducerAdmissionPort,
    pub(crate) create_topics_admission: CreateTopicsAdmissionPort,
    pub(crate) delete_topics_admission: DeleteTopicsAdmissionPort,
    pub(crate) describe_cluster_admission: DescribeClusterAdmissionPort,
    pub(crate) create_partitions_admission: CreatePartitionsAdmissionPort,
    pub(crate) describe_topics_admission: DescribeTopicsAdmissionPort,
    pub(crate) describe_configs_admission: DescribeConfigsAdmissionPort,
    pub(crate) assigned_consumer: AssignedConsumerPort,
    pub(crate) clock: Arc<MonotonicClock>,
    pub(crate) control: Arc<EngineHostControl>,
    pub(crate) lifecycle: Arc<EngineLifecycle>,
}

pub(super) fn cancel_start(
    sender: SyncSender<EngineHostResources>,
    handle: JoinHandle<()>,
    error: EngineStartError,
) -> Result<StartedEngineHost, EngineStartError> {
    drop(sender);
    join_cancelled(handle);
    Err(error)
}

pub(super) fn join_cancelled(handle: JoinHandle<()>) {
    let _join_result = handle.join();
}
