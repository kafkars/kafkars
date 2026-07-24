//! Leak-free resource handoff into one self-cleaning native host.

use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, mpsc::sync_channel},
    thread::{self, JoinHandle},
};

use crate::{
    EngineConfig,
    admin::{
        AdminCompletionNotifier, AdminCompletionPorts, CreateTopicsHost, CreateTopicsShardOwner,
        DeleteTopicsHost, DeleteTopicsShardOwner, DescribeClusterHost, DescribeClusterShardOwner,
    },
    clock::MonotonicClock,
    config::ValidatedEngineConfig,
    driver::DriverOwner,
    producer::{
        ProducerHost,
        ingress::{ProducerAdmissionPort, ProducerShardOwner},
    },
};

use super::{
    EngineHostControl, EngineHostError, EngineHostExit, EngineHostResources, EngineLifecycle,
    EngineStartError, recover, run,
};

const HOST_THREAD_NAME: &str = "kafka-client-engine";

pub(crate) struct StartedEngineHost {
    pub(crate) admission: ProducerAdmissionPort,
    pub(crate) create_topics_admission: crate::admin::CreateTopicsAdmissionPort,
    pub(crate) delete_topics_admission: crate::admin::DeleteTopicsAdmissionPort,
    pub(crate) describe_cluster_admission: crate::admin::DescribeClusterAdmissionPort,
    pub(crate) clock: Arc<MonotonicClock>,
    pub(crate) control: Arc<EngineHostControl>,
    pub(crate) lifecycle: Arc<EngineLifecycle>,
}

pub(crate) fn start(
    config: &EngineConfig,
    validated: ValidatedEngineConfig,
) -> Result<StartedEngineHost, EngineStartError> {
    let lifecycle = Arc::new(EngineLifecycle::new());
    let host_lifecycle = Arc::clone(&lifecycle);
    let (sender, receiver) = sync_channel::<EngineHostResources>(1);
    let handle = thread::Builder::new()
        .name(HOST_THREAD_NAME.to_owned())
        .spawn(move || match receiver.recv() {
            Ok(resources) => finish_host(resources, &host_lifecycle),
            Err(_) => host_lifecycle.publish(None),
        })
        .map_err(|error| EngineStartError::host_thread(&error))?;

    let driver = match DriverOwner::build(config) {
        Ok(driver) => driver,
        Err(error) => return cancel_start(sender, handle, EngineStartError::driver(&error)),
    };
    let clock = Arc::new(MonotonicClock::new());
    let wake = driver.reactor_wake();
    let control = Arc::new(EngineHostControl::new(wake.clone()));
    let (mut admin_notifier, admin_ports) = match AdminCompletionNotifier::start() {
        Ok(owner) => owner,
        Err(error) => {
            return cancel_start(sender, handle, EngineStartError::admin_notifier(&error));
        }
    };
    let AdminCompletionPorts {
        create_topics,
        delete_topics,
        describe_cluster,
    } = admin_ports;
    let create_topics = CreateTopicsHost::new(create_topics);
    let delete_topics = DeleteTopicsHost::new(delete_topics);
    let describe_cluster = DescribeClusterHost::new(describe_cluster);
    let producer = match ProducerHost::new(validated.host_limits) {
        Ok(producer) => producer,
        Err(error) => {
            if let Some(notifier) = admin_notifier.take_join() {
                let _join_result = notifier.join_off_notifier();
            }
            return cancel_start(sender, handle, EngineStartError::producer(&error));
        }
    };
    if let Some(thread_id) = producer.notifier_thread_id() {
        lifecycle.install_notifier_thread(thread_id);
    }
    let producer = ProducerShardOwner::new(producer, Arc::new(wake));
    let admission = producer.admission_port();
    if let Some(thread_id) = admin_notifier.thread_id() {
        lifecycle.install_notifier_thread(thread_id);
    }
    let create_topics = CreateTopicsShardOwner::new(create_topics, Arc::new(driver.reactor_wake()));
    let create_topics_admission = create_topics.admission_port();
    let delete_topics = DeleteTopicsShardOwner::new(delete_topics, Arc::new(driver.reactor_wake()));
    let delete_topics_admission = delete_topics.admission_port();
    let describe_cluster =
        DescribeClusterShardOwner::new(describe_cluster, Arc::new(driver.reactor_wake()));
    let describe_cluster_admission = describe_cluster.admission_port();
    let produce_calls =
        crate::driver::TrackedProduceCalls::new(validated.host_limits.batch_capacity);
    let resources = EngineHostResources {
        driver: Some(driver),
        producer,
        admin_notifier,
        create_topics,
        delete_topics,
        describe_cluster,
        clock: Arc::clone(&clock),
        control: Arc::clone(&control),
        budget: validated.turn_budget,
        produce_calls,
        create_topics_calls: crate::driver::TrackedCreateTopicsCalls::new(
            crate::admin::CREATE_TOPICS_CAPACITY,
        ),
        delete_topics_calls: crate::driver::TrackedDeleteTopicsCalls::new(
            crate::admin::DELETE_TOPICS_CAPACITY,
        ),
        describe_cluster_calls: crate::driver::DescribeClusterCalls::new(
            crate::admin::DESCRIBE_CLUSTER_CAPACITY,
        ),
    };
    if let Err(error) = sender.send(resources) {
        control.request_shutdown();
        finish_host(error.0, &lifecycle);
        join_cancelled(handle);
        return Err(EngineStartError::handoff());
    }

    // The host self-cleans and publishes a retained terminal report after
    // joining its notifier. External shutdown observes that report, not this
    // operating-system join token.
    drop(handle);
    Ok(StartedEngineHost {
        admission,
        create_topics_admission,
        delete_topics_admission,
        describe_cluster_admission,
        clock,
        control,
        lifecycle,
    })
}

fn cancel_start(
    sender: std::sync::mpsc::SyncSender<EngineHostResources>,
    handle: JoinHandle<()>,
    error: EngineStartError,
) -> Result<StartedEngineHost, EngineStartError> {
    drop(sender);
    join_cancelled(handle);
    Err(error)
}

fn finish_host(mut resources: EngineHostResources, lifecycle: &EngineLifecycle) {
    publish_caught(lifecycle, move || {
        let outcome = catch_unwind(AssertUnwindSafe(|| run(&mut resources)));
        let exit = match outcome {
            Ok(Ok(exit)) => exit,
            Ok(Err(error)) => recover(&mut resources, error),
            Err(_panic) => recover(&mut resources, EngineHostError::HostPanicked),
        };
        let failure = finalize_exit(exit);
        drop(resources);
        failure
    });
}

pub(super) fn publish_caught(
    lifecycle: &EngineLifecycle,
    finalize: impl FnOnce() -> Option<EngineHostError>,
) {
    let outcome = catch_unwind(AssertUnwindSafe(finalize));
    match outcome {
        Ok(failure) => lifecycle.publish(failure.as_ref()),
        Err(_panic) => lifecycle.publish(Some(&EngineHostError::HostPanicked)),
    }
}

pub(super) fn finalize_exit(mut exit: EngineHostExit) -> Option<EngineHostError> {
    let mut failure = exit.failure.take();
    if let Err(cleanup) = exit.notifier.join_off_notifier() {
        failure = Some(attach_cleanup(failure, EngineHostError::Notifier(cleanup)));
    }
    failure
}

fn attach_cleanup(primary: Option<EngineHostError>, cleanup: EngineHostError) -> EngineHostError {
    match primary {
        Some(primary) => primary.with_cleanup(cleanup),
        None => cleanup,
    }
}

fn join_cancelled(handle: JoinHandle<()>) {
    let _join_result = handle.join();
}
