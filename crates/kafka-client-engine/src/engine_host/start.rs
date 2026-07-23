//! Leak-free resource handoff into one self-cleaning native host.

use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, mpsc::sync_channel},
    thread::{self, JoinHandle},
};

use crate::{
    EngineConfig,
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
    let producer = match ProducerHost::new(validated.host_limits) {
        Ok(producer) => producer,
        Err(error) => return cancel_start(sender, handle, EngineStartError::producer(&error)),
    };
    if let Some(thread_id) = producer.notifier_thread_id() {
        lifecycle.install_notifier_thread(thread_id);
    }
    let producer = ProducerShardOwner::new(producer, Arc::new(wake));
    let admission = producer.admission_port();
    let produce_calls =
        crate::driver::TrackedProduceCalls::new(validated.host_limits.batch_capacity);
    let resources = EngineHostResources {
        driver: Some(driver),
        producer,
        clock: Arc::clone(&clock),
        control: Arc::clone(&control),
        budget: validated.turn_budget,
        produce_calls,
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
