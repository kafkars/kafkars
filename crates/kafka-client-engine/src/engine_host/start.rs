//! Leak-free startup handoff and off-reactor host joining.

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
    EngineHostControl, EngineHostError, EngineHostExit, EngineHostResources, EngineStartError,
    recover, run,
};

const HOST_THREAD_NAME: &str = "kafka-client-engine";
const REAPER_THREAD_NAME: &str = "kafka-client-engine-reaper";

pub(crate) struct StartedEngineHost {
    pub(crate) admission: ProducerAdmissionPort,
    pub(crate) clock: Arc<MonotonicClock>,
    pub(crate) join: EngineHostJoin,
}

pub(crate) fn start(
    config: &EngineConfig,
    validated: ValidatedEngineConfig,
) -> Result<StartedEngineHost, EngineStartError> {
    let (sender, receiver) = sync_channel::<EngineHostResources>(1);
    let handle = thread::Builder::new()
        .name(HOST_THREAD_NAME.to_owned())
        .spawn(move || match receiver.recv() {
            Ok(mut resources) => {
                let outcome = catch_unwind(AssertUnwindSafe(|| run(&mut resources)));
                match outcome {
                    Ok(Ok(exit)) => Ok(Some(exit)),
                    Ok(Err(error)) => recover(&mut resources, error).map(Some),
                    Err(_panic) => recover(&mut resources, EngineHostError::HostPanicked).map(Some),
                }
            }
            Err(_) => Ok(None),
        })
        .map_err(|error| EngineStartError::host_thread(&error))?;
    let driver = match DriverOwner::build(config) {
        Ok(driver) => driver,
        Err(error) => {
            drop(sender);
            join_cancelled(handle);
            return Err(EngineStartError::driver(&error));
        }
    };
    let clock = Arc::new(MonotonicClock::new());
    let wake = driver.producer_wake();
    let control = Arc::new(EngineHostControl::new(wake.clone()));
    let producer = match ProducerHost::new(validated.host_limits) {
        Ok(producer) => producer,
        Err(error) => {
            drop(sender);
            join_cancelled(handle);
            return Err(EngineStartError::producer(&error));
        }
    };
    let producer = ProducerShardOwner::new(producer, Arc::new(wake));
    let admission = producer.admission_port();
    let resources = EngineHostResources {
        driver,
        producer,
        clock: Arc::clone(&clock),
        control: Arc::clone(&control),
        budget: validated.turn_budget,
    };
    if let Err(error) = sender.send(resources) {
        drop(sender);
        control.request_shutdown();
        let mut resources = error.0;
        let cleanup = run(&mut resources);
        if let Ok(exit) = cleanup {
            let _join_result = exit.notifier.join();
        }
        join_cancelled(handle);
        return Err(EngineStartError::handoff());
    }
    Ok(StartedEngineHost {
        admission,
        clock,
        join: EngineHostJoin {
            control,
            handle: Some(handle),
        },
    })
}

pub(crate) struct EngineHostJoin {
    control: Arc<EngineHostControl>,
    handle: Option<JoinHandle<Result<Option<EngineHostExit>, EngineHostError>>>,
}

impl EngineHostJoin {
    pub(crate) fn shutdown_and_join(&mut self) -> Result<(), EngineHostError> {
        self.control.request_shutdown();
        let Some(handle) = self.handle.take() else {
            return Ok(());
        };
        let exit = handle.join().map_err(|_| EngineHostError::HostPanicked)??;
        let Some(exit) = exit else {
            return Err(EngineHostError::HostPanicked);
        };
        exit.notifier.join().map_err(EngineHostError::Notifier)?;
        exit.failure.map_or(Ok(()), Err)
    }

    pub(crate) fn shutdown_detached(mut self) {
        self.control.request_shutdown();
        let _spawn_result = thread::Builder::new()
            .name(REAPER_THREAD_NAME.to_owned())
            .spawn(move || {
                let _shutdown_result = self.shutdown_and_join();
            });
    }

    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> super::EngineHostSnapshot {
        self.control.snapshot()
    }

    #[cfg(test)]
    pub(crate) fn force_failure(&self) {
        self.control.request_failure();
    }
}

impl Drop for EngineHostJoin {
    fn drop(&mut self) {
        self.control.request_shutdown();
        let _detached = self.handle.take();
    }
}

fn join_cancelled(handle: JoinHandle<Result<Option<EngineHostExit>, EngineHostError>>) {
    let _join_result = handle.join();
}
