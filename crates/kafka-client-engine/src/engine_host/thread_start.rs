//! Native host-thread acquisition, local reactor construction, and startup publication.

use std::{sync::mpsc::sync_channel, thread};

use crate::{EngineConfig, config::ValidatedEngineConfig};

use super::{
    EngineLifecycle, EngineStartError, finalize::finish_host, start_handoff::StartedEngineHost,
    start_handoff::join_cancelled,
};

const HOST_THREAD_NAME: &str = "kafka-client-engine";

pub(crate) fn start(
    config: &EngineConfig,
    validated: ValidatedEngineConfig,
) -> Result<StartedEngineHost, EngineStartError> {
    let lifecycle = std::sync::Arc::new(EngineLifecycle::new());
    let host_lifecycle = std::sync::Arc::clone(&lifecycle);
    let host_config = config.clone();
    let (sender, receiver) = sync_channel(1);
    let handle = thread::Builder::new()
        .name(HOST_THREAD_NAME.to_owned())
        .spawn(
            move || match super::start::prepare(&host_config, validated, &host_lifecycle) {
                Ok((resources, started)) => {
                    if sender.send(Ok(started)).is_err() {
                        resources.control.request_shutdown();
                    }
                    finish_host(resources, &host_lifecycle);
                }
                Err(error) => {
                    let _send_result = sender.send(Err(error));
                    host_lifecycle.publish(None);
                }
            },
        )
        .map_err(|error| EngineStartError::host_thread(&error))?;
    match receiver.recv() {
        Ok(Ok(started)) => {
            drop(handle);
            Ok(started)
        }
        Ok(Err(error)) => {
            join_cancelled(handle);
            Err(error)
        }
        Err(_disconnected) => {
            join_cancelled(handle);
            Err(EngineStartError::handoff())
        }
    }
}
