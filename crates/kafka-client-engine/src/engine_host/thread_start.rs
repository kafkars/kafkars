//! Native host-thread acquisition before startup resource handoff.

use std::{
    sync::{Arc, mpsc::SyncSender, mpsc::sync_channel},
    thread::{self, JoinHandle},
};

use super::{EngineHostResources, EngineLifecycle, EngineStartError, finalize::finish_host};

const HOST_THREAD_NAME: &str = "kafka-client-engine";

pub(super) fn start(
    lifecycle: &Arc<EngineLifecycle>,
) -> Result<(SyncSender<EngineHostResources>, JoinHandle<()>), EngineStartError> {
    let host_lifecycle = Arc::clone(lifecycle);
    let (sender, receiver) = sync_channel::<EngineHostResources>(1);
    let handle = thread::Builder::new()
        .name(HOST_THREAD_NAME.to_owned())
        .spawn(move || match receiver.recv() {
            Ok(resources) => finish_host(resources, &host_lifecycle),
            Err(_) => host_lifecycle.publish(None),
        })
        .map_err(|error| EngineStartError::host_thread(&error))?;
    Ok((sender, handle))
}
