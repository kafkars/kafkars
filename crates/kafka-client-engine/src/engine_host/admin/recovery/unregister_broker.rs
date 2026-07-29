//! Post-driver recovery for the explicit broker-unregistration owner.

use super::super::super::{EngineHostError, EngineHostResources};

pub(super) fn recover(
    resources: &EngineHostResources,
    mut failure: EngineHostError,
) -> EngineHostError {
    let mut unregister_broker = resources.unregister_broker.terminal_host();
    if let Some(cleanup) = unregister_broker
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::UnregisterBroker)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(unregister_broker);
    failure
}
