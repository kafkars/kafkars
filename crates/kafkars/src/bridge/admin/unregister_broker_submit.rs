//! Admission handoff for public Admin `UnregisterBroker`.

use std::time::Duration;

use super::AdminEngine;
use crate::bridge::unregister_broker::AdminUnregisterBroker;

impl AdminEngine {
    pub(crate) fn submit_unregister_broker(
        &self,
        broker_id: i32,
        timeout: Duration,
    ) -> AdminUnregisterBroker {
        AdminUnregisterBroker::from_admission(self.handle.try_unregister_broker(broker_id, timeout))
    }
}
