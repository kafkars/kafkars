//! Exact call-slot mutation for one hosted share membership lifetime.

use crate::driver::share_group_heartbeat::ShareGroupHeartbeatCall;

use super::entry::ShareConsumerEntry;

impl ShareConsumerEntry {
    pub(super) fn install_heartbeat_call(
        &mut self,
        call: ShareGroupHeartbeatCall,
    ) -> Result<(), ShareGroupHeartbeatCall> {
        if self.heartbeat_call.is_some() {
            return Err(call);
        }
        self.heartbeat_call = Some(call);
        Ok(())
    }

    pub(super) fn take_heartbeat_call(&mut self) -> Option<ShareGroupHeartbeatCall> {
        self.heartbeat_call.take()
    }
}
