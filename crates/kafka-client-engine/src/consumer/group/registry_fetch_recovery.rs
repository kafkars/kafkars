//! Terminal registry release of group Fetch and accepted-close owners after driver teardown.

use super::{
    registry::GroupConsumerRegistry, registry_close::GroupConsumerRemovalError,
    registry_entry::GroupConsumerEntry,
};

impl GroupConsumerRegistry {
    /// Consumes terminal group entries only after membership recovery no longer
    /// needs their catalog or Fetch retirement authority.
    ///
    /// Recovery capacity is reserved when the registry starts. Since one report
    /// is emitted for each of at most `GROUP_CONSUMER_CAPACITY` entries, this
    /// terminal path performs no allocation after the driver has been destroyed.
    pub(in crate::consumer) fn recover_fetch_after_driver_shutdown(
        &mut self,
    ) -> Result<(), GroupConsumerRemovalError> {
        let available = self
            .fetch_shutdown_recoveries
            .capacity()
            .saturating_sub(self.fetch_shutdown_recoveries.len());
        debug_assert!(self.entries.len() <= available);
        let mut first_error = None;
        for entry in self.entries.drain(..) {
            let bytes = entry.group_bytes();
            if let Some(retained) = self.retained_group_bytes.checked_sub(bytes) {
                self.retained_group_bytes = retained;
            } else {
                first_error.get_or_insert(GroupConsumerRemovalError::RetainedBytesInvariant);
                self.retained_group_bytes = 0;
            }
            let group_id = entry.group_id();
            let GroupConsumerEntry {
                fetch, mut leave, ..
            } = entry;
            self.fetch_shutdown_recoveries
                .push((group_id, fetch.release_after_driver_shutdown()));
            if !leave.publish_terminal() {
                first_error.get_or_insert(GroupConsumerRemovalError::TerminalInvariant);
            }
        }
        self.retained_group_bytes = 0;
        first_error.map_or(Ok(()), Err)
    }

    #[cfg(test)]
    pub(super) fn fetch_shutdown_recovery(
        &self,
        group_id: kafka_client_core::GroupId,
    ) -> Option<&super::classic_group_fetch::ClassicGroupFetchShutdownRecovery> {
        self.fetch_shutdown_recoveries
            .iter()
            .find_map(|(retained_group_id, recovery)| {
                (*retained_group_id == group_id).then_some(recovery)
            })
    }
}
