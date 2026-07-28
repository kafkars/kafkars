//! Terminal registry release of group Fetch owners after driver teardown.

use super::{registry::GroupConsumerRegistry, registry_entry::GroupConsumerEntry};

impl GroupConsumerRegistry {
    /// Consumes terminal group entries only after membership recovery no longer
    /// needs their catalog or Fetch retirement authority.
    ///
    /// Recovery capacity is reserved when the registry starts. Since one report
    /// is emitted for each of at most `GROUP_CONSUMER_CAPACITY` entries, this
    /// terminal path performs no allocation after the driver has been destroyed.
    pub(in crate::consumer) fn recover_fetch_after_driver_shutdown(&mut self) {
        let available = self
            .fetch_shutdown_recoveries
            .capacity()
            .saturating_sub(self.fetch_shutdown_recoveries.len());
        debug_assert!(self.entries.len() <= available);
        for entry in self.entries.drain(..) {
            let group_id = entry.group_id();
            let GroupConsumerEntry { fetch, .. } = entry;
            self.fetch_shutdown_recoveries
                .push((group_id, fetch.release_after_driver_shutdown()));
        }
        self.retained_group_bytes = 0;
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
