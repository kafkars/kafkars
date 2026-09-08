//! Complete ownership predicate required before a closing group can be removed.

use kafka_client_core::{ClassicGroupPhase, ConsumerGroupHeartbeatPhase};

use super::super::registry_entry::{GroupConsumerEntry, GroupConsumerEntryState};

pub(super) fn group_close_is_drained(entry: &GroupConsumerEntry) -> bool {
    let protocol_is_closed = entry.consumer.as_ref().map_or_else(
        || {
            entry.classic.machine().phase() == ClassicGroupPhase::Closed
                && entry.classic.pending().is_none()
        },
        |consumer| {
            consumer.machine().phase() == ConsumerGroupHeartbeatPhase::Closed
                && consumer.prepared().is_none()
                && consumer.heartbeat_call().is_none()
                && consumer.topic_identity_call().is_none()
                && entry.consumer_revocation.is_none()
                && entry.consumer_reconciliation.is_none()
        },
    );
    entry.state == GroupConsumerEntryState::Closing
        && protocol_is_closed
        && entry.classic_reconciliation.is_none()
        && entry.catalog.live_assignment().is_none()
        && entry.execution.is_idle()
        && entry.heartbeat.is_dormant()
        && entry.position.is_dormant()
        && entry.processing_lease.active_schedule().is_none()
        && entry.processing_lease.pending_expiration().is_none()
        && entry.rejoin.is_dormant()
        && !entry.rediscovery.blocks_join()
        && entry.fetch.is_idle()
        && entry.leave.allows_local_close()
        && entry.revocation.is_dormant()
}
