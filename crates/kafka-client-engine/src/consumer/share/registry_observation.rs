//! Aggregate share membership deadlines and retained host obligations.

use kafka_client_core::{Deadline, ShareGroupHeartbeatPhase};

use super::{entry::ShareConsumerEntry, registry::ShareConsumerRegistry};

impl ShareConsumerRegistry {
    pub(crate) fn unsettled(&self) -> usize {
        self.invalidations.retained_count().saturating_add(
            self.entries
                .iter()
                .map(ShareConsumerEntry::unsettled)
                .sum::<usize>(),
        )
    }

    pub(crate) fn next_deadline(&self) -> Option<Deadline> {
        self.entries
            .iter()
            .filter_map(ShareConsumerEntry::next_deadline)
            .min()
    }
}

impl ShareConsumerEntry {
    fn unsettled(&self) -> usize {
        1usize
            .saturating_add(usize::from(self.start.is_some()))
            .saturating_add(usize::from(self.topic_call.is_some()))
            .saturating_add(usize::from(self.heartbeat_call.is_some()))
            .saturating_add(self.membership.as_ref().map_or(0, |membership| {
                usize::from(membership.prepared().is_some()).saturating_add(usize::from(!matches!(
                    membership.machine().phase(),
                    ShareGroupHeartbeatPhase::Dormant
                        | ShareGroupHeartbeatPhase::Fatal
                        | ShareGroupHeartbeatPhase::Closed
                )))
            }))
    }

    fn next_deadline(&self) -> Option<Deadline> {
        let start = self
            .start
            .filter(|_capture| self.membership.is_none())
            .map(crate::clock::DeadlineCapture::deadline);
        [
            start,
            self.membership
                .as_ref()
                .and_then(super::ShareMembershipInterpreter::next_deadline),
        ]
        .into_iter()
        .flatten()
        .min()
    }
}
