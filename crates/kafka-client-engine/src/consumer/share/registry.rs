//! Sole bounded registry owner for hosted share-member lifetimes.

use crate::consumer::share_recv::ShareConsumerRecvNotificationResources;
use crate::driver::share_group_heartbeat::ShareCoordinatorInvalidations;
use kafka_client_core::GroupId;

use super::{close_state::ShareConsumerCloseTerminal, entry::ShareConsumerEntry};
use crate::completion::CompletionRegistry;
use crate::consumer::ShareAcknowledgeOutcome;

pub(super) const SHARE_CONSUMER_CAPACITY: usize = 8;
pub(super) const SHARE_ACKNOWLEDGEMENT_CAPACITY: usize = SHARE_CONSUMER_CAPACITY * 4;
pub(super) const SHARE_COORDINATOR_INVALIDATION_CAPACITY: usize = SHARE_CONSUMER_CAPACITY;
pub(super) const SHARE_CONSUMER_RETAINED_NAME_BYTES: usize = SHARE_CONSUMER_CAPACITY
    * (3 + super::entry::SHARE_TOPIC_CAPACITY)
    * super::entry::SHARE_NAME_BYTE_LIMIT;

pub(crate) struct ShareConsumerRegistry {
    pub(super) entries: Vec<ShareConsumerEntry>,
    pub(super) next_group_id: Option<GroupId>,
    pub(super) retained_name_bytes: usize,
    pub(super) accepting: bool,
    pub(super) invalidations: ShareCoordinatorInvalidations,
    pub(super) close_completions: CompletionRegistry<ShareConsumerCloseTerminal>,
    pub(super) acknowledgement_completions: CompletionRegistry<ShareAcknowledgeOutcome>,
    pub(super) recv_notifications: Option<ShareConsumerRecvNotificationResources>,
}

impl ShareConsumerRegistry {
    pub(crate) fn start() -> std::io::Result<Self> {
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(SHARE_CONSUMER_CAPACITY)
            .map_err(|_error| std::io::Error::other("share consumer reservation failed"))?;
        let invalidations =
            ShareCoordinatorInvalidations::try_new(SHARE_COORDINATOR_INVALIDATION_CAPACITY)
                .map_err(|_error| {
                    std::io::Error::other("share coordinator invalidation reservation failed")
                })?;
        let mut close_completions = CompletionRegistry::start(SHARE_CONSUMER_CAPACITY)?;
        let mut acknowledgement_completions =
            match CompletionRegistry::start(SHARE_ACKNOWLEDGEMENT_CAPACITY) {
                Ok(completions) => completions,
                Err(error) => {
                    if let Some(join) = close_completions.take_notifier() {
                        let _result = join.join();
                    }
                    return Err(error);
                }
            };
        let recv_notifications = match ShareConsumerRecvNotificationResources::start() {
            Ok(resources) => resources,
            Err(error) => {
                if let Some(join) = close_completions.take_notifier() {
                    let _result = join.join();
                }
                if let Some(join) = acknowledgement_completions.take_notifier() {
                    let _result = join.join();
                }
                return Err(error);
            }
        };
        Ok(Self {
            entries,
            next_group_id: GroupId::try_from_raw(1),
            retained_name_bytes: 0,
            accepting: true,
            invalidations,
            close_completions,
            acknowledgement_completions,
            recv_notifications: Some(recv_notifications),
        })
    }

    pub(in crate::consumer) fn close_admission(&mut self) {
        self.accepting = false;
    }

    pub(super) fn entry(&self, group_id: GroupId) -> Option<&ShareConsumerEntry> {
        self.entries
            .iter()
            .find(|entry| entry.group_id() == group_id)
    }

    pub(super) fn entry_mut(&mut self, group_id: GroupId) -> Option<&mut ShareConsumerEntry> {
        self.entries
            .iter_mut()
            .find(|entry| entry.group_id() == group_id)
    }

    pub(in crate::consumer) fn registered_count(&self) -> usize {
        self.entries.len()
    }

    pub(in crate::consumer) const fn retained_name_bytes(&self) -> usize {
        self.retained_name_bytes
    }
}
