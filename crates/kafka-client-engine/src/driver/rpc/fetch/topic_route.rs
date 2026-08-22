//! Broker-issued topic identity and partition-leader generation for Fetch v16.

/// Immutable topic metadata retained by one prepared broker-routed Fetch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FetchTopicRoute {
    topic_id: [u8; 16],
    leader_epoch: Option<i32>,
}

impl FetchTopicRoute {
    pub(crate) const fn new(topic_id: [u8; 16], leader_epoch: Option<i32>) -> Self {
        Self {
            topic_id,
            leader_epoch,
        }
    }

    pub(crate) const fn topic_id(self) -> [u8; 16] {
        self.topic_id
    }

    pub(crate) const fn leader_epoch(self) -> Option<i32> {
        self.leader_epoch
    }

    pub(crate) const fn with_leader_epoch(self, leader_epoch: i32) -> Self {
        Self {
            topic_id: self.topic_id,
            leader_epoch: Some(leader_epoch),
        }
    }

    pub(crate) const fn without_leader_epoch(self) -> Self {
        Self {
            topic_id: self.topic_id,
            leader_epoch: None,
        }
    }
}
