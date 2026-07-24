//! Exact position and Fetch fences retained by one assigned partition.

use kafka_client_core::{AssignedTopicPartition, FetchFence, PositionFence};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum EventClaim {
    Position(PositionFence),
    Fetch(FetchFence),
}

impl EventClaim {
    pub(super) const fn partition(self) -> AssignedTopicPartition {
        self.position().partition()
    }

    pub(super) const fn position(self) -> PositionFence {
        match self {
            Self::Position(fence) => fence,
            Self::Fetch(fence) => fence.position(),
        }
    }

    pub(super) fn can_advance_to(self, next: Self) -> bool {
        match (self, next) {
            (Self::Position(position), Self::Fetch(fetch)) => position == fetch.position(),
            (Self::Fetch(current), Self::Fetch(next)) => {
                current.position() == next.position() && current.revision() < next.revision()
            }
            _ => false,
        }
    }

    pub(super) fn is_older_than(self, fence: PositionFence) -> bool {
        let current = self.position();
        current.partition() == fence.partition()
            && (current.assignment_epoch() < fence.assignment_epoch()
                || (current.assignment_epoch() == fence.assignment_epoch()
                    && current.position_epoch() < fence.position_epoch()))
    }
}
