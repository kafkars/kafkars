//! Sole mutation owner for one partition's next-fetch generation and phase.

use super::{
    AssignedConsumerEffect, AssignedConsumerMachineError, AssignedTopicPartition, FetchFence,
    FetchRevision, NextFetchOffset, PositionEpoch, PositionFence, StartPosition,
};

#[derive(Debug)]
pub(super) struct PartitionPosition {
    epoch: PositionEpoch,
    next_fetch_revision: FetchRevision,
    phase: PositionPhase,
}

#[derive(Clone, Copy, Debug)]
enum PositionPhase {
    AwaitingResolution(StartPosition),
    Ready(NextFetchOffset),
    Fetching {
        fence: FetchFence,
        next_offset: NextFetchOffset,
    },
}

impl PartitionPosition {
    pub(super) const fn new(position: StartPosition) -> Self {
        let phase = match position {
            position @ (StartPosition::Beginning | StartPosition::End) => {
                PositionPhase::AwaitingResolution(position)
            }
            StartPosition::Offset(next_offset) => PositionPhase::Ready(next_offset),
        };
        Self {
            epoch: PositionEpoch::initial(),
            next_fetch_revision: FetchRevision::initial(),
            phase,
        }
    }

    pub(super) const fn epoch(&self) -> PositionEpoch {
        self.epoch
    }

    pub(super) const fn is_awaiting_resolution(&self) -> bool {
        matches!(self.phase, PositionPhase::AwaitingResolution(_))
    }

    pub(super) fn replace(&mut self, position: StartPosition) {
        self.phase = match position {
            position @ (StartPosition::Beginning | StartPosition::End) => {
                PositionPhase::AwaitingResolution(position)
            }
            StartPosition::Offset(next_offset) => PositionPhase::Ready(next_offset),
        };
    }

    pub(super) fn resolve(&mut self, next_offset: NextFetchOffset) {
        self.phase = PositionPhase::Ready(next_offset);
    }

    pub(super) fn advance_and_activate(
        &mut self,
        supplied: FetchFence,
        next_offset: NextFetchOffset,
        partition: AssignedTopicPartition,
    ) -> Result<AssignedConsumerEffect, AssignedConsumerMachineError> {
        let PositionPhase::Fetching {
            fence,
            next_offset: requested,
        } = self.phase
        else {
            return Err(AssignedConsumerMachineError::StaleFetch { supplied });
        };
        if fence != supplied {
            return Err(AssignedConsumerMachineError::StaleFetch { supplied });
        }
        if next_offset < requested {
            return Err(AssignedConsumerMachineError::OffsetRegression {
                requested,
                observed: next_offset,
            });
        }
        self.start_fetch(supplied.position(), next_offset, partition)
    }

    pub(super) fn activate(
        &mut self,
        fence: PositionFence,
        partition: AssignedTopicPartition,
    ) -> Result<Option<AssignedConsumerEffect>, AssignedConsumerMachineError> {
        match self.phase {
            PositionPhase::AwaitingResolution(position) => {
                Ok(Some(AssignedConsumerEffect::ResolvePosition {
                    fence,
                    position,
                }))
            }
            PositionPhase::Ready(next_offset) => {
                self.start_fetch(fence, next_offset, partition).map(Some)
            }
            PositionPhase::Fetching { .. } => Ok(None),
        }
    }

    fn start_fetch(
        &mut self,
        position: PositionFence,
        next_offset: NextFetchOffset,
        partition: AssignedTopicPartition,
    ) -> Result<AssignedConsumerEffect, AssignedConsumerMachineError> {
        let revision = self.next_fetch_revision;
        let next_revision = revision
            .checked_next()
            .ok_or(AssignedConsumerMachineError::FetchRevisionExhausted { partition })?;
        let fence = FetchFence::new(position, revision);
        self.next_fetch_revision = next_revision;
        self.phase = PositionPhase::Fetching { fence, next_offset };
        Ok(AssignedConsumerEffect::Fetch { fence, next_offset })
    }

    pub(super) fn fence(
        &mut self,
        partition: AssignedTopicPartition,
    ) -> Result<(), AssignedConsumerMachineError> {
        let next = self
            .epoch
            .checked_next()
            .ok_or(AssignedConsumerMachineError::PositionEpochExhausted { partition })?;
        if let PositionPhase::Fetching { next_offset, .. } = self.phase {
            self.phase = PositionPhase::Ready(next_offset);
        }
        self.epoch = next;
        self.next_fetch_revision = FetchRevision::initial();
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn replace_next_fetch_revision_for_test(&mut self, revision: FetchRevision) {
        self.next_fetch_revision = revision;
    }
}
