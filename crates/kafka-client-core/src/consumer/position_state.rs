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

    pub(super) fn advance(
        &mut self,
        supplied: FetchFence,
        next_offset: NextFetchOffset,
    ) -> Result<(), AssignedConsumerMachineError> {
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
        self.phase = PositionPhase::Ready(next_offset);
        Ok(())
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
                let revision = self.next_fetch_revision;
                let next_revision = revision
                    .checked_next()
                    .ok_or(AssignedConsumerMachineError::FetchRevisionExhausted { partition })?;
                let fetch_fence = FetchFence::new(fence, revision);
                self.next_fetch_revision = next_revision;
                self.phase = PositionPhase::Fetching {
                    fence: fetch_fence,
                    next_offset,
                };
                Ok(Some(AssignedConsumerEffect::Fetch {
                    fence: fetch_fence,
                    next_offset,
                }))
            }
            PositionPhase::Fetching { .. } => Ok(None),
        }
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
}
