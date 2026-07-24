//! Bounded direct-consumer throttle timers driven by supplied monotonic moments.

use core::cmp::Ordering;

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedTopicPartition, Deadline, FetchFence,
    Moment, PositionFence,
};

use super::assigned_timer_model::{
    AssignedTimerDisposition, AssignedTimerError, AssignedTimerKind,
};

#[derive(Debug)]
struct AssignedTimerEntry {
    kind: AssignedTimerKind,
    deadline: Deadline,
    insertion_sequence: u64,
}

/// Partition-bounded owner of direct-consumer throttle deadlines.
#[derive(Debug)]
pub(crate) struct AssignedTimers {
    partition_capacity: usize,
    next_sequence: u64,
    entries: Vec<AssignedTimerEntry>,
}

impl AssignedTimers {
    pub(crate) fn new(partition_capacity: usize) -> Self {
        Self {
            partition_capacity,
            next_sequence: 0,
            entries: Vec::new(),
        }
    }

    pub(crate) fn arm_position(
        &mut self,
        fence: PositionFence,
        deadline: Deadline,
    ) -> Result<AssignedTimerDisposition, AssignedTimerError> {
        self.arm(AssignedTimerKind::Position(fence), deadline)
    }

    pub(crate) fn arm_fetch(
        &mut self,
        fence: FetchFence,
        deadline: Deadline,
    ) -> Result<AssignedTimerDisposition, AssignedTimerError> {
        self.arm(AssignedTimerKind::Fetch(fence), deadline)
    }

    pub(crate) fn observe_control(&mut self, effect: AssignedConsumerEffect) -> bool {
        let removed = match effect {
            AssignedConsumerEffect::Suspend { fence } => self
                .remove_partition_if(fence.partition(), |entry| {
                    position_precedes(entry.kind.position(), fence)
                }),
            AssignedConsumerEffect::Revoke {
                assignment_epoch,
                partition,
            } => self.remove_partition_if(partition, |entry| {
                entry.kind.position().assignment_epoch() <= assignment_epoch
            }),
            _ => false,
        };
        self.reset_sequence_if_empty();
        removed
    }

    pub(crate) fn pop_due(&mut self, now: Moment) -> Option<AssignedConsumerInput> {
        let index = self
            .entries
            .iter()
            .enumerate()
            .filter(|(_, entry)| entry.deadline.is_elapsed_at(now))
            .min_by_key(|(_, entry)| (entry.deadline, entry.insertion_sequence))
            .map(|(index, _)| index)?;
        let entry = self.entries.remove(index);
        self.reset_sequence_if_empty();
        Some(entry.kind.input(now))
    }

    pub(crate) fn next_deadline(&self) -> Option<Deadline> {
        self.entries.iter().map(|entry| entry.deadline).min()
    }

    pub(crate) const fn timer_count(&self) -> usize {
        self.entries.len()
    }

    #[cfg(test)]
    pub(super) fn arm_position_with_allocation_failure_for_test(
        &mut self,
        fence: PositionFence,
        deadline: Deadline,
    ) -> Result<AssignedTimerDisposition, AssignedTimerError> {
        self.arm_with_reservation(AssignedTimerKind::Position(fence), deadline, |_entries| {
            Err(())
        })
    }

    #[cfg(test)]
    pub(super) fn replace_next_sequence_for_test(&mut self, next_sequence: u64) {
        self.next_sequence = next_sequence;
    }

    fn arm(
        &mut self,
        kind: AssignedTimerKind,
        deadline: Deadline,
    ) -> Result<AssignedTimerDisposition, AssignedTimerError> {
        self.arm_with_reservation(kind, deadline, |entries| {
            entries.try_reserve(1).map_err(|_| ())
        })
    }

    fn arm_with_reservation(
        &mut self,
        kind: AssignedTimerKind,
        deadline: Deadline,
        reserve: impl FnOnce(&mut Vec<AssignedTimerEntry>) -> Result<(), ()>,
    ) -> Result<AssignedTimerDisposition, AssignedTimerError> {
        let existing = self
            .entries
            .iter()
            .position(|entry| entry.kind.partition() == kind.partition());
        if let Some(index) = existing {
            match kind_generation(&kind, &self.entries[index].kind) {
                Ordering::Less => return Ok(AssignedTimerDisposition::Fenced),
                Ordering::Equal if deadline == self.entries[index].deadline => {
                    return Ok(AssignedTimerDisposition::Idempotent);
                }
                Ordering::Equal => {
                    return Err(AssignedTimerError::DeadlineConflict {
                        active_deadline: self.entries[index].deadline,
                        effect: kind.effect(deadline),
                    });
                }
                Ordering::Greater => {}
            }
            let effect = kind.effect(deadline);
            let insertion_sequence = self.allocate_sequence(effect)?;
            self.entries[index] = AssignedTimerEntry {
                kind,
                deadline,
                insertion_sequence,
            };
            return Ok(AssignedTimerDisposition::Replaced);
        }
        if self.entries.len() >= self.partition_capacity {
            return Err(AssignedTimerError::Capacity {
                capacity: self.partition_capacity,
                effect: kind.effect(deadline),
            });
        }
        let effect = kind.effect(deadline);
        reserve(&mut self.entries).map_err(|()| AssignedTimerError::Allocation { effect })?;
        let insertion_sequence = self.allocate_sequence(effect)?;
        self.entries.push(AssignedTimerEntry {
            kind,
            deadline,
            insertion_sequence,
        });
        Ok(AssignedTimerDisposition::Inserted)
    }

    fn allocate_sequence(
        &mut self,
        effect: AssignedConsumerEffect,
    ) -> Result<u64, AssignedTimerError> {
        let sequence = self.next_sequence;
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or(AssignedTimerError::InsertionSequenceExhausted { effect })?;
        Ok(sequence)
    }

    fn remove_partition_if(
        &mut self,
        partition: AssignedTopicPartition,
        predicate: impl FnOnce(&AssignedTimerEntry) -> bool,
    ) -> bool {
        let Some(index) = self
            .entries
            .iter()
            .position(|entry| entry.kind.partition() == partition)
        else {
            return false;
        };
        if !predicate(&self.entries[index]) {
            return false;
        }
        self.entries.remove(index);
        true
    }

    fn reset_sequence_if_empty(&mut self) {
        if self.entries.is_empty() {
            self.next_sequence = 0;
        }
    }
}

fn kind_generation(incoming: &AssignedTimerKind, active: &AssignedTimerKind) -> Ordering {
    let position = compare_positions(incoming.position(), active.position());
    if position != Ordering::Equal {
        return position;
    }
    match (incoming, active) {
        (AssignedTimerKind::Position(_), AssignedTimerKind::Position(_)) => Ordering::Equal,
        (AssignedTimerKind::Position(_), AssignedTimerKind::Fetch(_)) => Ordering::Less,
        (AssignedTimerKind::Fetch(_), AssignedTimerKind::Position(_)) => Ordering::Greater,
        (AssignedTimerKind::Fetch(incoming), AssignedTimerKind::Fetch(active)) => {
            incoming.revision().cmp(&active.revision())
        }
    }
}

fn compare_positions(incoming: PositionFence, active: PositionFence) -> Ordering {
    incoming
        .assignment_epoch()
        .cmp(&active.assignment_epoch())
        .then_with(|| incoming.position_epoch().cmp(&active.position_epoch()))
}

fn position_precedes(active: PositionFence, control: PositionFence) -> bool {
    compare_positions(active, control) == Ordering::Less
}
