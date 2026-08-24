//! Atomic incremental direct-assignment admission and unchanged-owner retention.

#[cfg(test)]
use std::time::Duration;

use kafka_client_core::{
    AssignedConsumerInput, AssignedConsumerMachineError, AssignedTopicPartition, AssignmentEpoch,
};

use crate::clock::DeadlineCapture;

use super::{
    assigned_host::{AssignedConsumerPartition, AssignedPartitionInput},
    assigned_owner::AssignedConsumerOwner,
    assigned_owner_admission::{commit_assignment_event_claims, rollback_assignment_event_claims},
    assigned_owner_fault::AssignedConsumerOwnerFault,
    assigned_owner_model::AssignedConsumerOwnerError,
};

impl AssignedConsumerOwner {
    /// Adds partitions after capturing one test call-boundary deadline.
    #[cfg(test)]
    pub(crate) fn add_assignments(
        &mut self,
        entries: Vec<AssignedPartitionInput>,
        resolution_timeout: Duration,
    ) -> Result<Option<AssignmentEpoch>, AssignedConsumerOwnerError> {
        let capture = self
            .clock
            .capture_deadline_after(resolution_timeout)
            .map_err(AssignedConsumerOwnerError::Clock)?;
        self.add_assignments_captured(entries, capture)
    }

    /// Adds partitions without rebuilding or refencing the retained assignment.
    pub(crate) fn add_assignments_captured(
        &mut self,
        entries: Vec<AssignedPartitionInput>,
        capture: DeadlineCapture,
    ) -> Result<Option<AssignmentEpoch>, AssignedConsumerOwnerError> {
        if entries.is_empty() {
            if self.is_faulted() {
                return Err(AssignedConsumerOwnerError::Faulted);
            }
            return self.apply_empty_assignment_change(AssignedConsumerInput::AddAssignments {
                partitions: Vec::new(),
                now: capture.now(),
                resolution_deadline: capture.deadline(),
            });
        }
        self.ensure_admission_ready()?;
        let prepared = self
            .topics
            .prepare_addition(entries)
            .map_err(AssignedConsumerOwnerError::Topics)?;
        let mut partitions = Vec::new();
        partitions
            .try_reserve_exact(prepared.added().len())
            .map_err(|_error| AssignedConsumerOwnerError::Allocation)?;
        partitions.extend_from_slice(prepared.added());
        let event_claims = self
            .events
            .prepare_addition(prepared.added())
            .map_err(AssignedConsumerOwnerError::Event)?;
        let transition = match self.machine.apply(AssignedConsumerInput::AddAssignments {
            partitions,
            now: capture.now(),
            resolution_deadline: capture.deadline(),
        }) {
            Ok(transition) => transition,
            Err(error) => {
                rollback_assignment_event_claims(event_claims);
                return Err(AssignedConsumerOwnerError::Core(error));
            }
        };
        let epoch = transition.assignment_epoch();
        prepared.commit();
        if let Err(error) = commit_assignment_event_claims(event_claims, transition.effects()) {
            self.fault = Some(AssignedConsumerOwnerFault::EventTransition { transition, error });
            return Err(AssignedConsumerOwnerError::Faulted);
        }
        if epoch.is_none() {
            self.retain_transition(transition, Some(capture.operation_deadline()));
            return Err(AssignedConsumerOwnerError::Faulted);
        }
        self.enqueue_transition(transition, Some(capture.operation_deadline()));
        Ok(epoch)
    }

    /// Removes exact partitions while preserving all survivor execution ownership.
    pub(crate) fn remove_assignments(
        &mut self,
        entries: &[AssignedConsumerPartition],
    ) -> Result<Option<AssignmentEpoch>, AssignedConsumerOwnerError> {
        if entries.is_empty() {
            if self.is_faulted() {
                return Err(AssignedConsumerOwnerError::Faulted);
            }
            return self.apply_empty_assignment_change(AssignedConsumerInput::RemoveAssignments {
                partitions: Vec::new(),
            });
        }
        self.ensure_admission_ready()?;
        if self.machine.assignment_epoch().is_none() {
            return Err(AssignedConsumerOwnerError::Core(
                AssignedConsumerMachineError::NoAssignment,
            ));
        }
        let prepared = self
            .topics
            .prepare_removal(entries)
            .map_err(AssignedConsumerOwnerError::Topics)?;
        let mut partitions = Vec::<AssignedTopicPartition>::new();
        partitions
            .try_reserve_exact(prepared.removed().len())
            .map_err(|_error| AssignedConsumerOwnerError::Allocation)?;
        partitions.extend_from_slice(prepared.removed());
        let event_claims = self.events.prepare_removal(partitions.len());
        let transition = match self
            .machine
            .apply(AssignedConsumerInput::RemoveAssignments { partitions })
        {
            Ok(transition) => transition,
            Err(error) => {
                rollback_assignment_event_claims(event_claims);
                return Err(AssignedConsumerOwnerError::Core(error));
            }
        };
        let epoch = transition.assignment_epoch();
        prepared.commit();
        if let Err(error) = commit_assignment_event_claims(event_claims, transition.effects()) {
            self.fault = Some(AssignedConsumerOwnerFault::EventTransition { transition, error });
            return Err(AssignedConsumerOwnerError::Faulted);
        }
        if epoch.is_none() {
            self.retain_transition(transition, None);
            return Err(AssignedConsumerOwnerError::Faulted);
        }
        self.enqueue_transition(transition, None);
        Ok(epoch)
    }

    fn apply_empty_assignment_change(
        &mut self,
        input: AssignedConsumerInput,
    ) -> Result<Option<AssignmentEpoch>, AssignedConsumerOwnerError> {
        let transition = self
            .machine
            .apply(input)
            .map_err(AssignedConsumerOwnerError::Core)?;
        if !transition.effects().is_empty() {
            self.retain_transition(transition, None);
            return Err(AssignedConsumerOwnerError::Faulted);
        }
        Ok(transition.assignment_epoch())
    }
}
