//! Registry-selected admission of one original-deadline membership owner.

use kafka_client_core::{GroupId, MembershipCycle};

use crate::clock::DeadlineCapture;

use super::{
    classic_group_execution::ClassicGroupExecutionError,
    consumer_group_execution::ConsumerGroupExecutionAdmissionError,
    registry::GroupConsumerRegistry, registry_entry::GroupConsumerEntry,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GroupConsumerCycleAcceptance {
    Classic(MembershipCycle),
    Consumer,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GroupConsumerCycleAdmissionError {
    RegistryClosed,
    EntryFault,
    UnknownGroup,
    GroupClosing,
    Execution(ClassicGroupExecutionError),
    ConsumerExecution(ConsumerGroupExecutionAdmissionError),
    ProtocolMismatch,
}

impl GroupConsumerRegistry {
    pub(super) fn try_begin_cycle(
        &mut self,
        group_id: GroupId,
        capture: DeadlineCapture,
    ) -> Result<GroupConsumerCycleAcceptance, GroupConsumerCycleAdmissionError> {
        let entry = self.entry_for_cycle(group_id)?;
        if entry.uses_consumer_group_protocol() {
            let execution = entry
                .consumer
                .as_mut()
                .ok_or(GroupConsumerCycleAdmissionError::ProtocolMismatch)?;
            execution
                .begin(capture)
                .map_err(GroupConsumerCycleAdmissionError::ConsumerExecution)?;
            return Ok(GroupConsumerCycleAcceptance::Consumer);
        }
        let cycle = entry
            .execution
            .begin(&mut entry.classic, capture)
            .map_err(GroupConsumerCycleAdmissionError::Execution)?;
        Ok(GroupConsumerCycleAcceptance::Classic(cycle))
    }

    #[cfg(test)]
    pub(super) fn try_begin_classic_cycle(
        &mut self,
        group_id: GroupId,
        capture: DeadlineCapture,
    ) -> Result<MembershipCycle, GroupConsumerCycleAdmissionError> {
        let entry = self.entry_for_cycle(group_id)?;
        if entry.uses_consumer_group_protocol() {
            return Err(GroupConsumerCycleAdmissionError::ProtocolMismatch);
        }
        entry
            .execution
            .begin(&mut entry.classic, capture)
            .map_err(GroupConsumerCycleAdmissionError::Execution)
    }

    fn entry_for_cycle(
        &mut self,
        group_id: GroupId,
    ) -> Result<&mut GroupConsumerEntry, GroupConsumerCycleAdmissionError> {
        if !self.accepting {
            return Err(GroupConsumerCycleAdmissionError::RegistryClosed);
        }
        let Some(entry) = self
            .entries
            .iter_mut()
            .find(|entry| entry.group_id() == group_id)
        else {
            return Err(GroupConsumerCycleAdmissionError::UnknownGroup);
        };
        if entry.fault.is_some() {
            return Err(GroupConsumerCycleAdmissionError::EntryFault);
        }
        if !entry.is_active() {
            return Err(GroupConsumerCycleAdmissionError::GroupClosing);
        }
        Ok(entry)
    }
}
