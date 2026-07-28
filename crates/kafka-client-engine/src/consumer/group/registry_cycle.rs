//! Registry-selected admission of one original-deadline classic membership cycle.

use kafka_client_core::{GroupId, MembershipCycle};

use crate::clock::DeadlineCapture;

use super::{classic_group_execution::ClassicGroupExecutionError, registry::GroupConsumerRegistry};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GroupConsumerCycleAdmissionError {
    RegistryClosed,
    EntryFault,
    UnknownGroup,
    GroupClosing,
    Execution(ClassicGroupExecutionError),
}

impl GroupConsumerRegistry {
    pub(super) fn try_begin_classic_cycle(
        &mut self,
        group_id: GroupId,
        capture: DeadlineCapture,
    ) -> Result<MembershipCycle, GroupConsumerCycleAdmissionError> {
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
        entry
            .execution
            .begin(&mut entry.classic, capture)
            .map_err(GroupConsumerCycleAdmissionError::Execution)
    }
}
