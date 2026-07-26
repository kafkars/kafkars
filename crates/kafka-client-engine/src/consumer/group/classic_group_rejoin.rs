//! Linear installation and exact clearing of one core-owned classic rejoin schedule.

use kafka_client_core::{ClassicRejoinSchedule, Deadline};

/// Sole mechanism owner for one exact future classic-group rejoin.
pub(super) struct ClassicGroupRejoinExecution {
    rejoin_execution_state: ClassicGroupRejoinState,
}

enum ClassicGroupRejoinState {
    Dormant,
    Waiting(ClassicRejoinSchedule),
}

/// Prevalidated transfer of one core-emitted rejoin schedule.
#[must_use = "a prepared classic rejoin schedule must be committed"]
pub(super) struct PreparedClassicRejoinInstall<'a> {
    execution: &'a mut ClassicGroupRejoinExecution,
    schedule: ClassicRejoinSchedule,
}

impl ClassicGroupRejoinExecution {
    pub(super) const fn new() -> Self {
        Self {
            rejoin_execution_state: ClassicGroupRejoinState::Dormant,
        }
    }

    pub(super) const fn is_dormant(&self) -> bool {
        matches!(
            self.rejoin_execution_state,
            ClassicGroupRejoinState::Dormant
        )
    }

    pub(super) const fn schedule(&self) -> Option<ClassicRejoinSchedule> {
        match self.rejoin_execution_state {
            ClassicGroupRejoinState::Dormant => None,
            ClassicGroupRejoinState::Waiting(schedule) => Some(schedule),
        }
    }

    pub(super) fn prepare_rejoin_install(
        &mut self,
        schedule: ClassicRejoinSchedule,
    ) -> Result<PreparedClassicRejoinInstall<'_>, ClassicGroupRejoinError> {
        if !self.is_dormant() {
            return Err(ClassicGroupRejoinError::Occupied);
        }
        Ok(PreparedClassicRejoinInstall {
            execution: self,
            schedule,
        })
    }

    pub(super) fn clear_rejoin_exact(
        &mut self,
        schedule: ClassicRejoinSchedule,
    ) -> Result<(), ClassicGroupRejoinError> {
        match self.schedule() {
            Some(waiting) if waiting == schedule => {
                self.rejoin_execution_state = ClassicGroupRejoinState::Dormant;
                Ok(())
            }
            Some(_) => Err(ClassicGroupRejoinError::ScheduleMismatch),
            None => Err(ClassicGroupRejoinError::Dormant),
        }
    }

    pub(super) const fn next_deadline(&self) -> Option<Deadline> {
        match self.schedule() {
            Some(schedule) => Some(schedule.due()),
            None => None,
        }
    }

    pub(super) const fn unsettled(&self) -> usize {
        if self.is_dormant() { 0 } else { 1 }
    }
}

impl PreparedClassicRejoinInstall<'_> {
    pub(super) fn commit(self) {
        self.execution.rejoin_execution_state = ClassicGroupRejoinState::Waiting(self.schedule);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ClassicGroupRejoinError {
    Occupied,
    Dormant,
    ScheduleMismatch,
}
