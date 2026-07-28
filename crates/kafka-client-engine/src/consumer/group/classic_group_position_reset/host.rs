//! One-action registry scheduling for sequential missing-offset execution.

use kafka_client_core::{GroupPositionBootstrapTerminal, Moment};

use crate::driver::DriverOwner;

use super::{
    super::{
        classic_group_fetch::current_position_fence,
        classic_group_position::{
            ClassicGroupPositionExecutionError, ClassicGroupPositionExecutionState,
        },
        registry::GroupConsumerRegistry,
        registry_entry::GroupConsumerEntry,
    },
    settlement::settle_reset,
    state::ClassicGroupPositionResetTurn,
    submission::submit_reset,
};

impl GroupConsumerRegistry {
    pub(in crate::consumer::group) fn begin_one_classic_group_position_reset(
        &mut self,
        now: Moment,
    ) -> Result<ClassicGroupPositionResetTurn, ClassicGroupPositionExecutionError> {
        let Some(index) = self.entries.iter().position(reset_is_required) else {
            return Ok(ClassicGroupPositionResetTurn::Idle);
        };
        let entry = &mut self.entries[index];
        let current = current_position_fence(&entry.classic, &entry.catalog)
            .map_err(|_error| ClassicGroupPositionExecutionError::ResetCurrentFence)?;
        entry.position.begin_missing_offset_reset(current, now)?;
        Ok(ClassicGroupPositionResetTurn::Progress)
    }

    pub(in crate::consumer::group) fn submit_one_classic_group_position_reset(
        &mut self,
        driver: &DriverOwner,
        now: Moment,
    ) -> Result<ClassicGroupPositionResetTurn, ClassicGroupPositionExecutionError> {
        let Some(index) = self.entries.iter().position(reset_is_prepared) else {
            return Ok(ClassicGroupPositionResetTurn::Idle);
        };
        submit_reset(&mut self.entries[index], driver, now)?;
        Ok(ClassicGroupPositionResetTurn::Progress)
    }

    pub(in crate::consumer::group) fn settle_one_classic_group_position_reset(
        &mut self,
        now: Moment,
    ) -> Result<ClassicGroupPositionResetTurn, ClassicGroupPositionExecutionError> {
        for entry in &mut self.entries {
            if entry.fault.is_some() {
                continue;
            }
            let result = match entry.position.state() {
                ClassicGroupPositionExecutionState::ResetDriverOwned(owner) => {
                    owner.call.try_result()
                }
                _ => None,
            };
            if let Some(result) = result {
                settle_reset(entry, now, result)?;
                return Ok(ClassicGroupPositionResetTurn::Progress);
            }
        }
        Ok(ClassicGroupPositionResetTurn::Idle)
    }
}

fn reset_is_required(entry: &GroupConsumerEntry) -> bool {
    entry.is_active()
        && matches!(
            entry.position.state(),
            ClassicGroupPositionExecutionState::Complete(completed)
                if matches!(
                    completed.terminal(),
                    GroupPositionBootstrapTerminal::ResetRequired(_)
                )
        )
}

fn reset_is_prepared(entry: &GroupConsumerEntry) -> bool {
    entry.is_active()
        && entry.execution.is_idle()
        && matches!(
            entry.position.state(),
            ClassicGroupPositionExecutionState::ResetPrepared(_)
        )
}
