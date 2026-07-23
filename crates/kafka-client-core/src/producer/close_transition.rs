//! Atomic producer admission closure and drain-barrier creation.

use crate::{ProducerMachineError, ProducerTransition};

use super::ProducerMachine;

impl ProducerMachine {
    pub(crate) fn close_requested(&mut self) -> Result<ProducerTransition, ProducerMachineError> {
        let effects = self
            .flushes
            .request(self.next_operation_id, &self.operations)
            .map_err(ProducerMachineError::Flush)?;
        self.close_admission();
        Ok(ProducerTransition::from_effects(effects))
    }
}
