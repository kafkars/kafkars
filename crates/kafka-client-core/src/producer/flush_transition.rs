//! Producer-machine coordination for flush creation and result reclamation.

use crate::{FlushId, ProducerMachineError, ProducerTransition};

use super::ProducerMachine;

impl ProducerMachine {
    pub(crate) fn flush_requested(&mut self) -> Result<ProducerTransition, ProducerMachineError> {
        let effects = self
            .flushes
            .request(self.next_operation_id, &self.operations)
            .map_err(ProducerMachineError::Flush)?;
        Ok(ProducerTransition::from_effects(effects))
    }

    pub(crate) fn reclaim_flush(&mut self, flush_id: FlushId) -> Result<(), ProducerMachineError> {
        self.flushes
            .reclaim(flush_id)
            .map_err(ProducerMachineError::Flush)
    }

    pub(crate) fn settle_ready_flushes(&mut self) -> Vec<crate::ProducerEffect> {
        self.flushes.settle_ready(&self.operations)
    }
}
