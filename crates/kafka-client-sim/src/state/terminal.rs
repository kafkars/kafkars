//! Virtual record-terminal ownership from publication through core reclamation.

use std::collections::btree_map::Entry;

use kafka_client_core::{OperationId, ProducerCompletion};

use super::VirtualProducerState;
use crate::SimulationError;

impl VirtualProducerState {
    pub(super) fn complete(
        &mut self,
        operation_id: OperationId,
        completion: ProducerCompletion,
    ) -> Result<(), SimulationError> {
        let payload_retained = self
            .operation_payloads
            .get(&operation_id)
            .is_some_and(|payload_id| self.payloads.contains_key(payload_id));
        let batch_retained = self
            .batches
            .values()
            .any(|batch| batch.contains(operation_id));
        if payload_retained || batch_retained {
            return Err(SimulationError::ResourceStillRetained(operation_id));
        }
        self.operation_payloads.remove(&operation_id);
        match self.terminals.entry(operation_id) {
            Entry::Occupied(_) => Err(SimulationError::DuplicateTerminal(operation_id)),
            Entry::Vacant(slot) => {
                slot.insert(completion);
                Ok(())
            }
        }
    }

    pub(crate) fn release_terminal(
        &mut self,
        operation_id: OperationId,
    ) -> Result<ProducerCompletion, SimulationError> {
        let completion = self
            .terminals
            .remove(&operation_id)
            .ok_or(SimulationError::UnknownTerminal(operation_id))?;
        self.released_terminals.insert(operation_id);
        Ok(completion)
    }

    pub(crate) fn require_released_terminal(
        &self,
        operation_id: OperationId,
    ) -> Result<(), SimulationError> {
        self.released_terminals
            .contains(&operation_id)
            .then_some(())
            .ok_or(SimulationError::TerminalStillRetained(operation_id))
    }

    pub(crate) fn finish_reclaim(&mut self, operation_id: OperationId) {
        self.released_terminals.remove(&operation_id);
    }

    pub(crate) fn terminal(&self, operation_id: OperationId) -> Option<ProducerCompletion> {
        self.terminals.get(&operation_id).copied()
    }
}
