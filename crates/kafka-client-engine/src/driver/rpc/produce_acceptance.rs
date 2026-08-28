//! Linear acceptance receipt for one exact driver-owned Produce execution.

use kafka_client_core::{BatchExecutionId, ProducerInput};

/// Linear proof that the driver accepted one exact Produce execution.
#[must_use = "driver acceptance must be applied to the exact producer execution"]
pub(crate) struct AcceptedProduceCall {
    execution: BatchExecutionId,
}

impl AcceptedProduceCall {
    pub(super) const fn new(execution: BatchExecutionId) -> Self {
        Self { execution }
    }

    /// Returns the exact driver-owned execution identity.
    #[cfg(test)]
    pub(crate) const fn execution(&self) -> BatchExecutionId {
        self.execution
    }

    /// Creates the only core fact authorized by this accepted-call receipt.
    pub(crate) const fn driver_accepted(&self) -> ProducerInput {
        ProducerInput::DriverAccepted {
            execution: self.execution,
        }
    }

    /// Consumes the receipt after core and engine ownership accepted its fact.
    pub(crate) const fn confirm_receipt(self) {
        let Self {
            execution: _confirmed_execution,
        } = self;
    }
}
