//! Linear retention of accepted API-89 calls through post-driver settlement.

use kafka_client_core::{DeliveryStatus, DescribeStreamsGroupInput};

use super::{DescribeStreamsGroupHost, DescribeStreamsGroupHostError};

impl DescribeStreamsGroupHost {
    pub(super) fn retain_recovered_call(
        &mut self,
        index: usize,
    ) -> Result<(), DescribeStreamsGroupHostError> {
        if self.operations[index].recovered_call.is_some() {
            return Ok(());
        }
        if let Some(call) = self.operations[index].call.take() {
            self.operations[index].recovered_call = call.recover_after_driver_shutdown();
        }
        self.operations[index]
            .recovered_call
            .as_ref()
            .ok_or(DescribeStreamsGroupHostError::InvalidHandoff)
            .map(|_recovered| ())
    }

    pub(super) fn settle_recovered_transport(
        &mut self,
        index: usize,
    ) -> Result<(), DescribeStreamsGroupHostError> {
        let transition =
            self.operations[index]
                .machine
                .apply(DescribeStreamsGroupInput::TransportFailed {
                    delivery: DeliveryStatus::PossiblySent,
                })?;
        let effect = transition
            .into_effect()
            .ok_or(DescribeStreamsGroupHostError::MissingTerminal)?;
        let recovered = self.operations[index]
            .recovered_call
            .take()
            .ok_or(DescribeStreamsGroupHostError::InvalidHandoff)?;
        recovered.seal();
        self.install_effect(index, effect)
    }
}
