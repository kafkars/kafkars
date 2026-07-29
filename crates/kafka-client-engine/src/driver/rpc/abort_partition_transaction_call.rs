//! Linear ownership of one accepted leader-routed partition transaction abort.

use std::time::Instant;

use kafka_client_core::AbortPartitionTransactionPlan;
use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::WriteTxnMarkersResponse;

use crate::protocol::admin::abort_partition_transaction::abort_partition_transaction_request;

use super::{
    super::DriverOwner,
    abort_partition_transaction_terminal::{
        AbortPartitionTransactionRawTerminal, RecoveredAbortPartitionTransactionCall,
        retain_abort_partition_transaction_terminal,
    },
};

/// One accepted API27 call retained beside its deterministic owner.
#[must_use = "an accepted partition transaction abort must be terminally settled"]
pub(crate) struct AbortPartitionTransactionCall {
    call: Option<RoutedCall<WriteTxnMarkersResponse>>,
    plan: Option<AbortPartitionTransactionPlan>,
}

impl AbortPartitionTransactionCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        plan: AbortPartitionTransactionPlan,
        deadline: Instant,
    ) -> Result<Self, AbortPartitionTransactionCallAdmissionFailure> {
        let request = abort_partition_transaction_request(&plan);
        let call = driver
            .submit_tracked_abort_partition_transaction(&plan, request, deadline)
            .map_err(|_source| AbortPartitionTransactionCallAdmissionFailure::Driver)?;
        Ok(Self {
            call: Some(call),
            plan: Some(plan),
        })
    }

    /// Extracts a ready raw terminal without losing partition-route evidence.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<AbortPartitionTransactionRawTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        match result {
            Ok(outcome) => {
                let plan = self.plan.take()?;
                drop(self.call.take());
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_abort_partition_transaction_terminal(
                    selected_version,
                    result,
                    route_token,
                    plan,
                )))
            }
            Err(source) => Some(Err(source)),
        }
    }

    /// Seals unresolved ownership only after the unique driver is gone.
    pub(crate) fn recover_after_driver_shutdown(
        self,
    ) -> Option<RecoveredAbortPartitionTransactionCall> {
        let Self { call, plan } = self;
        match (call, plan) {
            (Some(call), Some(plan)) => {
                drop(call);
                Some(RecoveredAbortPartitionTransactionCall::new(plan))
            }
            _ => None,
        }
    }
}

/// Definitely-unsent failure before driver request ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AbortPartitionTransactionCallAdmissionFailure {
    Driver,
}
