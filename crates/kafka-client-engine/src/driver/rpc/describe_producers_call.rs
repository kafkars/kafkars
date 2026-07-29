//! Linear ownership of one accepted routed Admin `DescribeProducers` call.

use std::time::Instant;

use kafka_client_core::AdminDescribeProducerTarget;
use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::DescribeProducersResponse;

use crate::protocol::admin::describe_producers::describe_producers_request;

use super::{
    super::DriverOwner,
    describe_producers_terminal::{
        DescribeProducersRawTerminal, RecoveredDescribeProducersCall,
        retain_describe_producers_terminal,
    },
};

/// One accepted tracked driver call retained beside its concrete admin owner.
#[must_use = "an accepted DescribeProducers call must be terminally settled"]
pub(crate) struct DescribeProducersCall {
    call: Option<RoutedCall<DescribeProducersResponse>>,
}

impl DescribeProducersCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        target: &AdminDescribeProducerTarget,
        broker_id: Option<i32>,
        deadline: Instant,
    ) -> Result<Self, DescribeProducersCallAdmissionFailure> {
        let request = describe_producers_request(target);
        let call = driver
            .submit_tracked_describe_producers(
                target.topic(),
                target.partition(),
                broker_id,
                request,
                deadline,
            )
            .map_err(|_source| DescribeProducersCallAdmissionFailure)?;
        Ok(Self { call: Some(call) })
    }

    /// Extracts one ready raw terminal without blocking or releasing route evidence.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<DescribeProducersRawTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        match result {
            Ok(outcome) => {
                drop(self.call.take());
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_describe_producers_terminal(
                    selected_version,
                    result,
                    route_token,
                )))
            }
            Err(source) => Some(Err(source)),
        }
    }

    /// Seals unresolved ownership only after the unique driver is gone.
    pub(crate) fn recover_after_driver_shutdown(self) -> Option<RecoveredDescribeProducersCall> {
        self.call.map(|call| {
            drop(call);
            RecoveredDescribeProducersCall::new()
        })
    }
}

/// Definitely-unsent bounded-driver rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use = "a rejected DescribeProducers call must become operation input"]
pub(crate) struct DescribeProducersCallAdmissionFailure;
