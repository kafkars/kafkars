//! Linear ownership of one accepted exact-broker `DescribeLogDirs` call.

use std::time::Instant;

use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::DescribeLogDirsResponse;

use crate::protocol::admin::describe_log_dirs::{
    DescribeLogDirsSelectionRef, describe_log_dirs_request,
};

use super::{
    super::DriverOwner,
    describe_log_dirs_terminal::{
        DescribeLogDirsRawTerminal, RecoveredDescribeLogDirsCall, retain_describe_log_dirs_terminal,
    },
};

/// One accepted driver call retained beside its concrete admin owner.
#[must_use = "an accepted DescribeLogDirs call must be terminally settled"]
pub(crate) struct DescribeLogDirsCall {
    broker_id: i32,
    call: Option<RoutedCall<DescribeLogDirsResponse>>,
}

impl DescribeLogDirsCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        broker_id: i32,
        deadline: Instant,
    ) -> Result<Self, DescribeLogDirsCallAdmissionFailure> {
        let request = describe_log_dirs_request(DescribeLogDirsSelectionRef::AllTopics, 0)
            .map_err(|_source| DescribeLogDirsCallAdmissionFailure::Request)?;
        let call = driver
            .submit_tracked_describe_log_dirs(broker_id, request, deadline)
            .map_err(|_source| DescribeLogDirsCallAdmissionFailure::Submit)?;
        Ok(Self {
            broker_id,
            call: Some(call),
        })
    }

    /// Extracts a ready raw terminal without blocking or losing route evidence.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<DescribeLogDirsRawTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        match result {
            Ok(outcome) => {
                drop(self.call.take());
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_describe_log_dirs_terminal(
                    self.broker_id,
                    selected_version,
                    result,
                    route_token,
                )))
            }
            Err(source) => Some(Err(source)),
        }
    }

    /// Seals unresolved ownership only after the unique driver is destroyed.
    pub(crate) fn recover_after_driver_shutdown(self) -> Option<RecoveredDescribeLogDirsCall> {
        self.call.map(|call| {
            drop(call);
            RecoveredDescribeLogDirsCall::new()
        })
    }
}

/// Definitely-unsent route validation or bounded-driver rejection.
#[must_use = "a rejected DescribeLogDirs call must become operation input"]
pub(crate) enum DescribeLogDirsCallAdmissionFailure {
    Request,
    Submit,
}
